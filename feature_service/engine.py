"""In-memory relational engine for the feature-platform derivation model.

This module is deliberately independent of SQLite.  Origin and derived
relations are evaluated as ordered, multiplicity-carrying row bags, exactly as
the specification requires.  SQLite is used only for durable state
(:mod:`feature_service.store`).

Table representation
--------------------
``(columns, rows)`` where ``columns`` is a list of column specs and ``rows``
maps a value tuple to a *positive* integer weight (multiplicity).  Emission
order is canonical: rows are sorted by their underlying value tuples, with
integers compared numerically, strings by UTF-8 byte order, and ``null``
last; equal tuples are emitted consecutively, repeated according to weight.

Operators
---------
``projection``
    Keep the given zero-based columns in exactly the given order (indices may
    repeat).  Row multiplicities pass through unchanged, so duplicates are
    preserved.

``dedup``
    Whole-row duplicate elimination: every distinct row (with ``null == null``)
    keeps exactly one copy, regardless of its former positive multiplicity.

``left_join``
    * keys are equal-length, non-empty, same-typed column arrays on each side;
    * a null in any key component on either side never matches;
    * output columns are left columns followed by right columns;
    * multiplicities multiply for matches; unmatched left rows are emitted
      once per own occurrence with the right columns padded with null.

``group_count``
    Group by a key column array (empty array = one global group; ``null ==
    null``) and emit each group's key values, the accumulated total row count
    (sum of multiplicities) and the accumulated non-null count of the count
    column.  With an empty key array and empty input the single global group
    still emits ``(0, 0)``; with non-empty keys an empty input emits nothing.

Determinism and publication
---------------------------
Each operator is a pure function of the input row bags.  Therefore, for any
exit row, ``weight(new source) - weight(old source)`` is exactly the net
multiplicity difference; publishing that difference and replaying all
published differences onto the version-0 snapshot (evaluated from the empty
source) reconstructs every current exit.  This invariant is exercised by the
replay/differential tests.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Errors and limits
# ---------------------------------------------------------------------------


class ModelError(Exception):
    """Invalid JSON model definition."""


class PayloadError(Exception):
    """Invalid batch payload (shape, types or values)."""


class EvalError(Exception):
    """Evaluation cannot complete (e.g. multiplicity overflow/negativity)."""


#: Resource guard against pathological fan-out; Python ints are unbounded.
MAX_WEIGHT = 10 ** 100

# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

ValidatedModel = Dict[str, Any]
Row = Tuple[Any, ...]
Rows = Dict[Row, int]


def is_int(v: Any) -> bool:
    """True for genuine integers; booleans are explicitly excluded."""
    return isinstance(v, int) and not isinstance(v, bool)


def kind_of_value(v: Any) -> str:
    return "int" if is_int(v) else "str"


def scalar_key(v: Any) -> Tuple[Any, ...]:
    """Order key for one value: ints numeric, strings UTF-8, null last."""
    if v is None:
        return (2, 0, 0)
    if is_int(v):
        return (0, 0, v)
    return (1, 0, v.encode("utf-8"))


def tuple_key(values: Row) -> Tuple[Any, ...]:
    return tuple(scalar_key(v) for v in values)


def eq_key(values: Row) -> Tuple[Any, ...]:
    """Equality key with null == null (dedup and grouping)."""
    return tuple(("null",) if v is None else (kind_of_value(v), v)
                 for v in values)


def match_key(values: Row) -> Optional[Tuple[Any, ...]]:
    """Join key; a null in any component makes the row unmatchable."""
    if any(v is None for v in values):
        return None
    return tuple((kind_of_value(v), v) for v in values)


def row_json(row: Row) -> str:
    """Stable text key preserving int/str/null and avoiding bool/float."""
    return json.dumps(list(row), ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False)


def row_from_json(text: str) -> Row:
    return tuple(json.loads(text))


# ---------------------------------------------------------------------------
# Model validation
# ---------------------------------------------------------------------------

NODE_TYPES = ("projection", "dedup", "left_join", "group_count")


def _require(cond: bool, msg: str, exc: type = ModelError) -> None:
    if not cond:
        raise exc(msg)


_URL_UNSAFE = set(" /%?#")


def _identifier(value: Any, where: str) -> str:
    """Unique, non-empty, printable ASCII identifier that is URL-addressable."""
    _require(isinstance(value, str) and len(value) > 0,
             f"{where} must be a non-empty string")
    for ch in value:  # type: ignore[union-attr]
        _require(0x21 <= ord(ch) <= 0x7E and ch not in _URL_UNSAFE,
                 f"{where} must contain only printable ASCII characters "
                 f"(no spaces, '/', '%', '?' or '#'), because identifiers "
                 f"are unique non-empty ASCII strings addressable in URLs")
    return value  # type: ignore[return-value]


def _column_spec(spec: Any, where: str) -> Dict[str, Any]:
    """Parse a column declaration.

    Accepted forms:
      * ``"int"`` / ``"str"`` (shorthand; nullable values allowed), or
      * ``{"type": "int"|"str", "nullable": true|false}``.
    """
    if isinstance(spec, str):
        _require(spec in ("int", "str"),
                 f"{where} type must be 'int' or 'str'")
        return {"type": spec, "nullable": True}
    _require(isinstance(spec, dict), f"{where} must be 'int', 'str' or an "
                                     f"object with type and nullable")
    _require(set(spec.keys()) == {"type", "nullable"},
             f"{where} must contain exactly type and nullable")
    _require(spec["type"] in ("int", "str"),
             f"{where}.type must be 'int' or 'str'")
    _require(isinstance(spec["nullable"], bool),
             f"{where}.nullable must be a boolean")
    return {"type": spec["type"], "nullable": spec["nullable"]}


def _column_specs(specs: Any, where: str, allow_empty: bool = False,
                  ) -> List[Dict[str, Any]]:
    _require(isinstance(specs, list), f"{where} must be an array")
    _require(allow_empty or len(specs) > 0, f"{where} must not be empty")
    return [_column_spec(s, f"{where}[{i}]") for i, s in enumerate(specs)]


def _column_indices(values: Any, where: str,
                    allow_repeats: bool = False) -> List[int]:
    _require(isinstance(values, list), f"{where} must be an array of "
                                       f"zero-based column positions")
    for v in values:
        _require(is_int(v), f"{where} entries must be integers "
                            f"(booleans and floats are rejected)")
    out = [int(v) for v in values]
    if not allow_repeats:
        _require(len(set(out)) == len(out),
                 f"{where} must not repeat a column position")
    return out


def _check_output_columns(specs: List[Dict[str, Any]], parent: List[Dict[str, Any]],
                          cols: Sequence[int], where: str) -> None:
    for c in cols:
        _require(0 <= c < len(parent),
                 f"{where} column position {c} is out of range "
                 f"(parent has {len(parent)} columns)")


def validate_model(model: Any) -> ValidatedModel:
    """Validate a JSON model and return its normalised, typed form.

    Raises :class:`ModelError` with a precise message on any violation.
    """
    _require(isinstance(model, dict), "model must be a JSON object")
    allowed = {"version", "source", "nodes", "exits"}
    _require(set(model.keys()) <= allowed and
             {"source", "nodes", "exits"} <= set(model.keys()),
             "model must contain source, nodes and exits "
             "(version is optional)")
    if "version" in model:
        _require(is_int(model["version"]) and model["version"] == 0,
                 "model.version, when present, must be the integer 0")

    source = model["source"]
    _require(isinstance(source, dict)
             and set(source.keys()) in ({"name", "columns"},
                                         {"name", "columns", "rows"}),
             "model.source must contain name and columns "
             "(optionally rows, which must be empty at registration)")
    origin_name = _identifier(source["name"], "model.source.name")
    origin_columns = _column_specs(source["columns"],
                                   "model.source.columns", allow_empty=False)
    if "rows" in source:
        _require(isinstance(source["rows"], list) and not source["rows"],
                 "model.source.rows must be an empty array: origin tables "
                 "start empty")

    nodes = model["nodes"]
    _require(isinstance(nodes, dict) and len(nodes) > 0,
             "model.nodes must be a non-empty object")
    node_names = list(nodes)
    for name in node_names:
        _identifier(name, "model.nodes key")
    _require(origin_name not in nodes,
             "a node name may not equal the source name")

    exits = model["exits"]
    _require(isinstance(exits, list) and len(exits) > 0,
             "model.exits must be a non-empty array")
    for i, e in enumerate(exits):
        _identifier(e, f"model.exits[{i}]")
    _require(len(set(exits)) == len(exits), "model.exits must be unique")

    known = set(node_names) | {origin_name}
    for name, node in nodes.items():
        where = f"model.nodes[{name}]"
        _require(isinstance(node, dict), f"{where} must be an object")
        kind = node.get("type")
        _require(kind in NODE_TYPES,
                 f"{where}.type must be one of " + ", ".join(NODE_TYPES))
        _require(set(node.keys()) == _node_fields(kind),
                 f"{where} must contain exactly "
                 + ", ".join(sorted(_node_fields(kind))))
        inputs = node["inputs"]
        _require(isinstance(inputs, list), f"{where}.inputs must be an array")
        expected = 2 if kind == "left_join" else 1
        _require(len(inputs) == expected,
                 f"{where}.inputs must contain exactly {expected} "
                 f"entr{'ies' if expected == 2 else 'y'}")
        for ref in inputs:
            _require(isinstance(ref, str) and ref in known,
                     f"{where}.inputs entry {ref!r} is not a node or the "
                     f"source name")

    resolved: Dict[str, Dict[str, Any]] = {}

    def resolve(ref: str, stack: Tuple[str, ...]) -> List[Dict[str, Any]]:
        if ref == origin_name:
            return origin_columns
        if ref in stack:
            raise ModelError("cycle detected: " + " -> ".join(stack + (ref,)))
        if ref in resolved:
            return resolved[ref]["output"]
        spec = _resolve_node(ref, stack + (ref,))
        resolved[ref] = spec
        return spec["output"]

    def _resolve_node(name: str, stack: Tuple[str, ...]) -> Dict[str, Any]:
        node = nodes[name]
        where = f"model.nodes[{name}]"
        kind = node["type"]
        inputs = list(node["inputs"])
        in_cols = [resolve(r, stack) for r in inputs]
        parent = in_cols[0]

        if kind == "projection":
            cols = _column_indices(node["columns"], f"{where}.columns",
                                   allow_repeats=True)
            _check_output_columns(cols, parent, cols, f"{where}.columns")
            out = [dict(parent[c]) for c in cols]
            return {"type": kind, "inputs": inputs, "columns": cols,
                    "output": out}

        if kind == "dedup":
            return {"type": kind, "inputs": inputs,
                    "output": [dict(s) for s in parent]}

        if kind == "left_join":
            join = node["columns"]
            _require(isinstance(join, dict)
                     and set(join.keys()) == {"left", "right"},
                     f"{where}.columns must contain exactly left and right")
            lcols = _column_indices(join["left"], f"{where}.columns.left")
            rcols = _column_indices(join["right"], f"{where}.columns.right")
            _require(len(lcols) == len(rcols),
                     f"{where}: left and right key arrays must have equal "
                     f"length")
            _require(len(lcols) > 0,
                     f"{where}: join key arrays must not be empty")
            left, right = in_cols
            _check_output_columns(lcols, left, lcols, f"{where}.columns.left")
            _check_output_columns(rcols, right, rcols,
                                  f"{where}.columns.right")
            for i, (lc, rc) in enumerate(zip(lcols, rcols)):
                _require(left[lc]["type"] == right[rc]["type"],
                         f"{where}: join key component {i} has mismatched "
                         f"types ({left[lc]['type']} vs {right[rc]['type']})")
            out = ([dict(s) for s in left]
                   + [{"type": s["type"], "nullable": True}
                      for s in right])
            return {"type": kind, "inputs": inputs,
                    "columns": {"left": lcols, "right": rcols},
                    "output": out}

        if kind == "group_count":
            keys = _column_indices(node["keys"], f"{where}.keys")
            _require(is_int(node["count"]),
                     f"{where}.count must be an integer column position "
                     f"(booleans and floats are rejected)")
            cnt = int(node["count"])
            _check_output_columns(keys, parent, keys, f"{where}.keys")
            _require(0 <= cnt < len(parent),
                     f"{where}.count position {cnt} is out of range "
                     f"(parent has {len(parent)} columns)")
            out = ([dict(parent[k]) for k in keys]
                   + [{"type": "int", "nullable": False},
                      {"type": "int", "nullable": False}])
            return {"type": kind, "inputs": inputs, "keys": keys,
                    "count": cnt, "output": out}

        raise AssertionError(kind)  # pragma: no cover

    for name in node_names:
        resolve(name, ())

    for i, e in enumerate(exits):
        _require(e in nodes,
                 f"model.exits[{i}] {e!r} is not a node in the model")

    return {
        "source": {"name": origin_name, "columns": origin_columns},
        "nodes": {name: resolved[name] for name in node_names},
        "outputs": {name: resolved[name]["output"] for name in node_names},
        "exits": list(exits),
    }


def _node_fields(kind: str) -> set:
    return {
        "projection": {"type", "inputs", "columns"},
        "dedup": {"type", "inputs"},
        "left_join": {"type", "inputs", "columns"},
        "group_count": {"type", "inputs", "keys", "count"},
    }[kind]


def canonical_model(validated: ValidatedModel) -> Dict[str, Any]:
    """Deterministic, self-validating JSON form of the model.

    Used for structural identity: node declaration order and exit order do not
    affect it, while every structural attribute (columns, types, inputs,
    operators and parameters) does.
    """
    nodes = {}
    for name, spec in validated["nodes"].items():
        node = {"type": spec["type"], "inputs": list(spec["inputs"])}
        if spec["type"] in ("projection", "left_join"):
            node["columns"] = spec["columns"]
        elif spec["type"] == "group_count":
            node["keys"] = spec["keys"]
            node["count"] = spec["count"]
        nodes[name] = node
    return {
        "version": 0,
        "source": {
            "name": validated["source"]["name"],
            "columns": [dict(c) for c in validated["source"]["columns"]],
        },
        "nodes": nodes,
        "exits": sorted(validated["exits"]),
    }


def canonical_model_json(validated: ValidatedModel) -> str:
    return json.dumps(canonical_model(validated), sort_keys=True,
                      ensure_ascii=False, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


class Table:
    """An evaluated relation: typed columns plus value->multiplicity rows."""

    __slots__ = ("columns", "rows")

    def __init__(self, columns: List[Dict[str, Any]],
                 rows: Optional[Rows] = None):
        self.columns = columns
        self.rows: Rows = rows if rows is not None else {}


def _guard(w: int) -> int:
    if w > MAX_WEIGHT:
        raise EvalError("row multiplicity overflow")
    return w


def _add(dst: Rows, key: Row, w: int) -> None:
    dst[key] = _guard(dst.get(key, 0) + w)


def _mul(a: int, b: int) -> int:
    return _guard(a * b)


def evaluate_exits(validated: ValidatedModel, source_rows: Rows,
                   names: Sequence[str]) -> Dict[str, Table]:
    """Evaluate the requested nodes (and their sub-DAG) over source_rows."""
    origin_name = validated["source"]["name"]
    origin = Table(validated["source"]["columns"], dict(source_rows))
    cache: Dict[str, Table] = {}

    def get(name: str) -> Table:
        if name == origin_name:
            return origin
        if name in cache:
            return cache[name]
        spec = validated["nodes"][name]
        kind = spec["type"]
        inputs = [get(r) for r in spec["inputs"]]
        parent = inputs[0]

        if kind == "projection":
            cols = spec["columns"]
            rows: Rows = {}
            for vals, w in parent.rows.items():
                _add(rows, tuple(vals[c] for c in cols), w)
            result = Table(spec["output"], rows)

        elif kind == "dedup":
            # Whole-row distinct: null == null, multiplicity normalised to 1.
            result = Table(spec["output"], {vals: 1 for vals in parent.rows})

        elif kind == "left_join":
            lcols = spec["columns"]["left"]
            rcols = spec["columns"]["right"]
            left, right = inputs
            bucket: Dict[Tuple[Any, ...], List[Tuple[Row, int]]] = {}
            for rvals, rw in right.rows.items():
                k = match_key(tuple(rvals[c] for c in rcols))
                if k is not None:
                    bucket.setdefault(k, []).append((rvals, rw))
            pad = tuple(None for _ in right.columns)
            rows = {}
            for lvals, lw in left.rows.items():
                k = match_key(tuple(lvals[c] for c in lcols))
                matched = bucket.get(k, []) if k is not None else []
                if not matched:
                    _add(rows, tuple(lvals) + pad, lw)
                else:
                    for rvals, rw in matched:
                        _add(rows, tuple(lvals) + tuple(rvals), _mul(lw, rw))
            result = Table(spec["output"], rows)

        elif kind == "group_count":
            keys = spec["keys"]
            cnt = spec["count"]
            group_vals: Dict[Tuple[Any, ...], Row] = {}
            totals: Dict[Tuple[Any, ...], int] = {}
            nonnull: Dict[Tuple[Any, ...], int] = {}
            order: List[Tuple[Any, ...]] = []
            for vals, w in parent.rows.items():
                kvals = tuple(vals[c] for c in keys)
                gk = eq_key(kvals)
                if gk not in group_vals:
                    group_vals[gk] = kvals
                    totals[gk] = 0
                    nonnull[gk] = 0
                    order.append(gk)
                totals[gk] = _guard(totals[gk] + w)
                if vals[cnt] is not None:
                    nonnull[gk] = _guard(nonnull[gk] + w)
            rows = {}
            if not parent.rows and not keys:
                # Empty global group still reports the two zero counters.
                rows[(0, 0)] = 1
            else:
                for gk in order:
                    _add(rows, tuple(group_vals[gk])
                         + (totals[gk], nonnull[gk]), 1)
            result = Table(spec["output"], rows)

        else:  # pragma: no cover
            raise AssertionError(kind)

        cache[name] = result
        return result

    return {name: get(name) for name in names}


def evaluate_node(validated: ValidatedModel, name: str,
                  source_rows: Rows) -> Table:
    """Evaluate one node over a source relation."""
    if name == validated["source"]["name"]:
        return Table(validated["source"]["columns"], dict(source_rows))
    return evaluate_exits(validated, source_rows, [name])[name]


def evaluate_all(validated: ValidatedModel, source_rows: Rows,
                 ) -> Dict[str, Table]:
    return evaluate_exits(validated, source_rows,
                          list(validated["nodes"]))


# ---------------------------------------------------------------------------
# Ordering / presentation / deltas
# ---------------------------------------------------------------------------


def sorted_items(rows: Rows) -> List[Tuple[Row, int]]:
    return [(vals, rows[vals]) for vals in sorted(rows, key=tuple_key)]


def expanded_rows(rows: Rows, limit: Optional[int] = None,
                  ) -> Tuple[List[List[Any]], int]:
    """Expand multiplicities into repeated rows in canonical order.

    Returns ``(emitted, total)``; ``total`` counts all occurrences even when
    ``limit`` truncates emission.
    """
    emitted: List[List[Any]] = []
    total = 0
    for vals, w in sorted_items(rows):
        total += int(w)
        if limit is None:
            emitted.extend([list(vals)] * int(w))
        elif len(emitted) < limit:
            emitted.extend([list(vals)] * min(int(w), limit - len(emitted)))
    return emitted, total


def row_deltas(old: Rows, new: Rows) -> List[Tuple[Row, int]]:
    """Net weight difference per row, merged per value, zero diffs omitted."""
    deltas: Rows = {}
    for vals, w in old.items():
        deltas[vals] = deltas.get(vals, 0) - int(w)
    for vals, w in new.items():
        deltas[vals] = deltas.get(vals, 0) + int(w)
    return [(vals, d) for vals, d in
            sorted(deltas.items(), key=lambda kv: tuple_key(kv[0]))
            if d != 0]


def apply_deltas(rows: Rows, deltas: Sequence[Tuple[Row, int]]) -> Rows:
    """Apply signed net deltas; resulting weights must be non-negative."""
    out = dict(rows)
    for vals, d in deltas:
        w = out.get(vals, 0) + int(d)
        if w < 0:
            raise EvalError("result multiplicity would become negative")
        if w == 0:
            out.pop(vals, None)
        else:
            out[vals] = _guard(w)
    return out


# ---------------------------------------------------------------------------
# Batch payload validation
# ---------------------------------------------------------------------------


def validate_batch(validated: ValidatedModel, payload: Any,
                   ) -> Tuple[str, Rows, Rows]:
    """Validate a batch body; merge same-row entries before any state change.

    Returns ``(batch_id, merged_diffs, merged_nonzero)`` where the first
    mapping keeps even net-zero rows (for structural batch identity) and the
    second contains only rows with a non-zero net difference (the effective
    batch).
    """
    _need(isinstance(payload, dict), "batch body must be a JSON object")
    _need(set(payload.keys()) == {"batch_id", "source", "changes"},
             "batch body must contain exactly batch_id, source and changes")
    batch_id = payload["batch_id"]
    _need(isinstance(batch_id, str) and len(batch_id) > 0,
             "batch_id must be a non-empty string")
    source_name = payload["source"]
    _need(source_name == validated["source"]["name"],
             f"batch source {source_name!r} does not match the modelled "
             f"source {validated['source']['name']!r}")

    changes = payload["changes"]
    _need(isinstance(changes, list) and len(changes) > 0,
             "batch changes must be a non-empty array")

    columns = validated["source"]["columns"]
    merged: Rows = {}
    for i, change in enumerate(changes):
        where = f"changes[{i}]"
        _need(isinstance(change, dict)
                 and set(change.keys()) == {"row", "diff"},
                 f"{where} must be an object with exactly row and diff")
        row = change["row"]
        _need(isinstance(row, list) and len(row) == len(columns),
                 f"{where}.row must be an array of exactly {len(columns)} "
                 f"values")
        for j, (value, spec) in enumerate(zip(row, columns)):
            _require_value(value, spec, f"{where}.row[{j}]")
        diff = change["diff"]
        _need(is_int(diff),
                 f"{where}.diff must be an integer "
                 f"(booleans and floats are rejected)")
        _need(diff != 0, f"{where}.diff must be non-zero")
        key = tuple(row)
        merged[key] = merged.get(key, 0) + int(diff)

    nonzero = {k: d for k, d in merged.items() if d != 0}
    return batch_id, merged, nonzero


def _need(cond: bool, msg: str) -> None:
    if not cond:
        raise PayloadError(msg)


def _require_value(value: Any, spec: Dict[str, Any], where: str) -> None:
    if value is None:
        _need(spec["nullable"],
                 f"{where} is null but the column is declared non-nullable")
        return
    if spec["type"] == "int":
        _need(is_int(value),
                 f"{where} must be an integer or null "
                 f"(booleans and floats are rejected)")
    else:
        _need(isinstance(value, str),
                 f"{where} must be a string or null")


def batch_signature(source_name: str, merged: Rows) -> str:
    """Structural identity of a batch (order-insensitive, values merged)."""
    items = [[list(vals), int(merged[vals])]
             for vals in sorted(merged, key=tuple_key)]
    return json.dumps({"source": source_name, "changes": items},
                      sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))
