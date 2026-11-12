"""Naive reference evaluator over *expanded* row lists.

Independent of the production engine: it materialises every duplicate as its
own list element and uses straightforward nested loops.  The differential
tests compare the engine's multiplicities (expanded canonically) against this
implementation, which is the "展开重复行对拍" required by the specification.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple

from feature_service.engine import tuple_key

Row = Tuple[Any, ...]


def mkey(row: Row, cols: Sequence[int]):
    vals = tuple(row[c] for c in cols)
    if any(v is None for v in vals):
        return None
    return tuple((type(v).__name__, v) for v in vals)


def evaluate(validated: Dict[str, Any], name: str,
             source_expanded: Sequence[Row]) -> List[Row]:
    cache: Dict[str, List[Row]] = {}
    origin = validated["source"]["name"]

    def get(node_name: str) -> List[Row]:
        if node_name == origin:
            return list(source_expanded)
        if node_name in cache:
            return cache[node_name]
        spec = validated["nodes"][node_name]
        kind = spec["type"]
        parents = [get(r) for r in spec["inputs"]]
        if kind == "projection":
            cols = spec["columns"]
            out = [tuple(row[c] for c in cols) for row in parents[0]]
        elif kind == "dedup":
            seen = set()
            out = []
            for row in parents[0]:
                if row not in seen:
                    seen.add(row)
                    out.append(row)
        elif kind == "left_join":
            lcols = spec["columns"]["left"]
            rcols = spec["columns"]["right"]
            left, right = parents
            width = len(validated["outputs"][spec["inputs"][1]])
            pad = (None,) * width
            out = []
            for lrow in left:
                lk = mkey(lrow, lcols)
                matched = False
                if lk is not None:
                    for rrow in right:
                        rk = mkey(rrow, rcols)
                        if rk is not None and rk == lk:
                            out.append(lrow + rrow)
                            matched = True
                if not matched:
                    out.append(lrow + pad)
        elif kind == "group_count":
            keys = spec["keys"]
            cnt = spec["count"]
            groups: Dict[Row, List[int]] = {}
            for row in parents[0]:
                gk = tuple(row[k] for k in keys)
                stats = groups.setdefault(gk, [0, 0])
                stats[0] += 1
                if row[cnt] is not None:
                    stats[1] += 1
            if not parents[0]:
                out = [(0, 0)] if not keys else []
            else:
                out = [gk + (stats[0], stats[1])
                       for gk, stats in groups.items()]
        else:  # pragma: no cover
            raise AssertionError(kind)
        cache[node_name] = out
        return out

    return get(name)


def canonical(rows: Sequence[Row]) -> List[Row]:
    return sorted(rows, key=tuple_key)
