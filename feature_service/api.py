"""JSON HTTP API (Python standard library only).

Endpoints
---------
``POST /model``
    Register a JSON model.  Invalid models return 400 with a precise message.
    Re-registering a structurally identical model (same signature) is
    idempotent and returns the original model version (0).  A structurally
    different model against an existing registration returns 409.  On first
    registration the version-0 node/exit results (evaluated over the empty
    source) are persisted; no change is published.

``GET /model``
    Summary: source, nodes, exits and current version.

``GET /source``
    Origin rows, expanded one array per occurrence in canonical order, plus
    ``row_weights`` with the aggregate multiplicity per distinct value.

``GET /nodes/<name>``
    Stored node result at the current version (same row shape as ``/source``).

``GET /nodes/<name>?evaluate=1``
    Re-evaluate the node from the stored origin rows (diagnostics/对拍).

``GET /exits`` ``GET /exits/<name>``
    Current published exit rows (exits are nodes with published diffs).

``GET /snapshot``
    The persisted version-0 exit snapshot (empty-source evaluation, no
    changes published).

``POST /batches``
    ``{"batch_id", "source", "changes": [{"row", "diff"}, ...]}``.  All
    validation happens before any write; the batch applies all-at-once.
    Same-row entries are merged first, then the final origin multiplicity must
    stay non-negative.  Success increments the version by one and publishes
    per-exit net diffs (merged per row, zero differences omitted).  A repeated
    batch id with identical structure returns its original version; the same
    id with different content returns 409.

``GET /changes?cursor=<int>``
    Whole versions strictly greater than the cursor, as ``batches`` plus a
    flat ``changes`` list, and the current version.  ``since`` caps the pull at
    one particular version; omitting ``cursor`` continues from the version-0
    snapshot.  Replaying these diffs onto the snapshot reconstructs the
    current exits exactly.

Errors are ``{"error": {"code": ..., "message": ...}}``.
"""

from __future__ import annotations

import json
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from . import engine
from .engine import Row
from .store import Store, StoreError

MAX_BODY = 8 * 1024 * 1024

_MODEL_RE = re.compile(r"^/model/?$")
_SOURCE_RE = re.compile(r"^/source/?$")
_EXITS_RE = re.compile(r"^/exits/?$")
_EXIT_RE = re.compile(r"^/exits/([^/]+)/?$")
_NODE_RE = re.compile(r"^/nodes/([^/]+)/?$")
_SNAPSHOT_RE = re.compile(r"^/snapshot/?$")
_CHANGES_RE = re.compile(r"^/changes/?$")
_BATCH_RE = re.compile(r"^/batches/?$")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = int(status)
        self.code = code
        self.message = message


def _rows_payload(rows: Dict[Row, int]) -> Dict[str, Any]:
    """Canonical presentation: ``rows`` is fully expanded, one array per
    duplicate occurrence, in the specified ordering.  ``row_weights`` keeps
    the same order with the aggregate multiplicity per distinct value."""
    items = engine.sorted_items(rows)
    expanded = [list(vals) for vals, w in items for _ in range(int(w))]
    return {
        "rows": expanded,
        "row_weights": [{"row": list(vals), "weight": int(w)}
                        for vals, w in items],
        "expanded_rows": expanded,
    }


def _node_payload(rows: Dict[Row, int], columns: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"columns": columns, **_rows_payload(rows)}


class Service:
    """HTTP-independent application logic; safe for concurrent threads."""

    def __init__(self, store: Store):
        self.store = store
        self._lock = threading.RLock()
        self.validated: Optional[engine.ValidatedModel] = None
        self.model_json: Optional[str] = None
        header = store.read_model_header()
        if header is not None:
            self.model_json = header["model_json"]
            self.validated = engine.validate_model(json.loads(header["model_json"]))

    # -- model --------------------------------------------------------------

    def _require_model(self) -> engine.ValidatedModel:
        if self.validated is None:
            raise ApiError(HTTPStatus.CONFLICT, "no_model",
                           "no model is registered; POST /model first")
        return self.validated

    def register_model(self, body: Any) -> Dict[str, Any]:
        with self._lock:
            if not isinstance(body, dict):
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_model",
                               "model must be a JSON object")
            try:
                validated = engine.validate_model(body)
                signature = engine.canonical_model_json(validated)
            except engine.ModelError as exc:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_model",
                               str(exc)) from exc

            header = self.store.read_model_header()
            if header is not None:
                if header["model_sig"] != signature:
                    raise ApiError(
                        HTTPStatus.CONFLICT, "model_conflict",
                        "a structurally different model is already registered;"
                        " the model is immutable")
                self.validated = validated
                self.model_json = header["model_json"]
                return {"version": header["version"], "replayed": True,
                        "nodes": self._initial_node_payload()}

            initial = engine.evaluate_all(validated, {})
            node_payload = {}
            for name in validated["nodes"]:
                table = initial[name]
                node_payload[name] = {
                    "columns": table.columns,
                    "rows": [(engine.row_json(vals), int(w))
                             for vals, w in engine.sorted_items(table.rows)],
                }
            initial_exits = {name: node_payload[name]
                             for name in validated["exits"]}
            try:
                self.store.initialize_model(
                    signature, signature,
                    validated["source"]["name"],
                    json.dumps(validated["source"]["columns"], sort_keys=True),
                    node_payload, initial_exits)
            except StoreError as exc:
                raise ApiError(HTTPStatus.CONFLICT, "model_conflict",
                               str(exc)) from exc
            self.validated = validated
            self.model_json = signature
            return {
                "version": 0,
                "replayed": False,
                "nodes": {name: _node_payload_from_stored(payload)
                          for name, payload in node_payload.items()},
                "exits": {name: _node_payload_from_stored(initial_exits[name])
                          for name in validated["exits"]},
            }

    def _initial_node_payload(self) -> Dict[str, Any]:
        v = self._require_model()
        _, cols, nodes = self.store.read_node_bundle()
        return {name: _node_payload(nodes.get(name, {}), cols[name])
                for name in v["nodes"]}

    def model_status(self) -> Dict[str, Any]:
        v = self._require_model()
        return {
            "version": self.store.current_version(),
            "source": {"name": v["source"]["name"],
                       "columns": v["source"]["columns"]},
            "nodes": {name: {"type": spec["type"],
                             "inputs": spec["inputs"]}
                      for name, spec in v["nodes"].items()},
            "exits": list(v["exits"]),
        }

    def initial_snapshot(self) -> Dict[str, Any]:
        v = self._require_model()
        _, cols, _ = self.store.read_node_bundle()
        initial = self.store.read_initial_exits()
        return {"version": 0,
                "exits": {name: _node_payload(initial.get(name, {}), cols[name])
                          for name in v["exits"]}}

    # -- source / nodes ------------------------------------------------------

    def source_view(self) -> Dict[str, Any]:
        v = self._require_model()
        version, rows = self.store.read_source_snapshot()
        return {"name": v["source"]["name"], "version": version,
                "columns": v["source"]["columns"], **_rows_payload(rows)}

    def node_view(self, name: str, evaluate: bool = False) -> Dict[str, Any]:
        v = self._require_model()
        if name not in v["nodes"]:
            raise ApiError(HTTPStatus.NOT_FOUND, "unknown_node",
                           f"{name!r} is not a node in this model")
        if evaluate:
            version, source_rows = self.store.read_source_snapshot()
            table = engine.evaluate_node(v, name, source_rows)
            return {"name": name, "version": version, "evaluated": True,
                    **_node_payload(table.rows, table.columns)}
        version, cols, nodes = self.store.read_node_bundle()
        return {"name": name, "version": version, "evaluated": False,
                **_node_payload(nodes.get(name, {}), cols[name])}

    def exits_view(self) -> Dict[str, Any]:
        v = self._require_model()
        version, cols, nodes = self.store.read_node_bundle()
        return {"version": version,
                "exits": {name: _node_payload(nodes.get(name, {}), cols[name])
                          for name in v["exits"]}}

    def exit_view(self, name: str) -> Dict[str, Any]:
        v = self._require_model()
        if name not in v["exits"]:
            raise ApiError(HTTPStatus.NOT_FOUND, "unknown_exit",
                           f"{name!r} is not a published exit")
        version, cols, nodes = self.store.read_node_bundle()
        return {"name": name, "version": version,
                **_node_payload(nodes.get(name, {}), cols[name])}

    # -- batches -------------------------------------------------------------

    def submit_batch(self, body: Any) -> Dict[str, Any]:
        """Validate, compute and atomically publish one batch.

        Computation is retried against the current version if another writer
        commits in between, so concurrent submissions behave exactly like some
        serial order (each accepted batch bumps the version once).
        """
        with self._lock:
            if self.validated is None:
                raise ApiError(HTTPStatus.CONFLICT, "no_model",
                               "no model is registered; POST /model first")
            try:
                batch_id, merged, nonzero = engine.validate_batch(
                    self.validated, body)
                signature = engine.batch_signature(
                    self.validated["source"]["name"], merged)
            except engine.PayloadError as exc:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_batch",
                               str(exc)) from exc
            v = self.validated

            for _attempt in range(8):
                prior = self.store.find_batch(batch_id)
                if prior is not None:
                    if prior[0] != signature:
                        raise ApiError(
                            HTTPStatus.CONFLICT, "batch_id_conflict",
                            "batch_id was already committed with different "
                            "content")
                    return {"batch_id": batch_id, "version": prior[1],
                            "replayed": True, "changes": {}}

                version, before_source = self.store.read_source_snapshot()
                try:
                    after_source = engine.apply_deltas(
                        before_source, list(nonzero.items()))
                except engine.EvalError as exc:
                    raise ApiError(
                        HTTPStatus.CONFLICT, "batch_rejected",
                        "batch would make an origin row multiplicity "
                        "negative: " + str(exc)) from exc
                try:
                    after_tables = engine.evaluate_all(v, after_source)
                except engine.EvalError as exc:
                    raise ApiError(HTTPStatus.CONFLICT, "batch_rejected",
                                   str(exc)) from exc

                observed_version, _, before_nodes =                     self.store.read_node_bundle()
                if observed_version != version:
                    continue  # another writer advanced the state; retry

                node_deltas: Dict[str, List[Tuple[Row, int]]] = {}
                for name in v["nodes"]:
                    old = before_nodes.get(name, {})
                    new = after_tables[name].rows
                    deltas = engine.row_deltas(old, new)
                    if engine.apply_deltas(dict(old), deltas) != new:
                        raise ApiError(HTTPStatus.INTERNAL_SERVER_ERROR,
                                       "internal_error",
                                       "net diff self-check failed for node "
                                       f"{name!r}")
                    node_deltas[name] = deltas
                for exit_name in v["exits"]:
                    rebuilt = engine.apply_deltas(
                        dict(before_nodes.get(exit_name, {})),
                        node_deltas[exit_name])
                    if rebuilt != after_tables[exit_name].rows:
                        raise ApiError(
                            HTTPStatus.INTERNAL_SERVER_ERROR,
                            "internal_error",
                            "published diff self-check failed for exit "
                            f"{exit_name!r}")
                exit_deltas = {name: node_deltas[name] for name in v["exits"]}

                new_version = version + 1
                try:
                    self.store.commit_batch(batch_id, signature, nonzero,
                                            node_deltas, exit_deltas,
                                            new_version, version)
                except StoreError as exc:
                    msg = str(exc)
                    if "state changed concurrently" in msg:
                        continue
                    prior = self.store.find_batch(batch_id)
                    if prior is not None and prior[0] == signature:
                        return {"batch_id": batch_id, "version": prior[1],
                                "replayed": True, "changes": {}}
                    raise ApiError(HTTPStatus.CONFLICT, "batch_rejected",
                                   msg) from exc

                return {
                    "batch_id": batch_id,
                    "version": new_version,
                    "replayed": False,
                    "changes": {
                        name: [{"row": list(row), "diff": int(d)}
                               for row, d in exit_deltas[name]]
                        for name in v["exits"]
                    },
                    "node_changes": {
                        name: [{"row": list(row), "diff": int(d)}
                               for row, d in node_deltas[name]]
                        for name in v["nodes"]
                    },
                }
            raise ApiError(HTTPStatus.CONFLICT, "batch_rejected",
                           "too much write contention; retry the batch")

    # -- changes -------------------------------------------------------------

    def changes_after(self, cursor: Optional[int],
                      since: Optional[int] = None) -> Dict[str, Any]:
        """Continuation from a snapshot.

        ``cursor`` is the last version the caller has applied (``None`` when
        continuing directly from the version-0 snapshot).  ``since`` requests
        the next single batch (``cursor < version <= since``), which is the
        "按版本续取" pull in whole-version units.  Responses always contain
        whole versions only.
        """
        self._require_model()
        if cursor is not None and cursor < 0:
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_cursor",
                           "cursor must be a non-negative integer")
        if since is not None and (since < 0 or
                                  (cursor is not None and since <= cursor)):
            raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_since",
                           "since must be greater than the cursor")
        return self.store.read_changes_after(cursor, since)


def _optional_int(query, name: str) -> Optional[int]:
    if name not in query:
        return None
    raw = query[name][0]
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ApiError(HTTPStatus.BAD_REQUEST, f"invalid_{name}",
                       f"{name} must be an integer") from None


def _node_payload_from_stored(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = {engine.row_from_json(rj): int(w) for rj, w in payload["rows"]}
    return _node_payload(rows, payload["columns"])


# ---------------------------------------------------------------------------
# HTTP plumbing
# ---------------------------------------------------------------------------


def make_server(store: Store, host: str = "127.0.0.1",
                port: int = 0) -> ThreadingHTTPServer:
    service = Service(store)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _dispatch(self, method: str) -> None:
            try:
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query, keep_blank_values=True)
                if method == "GET":
                    self._handle_get(parsed.path, query)
                else:
                    self._handle_post(parsed.path)
            except ApiError as exc:
                _error(self, exc.status, exc.code, exc.message)
            except Exception as exc:  # pragma: no cover - defensive
                _error(self, HTTPStatus.INTERNAL_SERVER_ERROR,
                       "internal_error", f"{type(exc).__name__}: {exc}")

        def _read_body(self) -> Any:
            if "Content-Length" not in self.headers:
                raise ApiError(HTTPStatus.LENGTH_REQUIRED, "invalid_request",
                               "Content-Length header is required")
            try:
                length = int(self.headers["Content-Length"])
            except ValueError:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_request",
                               "Content-Length must be an integer") from None
            if length < 0 or length > MAX_BODY:
                raise ApiError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                               "body_too_large",
                               "body exceeds the 8 MiB limit")
            raw = self.rfile.read(length) if length else b""
            if not raw:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_request",
                               "request body must not be empty")
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ApiError(HTTPStatus.BAD_REQUEST, "invalid_json",
                               f"body is not valid JSON: {exc}") from exc

        def _handle_get(self, path: str, query) -> None:
            if _MODEL_RE.match(path):
                _json_response(self, HTTPStatus.OK, service.model_status())
            elif _SOURCE_RE.match(path):
                _json_response(self, HTTPStatus.OK, service.source_view())
            elif _SNAPSHOT_RE.match(path):
                _json_response(self, HTTPStatus.OK, service.initial_snapshot())
            elif _EXITS_RE.match(path):
                _json_response(self, HTTPStatus.OK, service.exits_view())
            elif (m := _EXIT_RE.match(path)):
                _json_response(self, HTTPStatus.OK,
                               service.exit_view(m.group(1)))
            elif (m := _NODE_RE.match(path)):
                evaluate = query.get("evaluate", ["0"])[0] not in ("0", "", "false")
                _json_response(self, HTTPStatus.OK,
                               service.node_view(m.group(1), evaluate))
            elif _CHANGES_RE.match(path):
                cursor = _optional_int(query, "cursor")
                since = _optional_int(query, "since")
                _json_response(self, HTTPStatus.OK,
                               service.changes_after(cursor, since))
            else:
                raise ApiError(HTTPStatus.NOT_FOUND, "not_found",
                               f"no GET route for {path}")

        def _handle_post(self, path: str) -> None:
            if _MODEL_RE.match(path):
                _json_response(self, HTTPStatus.OK,
                               service.register_model(self._read_body()))
            elif _BATCH_RE.match(path):
                _json_response(self, HTTPStatus.OK,
                               service.submit_batch(self._read_body()))
            else:
                raise ApiError(HTTPStatus.NOT_FOUND, "not_found",
                               f"no POST route for {path}")

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.service = service  # type: ignore[attr-defined]
    return server


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   payload: Any) -> None:
    body = json.dumps(payload, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def _error(handler: BaseHTTPRequestHandler, status: int, code: str,
           message: str) -> None:
    _json_response(handler, status, {"error": {"code": code,
                                               "message": message}})


def serve(db_path: str, host: str = "127.0.0.1",
          port: int = 0) -> Tuple[ThreadingHTTPServer, Store]:
    store = Store(db_path)
    return make_server(store, host, port), store
