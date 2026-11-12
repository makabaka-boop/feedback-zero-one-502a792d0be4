"""Shared test helpers: HTTP client, restartable service harness."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from feature_service.api import make_server
from feature_service.store import Store


class Client:
    """Minimal blocking JSON HTTP client."""

    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def request(self, method: str, path: str, body=None, timeout=30.0):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"error": {"code": "unknown", "message": raw}}
            return exc.code, payload

    def get(self, path: str):
        return self.request("GET", path)

    def post(self, path: str, body):
        return self.request("POST", path, body)


class ServiceHarness:
    """Run the HTTP server on an ephemeral port against a temp database.

    ``restart()`` closes the server/store and reopens them from the same
    database file, exercising persistence and version-0 snapshot recovery.
    """

    def __init__(self, db_dir: Optional[str] = None):
        self._owns_dir = db_dir is None
        self.db_dir = db_dir or tempfile.mkdtemp(prefix="feature-test-")
        self.db_path = os.path.join(self.db_dir, "feature.db")
        self.server = None
        self.store = None
        self.client: Optional[Client] = None
        self.start()

    def start(self) -> None:
        self.store = Store(self.db_path)
        self.server = make_server(self.store, "127.0.0.1", 0)
        host, port = self.server.server_address[:2]
        self.client = Client(f"http://{host}:{port}")
        self._thread = threading.Thread(target=self.server.serve_forever,
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self._thread.join(timeout=10)
            self.server = None
        if self.store is not None:
            self.store.close()
            self.store = None

    def restart(self) -> Client:
        self.stop()
        self.start()
        return self.client  # type: ignore[return-value]

    def second_server(self) -> 'ServiceHarness':
        """Open another server over the same database file (two processes)."""
        return ServiceHarness(db_dir=self.db_dir)

    def cleanup(self) -> None:
        self.stop()
        if self._owns_dir:
            shutil.rmtree(self.db_dir, ignore_errors=True)


def canonical_exit_state(payload) -> dict:
    """Compare an exit payload as a {value tuple: multiplicity} map.

    ``rows`` is the expanded list; ``row_weights`` carries the multiplicities.
    """
    parts = payload.get("row_weights")
    if parts is None:
        parts = payload.get("rows", [])
    state = {}
    for item in parts:
        key = tuple(item["row"])
        state[key] = state.get(key, 0) + int(item["weight"])
    return state


def replay(snapshot_payload, changes) -> dict:
    """Replay change entries (flat list) onto an initial exit snapshot.

    Deltas are added in order; weights must stay non-negative and zero
    weights are dropped.  Returns only exits with at least one row.
    """
    state = {}
    for exit_name, payload in snapshot_payload["exits"].items():
        state[exit_name] = canonical_exit_state(payload)
    for entry in changes:
        exit_name = entry["exit"]
        row = tuple(entry["row"])
        w = state.setdefault(exit_name, {}).get(row, 0) + int(entry["diff"])
        if w < 0:
            raise AssertionError(f"replay drove {exit_name} {row} negative")
        if w == 0:
            state[exit_name].pop(row, None)
        else:
            state[exit_name][row] = w
    return {name: rows for name, rows in state.items() if rows}


def expanded(payload) -> list:
    """Expanded row list of a payload (``rows`` is already expanded)."""
    return [list(row) for row in payload["rows"]]


class WheelTestCase(unittest.TestCase):
    """Base class with a harness lifecycle per test."""

    def setUp(self):
        self.harness = ServiceHarness()

    def tearDown(self):
        self.harness.cleanup()

    def c(self) -> Client:
        assert self.harness.client is not None
        return self.harness.client
