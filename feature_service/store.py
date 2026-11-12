"""SQLite persistence for the feature service.

Durable state only; no relational evaluation happens in SQLite.  Stored items:

* the model JSON (canonical, validated form) and its structural signature,
* the version counter,
* origin row multiplicities,
* **node results** for every node at the current version (columns + weighted
  rows) — exits are simply nodes,
* the immutable version-0 exit snapshot used for replay,
* published net diffs per version (exits only),
* batch idempotency records (batch id -> structural signature + version).

Atomicity and isolation
-----------------------
* Every mutation runs in one ``BEGIN IMMEDIATE`` transaction.  Source rows,
  all node results, the version bump, published diffs and the batch id are
  committed together or rolled back together.
* Mutations and multi-reads hold the store lock, so a reader never observes a
  half-applied version, and concurrent submissions serialise (equivalent to
  some serial order).
* A rejected request performs no write and leaves the version untouched.
* The commit re-checks the expected version inside the write transaction, so
  a stale computation can never overwrite a newer version.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .engine import Row, row_from_json, row_json, tuple_key

SCHEMA = """
CREATE TABLE IF NOT EXISTS model (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    model_json     TEXT NOT NULL,
    model_sig      TEXT NOT NULL,
    source_name    TEXT NOT NULL,
    source_columns TEXT NOT NULL,
    version        INTEGER NOT NULL CHECK (version >= 0)
);
CREATE TABLE IF NOT EXISTS node_columns (
    node_name TEXT PRIMARY KEY,
    columns   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS node_rows (
    node_name TEXT NOT NULL,
    row_json  TEXT NOT NULL,
    weight    INTEGER NOT NULL CHECK (weight > 0),
    PRIMARY KEY (node_name, row_json)
);
CREATE TABLE IF NOT EXISTS exit_initial (
    exit_name TEXT NOT NULL,
    row_json  TEXT NOT NULL,
    weight    INTEGER NOT NULL CHECK (weight > 0),
    PRIMARY KEY (exit_name, row_json)
);
CREATE TABLE IF NOT EXISTS versions (
    version INTEGER PRIMARY KEY CHECK (version > 0)
);
CREATE TABLE IF NOT EXISTS version_diffs (
    version   INTEGER NOT NULL CHECK (version > 0),
    exit_name TEXT NOT NULL,
    row_json  TEXT NOT NULL,
    diff      INTEGER NOT NULL CHECK (diff <> 0),
    PRIMARY KEY (version, exit_name, row_json)
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id  TEXT PRIMARY KEY,
    canonical TEXT NOT NULL,
    version   INTEGER NOT NULL CHECK (version > 0)
);
CREATE TABLE IF NOT EXISTS source_rows (
    row_json TEXT PRIMARY KEY,
    weight   INTEGER NOT NULL CHECK (weight > 0)
);
"""


class StoreError(Exception):
    """Persistence failure that maps to a rejected request (409)."""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, isolation_level=None, timeout=30.0,
                           check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(SCHEMA)
    return conn


class Store:
    """Durable state access; each public method is a locked unit of work."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = connect(path)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- transactions --------------------------------------------------------

    def _begin_read(self) -> None:
        """Open a read transaction so a multi-query read is one version."""
        self._conn.execute("BEGIN")

    def _end_read(self) -> None:
        try:
            self._conn.execute("COMMIT")
        except sqlite3.OperationalError:  # pragma: no cover
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass

    def _reset_connection(self) -> None:
        """Recover from an unexpected transaction state after an error."""
        try:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
        except sqlite3.Error:  # pragma: no cover
            pass

    def _begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._conn.execute("COMMIT")

    def _rollback(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError:  # pragma: no cover
            pass

    def _version_unlocked(self) -> int:
        row = self._conn.execute(
            "SELECT version FROM model WHERE id=1").fetchone()
        return -1 if row is None else int(row[0])

    # -- model ---------------------------------------------------------------

    def read_model_header(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT model_json, model_sig, source_name, source_columns,"
                " version FROM model WHERE id=1").fetchone()
            if row is None:
                return None
            return {"model_json": row[0], "model_sig": row[1],
                    "source_name": row[2], "source_columns": row[3],
                    "version": int(row[4])}

    def initialize_model(self, model_json: str, model_sig: str,
                         source_name: str, source_columns: str,
                         nodes: Dict[str, Dict[str, Any]],
                         initial_exits: Dict[str, Dict[str, Any]]) -> int:
        """Persist the model plus version-0 node results atomically.

        ``nodes`` maps every node name to ``{"columns", "rows"}`` evaluated
        over the empty source.  ``initial_exits`` is the persisted immutable
        snapshot for exits.  Idempotent for an identical signature.
        """
        with self._lock:
            self._begin()
            try:
                existing = self._conn.execute(
                    "SELECT model_sig, version FROM model WHERE id=1"
                ).fetchone()
                if existing is not None:
                    if existing[0] == model_sig:
                        self._commit()
                        return int(existing[1])
                    raise StoreError(
                        "a structurally different model is already registered")
                self._conn.execute(
                    "INSERT INTO model(id, model_json, model_sig, source_name,"
                    " source_columns, version) VALUES(1, ?, ?, ?, ?, 0)",
                    (model_json, model_sig, source_name, source_columns))
                for name, payload in nodes.items():
                    self._conn.execute(
                        "INSERT INTO node_columns(node_name, columns)"
                        " VALUES(?, ?)",
                        (name, json.dumps(payload["columns"])))
                    for rj, weight in payload["rows"]:
                        self._conn.execute(
                            "INSERT INTO node_rows(node_name, row_json,"
                            " weight) VALUES(?, ?, ?)",
                            (name, rj, weight))
                for exit_name, payload in initial_exits.items():
                    for rj, weight in payload["rows"]:
                        self._conn.execute(
                            "INSERT INTO exit_initial(exit_name, row_json,"
                            " weight) VALUES(?, ?, ?)",
                            (exit_name, rj, weight))
                self._commit()
            except Exception:
                self._rollback()
                raise
            return 0

    def current_version(self) -> int:
        with self._lock:
            return self._version_unlocked()

    def find_batch(self, batch_id: str) -> Optional[Tuple[str, int]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT canonical, version FROM batches WHERE batch_id=?",
                (batch_id,)).fetchone()
            return None if row is None else (row[0], int(row[1]))

    # -- origin rows ----------------------------------------------------------

    def read_source_snapshot(self) -> Tuple[int, Dict[Row, int]]:
        """Consistent ``(version, origin rows)`` at one instant."""
        with self._lock:
            version = self._version_unlocked()
            rows: Dict[Row, int] = {}
            for rj, weight in self._conn.execute(
                    "SELECT row_json, weight FROM source_rows"):
                rows[row_from_json(rj)] = int(weight)
            return version, rows

    # -- node results ---------------------------------------------------------

    def read_node_columns(self) -> Dict[str, List[Dict[str, Any]]]:
        with self._lock:
            return {name: json.loads(cols) for name, cols in self._conn.execute(
                "SELECT node_name, columns FROM node_columns"
                " ORDER BY node_name")}

    def _node_rows_unlocked(self, name: str,
                            table: str = "node_rows") -> Dict[Row, int]:
        rows: Dict[Row, int] = {}
        for rj, weight in self._conn.execute(
                f"SELECT row_json, weight FROM {table} WHERE node_name=?",
                (name,)):
            rows[row_from_json(rj)] = int(weight)
        return rows

    def read_node_snapshot(self, name: str) -> Tuple[int, Dict[Row, int]]:
        with self._lock:
            return self._version_unlocked(), self._node_rows_unlocked(name)

    def read_all_node_snapshot(self) -> Tuple[int, Dict[str, Dict[Row, int]]]:
        with self._lock:
            out: Dict[str, Dict[Row, int]] = {}
            for name, rj, weight in self._conn.execute(
                    "SELECT node_name, row_json, weight FROM node_rows"):
                out.setdefault(name, {})[row_from_json(rj)] = int(weight)
            return self._version_unlocked(), out

    def read_node_bundle(self) -> Tuple[int, Dict[str, List[Dict[str, Any]]],
                                        Dict[str, Dict[Row, int]]]:
        """Version, all node columns and all node rows at one instant."""
        with self._lock:
            self._reset_connection()
            self._begin_read()
            try:
                columns = {name: json.loads(cols)
                           for name, cols in self._conn.execute(
                               "SELECT node_name, columns FROM node_columns"
                               " ORDER BY node_name")}
                rows: Dict[str, Dict[Row, int]] = {}
                for name, rj, weight in self._conn.execute(
                        "SELECT node_name, row_json, weight FROM node_rows"):
                    rows.setdefault(name, {})[row_from_json(rj)] = int(weight)
                version = self._version_unlocked()
                self._end_read()
                return version, columns, rows
            except Exception:
                self._reset_connection()
                raise

    def read_initial_exits(self) -> Dict[str, Dict[Row, int]]:
        with self._lock:
            out: Dict[str, Dict[Row, int]] = {}
            for name, rj, weight in self._conn.execute(
                    "SELECT exit_name, row_json, weight FROM exit_initial"
                    " ORDER BY exit_name"):
                out.setdefault(name, {})[row_from_json(rj)] = int(weight)
            return out

    # -- changes -------------------------------------------------------------

    def read_changes_after(self, cursor: Optional[int] = None,
                           since_version: Optional[int] = None,
                           ) -> Dict[str, Any]:
        """Published exit diffs with version strictly greater than the cursor.

        ``cursor=None`` means "from the beginning" (used by the initial
        snapshot continuation).  ``since_version`` optionally caps the batch
        list at a version observed by the caller, so a continuation can ask
        for exactly the next batch.  All reads happen in one transaction, so
        version and diffs always agree.
        """
        with self._lock:
            self._reset_connection()
            self._begin_read()
            try:
                version = self._version_unlocked()
                batches: List[Dict[str, Any]] = []
                flat: List[Dict[str, Any]] = []
                current: Optional[Dict[str, Any]] = None
                lower = -1 if cursor is None else int(cursor)
                sql = "SELECT version FROM versions WHERE version > ?"
                params: List[Any] = [lower]
                if since_version is not None:
                    sql += " AND version <= ?"
                    params.append(int(since_version))
                sql += " ORDER BY version"
                batch_versions = [int(v) for (v,) in
                                  self._conn.execute(sql, params)]
                diff_sql = ("SELECT version, exit_name, row_json, diff FROM"
                            " version_diffs WHERE version > ?")
                diff_params: List[Any] = [lower]
                if since_version is not None:
                    diff_sql += " AND version <= ?"
                    diff_params.append(int(since_version))
                records = [
                    (int(ver), exit_name, row_from_json(rj), int(diff))
                    for ver, exit_name, rj, diff in self._conn.execute(
                        diff_sql, diff_params)
                ]
                # Canonical row order inside each version and exit.
                records.sort(key=lambda r: (r[0], r[1], tuple_key(r[2])))
                per_version: Dict[int, Dict[str, Any]] = {}
                for ver in batch_versions:
                    block = {"version": ver, "changes": []}
                    batches.append(block)
                    per_version[ver] = block
                for ver, exit_name, row, diff in records:
                    entry = {"version": ver, "exit": exit_name,
                             "row": list(row), "diff": diff}
                    flat.append(entry)
                    block = per_version.get(ver)
                    if block is None:
                        block = {"version": ver, "changes": []}
                        batches.append(block)
                        per_version[ver] = block
                    block["changes"].append(
                        {"exit": exit_name, "row": entry["row"],
                         "diff": entry["diff"]})
                batches.sort(key=lambda b: b["version"])
                self._end_read()
            except Exception:
                self._reset_connection()
                raise
            return {"cursor": -1 if cursor is None else int(cursor),
                    "version": version, "batches": batches,
                    "changes": flat}

    # -- atomic batch commit ---------------------------------------------------

    def commit_batch(self, batch_id: str, canonical: str,
                     source_delta: Dict[Row, int],
                     node_deltas: Dict[str, List[Tuple[Row, int]]],
                     exit_deltas: Dict[str, List[Tuple[Row, int]]],
                     new_version: int, expected_version: int) -> None:
        """Apply all batch effects atomically; roll back on any failure."""
        with self._lock:
            self._begin()
            try:
                if self._conn.execute(
                        "SELECT 1 FROM batches WHERE batch_id=?",
                        (batch_id,)).fetchone() is not None:
                    raise StoreError("batch already committed")
                current = self._version_unlocked()
                if current != expected_version:
                    raise StoreError(
                        f"state changed concurrently (version is {current}, "
                        f"expected {expected_version})")
                self._check_source_delta(source_delta)
                for name, deltas in node_deltas.items():
                    for row, diff in deltas:
                        self._apply_node_delta(name, row, diff)
                for exit_name, deltas in exit_deltas.items():
                    for row, diff in deltas:
                        self._insert_diff(new_version, exit_name, row, diff)
                self._apply_source_delta(source_delta)
                self._conn.execute(
                    "UPDATE model SET version=? WHERE id=1", (new_version,))
                self._conn.execute(
                    "INSERT INTO batches(batch_id, canonical, version)"
                    " VALUES(?, ?, ?)", (batch_id, canonical, new_version))
                self._conn.execute(
                    "INSERT INTO versions(version) VALUES(?)", (new_version,))
                self._commit()
            except Exception:
                self._rollback()
                raise

    def _check_source_delta(self, delta: Dict[Row, int]) -> None:
        for row, d in delta.items():
            found = self._conn.execute(
                "SELECT weight FROM source_rows WHERE row_json=?",
                (row_json(row),)).fetchone()
            if (0 if found is None else int(found[0])) + d < 0:
                raise StoreError(
                    "batch would make an origin row multiplicity negative")

    def _apply_source_delta(self, delta: Dict[Row, int]) -> None:
        for row, d in delta.items():
            rj = row_json(row)
            found = self._conn.execute(
                "SELECT weight FROM source_rows WHERE row_json=?",
                (rj,)).fetchone()
            new = (0 if found is None else int(found[0])) + d
            if new < 0:  # pragma: no cover - checked by _check_source_delta
                raise StoreError("source row multiplicity became negative")
            if new == 0:
                self._conn.execute(
                    "DELETE FROM source_rows WHERE row_json=?", (rj,))
            else:
                self._conn.execute(
                    "INSERT INTO source_rows(row_json, weight) VALUES(?, ?)"
                    " ON CONFLICT(row_json) DO UPDATE"
                    " SET weight=excluded.weight", (rj, new))

    def _apply_node_delta(self, name: str, row: Row, diff: int) -> None:
        rj = row_json(row)
        found = self._conn.execute(
            "SELECT weight FROM node_rows WHERE node_name=? AND row_json=?",
            (name, rj)).fetchone()
        new = (0 if found is None else int(found[0])) + diff
        if new < 0:  # pragma: no cover - diffs derive from real weights
            raise StoreError(f"node {name!r} row weight became negative")
        if new == 0:
            self._conn.execute(
                "DELETE FROM node_rows WHERE node_name=? AND row_json=?",
                (name, rj))
        else:
            self._conn.execute(
                "INSERT INTO node_rows(node_name, row_json, weight)"
                " VALUES(?, ?, ?) ON CONFLICT(node_name, row_json)"
                " DO UPDATE SET weight=excluded.weight", (name, rj, new))

    def _insert_diff(self, version: int, exit_name: str, row: Row,
                     diff: int) -> None:
        rj = row_json(row)
        found = self._conn.execute(
            "SELECT diff FROM version_diffs WHERE version=? AND exit_name=?"
            " AND row_json=?", (version, exit_name, rj)).fetchone()
        if found is None:
            self._conn.execute(
                "INSERT INTO version_diffs(version, exit_name, row_json, diff)"
                " VALUES(?, ?, ?, ?)", (version, exit_name, rj, diff))
            return
        merged = int(found[0]) + diff
        if merged == 0:
            self._conn.execute(
                "DELETE FROM version_diffs WHERE version=? AND exit_name=?"
                " AND row_json=?", (version, exit_name, rj))
        else:
            self._conn.execute(
                "UPDATE version_diffs SET diff=? WHERE version=? AND"
                " exit_name=? AND row_json=?",
                (merged, version, exit_name, rj))
