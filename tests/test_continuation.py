"""Windowed / version-continuation semantics of node snapshots.

A feature-platform consumer normally loads the version-0 exports once, then
pulls only *changes* after a cursor.  This test suite asserts that this is
equivalent to reading the node result at every version.
"""

from __future__ import annotations

import unittest

from feature_service import engine
from tests.helpers import WheelTestCase, canonical_exit_state, replay

MODEL = {
    "source": {"name": "src", "columns": [
        {"type": "int", "nullable": False},
        {"type": "int", "nullable": True},
        {"type": "str", "nullable": True},
    ]},
    "nodes": {
        "proj": {"type": "projection", "inputs": ["src"], "columns": [1, 2]},
        "dedup": {"type": "dedup", "inputs": ["proj"]},
        "joined": {"type": "left_join", "inputs": ["src", "dedup"],
                   "columns": {"left": [1], "right": [0]}},
        "counts": {"type": "group_count", "inputs": ["joined"],
                   "keys": [1], "count": 3},
        "global": {"type": "group_count", "inputs": ["src"], "keys": [],
                   "count": 2},
    },
    "exits": ["dedup", "joined", "counts", "global"],
}

SCRIPT = [
    ("b1", [{"row": [1, 7, "x"], "diff": 2}]),
    ("b2", [{"row": [2, 8, None], "diff": 1}]),
    ("b3", [{"row": [1, 7, "x"], "diff": -2}]),
    ("b4", [{"row": [3, None, "z"], "diff": 1},
            {"row": [4, 7, None], "diff": 1}]),
    ("b5", [{"row": [4, 7, None], "diff": -1}]),
]


class SnapshotWindowTests(WheelTestCase):
    def setUp(self):
        super().setUp()
        self.c().post("/model", MODEL)

    def pull(self, cursor):
        _, body = self.c().get(f"/changes?cursor={cursor}")
        return body

    def test_each_change_window_applied_to_snapshot_matches_exits(self):
        _, snapshot = self.c().get("/snapshot")
        cursor = 0
        applied = []
        for batch_id, changes in SCRIPT:
            status, body = self.c().post(
                "/batches", {"batch_id": batch_id, "source": "src",
                             "changes": changes})
            self.assertEqual(status, 200, body)
            window = self.pull(cursor)
            self.assertEqual(window["version"], body["version"])
            self.assertEqual(sorted(b["version"] for b in window["batches"]),
                             [body["version"]])
            applied.extend(window["changes"])
            cursor = window["version"]
            rebuilt = replay(snapshot, applied)
            _, exits = self.c().get("/exits")
            current = {name: canonical_exit_state(payload)
                       for name, payload in exits["exits"].items()}
            self.assertEqual({k: v for k, v in rebuilt.items() if v},
                             {k: v for k, v in current.items() if v},
                             msg=f"window after {batch_id} diverged")

    def test_persisted_nodes_match_fresh_evaluation_every_version(self):
        for batch_id, changes in SCRIPT:
            status, body = self.c().post(
                "/batches", {"batch_id": batch_id, "source": "src",
                             "changes": changes})
            self.assertEqual(status, 200, body)
            for node in MODEL["nodes"]:
                _, stored = self.c().get(f"/nodes/{node}")
                _, fresh = self.c().get(f"/nodes/{node}?evaluate=1")
                self.assertEqual(stored["rows"], fresh["rows"])
                self.assertEqual(stored["version"], body["version"])

    def test_continuation_survives_restart(self):
        self.c().post("/batches", {"batch_id": "b1", "source": "src",
                                   "changes": [{"row": [1, 7, "x"],
                                                "diff": 1}]})
        client = self.harness.restart()
        _, window = client.get("/changes?cursor=1")
        self.assertEqual(window["batches"], [])
        _, model = client.get("/model")
        self.assertEqual(model["version"], 1)

    def test_dedup_crossing_zero_publishes_no_change(self):
        base = {"batch_id": "b1", "source": "src",
                "changes": [{"row": [1, 7, "x"], "diff": 2}]}
        self.c().post("/batches", base)
        status, body = self.c().post(
            "/batches", {"batch_id": "b2", "source": "src",
                         "changes": [{"row": [1, 7, "x"], "diff": -1}]})
        self.assertEqual(status, 200)
        self.assertEqual(body["changes"]["dedup"], [])
        # But the joined/counts exits see the origin multiplicity drop.
        self.assertNotEqual(body["changes"]["global"], [])


class NodeSnapshotTests(unittest.TestCase):
    """Engine-level version-0 snapshot behaviour for non-exit nodes."""

    MODEL = {
        "source": {"name": "src", "columns": [
            {"type": "int", "nullable": False},
            {"type": "int", "nullable": True},
        ]},
        "nodes": {
            "g": {"type": "group_count", "inputs": ["src"], "keys": [],
                  "count": 1},
            "p": {"type": "projection", "inputs": ["src"], "columns": [1]},
        },
        "exits": ["g"],
    }

    def test_initial_snapshot_contains_all_nodes(self):
        v = engine.validate_model(self.MODEL)
        initial = engine.evaluate_all(v, {})
        self.assertEqual(initial["g"].rows, {(0, 0): 1})
        self.assertEqual(initial["p"].rows, {})


if __name__ == "__main__":
    unittest.main()
