"""Differential tests: engine multiplicities vs an expanded-row reference.

Each test expands every source occurrence into its own row list, runs the
naive reference evaluator and compares it to the production engine's
canonical expansion.  This covers the required differential matrix:

* null-padding after a withdrawn duplicate is restored,
* dedup crossing zero,
* empty global group,
* non-null-keyed self join whose multiplicity goes 1 -> 2 (result 1 -> 4),
* a mixed random model through both evaluators,
* full replay from the version-0 snapshot after restart.
"""

from __future__ import annotations

import random
import unittest

from feature_service import engine
from tests.helpers import WheelTestCase, canonical_exit_state, replay
from tests import reference

SRC = [
    {"type": "int", "nullable": False},   # 0 id
    {"type": "int", "nullable": True},    # 1 k
    {"type": "str", "nullable": True},    # 2 s
]

MODEL = {
    "source": {"name": "src", "columns": SRC},
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


def expand(source_rows):
    return [vals for vals, w in engine.sorted_items(source_rows)
            for _ in range(w)]


def engine_expanded(validated, node, source_rows):
    table = engine.evaluate_node(validated, node, source_rows)
    return reference.canonical(expand(table.rows))


class ReferenceAgreementTests(unittest.TestCase):
    def setUp(self):
        self.validated = engine.validate_model(MODEL)

    def compare_all(self, source_rows):
        source_expanded = expand(source_rows)
        for node in MODEL["nodes"]:
            self.assertEqual(
                engine_expanded(self.validated, node, source_rows),
                reference.canonical(reference.evaluate(
                    self.validated, node, source_expanded)),
                msg=f"node {node} disagrees for source {source_rows}")

    def test_empty_source(self):
        self.compare_all({})

    def test_nulls_and_duplicates(self):
        self.compare_all({
            (1, None, "a"): 3,
            (2, 5, None): 1,
            (3, 5, "b"): 2,
            (4, 9, "a"): 1,
        })

    def test_randomised_models(self):
        rng = random.Random(20241112)
        for _ in range(25):
            source_rows = {}
            for i in range(rng.randint(0, 8)):
                row = (i, rng.choice([None, rng.randint(0, 3)]),
                       rng.choice([None, "a", "b", "é"]))
                source_rows[row] = rng.randint(1, 4)
            self.compare_all(source_rows)


class RequiredScenarioTests(unittest.TestCase):
    """The scenarios explicitly demanded by the specification."""

    def setUp(self):
        self.validated = engine.validate_model(MODEL)

    def test_withdraw_supplier_restores_null_padded_row(self):
        """Withdrawing the only supplier of a match restores null padding.

        Model: left = project src[1] (k), right = project src[2] (m), joined
        on k == m.  Source row ``(2, 7, 5)`` supplies both left key 7 and
        right value 5, so left key 5 finds a match through that row's m.
        Withdrawing ``(2, 7, 5)`` removes the match and the outer join emits
        the null-padded row ``(5, None)`` instead.
        """
        m = engine.validate_model({
            "source": {"name": "src", "columns": [
                {"type": "int", "nullable": False},
                {"type": "int", "nullable": True},
                {"type": "int", "nullable": True},
            ]},
            "nodes": {
                "l": {"type": "projection", "inputs": ["src"], "columns": [1]},
                "r": {"type": "projection", "inputs": ["src"], "columns": [2]},
                "j": {"type": "left_join", "inputs": ["l", "r"],
                      "columns": {"left": [0], "right": [0]}},
            },
            "exits": ["j"],
        })
        with_supplier = {(1, 5, 9): 1, (2, 7, 5): 1}
        withdrawn = {(1, 5, 9): 1}
        matched = engine.evaluate_node(m, "j", with_supplier).rows
        padded = engine.evaluate_node(m, "j", withdrawn).rows
        self.assertEqual(matched, {(5, 5): 1, (7, None): 1})
        self.assertEqual(padded, {(5, None): 1})
        deltas = engine.row_deltas(matched, padded)
        self.assertEqual(deltas, [( (5, 5), -1), ((5, None), 1), ((7, None), -1)])

    def test_dedup_crosses_zero_without_publishing_change(self):
        v = self.validated
        with_two = {(1, 7, "x"): 2}
        with_one = {(1, 7, "x"): 1}
        a = engine.evaluate_node(v, "dedup", with_two).rows
        b = engine.evaluate_node(v, "dedup", with_one).rows
        self.assertEqual(a, b)
        self.assertEqual(engine.row_deltas(a, b), [])

    def test_empty_global_group_two_zeros(self):
        v = self.validated
        self.assertEqual(engine.evaluate_node(v, "global", {}).rows,
                         {(0, 0): 1})

    def test_self_join_nonnull_key_weight_one_to_two_gives_one_to_four(self):
        m = engine.validate_model({
            "source": {"name": "src", "columns": SRC[:1]},
            "nodes": {
                "l": {"type": "projection", "inputs": ["src"],
                      "columns": [0]},
                "j": {"type": "left_join", "inputs": ["l", "l"],
                      "columns": {"left": [0], "right": [0]}},
            },
            "exits": ["j"],
        })
        one = engine.evaluate_node(m, "j", {(5,): 1}).rows
        two = engine.evaluate_node(m, "j", {(5,): 2}).rows
        self.assertEqual(one, {(5, 5): 1})
        self.assertEqual(two, {(5, 5): 4})
        deltas = engine.row_deltas(one, two)
        self.assertEqual(deltas, [((5, 5), 3)])
        self.assertEqual(engine.apply_deltas(one, deltas), two)

    def test_self_join_null_key_never_matches(self):
        m = engine.validate_model({
            "source": {"name": "src", "columns": SRC[1:2]},
            "nodes": {
                "j": {"type": "left_join", "inputs": ["src", "src"],
                      "columns": {"left": [0], "right": [0]}},
            },
            "exits": ["j"],
        })
        out = engine.evaluate_node(m, "j", {(None,): 2}).rows
        self.assertEqual(out, {(None, None): 2})


class ReplayTests(WheelTestCase):
    def test_full_replay_after_restart_restores_current_exits(self):
        self.c().post("/model", MODEL)
        batches = [
            ("b1", [{"row": [1, 7, "x"], "diff": 3},
                    {"row": [2, 8, None], "diff": 1}]),
            ("b2", [{"row": [1, 7, "x"], "diff": -2}]),
            ("b3", [{"row": [3, None, "y"], "diff": 2}]),
            ("b4", [{"row": [4, 7, None], "diff": 1},
                    {"row": [2, 8, None], "diff": -1}]),
        ]
        for batch_id, changes in batches:
            status, body = self.c().post(
                "/batches", {"batch_id": batch_id, "source": "src",
                             "changes": changes})
            self.assertEqual(status, 200, body)

        client = self.harness.restart()
        _, snapshot = client.get("/snapshot")
        _, changes = client.get("/changes?cursor=0")
        rebuilt = replay(snapshot, changes["changes"])

        _, exits = client.get("/exits")
        current = {name: canonical_exit_state(payload)
                   for name, payload in exits["exits"].items()}
        current = {name: rows for name, rows in current.items() if rows}
        self.assertEqual(current, {name: rows for name, rows in rebuilt.items()
                                   if rows})

        # And the persisted node results agree with fresh evaluation.
        for node in MODEL["nodes"]:
            _, stored = client.get(f"/nodes/{node}")
            _, evaluated = client.get(f"/nodes/{node}?evaluate=1")
            self.assertEqual(stored["rows"], evaluated["rows"],
                             msg=f"node {node} persisted result is stale")
            self.assertEqual(stored["version"], evaluated["version"])


class DirectReplayTests(unittest.TestCase):
    """Replay without restart, entirely in-process against the API service."""

    def test_replay_matches_exits_at_each_step(self):
        from tests.helpers import ServiceHarness

        harness = ServiceHarness()
        try:
            client = harness.client
            client.post("/model", MODEL)
            _, snapshot = client.get("/snapshot")
            cursor = 0
            scripted = [
                ("b1", [{"row": [1, 7, "x"], "diff": 1}]),
                ("b2", [{"row": [1, 7, "x"], "diff": 1}]),
                ("b3", [{"row": [1, 7, "x"], "diff": -2}]),
                ("b4", [{"row": [2, None, "a"], "diff": 2}]),
            ]
            accumulated = []
            for batch_id, changes in scripted:
                status, body = client.post(
                    "/batches", {"batch_id": batch_id, "source": "src",
                                 "changes": changes})
                self.assertEqual(status, 200, body)
                _, delta = client.get(f"/changes?cursor={cursor}")
                accumulated.extend(delta["changes"])
                cursor = delta["version"]
                rebuilt = replay(snapshot, accumulated)
                _, exits = client.get("/exits")
                current = {name: canonical_exit_state(payload)
                           for name, payload in exits["exits"].items()}
                current = {name: rows for name, rows in current.items() if rows}
                self.assertEqual(
                    {k: v for k, v in rebuilt.items() if v}, current,
                    msg=f"replay diverged after {batch_id}")
        finally:
            harness.stop()


if __name__ == "__main__":
    unittest.main()
