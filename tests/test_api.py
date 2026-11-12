"""Service-level HTTP tests: registration, snapshots, batches, changes."""

from __future__ import annotations

import json
import unittest
from concurrent.futures import ThreadPoolExecutor

from tests.helpers import ServiceHarness, WheelTestCase, canonical_exit_state, replay

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
                   "keys": [1], "count": [3][0]},
        "global": {"type": "group_count", "inputs": ["src"], "keys": [],
                   "count": [1][0]},
    },
    "exits": ["dedup", "joined", "counts", "global"],
}


def batch(batch_id, changes):
    return {"batch_id": batch_id, "source": "src", "changes": changes}


class RegistrationTests(WheelTestCase):
    def test_register_returns_version_zero_snapshot(self):
        status, body = self.c().post("/model", MODEL)
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 0)
        self.assertEqual(body["replayed"], False)
        for name, payload in body["exits"].items():
            self.assertIn("columns", payload)
            self.assertIn("rows", payload)
        # Empty source: global group still emits two zeros.
        self.assertEqual(canonical_exit_state(body["exits"]["global"]),
                         {(0, 0): 1})
        self.assertEqual(canonical_exit_state(body["exits"]["counts"]), {})

    def test_registration_is_idempotent(self):
        _, first = self.c().post("/model", MODEL)
        _, second = self.c().post("/model", MODEL)
        self.assertEqual(second["version"], 0)
        self.assertEqual(second["replayed"], True)
        self.assertEqual(first["nodes"], second["nodes"])

    def test_reordered_inputs_are_structurally_identical(self):
        m2 = json.loads(json.dumps(MODEL))
        m2["nodes"]["dedup"]["inputs"] = ["proj"]
        m2["exits"] = ["global", "counts", "joined", "dedup"]
        _, body = self.c().post("/model", MODEL)
        _, again = self.c().post("/model", m2)
        self.assertEqual(again["version"], 0)
        self.assertEqual(again["replayed"], True)

    def test_different_model_rejected(self):
        self.c().post("/model", MODEL)
        m2 = json.loads(json.dumps(MODEL))
        m2["nodes"]["global"]["keys"] = [2]
        status, body = self.c().post("/model", m2)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "model_conflict")

    def test_invalid_model_returns_400(self):
        m = {"source": {"name": "src", "columns": ["int"]},
             "nodes": {"p": {"type": "projection", "inputs": ["src"],
                             "columns": [5]}},
             "exits": ["p"]}
        status, body = self.c().post("/model", m)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_model")

    def test_initial_snapshot_endpoint(self):
        self.c().post("/model", MODEL)
        status, body = self.c().get("/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 0)

    def test_no_model_changes_returns_409(self):
        status, body = self.c().get("/changes?cursor=0")
        self.assertEqual(status, 409)

    def test_unknown_node_404(self):
        self.c().post("/model", MODEL)
        status, body = self.c().get("/nodes/nope")
        self.assertEqual(status, 404)


class BatchTests(WheelTestCase):
    def setUp(self):
        super().setUp()
        self.c().post("/model", MODEL)

    def test_batch_increments_version_and_publishes_diffs(self):
        status, body = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 1)
        self.assertFalse(body["replayed"])
        self.assertEqual(body["changes"]["global"],
                         [{"row": [0, 0], "diff": -1},
                          {"row": [2, 2], "diff": 1}])

    def test_same_batch_replay_returns_original_version(self):
        _, first = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        _, second = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        self.assertEqual(second["version"], first["version"])
        self.assertTrue(second["replayed"])
        self.assertEqual(self.c().get("/model")[1]["version"], 1)

    def test_merged_same_row_entries(self):
        _, body = self.c().post("/batches", batch("b1", [
            {"row": [1, 7, "x"], "diff": 2},
            {"row": [1, 7, "x"], "diff": -1},
        ]))
        self.assertEqual(body["version"], 1)
        _, src = self.c().get("/source")
        self.assertEqual(src["expanded_rows"], [[1, 7, "x"]])

    def test_net_zero_batch_is_accepted_without_effect(self):
        _, body = self.c().post("/batches", batch("b1", [
            {"row": [1, 7, "x"], "diff": 1},
            {"row": [1, 7, "x"], "diff": -1},
        ]))
        self.assertEqual(body["version"], 1)
        self.assertEqual(body["changes"]["global"], [])

    def test_batch_id_conflict(self):
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        status, body = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_id_conflict")

    def test_negative_source_multiplicity_rejected(self):
        status, body = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, None], "diff": -1}]))
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "batch_rejected")
        self.assertEqual(self.c().get("/model")[1]["version"], 0)
        self.assertEqual(self.c().get("/source")[1]["expanded_rows"], [])

    def test_whole_batch_rejected_when_one_row_goes_negative(self):
        self.c().post("/batches", batch("b0", [{"row": [1, 7, None], "diff": 1}]))
        status, body = self.c().post("/batches", batch("b1", [
            {"row": [1, 7, None], "diff": 1},
            {"row": [1, 7, None], "diff": -5},
        ]))
        self.assertEqual(status, 409)
        # Nothing changed: version and source untouched.
        self.assertEqual(self.c().get("/model")[1]["version"], 1)
        self.assertEqual(self.c().get("/source")[1]["expanded_rows"],
                         [[1, 7, None]])

    def test_diff_bool_rejected(self):
        status, body = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, None], "diff": True}]))
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_batch")

    def test_diff_zero_rejected(self):
        status, _ = self.c().post(
            "/batches", batch("b1", [{"row": [1, 7, None], "diff": 0}]))
        self.assertEqual(status, 400)

    def test_bool_in_int_column_rejected(self):
        status, _ = self.c().post(
            "/batches", batch("b1", [{"row": [True, 7, None], "diff": 1}]))
        self.assertEqual(status, 400)

    def test_null_in_nonnullable_column_rejected(self):
        status, _ = self.c().post(
            "/batches", batch("b1", [{"row": [None, 7, None], "diff": 1}]))
        self.assertEqual(status, 400)

    def test_float_in_int_column_rejected(self):
        status, _ = self.c().post(
            "/batches", batch("b1", [{"row": [1.0, 7, None], "diff": 1}]))
        self.assertEqual(status, 400)

    def test_wrong_source_name_rejected(self):
        body = batch("b1", [{"row": [1, 7, None], "diff": 1}])
        body["source"] = "other"
        status, _ = self.c().post("/batches", body)
        self.assertEqual(status, 400)

    def test_missing_batch_id_rejected(self):
        status, _ = self.c().post(
            "/batches", {"source": "src",
                         "changes": [{"row": [1, 7, None], "diff": 1}]})
        self.assertEqual(status, 400)

    def test_empty_changes_rejected(self):
        status, _ = self.c().post("/batches", batch("b1", []))
        self.assertEqual(status, 400)


class ChangeContinuationTests(WheelTestCase):
    def setUp(self):
        super().setUp()
        self.c().post("/model", MODEL)

    def test_changes_only_after_cursor(self):
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        self.c().post("/batches", batch("b2", [{"row": [2, 7, None], "diff": 1}]))
        _, all_changes = self.c().get("/changes?cursor=0")
        self.assertEqual(all_changes["version"], 2)
        self.assertEqual([b["version"] for b in all_changes["batches"]],
                         [1, 2])
        _, later = self.c().get("/changes?cursor=1")
        self.assertEqual([b["version"] for b in later["batches"]], [2])
        _, none = self.c().get("/changes?cursor=2")
        self.assertEqual(none["batches"], [])

    def test_invalid_cursor(self):
        status, body = self.c().get("/changes?cursor=abc")
        self.assertEqual(status, 400)
        status, body = self.c().get("/changes?cursor=-1")
        self.assertEqual(status, 400)

    def test_continuation_from_initial_snapshot(self):
        """No cursor means "continue from version 0" (whole first batch)."""
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        self.c().post("/batches", batch("b2", [{"row": [2, 7, None], "diff": 1}]))
        _, first = self.c().get("/changes")
        self.assertEqual([b["version"] for b in first["batches"]], [1, 2])
        _, only_next = self.c().get("/changes?since=1")
        self.assertEqual([b["version"] for b in only_next["batches"]], [1])
        _, after = self.c().get("/changes?cursor=1")
        self.assertEqual([b["version"] for b in after["batches"]], [2])

    def test_whole_versions_only(self):
        self.c().post("/batches", batch("b1", [
            {"row": [1, 7, "x"], "diff": 1},
            {"row": [2, 8, None], "diff": 1},
        ]))
        _, changes = self.c().get("/changes?cursor=0")
        self.assertEqual(len(changes["batches"]), 1)
        batch_rows = changes["batches"][0]["changes"]
        versions = set()
        for entry in batch_rows:
            versions.add(changes["batches"][0]["version"])
        self.assertEqual(versions, {1})
        self.assertEqual(set(
            entry["version"] for entry in changes["changes"]), {1})

    def test_rows_are_expanded_in_canonical_order(self):
        self.c().post("/batches", batch("b1", [
            {"row": [2, 7, "z"], "diff": 1},
            {"row": [1, 7, "a"], "diff": 2},
        ]))
        _, node = self.c().get("/nodes/proj")
        self.assertEqual(node["rows"], [[7, "a"], [7, "a"], [7, "z"]])
        _, src = self.c().get("/source")
        self.assertEqual(src["rows"], [[1, 7, "a"], [1, 7, "a"], [2, 7, "z"]])

    def test_node_query_reflects_current_version(self):
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        _, node = self.c().get("/nodes/dedup")
        self.assertEqual(node["version"], 1)
        self.assertEqual(node["expanded_rows"], [[7, "x"]])


class PersistenceTests(WheelTestCase):
    def test_restart_preserves_state_and_snapshot(self):
        self.c().post("/model", MODEL)
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        self.c().post("/batches", batch("b2", [{"row": [2, 8, None], "diff": 1}]))
        client = self.harness.restart()
        _, model = client.get("/model")
        self.assertEqual(model["version"], 2)
        _, snap = client.get("/snapshot")
        self.assertEqual(snap["version"], 0)
        _, changes = client.get("/changes?cursor=0")
        self.assertEqual(len(changes["batches"]), 2)

    def test_replaying_all_changes_after_restart_restores_exits(self):
        self.c().post("/model", MODEL)
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 2}]))
        self.c().post("/batches", batch("b2", [{"row": [2, 8, None], "diff": 1},
                                               {"row": [1, 7, "x"], "diff": -1}]))
        client = self.harness.restart()
        _, snap = client.get("/snapshot")
        _, changes = client.get("/changes?cursor=0")
        rebuilt = replay(snap, changes["changes"])
        _, exits = client.get("/exits")
        current = {name: canonical_exit_state(payload)
                   for name, payload in exits["exits"].items()}
        current = {k: v for k, v in current.items() if v}
        self.assertEqual({k: v for k, v in rebuilt.items() if v}, current)

    def test_batch_idempotency_survives_restart(self):
        self.c().post("/model", MODEL)
        self.c().post("/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        client = self.harness.restart()
        _, replay_body = client.post(
            "/batches", batch("b1", [{"row": [1, 7, "x"], "diff": 1}]))
        self.assertTrue(replay_body["replayed"])
        self.assertEqual(replay_body["version"], 1)


class ConcurrencyTests(unittest.TestCase):
    def test_concurrent_batches_serialise(self):
        harness = ServiceHarness()
        try:
            client = harness.client
            client.post("/model", MODEL)

            def submit(i):
                return client.post("/batches", batch(f"b{i}", [
                    {"row": [i, i, None], "diff": 1}]))

            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(submit, range(1, 9)))
            versions = sorted(body["version"] for _, body in results)
            self.assertEqual(versions, list(range(1, 9)))
            _, model = client.get("/model")
            self.assertEqual(model["version"], 8)
            _, source = client.get("/source")
            self.assertEqual(len(source["expanded_rows"]), 8)
            # Every update's diffs replay to the finished exit state.
            _, snap = client.get("/snapshot")
            _, changes = client.get("/changes")
            rebuilt = replay(snap, changes["changes"])
            _, exits = client.get("/exits")
            current = {name: canonical_exit_state(payload)
                       for name, payload in exits["exits"].items()}
            self.assertEqual({k: v for k, v in rebuilt.items() if v},
                             {k: v for k, v in current.items() if v})
        finally:
            harness.stop()

    def test_two_servers_share_one_database_serially(self):
        harness = ServiceHarness()
        other = harness.second_server()
        try:
            harness.client.post("/model", MODEL)
            other.client.post("/model", MODEL)

            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = [
                    pool.submit(
                        (harness.client if i % 2 else other.client).post,
                        "/batches",
                        batch(f"b{i}", [{"row": [i, i, None], "diff": 1}]))
                    for i in range(1, 7)
                ]
                results = [f.result() for f in futures]
            for status, body in results:
                self.assertEqual(status, 200, body)
            versions = sorted(body["version"] for _, body in results
                              if "version" in body)
            self.assertEqual(versions, list(range(1, 7)))
            _, model = harness.client.get("/model")
            self.assertEqual(model["version"], 6)
        finally:
            other.stop()
            harness.stop()

    def test_duplicate_batch_race_returns_original_version(self):
        harness = ServiceHarness()
        other = harness.second_server()
        try:
            harness.client.post("/model", MODEL)
            other.client.post("/model", MODEL)
            payload = batch("same", [{"row": [1, 7, "x"], "diff": 2}])
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(
                    lambda c: c.post("/batches", payload),
                    [harness.client, other.client,
                     harness.client, other.client]))
            for status, body in results:
                self.assertEqual(status, 200, body)
                self.assertEqual(body["version"], 1)
            _, model = harness.client.get("/model")
            self.assertEqual(model["version"], 1)
            _, src = harness.client.get("/source")
            self.assertEqual(len(src["expanded_rows"]), 2)
        finally:
            other.stop()
            harness.stop()


if __name__ == "__main__":
    unittest.main()
