"""Model validation tests: identifiers, types, references, cycles, shapes."""

from __future__ import annotations

import unittest

from feature_service.engine import ModelError, validate_model

def source():
    return {"name": "src", "columns": [
        {"type": "int", "nullable": False},
        {"type": "str", "nullable": True},
        {"type": "int", "nullable": True},
    ]}


def model(nodes, exits=("p",)):
    return {"source": source(), "nodes": nodes, "exits": list(exits)}


P = {"type": "projection", "inputs": ["src"], "columns": [0]}


class ValidModelTests(unittest.TestCase):
    def test_minimal_valid_model(self):
        v = validate_model(model({"p": P}))
        self.assertEqual(v["source"]["name"], "src")
        self.assertEqual(v["exits"], ["p"])

    def test_version_zero_accepted_and_omitted(self):
        validate_model(model({"p": P}))
        m = model({"p": P})
        m["version"] = 0
        validate_model(m)

    def test_shared_subgraph_and_self_join(self):
        nodes = {
            "p": {"type": "projection", "inputs": ["src"], "columns": [0, 1]},
            "d": {"type": "dedup", "inputs": ["p"]},
            "j": {"type": "left_join", "inputs": ["d", "d"],
                  "columns": {"left": [0], "right": [0]}},
            "g": {"type": "group_count", "inputs": ["j"], "keys": [0],
                  "count": [1][0]},
        }
        v = validate_model(model(nodes, exits=("g",)))
        self.assertIn("d", v["nodes"])

    def test_string_column_shorthand(self):
        m = {"source": {"name": "s", "columns": ["int", "str"]},
             "nodes": {"p": {"type": "projection", "inputs": ["s"],
                             "columns": [1]}},
             "exits": ["p"]}
        v = validate_model(m)
        self.assertEqual(v["source"]["columns"],
                         [{"type": "int", "nullable": True},
                          {"type": "str", "nullable": True}])

    def test_projection_repeats_columns(self):
        v = validate_model(model(
            {"p": {"type": "projection", "inputs": ["src"],
                   "columns": [1, 1, 0]}}))
        self.assertEqual(len(v["outputs"]["p"]), 3)


class InvalidModelTests(unittest.TestCase):
    def assert_rejected(self, m, fragment):
        with self.assertRaises(ModelError) as ctx:
            validate_model(m)
        self.assertIn(fragment, str(ctx.exception))

    def test_version_must_be_zero(self):
        m = model({"p": P})
        m["version"] = 1
        self.assert_rejected(m, "version")

    def test_version_boolean_rejected(self):
        m = model({"p": P})
        m["version"] = False
        self.assert_rejected(m, "version")

    def test_empty_identifier_rejected(self):
        m = model({"p": P})
        m["source"]["name"] = ""
        self.assert_rejected(m, "source.name")

    def test_non_ascii_identifier_rejected(self):
        m = model({"p": P})
        m["source"]["name"] = "源表"
        self.assert_rejected(m, "ASCII")

    def test_space_identifier_rejected(self):
        m = model({"p": P})
        m["nodes"]["p q"] = dict(P, inputs=["src"])
        self.assert_rejected(m, "ASCII")

    def test_slash_identifier_rejected_for_url_safety(self):
        m = model({"p": P})
        m["nodes"]["a/b"] = dict(P, inputs=["src"])
        self.assert_rejected(m, "ASCII")

    def test_duplicate_exit_rejected(self):
        self.assert_rejected(model({"p": P}, exits=("p", "p")), "unique")

    def test_unknown_input_rejected(self):
        self.assert_rejected(model(
            {"p": {"type": "projection", "inputs": ["nope"],
                   "columns": [0]}}), "not a node")

    def test_cycle_rejected(self):
        nodes = {
            "a": {"type": "dedup", "inputs": ["b"]},
            "b": {"type": "dedup", "inputs": ["c"]},
            "c": {"type": "dedup", "inputs": ["a"]},
        }
        self.assert_rejected(model(nodes, exits=("a",)), "cycle")

    def test_self_cycle_rejected(self):
        self.assert_rejected(model({"a": {"type": "dedup", "inputs": ["a"]}},
                                   exits=("a",)), "cycle")

    def test_exit_must_be_node(self):
        self.assert_rejected(model({"p": P}, exits=("src",)), "not a node")

    def test_unknown_column_type(self):
        m = model({"p": P})
        m["source"]["columns"] = [{"type": "bool", "nullable": True}]
        self.assert_rejected(m, "type")

    def test_missing_nullable(self):
        m = model({"p": P})
        m["source"]["columns"] = [{"type": "int"}]
        self.assert_rejected(m, "nullable")

    def test_projection_index_out_of_range(self):
        self.assert_rejected(model(
            {"p": {"type": "projection", "inputs": ["src"],
                   "columns": [3]}}), "out of range")

    def test_negative_column_index(self):
        self.assert_rejected(model(
            {"p": {"type": "projection", "inputs": ["src"],
                   "columns": [-1]}}), "out of range")

    def test_boolean_column_index(self):
        self.assert_rejected(model(
            {"p": {"type": "projection", "inputs": ["src"],
                   "columns": [True]}}), "booleans")

    def test_join_key_lengths_must_match(self):
        nodes = {
            "a": {"type": "projection", "inputs": ["src"], "columns": [0, 1]},
            "j": {"type": "left_join", "inputs": ["a", "src"],
                  "columns": {"left": [0, 1], "right": [0]}},
        }
        self.assert_rejected(model(nodes, exits=("j",)), "equal length")

    def test_join_key_types_must_match(self):
        nodes = {
            "p": {"type": "projection", "inputs": ["src"], "columns": [1]},
            "j": {"type": "left_join", "inputs": ["src", "p"],
                  "columns": {"left": [0], "right": [0]}},
        }
        self.assert_rejected(model(nodes, exits=("j",)), "mismatched")

    def test_empty_join_key_rejected(self):
        nodes = {
            "j": {"type": "left_join", "inputs": ["src", "src"],
                  "columns": {"left": [], "right": []}},
        }
        self.assert_rejected(model(nodes, exits=("j",)), "must not be empty")

    def test_count_boolean_rejected(self):
        self.assert_rejected(model(
            {"g": {"type": "group_count", "inputs": ["src"], "keys": [0],
                   "count": True}}), "booleans")

    def test_duplicate_group_key_rejected(self):
        self.assert_rejected(model(
            {"g": {"type": "group_count", "inputs": ["src"], "keys": [0, 0],
                   "count": 1}}), "repeat")

    def test_extra_node_field_rejected(self):
        p = dict(P, extra=1)
        self.assert_rejected(model({"p": p}), "exactly")

    def test_unbalanced_join_inputs(self):
        self.assert_rejected(model(
            {"j": {"type": "left_join", "inputs": ["src"],
                   "columns": {"left": [0], "right": [0]}}}), "exactly 2")

    def test_empty_source_columns(self):
        m = model({"p": P})
        m["source"]["columns"] = []
        self.assert_rejected(m, "must not be empty")


if __name__ == "__main__":
    unittest.main()
