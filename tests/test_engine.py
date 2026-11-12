"""Engine semantics tests: projection, dedup, left join, group count."""

from __future__ import annotations

import unittest

from feature_service import engine
from feature_service.engine import evaluate_node, validate_model

SRC_SPECS = [
    {"type": "int", "nullable": False},   # 0 id
    {"type": "int", "nullable": True},    # 1 k (nullable int)
    {"type": "str", "nullable": True},    # 2 s (nullable str)
]


def build(nodes, exits):
    return validate_model({"source": {"name": "src", "columns": SRC_SPECS},
                           "nodes": nodes, "exits": exits})


def rows_of(v, name, src):
    return evaluate_node(v, name, src).rows


class OrderingTests(unittest.TestCase):
    def test_canonical_row_order_ints_strings_nulls(self):
        v = build({"p": {"type": "projection", "inputs": ["src"],
                         "columns": [0]}}, ["p"])
        src = {(10, None, None): 1, (2, None, None): 1, (-1, None, None): 1}
        # Projection onto column 0 merges distinct k/s, keeping values.
        out = evaluate_node(v, "p", src).rows
        keys = [vals for vals, _ in engine.sorted_items(out)]
        self.assertEqual(keys, [(-1,), (2,), (10,)])

    def test_utf8_order_and_null_last(self):
        v = build({"p": {"type": "projection", "inputs": ["src"],
                         "columns": [2]}}, ["p"])
        src = {(1, 0, "z"): 1, (2, 0, "a"): 1, (3, 0, None): 1,
               (4, 0, "é"): 1}
        out = evaluate_node(v, "p", src).rows
        keys = [vals for vals, _ in engine.sorted_items(out)]
        self.assertEqual(keys, [("a",), ("z",), ("é",), (None,)])


class ProjectionTests(unittest.TestCase):
    def test_projection_preserves_duplicates_and_order(self):
        v = build({"p": {"type": "projection", "inputs": ["src"],
                         "columns": [2, 0, 2]}}, ["p"])
        src = {(1, None, "x"): 2, (2, None, "x"): 3}
        out = evaluate_node(v, "p", src).rows
        self.assertEqual(out, {("x", 1, "x"): 2, ("x", 2, "x"): 3})

    def test_projection_merges_identical_projected_rows(self):
        v = build({"p": {"type": "projection", "inputs": ["src"],
                         "columns": [2]}}, ["p"])
        src = {(1, 0, "x"): 2, (2, 0, "x"): 3, (3, 0, "y"): 1}
        self.assertEqual(evaluate_node(v, "p", src).rows,
                         {("x",): 5, ("y",): 1})


class DedupTests(unittest.TestCase):
    def test_dedup_crosses_zero(self):
        """Weight 2 -> dedup 1: withdrawing one copy must publish no change."""
        v = build({"d": {"type": "dedup", "inputs": ["src"]}}, ["d"])
        t1 = evaluate_node(v, "d", {(1, 5, "x"): 2}).rows
        t2 = evaluate_node(v, "d", {(1, 5, "x"): 1}).rows
        self.assertEqual(t1, {(1, 5, "x"): 1})
        self.assertEqual(t2, {(1, 5, "x"): 1})
        t3 = evaluate_node(v, "d", {}).rows
        self.assertEqual(t3, {})

    def test_dedup_null_equals_null(self):
        v = build({"d": {"type": "dedup", "inputs": ["src"]}}, ["d"])
        src = {(1, None, None): 2, (2, None, None): 1}
        self.assertEqual(evaluate_node(v, "d", src).rows,
                         {(1, None, None): 1, (2, None, None): 1})

    def test_dedup_empty_input(self):
        v = build({"d": {"type": "dedup", "inputs": ["src"]}}, ["d"])
        self.assertEqual(evaluate_node(v, "d", {}).rows, {})


class LeftJoinTests(unittest.TestCase):
    def _join_model(self):
        return build({
            "l": {"type": "projection", "inputs": ["src"], "columns": [0, 1]},
            "r": {"type": "projection", "inputs": ["src"], "columns": [1, 0]},
            "j": {"type": "left_join", "inputs": ["l", "r"],
                  "columns": {"left": [1], "right": [0]}},
        }, ["j"])

    def test_null_pads_unmatched_rows(self):
        v = self._join_model()
        # l=(id,k); r=(k,id). key l[1]=k, r[0]=k.
        src = {(1, 5): 1, (2, None): 1}
        out = evaluate_node(v, "j", src).rows
        # (1,5) matches right row (5,1): (1,5,5,1); (2,None) has null key.
        self.assertEqual(out, {(1, 5, 5, 1): 1, (2, None, None, None): 1})

    def test_self_join_one_to_two_doubles_then_doubles_again(self):
        """Self join (non-null key) weight 1 -> 2 means result 1 -> 4."""
        v = build({
            "l": {"type": "projection", "inputs": ["src"], "columns": [0]},
            "j": {"type": "left_join", "inputs": ["l", "l"],
                  "columns": {"left": [0], "right": [0]}},
        }, ["j"])
        one = evaluate_node(v, "j", {(7, None, None): 1}).rows
        two = evaluate_node(v, "j", {(7, None, None): 2}).rows
        self.assertEqual(one, {(7, 7): 1})
        self.assertEqual(two, {(7, 7): 4})

    def test_null_key_right_never_matches_but_pads(self):
        v = self._join_model()
        out = evaluate_node(v, "j", {(1, None): 1}).rows
        self.assertEqual(out, {(1, None, None, None): 1})

    def test_multiple_right_matches_multiply(self):
        v = build({
            "l": {"type": "projection", "inputs": ["src"], "columns": [0]},
            "r": {"type": "projection", "inputs": ["src"], "columns": [1]},
            "j": {"type": "left_join", "inputs": ["l", "r"],
                  "columns": {"left": [0], "right": [0]}},
        }, ["j"])
        src = {(1, 10, None): 2, (1, 11, None): 3}
        # left value 1 weight 2; right keys 10 and 11 don't match; so pad.
        self.assertEqual(evaluate_node(v, "j", src).rows,
                         {(1, None): 5})


class GroupCountTests(unittest.TestCase):
    def test_empty_nonnull_key_outputs_nothing(self):
        v = build({"g": {"type": "group_count", "inputs": ["src"],
                         "keys": [1], "count": 2}}, ["g"])
        self.assertEqual(evaluate_node(v, "g", {}).rows, {})

    def test_empty_global_group_outputs_two_zeros(self):
        v = build({"g": {"type": "group_count", "inputs": ["src"],
                         "keys": [], "count": 2}}, ["g"])
        self.assertEqual(evaluate_node(v, "g", {}).rows, {(0, 0): 1})

    def test_null_key_groups_together_and_counts_nonnull(self):
        v = build({"g": {"type": "group_count", "inputs": ["src"],
                         "keys": [1], "count": 2}}, ["g"])
        src = {(1, None, "a"): 2, (2, None, None): 1, (3, 4, None): 3}
        self.assertEqual(evaluate_node(v, "g", src).rows,
                         {(None, 3, 2): 1, (4, 3, 0): 1})

    def test_counts_accumulate_multiplicity(self):
        v = build({"g": {"type": "group_count", "inputs": ["src"],
                         "keys": [], "count": 1}}, ["g"])
        src = {(1, 3, "x"): 2, (2, 3, None): 3}
        # Total = 5; non-null count column (index 1) = 2 + 3 = 5.
        self.assertEqual(evaluate_node(v, "g", src).rows, {(5, 5): 1})

    def test_group_does_not_merge_distinct_groups(self):
        v = build({"g": {"type": "group_count", "inputs": ["src"],
                         "keys": [1, 2], "count": 0}}, ["g"])
        src = {(1, 1, "a"): 1, (2, 1, None): 1}
        self.assertEqual(evaluate_node(v, "g", src).rows,
                         {(1, "a", 1, 1): 1, (1, None, 1, 1): 1})


class DeltaTests(unittest.TestCase):
    def test_row_deltas_merge_and_drop_zero(self):
        old = {("a",): 3}
        new = {("b",): 2}
        self.assertEqual(engine.row_deltas(old, new),
                         [(("a",), -3), (("b",), 2)])

    def test_row_deltas_zero_omitted(self):
        old = {("a",): 3, ("b",): 1}
        new = {("a",): 3, ("b",): 2}
        self.assertEqual(engine.row_deltas(old, new), [(("b",), 1)])

    def test_apply_deltas_negative_rejected(self):
        with self.assertRaises(engine.EvalError):
            engine.apply_deltas({}, [((), -1)])


if __name__ == "__main__":
    unittest.main()
