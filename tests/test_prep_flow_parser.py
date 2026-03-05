"""
Unit tests for tableau_export.prep_flow_parser — individual step handlers.

Tests every action type in _convert_action_to_m_step, expression conversion,
aggregate/join/union/pivot node parsing, and helper functions.
"""

import json
import os
import tempfile
import unittest

from tableau_export.prep_flow_parser import (
    _convert_action_to_m_step,
    _convert_prep_expression_to_m,
    _parse_clean_actions,
    _parse_aggregate_node,
    _parse_join_node,
    _parse_union_node,
    _parse_pivot_node,
    _get_node_type,
    _topological_sort,
    _find_upstream_nodes,
    _clean_m_table_ref,
    read_prep_flow,
)


# ═══════════════════════════════════════════════════════════════════
#  Expression conversion
# ═══════════════════════════════════════════════════════════════════


class TestConvertPrepExpressionToM(unittest.TestCase):
    """Unit tests for _convert_prep_expression_to_m."""

    def test_empty_expression(self):
        self.assertEqual(_convert_prep_expression_to_m(""), '""')
        self.assertEqual(_convert_prep_expression_to_m(None), '""')

    def test_if_then_else(self):
        result = _convert_prep_expression_to_m("IF [A] > 0 THEN 'yes' ELSE 'no' END")
        self.assertIn("if", result)
        self.assertIn("then", result)
        self.assertIn("else", result)
        self.assertNotIn("END", result)

    def test_elseif(self):
        result = _convert_prep_expression_to_m("IF [A] > 0 THEN 1 ELSEIF [A] = 0 THEN 0 ELSE -1 END")
        self.assertIn("else if", result)

    def test_and_or_not(self):
        result = _convert_prep_expression_to_m("[A] > 0 AND [B] < 10 OR NOT [C]")
        self.assertIn("and", result)
        self.assertIn("or", result)
        self.assertIn("not", result)

    def test_comparison_operators(self):
        result = _convert_prep_expression_to_m("[A] != [B] AND [C] == [D]")
        self.assertIn("<>", result)
        self.assertNotIn("!=", result)
        # == → = (note: one = remains)
        self.assertIn("=", result)

    def test_isnull(self):
        result = _convert_prep_expression_to_m("ISNULL([Col])")
        self.assertIn("null =", result)

    def test_string_functions(self):
        self.assertIn("Text.Contains(", _convert_prep_expression_to_m("CONTAINS([A], 'x')"))
        self.assertIn("Text.Length(", _convert_prep_expression_to_m("LEN([A])"))
        self.assertIn("Text.Upper(", _convert_prep_expression_to_m("UPPER([A])"))
        self.assertIn("Text.Lower(", _convert_prep_expression_to_m("LOWER([A])"))
        self.assertIn("Text.Trim(", _convert_prep_expression_to_m("TRIM([A])"))
        self.assertIn("Text.Start(", _convert_prep_expression_to_m("LEFT([A], 3)"))
        self.assertIn("Text.End(", _convert_prep_expression_to_m("RIGHT([A], 3)"))

    def test_case_insensitive(self):
        result = _convert_prep_expression_to_m("if [A] > 0 then 1 else 0 end")
        self.assertIn("if", result)
        self.assertIn("then", result)


# ═══════════════════════════════════════════════════════════════════
#  Individual action conversions
# ═══════════════════════════════════════════════════════════════════


class TestConvertActionToMStep(unittest.TestCase):
    """Unit tests for _convert_action_to_m_step."""

    def test_rename_column(self):
        action = {"columnName": "Old", "newColumnName": "New"}
        result = _convert_action_to_m_step("RenameColumn", action, {})
        self.assertIsNotNone(result)
        step_name, step_expr = result
        self.assertIn("RenameColumns", step_expr)

    def test_remove_column(self):
        action = {"columnName": "DropMe"}
        result = _convert_action_to_m_step("RemoveColumn", action, {})
        self.assertIsNotNone(result)
        step_name, step_expr = result
        self.assertIn("RemoveColumns", step_expr)
        self.assertIn("DropMe", step_expr)

    def test_duplicate_column(self):
        action = {"columnName": "Col", "newColumnName": "Col_copy"}
        result = _convert_action_to_m_step("DuplicateColumn", action, {})
        self.assertIsNotNone(result)
        step_name, step_expr = result
        self.assertIn("DuplicateColumn", step_expr)

    def test_change_column_type(self):
        counter = {}
        action = {"columnName": "Revenue", "newType": "real"}
        result = _convert_action_to_m_step("ChangeColumnType", action, counter)
        self.assertIsNotNone(result)
        step_name, step_expr = result
        self.assertEqual(step_name, '#"Changed Type"')
        self.assertIn("TransformColumnTypes", step_expr)
        self.assertIn("Revenue", step_expr)
        # Second call increments counter
        result2 = _convert_action_to_m_step("ChangeColumnType", action, counter)
        self.assertEqual(result2[0], '#"Changed Type 1"')

    def test_filter_operation_keep(self):
        counter = {}
        action = {"filterExpression": "[Qty] > 0", "filterType": "keep"}
        result = _convert_action_to_m_step("FilterOperation", action, counter)
        step_name, step_expr = result
        self.assertEqual(step_name, '#"Filtered Rows"')
        self.assertIn("SelectRows", step_expr)
        self.assertNotIn("not", step_expr)

    def test_filter_operation_remove(self):
        counter = {}
        action = {"filterExpression": "[Qty] = 0", "filterType": "remove"}
        result = _convert_action_to_m_step("FilterOperation", action, counter)
        step_name, step_expr = result
        self.assertIn("not", step_expr)

    def test_filter_values(self):
        action = {"columnName": "Status", "values": ["Active", "Pending"], "filterType": "keep"}
        result = _convert_action_to_m_step("FilterValues", action, {})
        self.assertIsNotNone(result)

    def test_filter_values_remove(self):
        action = {"columnName": "Status", "values": ["Closed"], "filterType": "remove"}
        result = _convert_action_to_m_step("FilterValues", action, {})
        self.assertIsNotNone(result)

    def test_filter_range(self):
        action = {"columnName": "Price", "min": 10, "max": 100}
        result = _convert_action_to_m_step("FilterRange", action, {})
        self.assertIsNotNone(result)

    def test_replace_values(self):
        action = {"columnName": "Region", "oldValue": "NA", "newValue": "North America"}
        result = _convert_action_to_m_step("ReplaceValues", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("ReplaceValue", step_expr)

    def test_replace_nulls(self):
        action = {"columnName": "Score", "replacement": "0"}
        result = _convert_action_to_m_step("ReplaceNulls", action, {})
        self.assertIsNotNone(result)

    def test_split_column(self):
        action = {"columnName": "FullName", "delimiter": " "}
        result = _convert_action_to_m_step("SplitColumn", action, {})
        self.assertIsNotNone(result)

    def test_merge_columns(self):
        action = {"columns": ["First", "Last"], "separator": " ", "newColumnName": "Full"}
        result = _convert_action_to_m_step("MergeColumns", action, {})
        self.assertIsNotNone(result)

    def test_add_column(self):
        action = {"columnName": "Calc", "expression": "[A] + [B]"}
        result = _convert_action_to_m_step("AddColumn", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("AddColumn", step_expr)

    def test_clean_operation_trim(self):
        action = {"columnName": "Name", "operation": "trim"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("Trim", step_expr)

    def test_clean_operation_upper(self):
        action = {"columnName": "Code", "operation": "upper"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_clean_operation_lower(self):
        action = {"columnName": "Code", "operation": "lower"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_clean_operation_proper(self):
        action = {"columnName": "Name", "operation": "proper"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_clean_operation_remove(self):
        action = {"columnName": "Mixed", "operation": "removeletters"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_fill_values_down(self):
        action = {"columnName": "Category", "direction": "down"}
        result = _convert_action_to_m_step("FillValues", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("FillDown", step_expr)

    def test_fill_values_up(self):
        action = {"columnName": "Category", "direction": "up"}
        result = _convert_action_to_m_step("FillValues", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("FillUp", step_expr)

    def test_group_replace(self):
        action = {
            "columnName": "State",
            "groupings": [
                {"from": "CA", "to": "California"},
                {"from": "NY", "to": "New York"},
            ],
        }
        result = _convert_action_to_m_step("GroupReplace", action, {})
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)

    def test_conditional_column(self):
        action = {
            "newColumnName": "Tier",
            "rules": [
                {"condition": "[Revenue] > 1000", "value": "High"},
                {"condition": "[Revenue] > 500", "value": "Medium"},
            ],
            "defaultValue": "Low",
        }
        result = _convert_action_to_m_step("ConditionalColumn", action, {})
        self.assertIsNotNone(result)

    def test_extract_values(self):
        counter = {}
        action = {"columnName": "Phone", "pattern": r"\d+", "newColumnName": "Digits"}
        result = _convert_action_to_m_step("ExtractValues", action, counter)
        self.assertIsNotNone(result)
        step_name, step_expr = result
        self.assertEqual(step_name, '#"Extracted Values"')
        self.assertIn("Digits", step_expr)

    def test_custom_calculation(self):
        action = {"columnName": "Total", "expression": "[Qty] * [Price]"}
        result = _convert_action_to_m_step("CustomCalculation", action, {})
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("AddColumn", step_expr)

    def test_unknown_action_returns_none(self):
        result = _convert_action_to_m_step("UnknownAction", {}, {})
        self.assertIsNone(result)

    def test_empty_column_name_returns_none(self):
        result = _convert_action_to_m_step("RemoveColumn", {"columnName": ""}, {})
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════
#  Clean actions batching
# ═══════════════════════════════════════════════════════════════════


class TestParseCleanActions(unittest.TestCase):
    """Tests for _parse_clean_actions — rename batching and multi-action nodes."""

    def test_empty_node(self):
        steps = _parse_clean_actions({})
        self.assertEqual(steps, [])

    def test_single_rename(self):
        node = {
            "beforeActionGroup": {
                "actions": [
                    {"actionType": ".v1.RenameColumn", "columnName": "A", "newColumnName": "B"},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        self.assertEqual(len(steps), 1)
        _, step_expr = steps[0]
        self.assertIn("RenameColumns", step_expr)

    def test_consecutive_renames_batched(self):
        node = {
            "beforeActionGroup": {
                "actions": [
                    {"actionType": ".v1.RenameColumn", "columnName": "A", "newColumnName": "Alpha"},
                    {"actionType": ".v1.RenameColumn", "columnName": "B", "newColumnName": "Beta"},
                    {"actionType": ".v1.RenameColumn", "columnName": "C", "newColumnName": "Gamma"},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        # All 3 renames should batch into 1 step
        self.assertEqual(len(steps), 1)
        _, step_expr = steps[0]
        self.assertIn("Alpha", step_expr)
        self.assertIn("Beta", step_expr)
        self.assertIn("Gamma", step_expr)

    def test_renames_flushed_before_other_action(self):
        node = {
            "beforeActionGroup": {
                "actions": [
                    {"actionType": ".v1.RenameColumn", "columnName": "A", "newColumnName": "B"},
                    {"actionType": ".v1.RemoveColumn", "columnName": "X"},
                    {"actionType": ".v1.RenameColumn", "columnName": "C", "newColumnName": "D"},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        # 3 steps: rename A→B, remove X, rename C→D
        self.assertEqual(len(steps), 3)

    def test_after_action_group_included(self):
        node = {
            "beforeActionGroup": {
                "actions": [
                    {"actionType": ".v1.RemoveColumn", "columnName": "X"},
                ]
            },
            "afterActionGroup": {
                "actions": [
                    {"actionType": ".v1.RemoveColumn", "columnName": "Y"},
                ]
            },
        }
        steps = _parse_clean_actions(node)
        self.assertEqual(len(steps), 2)


# ═══════════════════════════════════════════════════════════════════
#  Node parsing functions
# ═══════════════════════════════════════════════════════════════════


class TestParseAggregateNode(unittest.TestCase):
    """Tests for _parse_aggregate_node."""

    def test_basic_aggregation(self):
        node = {
            "groupByFields": [{"name": "Region"}],
            "aggregateFields": [
                {"name": "Revenue", "aggregation": "SUM", "newColumnName": "TotalRevenue"},
            ],
        }
        result = _parse_aggregate_node(node)
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("Group", step_expr)

    def test_multiple_aggregations(self):
        node = {
            "groupByFields": [{"name": "Category"}, {"name": "Region"}],
            "aggregateFields": [
                {"name": "Sales", "aggregation": "SUM", "newColumnName": "TotalSales"},
                {"name": "Qty", "aggregation": "COUNT", "newColumnName": "OrderCount"},
            ],
        }
        result = _parse_aggregate_node(node)
        self.assertIsNotNone(result)

    def test_empty_aggregation(self):
        result = _parse_aggregate_node({})
        self.assertIsNone(result)


class TestParseJoinNode(unittest.TestCase):
    """Tests for _parse_join_node."""

    def test_inner_join(self):
        node = {
            "joinType": "inner",
            "joinConditions": [
                {"leftColumn": "OrderID", "rightColumn": "OrderID"},
            ],
        }
        right_fields = [{"name": "OrderID"}, {"name": "ProductName"}]
        result = _parse_join_node(node, "Products", right_fields)
        self.assertIsNotNone(result)
        # Should be a list of steps (join + expand)
        self.assertIsInstance(result, list)

    def test_no_conditions_returns_none(self):
        node = {"joinType": "inner", "joinConditions": []}
        result = _parse_join_node(node, "RightTable", [])
        self.assertIsNone(result)


class TestParseUnionNode(unittest.TestCase):
    """Tests for _parse_union_node."""

    def test_union_with_tables(self):
        result = _parse_union_node({}, ["Table1", "Table2"])
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("Combine", step_expr)

    def test_union_empty_tables(self):
        result = _parse_union_node({}, [])
        self.assertIsNone(result)


class TestParsePivotNode(unittest.TestCase):
    """Tests for _parse_pivot_node."""

    def test_unpivot(self):
        node = {
            "pivotType": "columnsToRows",
            "pivotFields": [{"name": "Q1"}, {"name": "Q2"}, {"name": "Q3"}],
            "pivotValuesName": "Sales",
            "pivotNamesName": "Quarter",
        }
        result = _parse_pivot_node(node)
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("Unpivot", step_expr)

    def test_pivot(self):
        node = {
            "pivotType": "rowsToColumns",
            "pivotKeyField": {"name": "Region"},
            "pivotValueField": {"name": "Sales"},
            "aggregation": "SUM",
        }
        result = _parse_pivot_node(node)
        self.assertIsNotNone(result)
        _, step_expr = result
        self.assertIn("Pivot", step_expr)

    def test_unknown_pivot_type_returns_none(self):
        result = _parse_pivot_node({"pivotType": "unknown"})
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════
#  Helper functions
# ═══════════════════════════════════════════════════════════════════


class TestHelpers(unittest.TestCase):
    """Tests for helper functions."""

    def test_get_node_type_versioned(self):
        node = {"nodeType": ".v2018_3_3.SuperTransform"}
        self.assertEqual(_get_node_type(node), "SuperTransform")

    def test_get_node_type_simple(self):
        node = {"nodeType": ".v1.LoadCsv"}
        self.assertEqual(_get_node_type(node), "LoadCsv")

    def test_get_node_type_empty(self):
        self.assertEqual(_get_node_type({}), "")

    def test_clean_m_table_ref_csv(self):
        self.assertEqual(_clean_m_table_ref("orders.csv"), "orders")

    def test_clean_m_table_ref_xlsx(self):
        self.assertEqual(_clean_m_table_ref("Sales Data.xlsx"), "Sales_Data")

    def test_clean_m_table_ref_no_ext(self):
        self.assertEqual(_clean_m_table_ref("MyTable"), "MyTable")

    def test_topological_sort_linear(self):
        nodes = {
            "A": {"nextNodes": [{"nextNodeId": "B"}]},
            "B": {"nextNodes": [{"nextNodeId": "C"}]},
            "C": {"nextNodes": []},
        }
        order = _topological_sort(nodes)
        self.assertEqual(order, ["A", "B", "C"])

    def test_topological_sort_diamond(self):
        nodes = {
            "A": {"nextNodes": [{"nextNodeId": "B"}, {"nextNodeId": "C"}]},
            "B": {"nextNodes": [{"nextNodeId": "D"}]},
            "C": {"nextNodes": [{"nextNodeId": "D"}]},
            "D": {"nextNodes": []},
        }
        order = _topological_sort(nodes)
        self.assertEqual(order[0], "A")
        self.assertEqual(order[-1], "D")
        self.assertIn("B", order)
        self.assertIn("C", order)

    def test_find_upstream_nodes(self):
        nodes = {
            "A": {"nextNodes": [{"nextNodeId": "C"}]},
            "B": {"nextNodes": [{"nextNodeId": "C"}]},
            "C": {"nextNodes": []},
        }
        upstream = _find_upstream_nodes(nodes, "C")
        self.assertEqual(sorted(upstream), ["A", "B"])


class TestReadPrepFlow(unittest.TestCase):
    """Tests for read_prep_flow (.tfl reading)."""

    def test_read_tfl_file(self):
        flow_data = {"nodes": {"n1": {"baseType": "input"}}, "connections": {}}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tfl', delete=False) as f:
            json.dump(flow_data, f)
            f.flush()
            path = f.name
        try:
            result = read_prep_flow(path)
            self.assertEqual(result["nodes"]["n1"]["baseType"], "input")
        finally:
            os.unlink(path)

    def test_unsupported_extension(self):
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            path = f.name
        try:
            with self.assertRaises(ValueError):
                read_prep_flow(path)
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
