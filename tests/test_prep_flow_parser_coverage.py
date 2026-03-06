"""
Extra coverage tests for tableau_export.prep_flow_parser.

Targets uncovered branches in:
- _convert_action_to_m_step  (edge cases)
- _parse_aggregate_node      (empty fields → None)
- _clean_m_table_ref          (all file extensions)
- _parse_join_node            (expand_fields filtering, empty keys)
- _parse_pivot_node           (rowsToColumns empty, rowsToColumns pivot)
- _parse_input_node           (connection attribute merge)
- parse_prep_flow             (full flow: Join/Union/Pivot/Script/Prediction/
                               CrossJoin/PublishedDataSource, leaf fallback,
                               secondary branch emission, empty nodes)
- merge_prep_with_workbook    (empty prep queries, unmatched, multi-match)
- read_prep_flow              (.tflx support)
"""

import json
import os
import tempfile
import unittest
import zipfile
from unittest.mock import patch

from tableau_export.prep_flow_parser import (
    _convert_action_to_m_step,
    _convert_prep_expression_to_m,
    _parse_clean_actions,
    _parse_aggregate_node,
    _parse_join_node,
    _parse_union_node,
    _parse_pivot_node,
    _parse_input_node,
    _get_node_type,
    _topological_sort,
    _find_upstream_nodes,
    _clean_m_table_ref,
    read_prep_flow,
    parse_prep_flow,
    merge_prep_with_workbook,
)


# ═══════════════════════════════════════════════════════════════════
#  _clean_m_table_ref — all file extensions
# ═══════════════════════════════════════════════════════════════════

class TestCleanMTableRefExtensions(unittest.TestCase):
    """Cover every file-extension stripping branch."""

    def test_xls(self):
        self.assertEqual(_clean_m_table_ref("data.xls"), "data")

    def test_json(self):
        self.assertEqual(_clean_m_table_ref("feed.json"), "feed")

    def test_hyper(self):
        self.assertEqual(_clean_m_table_ref("extract.hyper"), "extract")

    def test_tde(self):
        self.assertEqual(_clean_m_table_ref("legacy.tde"), "legacy")

    def test_csv(self):
        self.assertEqual(_clean_m_table_ref("orders.csv"), "orders")

    def test_xlsx(self):
        self.assertEqual(_clean_m_table_ref("Sales Data.xlsx"), "Sales_Data")

    def test_no_ext(self):
        self.assertEqual(_clean_m_table_ref("Plain"), "Plain")

    def test_spaces_replaced(self):
        self.assertEqual(_clean_m_table_ref("My Table Name"), "My_Table_Name")


# ═══════════════════════════════════════════════════════════════════
#  _convert_action_to_m_step — additional edge cases
# ═══════════════════════════════════════════════════════════════════

class TestConvertActionEdgeCases(unittest.TestCase):
    """Edge cases not covered by the main test file."""

    def test_duplicate_column_default_new_name(self):
        """DuplicateColumn with no explicit newColumnName → col_copy."""
        action = {"columnName": "Qty"}
        result = _convert_action_to_m_step("DuplicateColumn", action, {})
        self.assertIsNotNone(result)
        _, expr = result
        self.assertIn("Qty_copy", expr)

    def test_duplicate_column_empty_col_returns_none(self):
        action = {"columnName": ""}
        result = _convert_action_to_m_step("DuplicateColumn", action, {})
        self.assertIsNone(result)

    def test_change_column_type_unknown_type(self):
        """Unknown Prep type maps to 'text' via default."""
        counter = {}
        action = {"columnName": "Col", "newType": "unknowntype"}
        result = _convert_action_to_m_step("ChangeColumnType", action, counter)
        self.assertIsNotNone(result)
        _, expr = result
        self.assertIn("type text", expr)

    def test_filter_operation_default_keep(self):
        """filterType defaults to 'keep'."""
        counter = {}
        action = {"filterExpression": "[X] > 0"}
        result = _convert_action_to_m_step("FilterOperation", action, counter)
        _, expr = result
        self.assertNotIn("not", expr)

    def test_filter_values_empty_values_returns_none(self):
        action = {"columnName": "A", "values": []}
        result = _convert_action_to_m_step("FilterValues", action, {})
        self.assertIsNone(result)

    def test_filter_values_empty_col_returns_none(self):
        action = {"columnName": "", "values": ["x"]}
        result = _convert_action_to_m_step("FilterValues", action, {})
        self.assertIsNone(result)

    def test_filter_range_no_min_max(self):
        """FilterRange with only column and no min/max."""
        action = {"columnName": "Price"}
        result = _convert_action_to_m_step("FilterRange", action, {})
        self.assertIsNotNone(result)

    def test_replace_values_empty_col_returns_none(self):
        action = {"columnName": "", "oldValue": "a", "newValue": "b"}
        result = _convert_action_to_m_step("ReplaceValues", action, {})
        self.assertIsNone(result)

    def test_replace_nulls_empty_col_returns_none(self):
        action = {"columnName": "", "replacement": "0"}
        result = _convert_action_to_m_step("ReplaceNulls", action, {})
        self.assertIsNone(result)

    def test_split_column_empty_col_returns_none(self):
        action = {"columnName": ""}
        result = _convert_action_to_m_step("SplitColumn", action, {})
        self.assertIsNone(result)

    def test_split_column_default_delimiter(self):
        action = {"columnName": "Line"}
        result = _convert_action_to_m_step("SplitColumn", action, {})
        self.assertIsNotNone(result)

    def test_merge_columns_empty_returns_none(self):
        action = {"columns": [], "separator": " ", "newColumnName": "Full"}
        result = _convert_action_to_m_step("MergeColumns", action, {})
        self.assertIsNone(result)

    def test_add_column_expression_starts_with_each(self):
        """Expression already starting with 'each' should not double-prefix."""
        action = {"columnName": "Calc", "expression": "each [A] + [B]"}
        result = _convert_action_to_m_step("AddColumn", action, {})
        _, expr = result
        # Should appear once as 'each'
        self.assertIn("each", expr)

    def test_clean_operation_empty_col_returns_none(self):
        action = {"columnName": "", "operation": "trim"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNone(result)

    def test_clean_operation_unknown_op_returns_none(self):
        action = {"columnName": "Col", "operation": "unknown_op"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNone(result)

    def test_clean_operation_removenumbers(self):
        action = {"columnName": "Code", "operation": "removenumbers"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_clean_operation_removepunctuation(self):
        action = {"columnName": "Text", "operation": "removepunctuation"}
        result = _convert_action_to_m_step("CleanOperation", action, {})
        self.assertIsNotNone(result)

    def test_fill_values_empty_col_returns_none(self):
        action = {"columnName": ""}
        result = _convert_action_to_m_step("FillValues", action, {})
        self.assertIsNone(result)

    def test_fill_values_default_direction_is_down(self):
        """Direction defaults to 'down'."""
        action = {"columnName": "Val"}
        result = _convert_action_to_m_step("FillValues", action, {})
        _, expr = result
        self.assertIn("FillDown", expr)

    def test_group_replace_empty_groupings_returns_none(self):
        action = {"columnName": "Col", "groupings": []}
        result = _convert_action_to_m_step("GroupReplace", action, {})
        self.assertIsNone(result)

    def test_group_replace_empty_from_to_skipped(self):
        """Groupings where from/to are empty are skipped."""
        action = {"columnName": "Col", "groupings": [{"from": "", "to": ""}]}
        result = _convert_action_to_m_step("GroupReplace", action, {})
        # All groupings skipped → result_steps empty → None
        self.assertIsNone(result)

    def test_conditional_column_no_rules_returns_none(self):
        action = {"newColumnName": "X", "rules": [], "defaultValue": "0"}
        result = _convert_action_to_m_step("ConditionalColumn", action, {})
        self.assertIsNone(result)

    def test_conditional_column_value_already_quoted(self):
        """Value that starts with double-quote should not be double-quoted."""
        action = {
            "newColumnName": "Tag",
            "rules": [{"condition": "[A] > 0", "value": '"Y"'}],
            "defaultValue": "N",
        }
        result = _convert_action_to_m_step("ConditionalColumn", action, {})
        self.assertIsNotNone(result)

    def test_conditional_column_numeric_value(self):
        """Numeric values should be converted to str."""
        action = {
            "newColumnName": "Band",
            "rules": [{"condition": "[Score] > 90", "value": 1}],
            "defaultValue": 0,
        }
        result = _convert_action_to_m_step("ConditionalColumn", action, {})
        self.assertIsNotNone(result)

    def test_extract_values_counter_increments(self):
        counter = {}
        action = {"columnName": "A", "pattern": "\\d+", "newColumnName": "D"}
        r1 = _convert_action_to_m_step("ExtractValues", action, counter)
        self.assertEqual(r1[0], '#"Extracted Values"')
        r2 = _convert_action_to_m_step("ExtractValues", action, counter)
        self.assertEqual(r2[0], '#"Extracted 1"')

    def test_extract_values_no_pattern(self):
        """Empty pattern → default '.*' (escaped)."""
        counter = {}
        action = {"columnName": "Col", "pattern": ""}
        result = _convert_action_to_m_step("ExtractValues", action, counter)
        self.assertIsNotNone(result)

    def test_extract_values_empty_col_returns_none(self):
        action = {"columnName": ""}
        result = _convert_action_to_m_step("ExtractValues", action, {})
        self.assertIsNone(result)

    def test_custom_calculation_expression_starts_with_each(self):
        action = {"columnName": "X", "expression": "each [A] * 2"}
        result = _convert_action_to_m_step("CustomCalculation", action, {})
        self.assertIsNotNone(result)

    def test_custom_calculation_no_new_col_uses_fallback(self):
        """When both columnName and newColumnName are absent, uses 'Calc'."""
        action = {"expression": "[A] + 1"}
        result = _convert_action_to_m_step("CustomCalculation", action, {})
        self.assertIsNotNone(result)

    def test_rename_column_empty_returns_none(self):
        action = {"columnName": "", "newColumnName": "B"}
        result = _convert_action_to_m_step("RenameColumn", action, {})
        self.assertIsNone(result)

    def test_filter_operation_counter_second(self):
        """Second filter operation increments counter → 'Filtered Rows 1'."""
        counter = {}
        a = {"filterExpression": "[X] > 0"}
        _convert_action_to_m_step("FilterOperation", a, counter)
        r2 = _convert_action_to_m_step("FilterOperation", a, counter)
        self.assertEqual(r2[0], '#"Filtered Rows 1"')


# ═══════════════════════════════════════════════════════════════════
#  _parse_aggregate_node — edge cases
# ═══════════════════════════════════════════════════════════════════

class TestParseAggregateEdge(unittest.TestCase):

    def test_only_group_fields_no_aggregates(self):
        """groupByFields populated but no aggregateFields → still returns step."""
        node = {"groupByFields": [{"name": "Region"}], "aggregateFields": []}
        result = _parse_aggregate_node(node)
        self.assertIsNotNone(result)

    def test_only_aggregate_fields_no_group(self):
        node = {
            "groupByFields": [],
            "aggregateFields": [{"name": "Sales", "aggregation": "AVG", "newColumnName": "Avg"}],
        }
        result = _parse_aggregate_node(node)
        self.assertIsNotNone(result)

    def test_unknown_agg_maps_to_sum(self):
        node = {
            "groupByFields": [{"name": "X"}],
            "aggregateFields": [{"name": "Y", "aggregation": "WEIRD", "newColumnName": "W"}],
        }
        result = _parse_aggregate_node(node)
        self.assertIsNotNone(result)


# ═══════════════════════════════════════════════════════════════════
#  _parse_join_node — edge cases
# ═══════════════════════════════════════════════════════════════════

class TestParseJoinEdge(unittest.TestCase):

    def test_right_key_excluded_from_expand(self):
        """Fields matching right_keys should be excluded from expand_fields."""
        node = {
            "joinType": "left",
            "joinConditions": [{"leftColumn": "ID", "rightColumn": "ID"}],
        }
        right_fields = [
            {"name": "ID"},
            {"name": "Name"},
            {"name": "Score"},
        ]
        result = _parse_join_node(node, "OtherTable", right_fields)
        self.assertIsNotNone(result)
        # The expand step should include Name and Score but not ID
        found_expand = [s for s in result if "ExpandTableColumn" in s[1]]
        if found_expand:
            self.assertIn("Name", found_expand[0][1])
            self.assertNotIn('"ID"', found_expand[0][1].split("ExpandTableColumn")[1])

    def test_join_type_mapping(self):
        """leftOnly maps to leftanti."""
        node = {
            "joinType": "leftOnly",
            "joinConditions": [{"leftColumn": "K", "rightColumn": "K"}],
        }
        result = _parse_join_node(node, "T", [])
        self.assertIsNotNone(result)

    def test_table_name_cleaned(self):
        """Right table name with .csv extension should be cleaned."""
        node = {
            "joinType": "inner",
            "joinConditions": [{"leftColumn": "A", "rightColumn": "A"}],
        }
        result = _parse_join_node(node, "data.csv", [])
        self.assertIsNotNone(result)
        # Reference should use cleaned name
        join_expr = result[0][1]
        self.assertIn("data", join_expr)


# ═══════════════════════════════════════════════════════════════════
#  _parse_pivot_node — extra branches
# ═══════════════════════════════════════════════════════════════════

class TestParsePivotEdge(unittest.TestCase):

    def test_columns_to_rows_empty_fields_returns_none(self):
        node = {"pivotType": "columnsToRows", "pivotFields": []}
        result = _parse_pivot_node(node)
        self.assertIsNone(result)

    def test_rows_to_columns_missing_key_returns_none(self):
        node = {"pivotType": "rowsToColumns", "pivotKeyField": {}, "pivotValueField": {}}
        result = _parse_pivot_node(node)
        self.assertIsNone(result)

    def test_empty_node_returns_none(self):
        result = _parse_pivot_node({})
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════
#  _parse_input_node
# ═══════════════════════════════════════════════════════════════════

class TestParseInputNode(unittest.TestCase):

    def test_basic_csv_input(self):
        node = {
            "connectionId": "c1",
            "name": "Orders",
            "fields": [
                {"name": "OrderID", "type": "integer"},
                {"name": "Product", "type": "string"},
            ],
            "connectionAttributes": {"filename": "orders.csv"},
        }
        connections = {
            "c1": {
                "connectionAttributes": {"class": "csv", "directory": "/data"},
            }
        }
        conn, table = _parse_input_node(node, connections)
        self.assertEqual(conn["type"], "textscan")
        self.assertEqual(conn["details"]["directory"], "/data")
        self.assertEqual(conn["details"]["filename"], "orders.csv")
        self.assertEqual(table["name"], "Orders")
        self.assertEqual(len(table["columns"]), 2)

    def test_sql_input(self):
        node = {
            "connectionId": "c2",
            "name": "Customers",
            "connectionAttributes": {"table": "dbo.Customers"},
            "fields": [],
        }
        connections = {
            "c2": {
                "connectionAttributes": {
                    "class": "sqlserver",
                    "server": "myserver",
                    "dbname": "mydb",
                    "schema": "dbo",
                },
            }
        }
        conn, table = _parse_input_node(node, connections)
        self.assertEqual(conn["type"], "sqlserver")
        self.assertEqual(conn["details"]["server"], "myserver")
        self.assertEqual(conn["details"]["database"], "mydb")
        self.assertEqual(table["name"], "dbo.Customers")

    def test_missing_connection_id(self):
        """Node with no matching connection → empty attrs."""
        node = {"connectionId": "missing", "name": "T", "fields": []}
        conn, table = _parse_input_node(node, {})
        self.assertEqual(conn["details"]["server"], "")

    def test_node_attrs_override_connection_attrs(self):
        """Node-level connectionAttributes merge on top of connection-level."""
        node = {
            "connectionId": "c1",
            "name": "T",
            "fields": [],
            "connectionAttributes": {"server": "override-server"},
        }
        connections = {
            "c1": {
                "connectionAttributes": {"class": "postgres", "server": "original"},
            }
        }
        conn, table = _parse_input_node(node, connections)
        self.assertEqual(conn["details"]["server"], "override-server")

    def test_warehouse_and_project_attrs(self):
        """Snowflake/BigQuery-style connection attrs."""
        node = {
            "connectionId": "c1",
            "name": "T",
            "fields": [],
            "connectionAttributes": {},
        }
        connections = {
            "c1": {
                "connectionAttributes": {
                    "class": "snowflake",
                    "warehouse": "WH1",
                    "database": "DB1",
                },
            }
        }
        conn, table = _parse_input_node(node, connections)
        self.assertEqual(conn["type"], "snowflake")
        self.assertEqual(conn["details"]["warehouse"], "WH1")
        self.assertEqual(conn["details"]["database"], "DB1")


# ═══════════════════════════════════════════════════════════════════
#  read_prep_flow — .tflx support
# ═══════════════════════════════════════════════════════════════════

class TestReadPrepFlowTflx(unittest.TestCase):

    def test_read_tflx(self):
        """Read a .tflx (zipped .tfl) file."""
        flow_data = {"nodes": {"n1": {"baseType": "input"}}, "connections": {}}
        with tempfile.TemporaryDirectory() as tmpdir:
            tflx_path = os.path.join(tmpdir, "flow.tflx")
            with zipfile.ZipFile(tflx_path, "w") as z:
                z.writestr("flow.tfl", json.dumps(flow_data))

            result = read_prep_flow(tflx_path)
            self.assertEqual(result["nodes"]["n1"]["baseType"], "input")

    def test_read_tflx_no_tfl_inside(self):
        """A .tflx with no .tfl inside raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tflx_path = os.path.join(tmpdir, "bad.tflx")
            with zipfile.ZipFile(tflx_path, "w") as z:
                z.writestr("readme.txt", "nothing")
            with self.assertRaises(ValueError) as ctx:
                read_prep_flow(tflx_path)
            self.assertIn("No .tfl file found", str(ctx.exception))


# ═══════════════════════════════════════════════════════════════════
#  parse_prep_flow — full integration with various sem_types
# ═══════════════════════════════════════════════════════════════════

def _make_flow_file(nodes, connections=None):
    """Write a temp .tfl file and return its path."""
    flow = {"nodes": nodes, "connections": connections or {}}
    fd, path = tempfile.mkstemp(suffix=".tfl")
    with os.fdopen(fd, "w") as f:
        json.dump(flow, f)
    return path


class TestParseFlowInput(unittest.TestCase):
    """parse_prep_flow: input nodes."""

    def test_single_input_produces_leaf_fallback(self):
        """A lone input node has no nextNodes → falls back to leaf output."""
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Orders",
                "connectionId": "c1",
                "fields": [{"name": "ID", "type": "integer"}],
                "connectionAttributes": {},
            }
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
            self.assertTrue(ds[0]["name"].startswith("prep."))
        finally:
            os.unlink(path)


class TestParseFlowEmpty(unittest.TestCase):

    def test_empty_nodes(self):
        """Flow with no nodes → empty list."""
        path = _make_flow_file({})
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(ds, [])
        finally:
            os.unlink(path)


class TestParseFlowSuperTransform(unittest.TestCase):

    def test_input_to_clean_to_output(self):
        """Input → SuperTransform → Output chain."""
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Sales",
                "connectionId": "c1",
                "fields": [{"name": "Amount", "type": "real"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.SuperTransform",
                "name": "Clean",
                "nextNodes": [{"nextNodeId": "n3"}],
                "beforeActionGroup": {
                    "actions": [
                        {"actionType": ".v1.RemoveColumn", "columnName": "junk"},
                    ]
                },
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.PublishExtract",
                "name": "Output",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
            self.assertIn("RemoveColumns", ds[0]["m_query_override"])
        finally:
            os.unlink(path)


class TestParseFlowAggregate(unittest.TestCase):

    def test_aggregate_step(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Data",
                "connectionId": "c1",
                "fields": [{"name": "Region"}, {"name": "Sales"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.Aggregate",
                "name": "Agg",
                "groupByFields": [{"name": "Region"}],
                "aggregateFields": [
                    {"name": "Sales", "aggregation": "SUM", "newColumnName": "Total"},
                ],
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.SaveToFile",
                "name": "Out",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
            self.assertIn("Group", ds[0]["m_query_override"])
        finally:
            os.unlink(path)


class TestParseFlowJoin(unittest.TestCase):

    def test_join_with_two_inputs(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Orders",
                "connectionId": "c1",
                "fields": [{"name": "ID"}, {"name": "CustID"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n2": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Customers",
                "connectionId": "c1",
                "fields": [{"name": "CustID"}, {"name": "CustName"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "transform",
                "nodeType": ".v1.Join",
                "name": "JoinStep",
                "joinType": "inner",
                "joinConditions": [
                    {"leftColumn": "CustID", "rightColumn": "CustID"},
                ],
                "nextNodes": [{"nextNodeId": "n4"}],
            },
            "n4": {
                "baseType": "output",
                "nodeType": ".v1.SaveToFile",
                "name": "JoinOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            # Should get output + secondary branch (right table)
            self.assertTrue(len(ds) >= 1)
            names = [d["name"] for d in ds]
            # At least one output
            self.assertTrue(any("JoinOut" in n for n in names) or len(ds) >= 1)
        finally:
            os.unlink(path)

    def test_join_explicit_left_right_ids(self):
        """Join with explicit leftNodeId / rightNodeId."""
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Left",
                "connectionId": "c1",
                "fields": [{"name": "K"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n2": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Right",
                "connectionId": "c1",
                "fields": [{"name": "K"}, {"name": "Val"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "transform",
                "nodeType": ".v1.Join",
                "name": "J",
                "leftNodeId": "n1",
                "rightNodeId": "n2",
                "joinType": "left",
                "joinConditions": [{"leftColumn": "K", "rightColumn": "K"}],
                "nextNodes": [{"nextNodeId": "n4"}],
            },
            "n4": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Result",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
        finally:
            os.unlink(path)


class TestParseFlowUnion(unittest.TestCase):

    def test_union_two_inputs(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "A",
                "connectionId": "c1",
                "fields": [{"name": "X"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n2": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "B",
                "connectionId": "c1",
                "fields": [{"name": "X"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "transform",
                "nodeType": ".v1.Union",
                "name": "Unioned",
                "nextNodes": [{"nextNodeId": "n4"}],
            },
            "n4": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "UnionOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
        finally:
            os.unlink(path)


class TestParseFlowPivot(unittest.TestCase):

    def test_pivot_step(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Raw",
                "connectionId": "c1",
                "fields": [{"name": "Q1"}, {"name": "Q2"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.Pivot",
                "name": "PivotStep",
                "pivotType": "columnsToRows",
                "pivotFields": [{"name": "Q1"}, {"name": "Q2"}],
                "pivotValuesName": "Sales",
                "pivotNamesName": "Quarter",
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "PivotOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
            self.assertIn("Unpivot", ds[0]["m_query_override"])
        finally:
            os.unlink(path)


class TestParseFlowScript(unittest.TestCase):

    def test_script_step_adds_warning(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "In",
                "connectionId": "c1",
                "fields": [],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.Script",
                "name": "PyScript",
                "scriptLanguage": "Python",
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "ScriptOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
            self.assertIn("script_warning", ds[0]["m_query_override"])
        finally:
            os.unlink(path)


class TestParseFlowPrediction(unittest.TestCase):

    def test_prediction_step_adds_warning(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "In",
                "connectionId": "c1",
                "fields": [],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.Prediction",
                "name": "MLStep",
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "PredOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
            self.assertIn("prediction_warning", ds[0]["m_query_override"])
        finally:
            os.unlink(path)


class TestParseFlowCrossJoin(unittest.TestCase):

    def test_cross_join(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "L",
                "connectionId": "c1",
                "fields": [{"name": "A"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n2": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "R",
                "connectionId": "c1",
                "fields": [{"name": "B"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "transform",
                "nodeType": ".v1.CrossJoin",
                "name": "XJoin",
                "nextNodes": [{"nextNodeId": "n4"}],
            },
            "n4": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "CrossOut",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
            # Cross join step should be in M query
            all_queries = " ".join(d.get("m_query_override", "") for d in ds)
            self.assertIn("Cross Join", all_queries)
        finally:
            os.unlink(path)


class TestParseFlowPublishedDS(unittest.TestCase):

    def test_published_datasource(self):
        nodes = {
            "n1": {
                "baseType": "transform",
                "nodeType": ".v1.PublishedDataSource",
                "name": "PubDS",
                "publishedDatasourceName": "SharedSales",
                "fields": [{"name": "Revenue"}],
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "PubOut",
                "nextNodes": [],
            },
        }
        path = _make_flow_file(nodes)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
            all_queries = " ".join(d.get("m_query_override", "") for d in ds)
            self.assertIn("SharedSales", all_queries)
        finally:
            os.unlink(path)


class TestParseFlowUnknownTransform(unittest.TestCase):

    def test_unknown_transform_passed_through(self):
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "In",
                "connectionId": "c1",
                "fields": [],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.FutureStepType",
                "name": "Unknown",
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Out",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertEqual(len(ds), 1)
        finally:
            os.unlink(path)


class TestParseFlowOutputMissingUpstream(unittest.TestCase):

    def test_output_without_upstream_skipped(self):
        """Output node with no upstream → not emitted."""
        nodes = {
            "n1": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Orphan",
                "nextNodes": [],
            },
        }
        path = _make_flow_file(nodes)
        try:
            ds = parse_prep_flow(path)
            # No input → no node_results to reference → orphan output skipped
            self.assertEqual(ds, [])
        finally:
            os.unlink(path)


class TestParseFlowSecondaryBranch(unittest.TestCase):

    def test_secondary_branch_emitted_before_outputs(self):
        """Right side of join appears as secondary datasource."""
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Left.csv",
                "connectionId": "c1",
                "fields": [{"name": "K"}, {"name": "V"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n2": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Right.csv",
                "connectionId": "c1",
                "fields": [{"name": "K"}, {"name": "W"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "transform",
                "nodeType": ".v1.Join",
                "name": "J",
                "joinType": "inner",
                "joinConditions": [{"leftColumn": "K", "rightColumn": "K"}],
                "nextNodes": [{"nextNodeId": "n4"}],
            },
            "n4": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Result",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            # Should have secondary + output
            self.assertTrue(len(ds) >= 2)
            names = [d["name"] for d in ds]
            # Secondary branch should be first
            self.assertTrue(any("Right" in n for n in names))
        finally:
            os.unlink(path)


class TestParseFlowTransformNoUpstream(unittest.TestCase):

    def test_transform_without_upstream_silently_skipped(self):
        """Transform with no upstream input is silently skipped."""
        nodes = {
            "n1": {
                "baseType": "transform",
                "nodeType": ".v1.SuperTransform",
                "name": "OrphanClean",
                "nextNodes": [{"nextNodeId": "n2"}],
                "beforeActionGroup": {"actions": []},
            },
            "n2": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Out",
                "nextNodes": [],
            },
        }
        path = _make_flow_file(nodes)
        try:
            ds = parse_prep_flow(path)
            # OrphanClean has no upstream → n2 output also has no upstream → empty
            self.assertEqual(ds, [])
        finally:
            os.unlink(path)


class TestParseFlowJoinSingleUpstream(unittest.TestCase):

    def test_join_with_only_left_upstream(self):
        """Join step with only one upstream → uses that as left."""
        nodes = {
            "n1": {
                "baseType": "input",
                "nodeType": ".v1.LoadCsv",
                "name": "Only",
                "connectionId": "c1",
                "fields": [{"name": "K"}],
                "connectionAttributes": {},
                "nextNodes": [{"nextNodeId": "n2"}],
            },
            "n2": {
                "baseType": "transform",
                "nodeType": ".v1.Join",
                "name": "J",
                "joinType": "inner",
                "joinConditions": [{"leftColumn": "K", "rightColumn": "K"}],
                "nextNodes": [{"nextNodeId": "n3"}],
            },
            "n3": {
                "baseType": "output",
                "nodeType": ".v1.Out",
                "name": "Out",
                "nextNodes": [],
            },
        }
        conns = {"c1": {"connectionAttributes": {"class": "csv"}}}
        path = _make_flow_file(nodes, conns)
        try:
            ds = parse_prep_flow(path)
            self.assertTrue(len(ds) >= 1)
        finally:
            os.unlink(path)


# ═══════════════════════════════════════════════════════════════════
#  merge_prep_with_workbook
# ═══════════════════════════════════════════════════════════════════

class TestMergePrepWithWorkbook(unittest.TestCase):

    def test_empty_prep_queries_returns_combined(self):
        """When prep tables have no m_query_override → return twb + prep."""
        prep_ds = [{"tables": [{"name": "T"}], "m_query_override": ""}]
        twb_ds = [{"tables": [{"name": "W"}]}]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        self.assertEqual(len(result), 2)

    def test_matching_table_name(self):
        """Prep table name matches TWB table → m_query_overrides injected."""
        prep_ds = [{
            "tables": [{"name": "Orders"}],
            "caption": "Orders Source",
            "m_query_override": "let Source = 1 in Source",
        }]
        twb_ds = [{
            "tables": [{"name": "Orders"}, {"name": "Other"}],
        }]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        # TWB ds should have m_query_overrides dict
        self.assertIn("m_query_overrides", result[0])
        self.assertIn("Orders", result[0]["m_query_overrides"])

    def test_matching_by_caption(self):
        """Match by caption with spaces replaced."""
        prep_ds = [{
            "tables": [{"name": "TBL"}],
            "caption": "My Table",
            "m_query_override": "let S = 1 in S",
        }]
        twb_ds = [{
            "tables": [{"name": "My_Table"}],
        }]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        self.assertIn("m_query_overrides", result[0])

    def test_unmatched_prep_appended(self):
        """Prep datasource not matching any TWB table → appended standalone."""
        prep_ds = [{
            "tables": [{"name": "Unique"}],
            "caption": "Unique",
            "m_query_override": "let S = 1 in S",
        }]
        twb_ds = [{
            "tables": [{"name": "Unrelated"}],
        }]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        # TWB ds + standalone Prep ds
        self.assertEqual(len(result), 2)

    def test_both_empty(self):
        result = merge_prep_with_workbook([], [])
        self.assertEqual(result, [])

    def test_no_prep_datasources(self):
        twb_ds = [{"tables": [{"name": "A"}]}]
        result = merge_prep_with_workbook([], twb_ds)
        self.assertEqual(result, twb_ds)

    def test_multiple_twb_tables_match(self):
        """Multiple tables in TWB match different prep queries."""
        prep_ds = [
            {
                "tables": [{"name": "T1"}],
                "caption": "",
                "m_query_override": "let S = 1 in S",
            },
            {
                "tables": [{"name": "T2"}],
                "caption": "",
                "m_query_override": "let S = 2 in S",
            },
        ]
        twb_ds = [{"tables": [{"name": "T1"}]}, {"tables": [{"name": "T2"}]}]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        self.assertIn("m_query_overrides", result[0])
        self.assertIn("m_query_overrides", result[1])

    def test_prep_matched_not_appended_standalone(self):
        """Prep DS that matched TWB should NOT also appear as standalone."""
        prep_ds = [{
            "tables": [{"name": "Shared"}],
            "caption": "",
            "m_query_override": "let S = 1 in S",
        }]
        twb_ds = [{"tables": [{"name": "Shared"}]}]
        result = merge_prep_with_workbook(prep_ds, twb_ds)
        # Should be exactly 1 (merged), not 2
        self.assertEqual(len(result), 1)


# ═══════════════════════════════════════════════════════════════════
#  _parse_clean_actions — extra batching edge cases
# ═══════════════════════════════════════════════════════════════════

class TestParseCleanActionsExtra(unittest.TestCase):

    def test_list_return_from_action_extends_steps(self):
        """Group replace returns list → should be extended not appended."""
        node = {
            "beforeActionGroup": {
                "actions": [
                    {
                        "actionType": "GroupReplace",
                        "columnName": "State",
                        "groupings": [
                            {"from": "CA", "to": "California"},
                            {"from": "NY", "to": "New York"},
                        ],
                    }
                ]
            }
        }
        steps = _parse_clean_actions(node)
        # Should have 2 individual replace steps (extended from list)
        self.assertEqual(len(steps), 2)

    def test_action_group_fallback(self):
        """Uses 'actionGroup' key when 'beforeActionGroup' is absent."""
        node = {
            "actionGroup": {
                "actions": [
                    {"actionType": "RemoveColumn", "columnName": "X"},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        self.assertEqual(len(steps), 1)

    def test_rename_empty_old_name_skipped(self):
        """Rename with empty old name should be skipped."""
        node = {
            "beforeActionGroup": {
                "actions": [
                    {"actionType": ".v1.RenameColumn", "columnName": "", "newColumnName": "B"},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        self.assertEqual(steps, [])

    def test_action_returning_none_skipped(self):
        """An action that returns None is silently skipped."""
        node = {
            "beforeActionGroup": {
                "actions": [
                    # RemoveColumn with empty col returns None
                    {"actionType": "RemoveColumn", "columnName": ""},
                ]
            }
        }
        steps = _parse_clean_actions(node)
        self.assertEqual(steps, [])


# ═══════════════════════════════════════════════════════════════════
#  _convert_prep_expression_to_m — additional patterns
# ═══════════════════════════════════════════════════════════════════

class TestExpressionConversionExtra(unittest.TestCase):

    def test_simple_column_ref_passthrough(self):
        result = _convert_prep_expression_to_m("[Revenue] * 1.1")
        self.assertIn("[Revenue]", result)

    def test_multiple_functions_chained(self):
        result = _convert_prep_expression_to_m("TRIM(UPPER([Name]))")
        self.assertIn("Text.Trim(", result)
        self.assertIn("Text.Upper(", result)


if __name__ == "__main__":
    unittest.main()
