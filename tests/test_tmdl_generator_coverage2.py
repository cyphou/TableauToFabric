"""Extra coverage tests for fabric_import/tmdl_generator.py — Phase 2.

Targets the remaining ~306 missing lines in functions:
  _add_date_table, _create_field_parameters, _create_calculation_groups,
  _create_rls_roles (calculated_security), _auto_date_hierarchies,
  _infer_cross_table_relationships, _detect_many_to_many,
  _fix_related_for_many_to_many, _replace_related_with_lookupvalue,
  _deactivate_ambiguous_paths, _process_sets_groups_bins (combined+RELATED)
"""

import re
import unittest

from fabric_import.tmdl_generator import (
    _add_date_table,
    _auto_date_hierarchies,
    _create_calculation_groups,
    _create_field_parameters,
    _create_rls_roles,
    _deactivate_ambiguous_paths,
    _detect_many_to_many,
    _fix_related_for_many_to_many,
    _infer_cross_table_relationships,
    _process_sets_groups_bins,
    _replace_related_with_lookupvalue,
)


# ═══════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════

def _base_model(tables=None, relationships=None, roles=None):
    """Create a minimal model dict."""
    return {
        "model": {
            "tables": tables or [],
            "relationships": relationships or [],
            "roles": roles or [],
        }
    }


def _fact_table(name="Orders", date_col=True, sum_measure=True, extra_cols=None, extra_measures=None):
    """Build a fact table dict with common defaults."""
    cols = []
    if date_col:
        cols.append({"name": "OrderDate", "dataType": "DateTime", "dataCategory": "DateTime"})
    cols.append({"name": "Amount", "dataType": "Double"})
    if extra_cols:
        cols.extend(extra_cols)
    measures = []
    if sum_measure:
        measures.append({"name": "Total Sales", "expression": "SUM('Orders'[Amount])"})
    if extra_measures:
        measures.extend(extra_measures)
    return {"name": name, "columns": cols, "measures": measures}


# ═══════════════════════════════════════════════════════════════════
#  _add_date_table
# ═══════════════════════════════════════════════════════════════════

class TestAddDateTable(unittest.TestCase):

    def test_default_years(self):
        model = _base_model([_fact_table()])
        _add_date_table(model)
        cal = next(t for t in model["model"]["tables"] if t["name"] == "Calendar")
        self.assertIn("2020", cal["partitions"][0]["source"]["expression"])
        self.assertIn("2030", cal["partitions"][0]["source"]["expression"])

    def test_custom_years(self):
        model = _base_model([_fact_table()])
        _add_date_table(model, calendar_start=2015, calendar_end=2025)
        cal = next(t for t in model["model"]["tables"] if t["name"] == "Calendar")
        self.assertIn("2015", cal["partitions"][0]["source"]["expression"])
        self.assertIn("2025", cal["partitions"][0]["source"]["expression"])

    def test_eight_columns(self):
        model = _base_model([_fact_table()])
        _add_date_table(model)
        cal = next(t for t in model["model"]["tables"] if t["name"] == "Calendar")
        col_names = {c["name"] for c in cal["columns"]}
        for expected in ("Date", "Year", "Quarter", "Month", "MonthName", "Day", "DayOfWeek", "DayName"):
            self.assertIn(expected, col_names)

    def test_time_intelligence_with_sum_measure(self):
        model = _base_model([_fact_table(sum_measure=True)])
        _add_date_table(model)
        cal = next(t for t in model["model"]["tables"] if t["name"] == "Calendar")
        ti_names = {m["name"] for m in cal["measures"]}
        self.assertIn("Year To Date", ti_names)
        self.assertIn("Previous Year", ti_names)
        self.assertIn("Year Over Year %", ti_names)

    def test_no_time_intelligence_without_sum(self):
        model = _base_model([_fact_table(sum_measure=False)])
        _add_date_table(model)
        cal = next(t for t in model["model"]["tables"] if t["name"] == "Calendar")
        self.assertEqual(len(cal["measures"]), 0)

    def test_relationship_created_for_date_column(self):
        model = _base_model([_fact_table()])
        _add_date_table(model)
        rels = model["model"]["relationships"]
        self.assertTrue(any(r["toTable"] == "Calendar" and r["fromColumn"] == "OrderDate" for r in rels))

    def test_skip_calculated_col_for_relationship(self):
        fact = _fact_table()
        fact["columns"].append({"name": "CalcDate", "dataType": "DateTime", "isCalculated": True})
        model = _base_model([fact])
        _add_date_table(model)
        rels = model["model"]["relationships"]
        # Should link OrderDate but NOT CalcDate
        self.assertTrue(any(r["fromColumn"] == "OrderDate" for r in rels))
        self.assertFalse(any(r["fromColumn"] == "CalcDate" for r in rels))

    def test_no_relationship_no_date_cols(self):
        model = _base_model([_fact_table(date_col=False)])
        _add_date_table(model)
        rels = model["model"]["relationships"]
        cal_rels = [r for r in rels if r["toTable"] == "Calendar"]
        self.assertEqual(len(cal_rels), 0)


# ═══════════════════════════════════════════════════════════════════
#  _auto_date_hierarchies
# ═══════════════════════════════════════════════════════════════════

class TestAutoDateHierarchies(unittest.TestCase):

    def test_creates_hierarchy_for_datetime(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [
                {"name": "OrderDate", "dataType": "dateTime"},
                {"name": "Amount", "dataType": "double"},
            ]
        }])
        _auto_date_hierarchies(model)
        table = model["model"]["tables"][0]
        hier_names = [h["name"] for h in table.get("hierarchies", [])]
        self.assertIn("OrderDate Hierarchy", hier_names)

    def test_creates_four_calc_columns(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [{"name": "OrderDate", "dataType": "dateTime"}]
        }])
        _auto_date_hierarchies(model)
        col_names = {c["name"] for c in model["model"]["tables"][0]["columns"]}
        for part in ("Year", "Quarter", "Month", "Day"):
            self.assertIn(f"OrderDate {part}", col_names)

    def test_skips_non_date_column(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [{"name": "Region", "dataType": "string"}]
        }])
        _auto_date_hierarchies(model)
        self.assertFalse(model["model"]["tables"][0].get("hierarchies"))

    def test_skips_column_already_in_hierarchy(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [{"name": "OrderDate", "dataType": "dateTime"}],
            "hierarchies": [{
                "name": "Custom Hier",
                "levels": [{"column": "OrderDate", "ordinal": 0}]
            }]
        }])
        _auto_date_hierarchies(model)
        hiers = model["model"]["tables"][0]["hierarchies"]
        # Should not create duplicate
        self.assertEqual(len(hiers), 1)

    def test_idempotent(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [{"name": "OrderDate", "dataType": "dateTime"}]
        }])
        _auto_date_hierarchies(model)
        _auto_date_hierarchies(model)
        hiers = model["model"]["tables"][0]["hierarchies"]
        matching = [h for h in hiers if h["name"] == "OrderDate Hierarchy"]
        self.assertEqual(len(matching), 1)

    def test_date_type_also_handled(self):
        model = _base_model([{
            "name": "Events",
            "columns": [{"name": "EventDate", "dataType": "date"}]
        }])
        _auto_date_hierarchies(model)
        hiers = model["model"]["tables"][0].get("hierarchies", [])
        self.assertEqual(len(hiers), 1)

    def test_existing_calc_col_reused(self):
        model = _base_model([{
            "name": "Orders",
            "columns": [
                {"name": "OrderDate", "dataType": "dateTime"},
                {"name": "OrderDate Year", "dataType": "int64"},
            ]
        }])
        _auto_date_hierarchies(model)
        col_names = [c["name"] for c in model["model"]["tables"][0]["columns"]]
        # Should not duplicate
        year_count = col_names.count("OrderDate Year")
        self.assertEqual(year_count, 1)


# ═══════════════════════════════════════════════════════════════════
#  _create_calculation_groups
# ═══════════════════════════════════════════════════════════════════

class TestCreateCalculationGroups(unittest.TestCase):

    def _model_with_measures(self):
        return _base_model([{
            "name": "Sales",
            "columns": [{"name": "Amount", "dataType": "Double"}],
            "measures": [
                {"name": "Total Sales", "expression": "SUM('Sales'[Amount])"},
                {"name": "Avg Sales", "expression": "AVERAGE('Sales'[Amount])"},
            ]
        }])

    def _params_matching(self, values):
        return [{
            "caption": "Metric Selector",
            "datatype": "string",
            "domain_type": "list",
            "allowable_values": [{"value": v, "type": "value"} for v in values],
        }]

    def test_creates_calc_group(self):
        model = self._model_with_measures()
        _create_calculation_groups(model, self._params_matching(["Total Sales", "Avg Sales"]), "Sales")
        cg = next((t for t in model["model"]["tables"] if t["name"] == "Metric Selector CalcGroup"), None)
        self.assertIsNotNone(cg)
        items = cg["calculationGroup"]["calculationItems"]
        self.assertEqual(len(items), 2)

    def test_skip_non_string(self):
        model = self._model_with_measures()
        params = [{"caption": "X", "datatype": "integer", "domain_type": "list",
                    "allowable_values": [{"value": "Total Sales"}, {"value": "Avg Sales"}]}]
        _create_calculation_groups(model, params, "Sales")
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_skip_non_list(self):
        model = self._model_with_measures()
        params = [{"caption": "X", "datatype": "string", "domain_type": "range",
                    "allowable_values": [{"value": "Total Sales"}, {"value": "Avg Sales"}]}]
        _create_calculation_groups(model, params, "Sales")
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_skip_less_than_two_matching(self):
        model = self._model_with_measures()
        params = self._params_matching(["Total Sales", "Unknown"])
        _create_calculation_groups(model, params, "Sales")
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_empty_params(self):
        model = self._model_with_measures()
        _create_calculation_groups(model, [], "Sales")
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_none_params(self):
        model = self._model_with_measures()
        _create_calculation_groups(model, None, "Sales")
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_duplicate_table_name_skipped(self):
        model = self._model_with_measures()
        model["model"]["tables"].append({"name": "Metric Selector CalcGroup"})
        _create_calculation_groups(model, self._params_matching(["Total Sales", "Avg Sales"]), "Sales")
        # Should not add a second
        cgs = [t for t in model["model"]["tables"] if t["name"] == "Metric Selector CalcGroup"]
        self.assertEqual(len(cgs), 1)


# ═══════════════════════════════════════════════════════════════════
#  _create_field_parameters
# ═══════════════════════════════════════════════════════════════════

class TestCreateFieldParameters(unittest.TestCase):

    def _model_with_cols(self):
        return _base_model([{
            "name": "Sales",
            "columns": [
                {"name": "Region", "dataType": "String"},
                {"name": "Category", "dataType": "String"},
                {"name": "Amount", "dataType": "Double"},
            ],
            "measures": []
        }])

    def _params(self, values):
        return [{
            "caption": "Dim Selector",
            "datatype": "string",
            "domain_type": "list",
            "allowable_values": [{"value": v, "type": "value"} for v in values],
        }]

    def test_creates_field_param_table(self):
        model = self._model_with_cols()
        col_map = {"Region": "Sales", "Category": "Sales"}
        _create_field_parameters(model, self._params(["Region", "Category"]), "Sales", col_map)
        fp = next((t for t in model["model"]["tables"] if t["name"] == "Dim Selector FieldParam"), None)
        self.assertIsNotNone(fp)
        self.assertEqual(len(fp["columns"]), 3)  # caption, _Order, _Fields
        # Partition has NAMEOF
        expr = fp["partitions"][0]["source"]["expression"]
        self.assertIn("NAMEOF(", expr)

    def test_skip_when_all_measures(self):
        model = _base_model([{
            "name": "Sales",
            "columns": [{"name": "Total", "dataType": "Double"}],
            "measures": [
                {"name": "Total", "expression": "SUM(...)"},
                {"name": "Avg", "expression": "AVG(...)"},
            ]
        }])
        params = self._params(["Total", "Avg"])
        _create_field_parameters(model, params, "Sales", {"Total": "Sales", "Avg": "Sales"})
        # All matching values are measures → skip
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_skip_fewer_than_two(self):
        model = self._model_with_cols()
        params = self._params(["Region", "Unknown"])
        _create_field_parameters(model, params, "Sales", {})
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_empty_params(self):
        model = self._model_with_cols()
        _create_field_parameters(model, [], "Sales", {})
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_none_params(self):
        model = self._model_with_cols()
        _create_field_parameters(model, None, "Sales", {})
        self.assertEqual(len(model["model"]["tables"]), 1)

    def test_duplicate_table_skipped(self):
        model = self._model_with_cols()
        model["model"]["tables"].append({"name": "Dim Selector FieldParam"})
        col_map = {"Region": "Sales", "Category": "Sales"}
        _create_field_parameters(model, self._params(["Region", "Category"]), "Sales", col_map)
        fps = [t for t in model["model"]["tables"] if t["name"] == "Dim Selector FieldParam"]
        self.assertEqual(len(fps), 1)


# ═══════════════════════════════════════════════════════════════════
#  _create_rls_roles — calculated_security branch
# ═══════════════════════════════════════════════════════════════════

class TestCreateRlsRolesCalcSecurity(unittest.TestCase):

    def test_ismemberof_groups(self):
        model = _base_model([{"name": "Sales", "columns": [], "measures": []}])
        uf = [{
            "type": "calculated_security",
            "name": "GroupSecurity",
            "formula": 'ISMEMBEROF("Admins")',
            "functions_used": ["ISMEMBEROF"],
            "ismemberof_groups": ["Admins", "Managers"]
        }]
        _create_rls_roles(model, uf, "Sales", {})
        roles = model["model"]["roles"]
        self.assertEqual(len(roles), 2)
        self.assertTrue(all("TRUE()" in r["tablePermissions"][0]["filterExpression"] for r in roles))

    def test_username_function(self):
        model = _base_model([{"name": "Sales", "columns": [], "measures": []}])
        uf = [{
            "type": "calculated_security",
            "name": "UserSec",
            "formula": '[Email] = USERNAME()',
            "functions_used": ["USERNAME"],
            "ismemberof_groups": []
        }]
        _create_rls_roles(model, uf, "Sales", {})
        roles = model["model"]["roles"]
        self.assertEqual(len(roles), 1)
        self.assertIn("_migration_note", roles[0])

    def test_fullname_function(self):
        model = _base_model([{"name": "Sales", "columns": [], "measures": []}])
        uf = [{
            "type": "calculated_security",
            "name": "FullSec",
            "formula": '[Name] = FULLNAME()',
            "functions_used": ["FULLNAME"],
            "ismemberof_groups": []
        }]
        _create_rls_roles(model, uf, "Sales", {})
        roles = model["model"]["roles"]
        self.assertEqual(len(roles), 1)

    def test_cross_table_ref_perm_table(self):
        model = _base_model([
            {"name": "Sales", "columns": [], "measures": []},
            {"name": "Users", "columns": [], "measures": []}
        ])
        uf = [{
            "type": "calculated_security",
            "name": "CrossSec",
            "formula": "'Users'[Email] = USERNAME()",
            "functions_used": ["USERNAME"],
            "ismemberof_groups": []
        }]
        _create_rls_roles(model, uf, "Sales", {})
        roles = model["model"]["roles"]
        self.assertEqual(len(roles), 1)
        # perm_table should be Users since formula references 'Users'
        perm_tbl = roles[0]["tablePermissions"][0]["name"]
        self.assertEqual(perm_tbl, "Users")

    def test_auto_detect_main_table(self):
        model = _base_model([{"name": "Fallback", "columns": [], "measures": []}])
        uf = [{
            "type": "calculated_security",
            "name": "Auto",
            "formula": "TRUE()",
            "functions_used": ["USERNAME"],
            "ismemberof_groups": []
        }]
        _create_rls_roles(model, uf, None, {})
        roles = model["model"]["roles"]
        self.assertEqual(roles[0]["tablePermissions"][0]["name"], "Fallback")

    def test_empty_filters(self):
        model = _base_model([])
        _create_rls_roles(model, [], "Sales", {})
        roles = model["model"].get("roles", [])
        self.assertEqual(len(roles), 0)

    def test_none_filters(self):
        model = _base_model([])
        _create_rls_roles(model, None, "Sales", {})
        roles = model["model"].get("roles", [])
        self.assertEqual(len(roles), 0)


# ═══════════════════════════════════════════════════════════════════
#  _infer_cross_table_relationships
# ═══════════════════════════════════════════════════════════════════

class TestInferCrossTableRelationships(unittest.TestCase):

    def test_exact_match_creates_relationship(self):
        model = _base_model([
            {
                "name": "Orders",
                "columns": [{"name": "ProductID", "dataType": "Int64"}],
                "measures": [{"name": "Count", "expression": "CALCULATE('Products'[ProductID])"}]
            },
            {
                "name": "Products",
                "columns": [{"name": "ProductID", "dataType": "Int64"}],
                "measures": []
            }
        ])
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertTrue(len(rels) >= 1)
        self.assertTrue(any("ProductID" in r.get("fromColumn", "") or "ProductID" in r.get("toColumn", "") for r in rels))

    def test_substring_match(self):
        model = _base_model([
            {
                "name": "Orders",
                "columns": [{"name": "ProdID", "dataType": "Int64"}],
                "measures": [{"name": "X", "expression": "CALCULATE('Products'[SomeCol])"}]
            },
            {
                "name": "Products",
                "columns": [{"name": "ProductID", "dataType": "Int64"}],
                "measures": []
            }
        ])
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertTrue(len(rels) >= 1)

    def test_prefix_match(self):
        model = _base_model([
            {
                "name": "Orders",
                "columns": [{"name": "Product", "dataType": "String"}],
                "measures": [{"name": "X", "expression": "CALCULATE('Products'[SomeCol])"}]
            },
            {
                "name": "Products",
                "columns": [{"name": "ProductKey", "dataType": "String"}],
                "measures": []
            }
        ])
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertTrue(len(rels) >= 1)

    def test_no_match_low_score(self):
        model = _base_model([
            {
                "name": "Orders",
                "columns": [{"name": "AB", "dataType": "String"}],
                "measures": [{"name": "X", "expression": "CALCULATE('Products'[AB])"}]
            },
            {
                "name": "Products",
                "columns": [{"name": "XY", "dataType": "String"}],
                "measures": []
            }
        ])
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertEqual(len(rels), 0)

    def test_already_connected_skipped(self):
        model = _base_model(
            tables=[
                {
                    "name": "Orders",
                    "columns": [{"name": "ProductID", "dataType": "Int64"}],
                    "measures": [{"name": "X", "expression": "CALCULATE('Products'[ProductID])"}]
                },
                {
                    "name": "Products",
                    "columns": [{"name": "ProductID", "dataType": "Int64"}],
                    "measures": []
                }
            ],
            relationships=[{
                "name": "existing",
                "fromTable": "Orders", "fromColumn": "ProductID",
                "toTable": "Products", "toColumn": "ProductID",
            }]
        )
        _infer_cross_table_relationships(model)
        # Should not add a duplicate
        rels = model["model"]["relationships"]
        self.assertEqual(len(rels), 1)

    def test_cross_ref_from_calc_column(self):
        model = _base_model([
            {
                "name": "Orders",
                "columns": [
                    {"name": "ProdID", "dataType": "Int64", "isCalculated": True,
                     "expression": "RELATED('Products'[ProdID])"},
                ],
                "measures": []
            },
            {
                "name": "Products",
                "columns": [{"name": "ProdID", "dataType": "Int64"}],
                "measures": []
            }
        ])
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertTrue(len(rels) >= 1)

    def test_cross_ref_from_rls_role(self):
        model = _base_model(
            tables=[
                {
                    "name": "Sales",
                    "columns": [{"name": "UserID", "dataType": "String"}],
                    "measures": []
                },
                {
                    "name": "Users",
                    "columns": [{"name": "UserID", "dataType": "String"}],
                    "measures": []
                }
            ],
            roles=[{
                "name": "SecurityRole",
                "tablePermissions": [{"name": "Sales", "filterExpression": "'Users'[Email] = USERPRINCIPALNAME()"}]
            }]
        )
        _infer_cross_table_relationships(model)
        rels = model["model"]["relationships"]
        self.assertTrue(len(rels) >= 1)


# ═══════════════════════════════════════════════════════════════════
#  _detect_many_to_many
# ═══════════════════════════════════════════════════════════════════

class TestDetectManyToMany(unittest.TestCase):

    def test_full_join_becomes_many_to_many(self):
        model = _base_model(relationships=[{
            "fromTable": "A", "fromColumn": "id",
            "toTable": "B", "toColumn": "id", "joinType": "full"
        }])
        _detect_many_to_many(model, [])
        rel = model["model"]["relationships"][0]
        self.assertEqual(rel["fromCardinality"], "many")
        self.assertEqual(rel["toCardinality"], "many")
        self.assertEqual(rel["crossFilteringBehavior"], "bothDirections")

    def test_left_join_becomes_many_to_one(self):
        model = _base_model(relationships=[{
            "fromTable": "A", "fromColumn": "id",
            "toTable": "B", "toColumn": "id", "joinType": "left"
        }])
        _detect_many_to_many(model, [])
        rel = model["model"]["relationships"][0]
        self.assertEqual(rel["fromCardinality"], "many")
        self.assertEqual(rel["toCardinality"], "one")
        self.assertEqual(rel["crossFilteringBehavior"], "oneDirection")

    def test_default_join_type(self):
        model = _base_model(relationships=[{
            "fromTable": "A", "fromColumn": "id",
            "toTable": "B", "toColumn": "id"
        }])
        _detect_many_to_many(model, [])
        rel = model["model"]["relationships"][0]
        self.assertEqual(rel["toCardinality"], "one")

    def test_multiple_relationships(self):
        model = _base_model(relationships=[
            {"fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id", "joinType": "full"},
            {"fromTable": "C", "fromColumn": "id", "toTable": "D", "toColumn": "id", "joinType": "left"},
        ])
        _detect_many_to_many(model, [])
        self.assertEqual(model["model"]["relationships"][0]["toCardinality"], "many")
        self.assertEqual(model["model"]["relationships"][1]["toCardinality"], "one")


# ═══════════════════════════════════════════════════════════════════
#  _fix_related_for_many_to_many
# ═══════════════════════════════════════════════════════════════════

class TestFixRelatedForManyToMany(unittest.TestCase):

    def test_replaces_related_in_column(self):
        model = _base_model(
            tables=[{
                "name": "Orders",
                "columns": [{"name": "CatName", "expression": "RELATED('Products'[Name])", "dataType": "String"}],
                "measures": []
            }],
            relationships=[{
                "fromTable": "Orders", "fromColumn": "ProductID",
                "toTable": "Products", "toColumn": "ProductID",
                "fromCardinality": "many", "toCardinality": "many"
            }]
        )
        _fix_related_for_many_to_many(model)
        expr = model["model"]["tables"][0]["columns"][0]["expression"]
        self.assertIn("LOOKUPVALUE(", expr)
        self.assertNotIn("RELATED(", expr)

    def test_replaces_related_in_measure(self):
        model = _base_model(
            tables=[{
                "name": "Orders",
                "columns": [],
                "measures": [{"name": "ProdName", "expression": "RELATED('Products'[Name])"}]
            }],
            relationships=[{
                "fromTable": "Orders", "fromColumn": "ProductID",
                "toTable": "Products", "toColumn": "ProductID",
                "fromCardinality": "many", "toCardinality": "many"
            }]
        )
        _fix_related_for_many_to_many(model)
        expr = model["model"]["tables"][0]["measures"][0]["expression"]
        self.assertIn("LOOKUPVALUE(", expr)

    def test_no_m2m_no_change(self):
        model = _base_model(
            tables=[{
                "name": "Orders",
                "columns": [{"name": "X", "expression": "RELATED('Products'[Name])", "dataType": "String"}],
                "measures": []
            }],
            relationships=[{
                "fromTable": "Orders", "fromColumn": "ProductID",
                "toTable": "Products", "toColumn": "ProductID",
                "fromCardinality": "many", "toCardinality": "one"
            }]
        )
        _fix_related_for_many_to_many(model)
        expr = model["model"]["tables"][0]["columns"][0]["expression"]
        self.assertIn("RELATED(", expr)


# ═══════════════════════════════════════════════════════════════════
#  _replace_related_with_lookupvalue
# ═══════════════════════════════════════════════════════════════════

class TestReplaceRelatedWithLookupvalue(unittest.TestCase):

    def test_basic_replacement(self):
        m2m = {"Products": ("ProductID", "Orders", "ProductID")}
        result = _replace_related_with_lookupvalue("RELATED('Products'[Name])", m2m)
        self.assertIn("LOOKUPVALUE(", result)
        self.assertIn("ProductID", result)

    def test_table_not_in_m2m_unchanged(self):
        m2m = {"Products": ("ProductID", "Orders", "ProductID")}
        result = _replace_related_with_lookupvalue("RELATED('Customers'[Name])", m2m)
        self.assertIn("RELATED(", result)

    def test_multiple_related_calls(self):
        m2m = {"Products": ("ProductID", "Orders", "ProductID")}
        expr = "RELATED('Products'[Name]) & RELATED('Products'[Category])"
        result = _replace_related_with_lookupvalue(expr, m2m)
        self.assertEqual(result.count("LOOKUPVALUE("), 2)

    def test_no_related_unchanged(self):
        m2m = {"Products": ("ProductID", "Orders", "ProductID")}
        result = _replace_related_with_lookupvalue("SUM('Sales'[Amount])", m2m)
        self.assertEqual(result, "SUM('Sales'[Amount])")


# ═══════════════════════════════════════════════════════════════════
#  _deactivate_ambiguous_paths
# ═══════════════════════════════════════════════════════════════════

class TestDeactivateAmbiguousPaths(unittest.TestCase):

    def test_cycle_detected_one_deactivated(self):
        model = _base_model(relationships=[
            {"name": "R1", "fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id"},
            {"name": "R2", "fromTable": "B", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
            {"name": "R3", "fromTable": "A", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
        ])
        _deactivate_ambiguous_paths(model)
        deactivated = [r for r in model["model"]["relationships"] if r.get("isActive") is False]
        self.assertEqual(len(deactivated), 1)

    def test_no_cycle_all_active(self):
        model = _base_model(relationships=[
            {"name": "R1", "fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id"},
            {"name": "R2", "fromTable": "B", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
        ])
        _deactivate_ambiguous_paths(model)
        deactivated = [r for r in model["model"]["relationships"] if r.get("isActive") is False]
        self.assertEqual(len(deactivated), 0)

    def test_empty_relationships(self):
        model = _base_model(relationships=[])
        _deactivate_ambiguous_paths(model)
        self.assertEqual(len(model["model"]["relationships"]), 0)

    def test_calendar_priority_deactivated_first(self):
        model = _base_model(relationships=[
            {"name": "Calendar_Orders_Date", "fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id"},
            {"name": "regular", "fromTable": "B", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
            {"name": "also_regular", "fromTable": "A", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
        ])
        _deactivate_ambiguous_paths(model)
        deactivated = [r for r in model["model"]["relationships"] if r.get("isActive") is False]
        self.assertEqual(len(deactivated), 1)
        self.assertTrue(deactivated[0]["name"].startswith("Calendar_"))

    def test_already_inactive_skipped(self):
        model = _base_model(relationships=[
            {"name": "R1", "fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id", "isActive": False},
            {"name": "R2", "fromTable": "B", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
            {"name": "R3", "fromTable": "A", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
        ])
        _deactivate_ambiguous_paths(model)
        # R1 was already inactive, R2+R3 form no cycle (A->B via R1 skipped, so B->C and A->C only)
        deactivated = [r for r in model["model"]["relationships"] if r.get("isActive") is False]
        self.assertEqual(len(deactivated), 1)  # only R1 was already inactive

    def test_inferred_priority(self):
        model = _base_model(relationships=[
            {"name": "inferred_A_B", "fromTable": "A", "fromColumn": "id", "toTable": "B", "toColumn": "id"},
            {"name": "regular", "fromTable": "B", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
            {"name": "also_regular", "fromTable": "A", "fromColumn": "id", "toTable": "C", "toColumn": "id"},
        ])
        _deactivate_ambiguous_paths(model)
        deactivated = [r for r in model["model"]["relationships"] if r.get("isActive") is False]
        self.assertEqual(len(deactivated), 1)
        self.assertTrue(deactivated[0]["name"].startswith("inferred_"))


# ═══════════════════════════════════════════════════════════════════
#  _process_sets_groups_bins — combined group with RELATED
# ═══════════════════════════════════════════════════════════════════

class TestProcessSetsGroupsBinsCombinedRelated(unittest.TestCase):

    def test_combined_group_cross_table_related(self):
        model = _base_model([
            {"name": "Orders", "columns": [{"name": "Status", "dataType": "String"}], "measures": []},
            {"name": "Products", "columns": [{"name": "Category", "dataType": "String"}], "measures": []}
        ])
        extra = {
            "groups": [{
                "name": "CrossGroup",
                "group_type": "combined",
                "source_fields": ["Status", "Category"],
            }],
            "sets": [], "bins": [], "_datasources": []
        }
        col_map = {"Status": "Orders", "Category": "Products"}
        _process_sets_groups_bins(model, extra, "Orders", col_map)
        cols = model["model"]["tables"][0]["columns"]
        cg = next((c for c in cols if c["name"] == "CrossGroup"), None)
        self.assertIsNotNone(cg)
        self.assertIn("RELATED(", cg["expression"])

    def test_combined_group_same_table_no_related(self):
        model = _base_model([
            {"name": "Orders", "columns": [
                {"name": "Status", "dataType": "String"},
                {"name": "Region", "dataType": "String"},
            ], "measures": []}
        ])
        extra = {
            "groups": [{
                "name": "Combo",
                "group_type": "combined",
                "source_fields": ["Status", "Region"],
            }],
            "sets": [], "bins": [], "_datasources": []
        }
        col_map = {"Status": "Orders", "Region": "Orders"}
        _process_sets_groups_bins(model, extra, "Orders", col_map)
        cols = model["model"]["tables"][0]["columns"]
        combo = next((c for c in cols if c["name"] == "Combo"), None)
        self.assertIsNotNone(combo)
        self.assertNotIn("RELATED(", combo["expression"])
        self.assertIn("&", combo["expression"])

    def test_combined_group_calc_map_lookup(self):
        """Source fields resolved via _datasources calculation map."""
        model = _base_model([
            {"name": "Sales", "columns": [{"name": "Profit", "dataType": "Double"}], "measures": []}
        ])
        extra = {
            "groups": [{
                "name": "CalcCombo",
                "group_type": "combined",
                "source_fields": ["calc_raw"],
            }],
            "sets": [], "bins": [],
            "_datasources": [{
                "calculations": [{"name": "[calc_raw]", "caption": "Profit"}]
            }]
        }
        col_map = {"Profit": "Sales"}
        _process_sets_groups_bins(model, extra, "Sales", col_map)
        cols = model["model"]["tables"][0]["columns"]
        cc = next((c for c in cols if c["name"] == "CalcCombo"), None)
        self.assertIsNotNone(cc)


if __name__ == "__main__":
    unittest.main()
