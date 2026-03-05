"""Tests for the pre-migration assessment module."""

import json
import os
import pytest

from fabric_import.assessment import (
    PASS, INFO, WARN, FAIL,
    CheckItem, CategoryResult, AssessmentReport,
    run_assessment,
    print_assessment_report,
    save_assessment_report,
    _check_datasources,
    _check_calculations,
    _check_visuals,
    _check_filters,
    _check_data_model,
    _check_interactivity,
    _check_extract_and_packaging,
    _check_migration_scope,
)


# ═══════════════════════════════════════════════════════════════════
#  Fixtures
# ═══════════════════════════════════════════════════════════════════

@pytest.fixture
def empty_extracted():
    """Minimal extracted data — empty workbook."""
    return {
        "datasources": [],
        "worksheets": [],
        "dashboards": [],
        "calculations": [],
        "parameters": [],
        "filters": [],
        "stories": [],
        "actions": [],
        "sets": [],
        "groups": [],
        "bins": [],
        "hierarchies": [],
        "sort_orders": [],
        "aliases": {},
        "custom_sql": [],
        "user_filters": [],
        "data_blending": [],
        "published_datasources": [],
        "hyper_files": [],
        "custom_shapes": [],
        "embedded_fonts": [],
        "custom_geocoding": [],
    }


@pytest.fixture
def simple_extracted(empty_extracted):
    """Simple workbook — fully supported, no issues."""
    d = dict(empty_extracted)
    d["datasources"] = [
        {
            "name": "Sales",
            "connection": {"type": "Excel"},
            "tables": [
                {"name": "Orders", "columns": [{"name": "id"}, {"name": "amount"}]},
            ],
            "columns": [{"name": "Region"}],
            "relationships": [],
        }
    ]
    d["worksheets"] = [
        {"name": "Sheet 1", "chart_type": "bar"},
        {"name": "Sheet 2", "chart_type": "line"},
    ]
    d["dashboards"] = [{"name": "Dashboard 1"}]
    d["calculations"] = [
        {"name": "Profit", "caption": "Profit", "formula": "[Sales] - [Cost]"},
    ]
    return d


@pytest.fixture
def complex_extracted(empty_extracted):
    """Complex workbook — many flags triggered."""
    d = dict(empty_extracted)
    d["datasources"] = [
        {
            "name": "BigData",
            "connection": {"type": "BigQuery"},
            "tables": [{"name": f"T{i}", "columns": [{"name": f"c{j}"} for j in range(30)]} for i in range(8)],
            "columns": [{"name": "Region"}],
            "relationships": [{"type": "inner"}] * 5,
        },
        {
            "name": "SAP",
            "connection": {"type": "Splunk"},
            "tables": [{"name": "sap_table", "columns": [{"name": "a"}]}],
            "columns": [],
            "relationships": [],
        },
    ]
    d["worksheets"] = [
        {"name": "Map", "chart_type": "map"},
        {"name": "Custom", "chart_type": "myCustomViz"},  # unmapped
        {"name": "Scatter", "chart_type": "scatter",
         "dual_axis": {"has_dual_axis": True}},
        {"name": "Tooltips", "chart_type": "bar",
         "tooltips": [{"is_viz_tooltip": True}]},
    ]
    d["dashboards"] = [
        {"name": "Dash 1", "device_layouts": [{"type": "phone"}]},
        {"name": "Dash 2"},
    ]
    d["calculations"] = [
        {"name": "c1", "caption": "RegexCalc", "formula": "REGEXP_MATCH([Field], 'a')"},
        {"name": "c2", "caption": "ScriptCalc", "formula": "SCRIPT_REAL('return x', [Sales])"},
        {"name": "c3", "caption": "LODCalc", "formula": "{ FIXED [Region] : SUM([Sales]) }"},
        {"name": "c4", "caption": "WindowCalc", "formula": "RUNNING_SUM(SUM([Sales]))"},
        {"name": "c5", "caption": "SimpleCalc", "formula": "[A] + [B]"},
        {"name": "c6", "caption": "LookupCalc", "formula": "LOOKUP(SUM([Sales]), -1)"},
    ]
    d["parameters"] = [
        {"name": "P1", "allowable_values": list(range(30))},
        {"name": "P2"},
    ]
    d["filters"] = [{"field": "Region"}] * 5
    d["user_filters"] = [{"type": "user_calc", "formula": "USERNAME()"}]
    d["custom_sql"] = [{"datasource": "BigData", "name": "Custom Q", "query": "SELECT 1"}]
    d["actions"] = [
        {"type": "filter"},
        {"type": "url"},
        {"type": "set"},
    ]
    d["stories"] = [{"name": "Story", "story_points": [{"name": "P1"}, {"name": "P2"}]}]
    d["data_blending"] = [{"left": "A", "right": "B"}]
    d["published_datasources"] = [{"name": "PubDS"}]
    d["sets"] = [{"name": "TopN"}]
    d["groups"] = [{"name": "Regions"}]
    d["bins"] = [{"name": "PriceBin"}]
    d["hierarchies"] = [{"name": "GeoHier"}]
    d["hyper_files"] = [{"path": "data.hyper", "size_bytes": 50_000_000}]
    d["custom_shapes"] = [{"path": "shapes/star.png"}]
    d["embedded_fonts"] = [{"path": "fonts/custom.ttf"}]
    d["custom_geocoding"] = [{"type": "custom_file", "path": "geo.csv"}]
    return d


# ═══════════════════════════════════════════════════════════════════
#  CheckItem / CategoryResult / AssessmentReport data classes
# ═══════════════════════════════════════════════════════════════════

class TestDataClasses:
    def test_check_item_creation(self):
        ck = CheckItem("Cat", "Test", PASS, "All good")
        assert ck.severity == PASS
        assert ck.category == "Cat"

    def test_category_worst_severity(self):
        cat = CategoryResult(name="Test")
        cat.checks = [
            CheckItem("Test", "a", PASS, "ok"),
            CheckItem("Test", "b", WARN, "hmm"),
            CheckItem("Test", "c", INFO, "fyi"),
        ]
        assert cat.worst_severity == WARN

    def test_category_worst_severity_fail(self):
        cat = CategoryResult(name="Test")
        cat.checks = [
            CheckItem("Test", "a", PASS, "ok"),
            CheckItem("Test", "b", FAIL, "bad"),
        ]
        assert cat.worst_severity == FAIL

    def test_category_empty(self):
        cat = CategoryResult(name="Test")
        assert cat.worst_severity == PASS
        assert cat.pass_count == 0

    def test_category_counts(self):
        cat = CategoryResult(name="Test")
        cat.checks = [
            CheckItem("Test", "a", PASS, ""),
            CheckItem("Test", "b", PASS, ""),
            CheckItem("Test", "c", WARN, ""),
            CheckItem("Test", "d", FAIL, ""),
        ]
        assert cat.pass_count == 2
        assert cat.warn_count == 1
        assert cat.fail_count == 1

    def test_report_overall_green(self, empty_extracted):
        report = run_assessment(empty_extracted)
        # Empty workbook — should be GREEN (no fail/warn from critical checks)
        assert report.overall_score in ("GREEN", "YELLOW")

    def test_report_overall_red(self, complex_extracted):
        report = run_assessment(complex_extracted)
        assert report.overall_score == "RED"

    def test_report_to_dict(self, simple_extracted):
        report = run_assessment(simple_extracted, workbook_name="Test")
        d = report.to_dict()
        assert d["workbook_name"] == "Test"
        assert "overall_score" in d
        assert "categories" in d
        assert isinstance(d["categories"], list)
        assert len(d["categories"]) == 8
        for cat in d["categories"]:
            assert "name" in cat
            assert "checks" in cat

    def test_report_total_checks(self, simple_extracted):
        report = run_assessment(simple_extracted)
        assert report.total_checks > 0
        assert report.total_checks == report.total_pass + report.total_warn + report.total_fail + sum(
            1 for c in report.categories for ck in c.checks if ck.severity == INFO
        )


# ═══════════════════════════════════════════════════════════════════
#  Category 1: Datasource Compatibility
# ═══════════════════════════════════════════════════════════════════

class TestCheckDatasources:
    def test_no_datasources(self, empty_extracted):
        cat = _check_datasources(empty_extracted)
        assert cat.worst_severity == WARN
        assert any("datasource" in c.detail.lower() for c in cat.checks)

    def test_fully_supported_connector(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "Excel"}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        assert len(conn_checks) == 1
        assert conn_checks[0].severity == PASS

    def test_partially_supported_connector(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "BigQuery"}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        assert conn_checks[0].severity == WARN

    def test_unsupported_connector(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "Splunk"}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        assert conn_checks[0].severity == FAIL

    def test_unknown_connector(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "Unknown"}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        assert conn_checks[0].severity == WARN

    def test_unrecognised_connector(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "CustomDB"}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        assert conn_checks[0].severity == WARN
        assert "Unrecognised" in conn_checks[0].detail

    def test_data_blending_warn(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "CSV"}}],
            "custom_sql": [], "published_datasources": [],
            "data_blending": [{"left": "A", "right": "B"}],
        }
        cat = _check_datasources(data)
        blend_checks = [c for c in cat.checks if "blending" in c.name]
        assert blend_checks[0].severity == INFO  # auto-converted to relationships

    def test_published_datasources_warn(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "CSV"}}],
            "custom_sql": [], "data_blending": [],
            "published_datasources": [{"name": "PubDS"}],
        }
        cat = _check_datasources(data)
        pub_checks = [c for c in cat.checks if "Published" in c.name]
        assert pub_checks[0].severity == INFO  # auto re-pointed to Fabric

    def test_custom_sql_warn(self):
        data = {
            "datasources": [{"name": "DS", "connection": {"type": "CSV"}}],
            "data_blending": [], "published_datasources": [],
            "custom_sql": [{"query": "SELECT 1"}],
        }
        cat = _check_datasources(data)
        sql_checks = [c for c in cat.checks if "Custom SQL" in c.name]
        assert sql_checks[0].severity == INFO  # auto-embedded as native query passthrough

    def test_all_pass(self, simple_extracted):
        cat = _check_datasources(simple_extracted)
        assert cat.fail_count == 0
        # Excel connector → PASS, no blending/published/custom SQL → 3× PASS
        pass_checks = [c for c in cat.checks if c.severity == PASS]
        assert len(pass_checks) >= 4

    def test_multiple_connector_types(self):
        data = {
            "datasources": [
                {"name": "DS1", "connection": {"type": "Excel"}},
                {"name": "DS2", "connection": {"type": "BigQuery"}},
                {"name": "DS3", "connection": {"type": "Splunk"}},
            ],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        conn_checks = [c for c in cat.checks if "Connector:" in c.name]
        severities = {c.severity for c in conn_checks}
        assert PASS in severities   # Excel
        assert WARN in severities   # BigQuery
        assert FAIL in severities   # Splunk


# ═══════════════════════════════════════════════════════════════════
#  Category 2: Calculation Readiness
# ═══════════════════════════════════════════════════════════════════

class TestCheckCalculations:
    def test_no_calculations(self, empty_extracted):
        cat = _check_calculations(empty_extracted)
        assert cat.worst_severity == PASS

    def test_simple_calculations_pass(self, simple_extracted):
        cat = _check_calculations(simple_extracted)
        assert cat.fail_count == 0
        assert cat.warn_count == 0

    def test_unsupported_functions_fail(self):
        data = {"calculations": [
            {"name": "c", "caption": "ScriptCalc", "formula": "SCRIPT_REAL('x', [A])"},
        ]}
        cat = _check_calculations(data)
        assert cat.fail_count == 1
        fail = [c for c in cat.checks if c.severity == FAIL][0]
        assert "ScriptCalc" in fail.detail

    def test_partial_functions_warn(self):
        data = {"calculations": [
            {"name": "c", "caption": "RegexCalc", "formula": "REGEXP_MATCH([F], 'a')"},
        ]}
        cat = _check_calculations(data)
        assert cat.warn_count >= 1
        warns = [c for c in cat.checks if c.severity == WARN]
        assert any("RegexCalc" in w.detail for w in warns)

    def test_lod_expressions_warn(self):
        data = {"calculations": [
            {"name": "c", "caption": "LODCalc", "formula": "{ FIXED [Region] : SUM([Sales]) }"},
        ]}
        cat = _check_calculations(data)
        lod_warns = [c for c in cat.checks if "LOD" in c.name]
        assert lod_warns[0].severity == INFO  # auto-converted to DAX CALCULATE

    def test_table_calcs_warn(self):
        data = {"calculations": [
            {"name": "c", "caption": "RunSum", "formula": "RUNNING_SUM(SUM([Sales]))"},
        ]}
        cat = _check_calculations(data)
        tc_warns = [c for c in cat.checks if "Table" in c.name]
        assert tc_warns[0].severity == INFO  # auto-converted to DAX window functions

    def test_mixed_calculations(self, complex_extracted):
        cat = _check_calculations(complex_extracted)
        assert cat.fail_count >= 1  # SCRIPT_REAL
        assert cat.warn_count >= 1  # REGEXP (partially-supported)
        # LOD and TableCalc are now INFO (auto-converted)

    def test_collect_spatial_fail(self):
        data = {"calculations": [
            {"name": "c", "caption": "SpatialAgg", "formula": "COLLECT([Geometry])"},
        ]}
        cat = _check_calculations(data)
        assert cat.fail_count == 1

    def test_many_unsupported_truncates_names(self):
        data = {"calculations": [
            {"name": f"c{i}", "caption": f"Script{i}", "formula": f"SCRIPT_INT('x{i}', [{i}])"}
            for i in range(8)
        ]}
        cat = _check_calculations(data)
        fail = [c for c in cat.checks if c.severity == FAIL][0]
        assert "+3 more" in fail.detail  # 8 - 5 = 3


# ═══════════════════════════════════════════════════════════════════
#  Category 3: Visual & Dashboard Coverage
# ═══════════════════════════════════════════════════════════════════

class TestCheckVisuals:
    def test_all_mapped_types(self, simple_extracted):
        cat = _check_visuals(simple_extracted)
        chart_checks = [c for c in cat.checks if "chart" in c.name.lower() or "Chart type" in c.name]
        assert all(c.severity == PASS for c in chart_checks)

    def test_unmapped_chart_type(self):
        data = {
            "worksheets": [{"name": "W", "chart_type": "myCustomViz"}],
            "dashboards": [],
        }
        cat = _check_visuals(data)
        unmapped = [c for c in cat.checks if "Unmapped" in c.name]
        assert len(unmapped) == 1
        assert unmapped[0].severity == WARN
        assert "mycustomviz" in unmapped[0].detail

    def test_viz_in_tooltip(self):
        data = {
            "worksheets": [
                {"name": "W", "chart_type": "bar", "tooltips": [{"is_viz_tooltip": True}]},
            ],
            "dashboards": [],
        }
        cat = _check_visuals(data)
        vit = [c for c in cat.checks if "Viz-in-tooltip" in c.name]
        assert len(vit) == 1
        assert vit[0].severity == WARN

    def test_dual_axis(self):
        data = {
            "worksheets": [
                {"name": "W", "chart_type": "bar", "dual_axis": {"has_dual_axis": True}},
            ],
            "dashboards": [],
        }
        cat = _check_visuals(data)
        da = [c for c in cat.checks if "Dual" in c.name]
        assert len(da) == 1
        assert da[0].severity == WARN

    def test_device_layouts(self):
        data = {
            "worksheets": [],
            "dashboards": [{"name": "D", "device_layouts": [{"type": "phone"}]}],
        }
        cat = _check_visuals(data)
        dl = [c for c in cat.checks if "Device" in c.name]
        assert len(dl) == 1
        assert dl[0].severity == INFO

    def test_empty_chart_type_ignored(self):
        data = {
            "worksheets": [{"name": "W", "chart_type": ""}],
            "dashboards": [],
        }
        cat = _check_visuals(data)
        # No unmapped warning for empty chart type
        assert not any("Unmapped" in c.name for c in cat.checks)


# ═══════════════════════════════════════════════════════════════════
#  Category 4: Filter & Parameter Complexity
# ═══════════════════════════════════════════════════════════════════

class TestCheckFilters:
    def test_no_filters_or_params(self, empty_extracted):
        cat = _check_filters(empty_extracted)
        rls_checks = [c for c in cat.checks if "RLS" in c.name]
        assert rls_checks[0].severity == PASS

    def test_user_filters_warn(self):
        data = {
            "filters": [], "parameters": [],
            "user_filters": [{"type": "user_calc"}],
        }
        cat = _check_filters(data)
        rls = [c for c in cat.checks if "RLS" in c.name]
        assert rls[0].severity == INFO  # auto-converted to TMDL RLS roles

    def test_complex_parameters_warn(self):
        data = {
            "filters": [],
            "parameters": [{"name": "P", "allowable_values": list(range(25))}],
            "user_filters": [],
        }
        cat = _check_filters(data)
        cp = [c for c in cat.checks if "Complex" in c.name]
        assert len(cp) == 1
        assert cp[0].severity == INFO  # auto-converted to What-If tables

    def test_small_parameter_no_warn(self):
        data = {
            "filters": [],
            "parameters": [{"name": "P", "allowable_values": list(range(5))}],
            "user_filters": [],
        }
        cat = _check_filters(data)
        cp = [c for c in cat.checks if "Complex" in c.name]
        assert len(cp) == 0


# ═══════════════════════════════════════════════════════════════════
#  Category 5: Data Model Complexity
# ═══════════════════════════════════════════════════════════════════

class TestCheckDataModel:
    def test_simple_model(self, simple_extracted):
        cat = _check_data_model(simple_extracted)
        assert cat.fail_count == 0

    def test_large_table_count_warns(self):
        data = {
            "datasources": [{
                "name": "DS",
                "tables": [{"name": f"T{i}", "columns": []} for i in range(25)],
                "columns": [], "relationships": [],
            }],
            "hierarchies": [], "sets": [], "groups": [], "bins": [],
        }
        cat = _check_data_model(data)
        tc = [c for c in cat.checks if "Table count" in c.name]
        assert tc[0].severity == WARN

    def test_wide_schema_warns(self):
        data = {
            "datasources": [{
                "name": "DS",
                "tables": [{"name": "T", "columns": [{"name": f"c{i}"} for i in range(250)]}],
                "columns": [], "relationships": [],
            }],
            "hierarchies": [], "sets": [], "groups": [], "bins": [],
        }
        cat = _check_data_model(data)
        cc = [c for c in cat.checks if "Column count" in c.name]
        assert cc[0].severity == WARN

    def test_sets_groups_bins(self):
        data = {
            "datasources": [{"name": "D", "tables": [], "columns": [], "relationships": []}],
            "hierarchies": [],
            "sets": [{"name": "S"}],
            "groups": [{"name": "G"}],
            "bins": [{"name": "B"}],
        }
        cat = _check_data_model(data)
        sgb = [c for c in cat.checks if "Sets" in c.name]
        assert len(sgb) == 1
        assert "1 set" in sgb[0].detail
        assert "1 group" in sgb[0].detail
        assert "1 bin" in sgb[0].detail


# ═══════════════════════════════════════════════════════════════════
#  Category 6: Interactivity & Actions
# ═══════════════════════════════════════════════════════════════════

class TestCheckInteractivity:
    def test_no_actions_or_stories(self, empty_extracted):
        cat = _check_interactivity(empty_extracted)
        assert cat.worst_severity == PASS

    def test_filter_action_pass(self):
        data = {"actions": [{"type": "filter"}], "stories": []}
        cat = _check_interactivity(data)
        fa = [c for c in cat.checks if "filter" in c.name]
        assert fa[0].severity == PASS

    def test_url_action_warn(self):
        data = {"actions": [{"type": "url"}], "stories": []}
        cat = _check_interactivity(data)
        ua = [c for c in cat.checks if "URL" in c.name]
        assert ua[0].severity == INFO  # auto-mapped to action buttons

    def test_set_action_warn(self):
        data = {"actions": [{"type": "set"}], "stories": []}
        cat = _check_interactivity(data)
        sa = [c for c in cat.checks if "Set" in c.name]
        assert sa[0].severity == WARN

    def test_stories_info(self):
        data = {
            "actions": [],
            "stories": [{"name": "S", "story_points": [{"n": 1}, {"n": 2}]}],
        }
        cat = _check_interactivity(data)
        st = [c for c in cat.checks if "Stories" in c.name]
        assert st[0].severity == INFO
        assert "2 story point" in st[0].detail

    def test_complex_actions(self, complex_extracted):
        cat = _check_interactivity(complex_extracted)
        assert cat.warn_count >= 1  # set actions remain WARN
        # URL actions are now INFO (auto-mapped to buttons)


# ═══════════════════════════════════════════════════════════════════
#  Category 7: Data Extracts & Packaging
# ═══════════════════════════════════════════════════════════════════

class TestCheckExtractAndPackaging:
    def test_no_packaging(self, empty_extracted):
        cat = _check_extract_and_packaging(empty_extracted)
        assert cat.fail_count == 0
        assert cat.warn_count == 0

    def test_hyper_files_info(self):
        data = {
            "hyper_files": [{"path": "data.hyper", "size_bytes": 1_000_000}],
            "custom_shapes": [], "embedded_fonts": [], "custom_geocoding": [],
        }
        cat = _check_extract_and_packaging(data)
        hf = [c for c in cat.checks if "Hyper" in c.name]
        assert hf[0].severity == INFO
        assert "1.0 MB" in hf[0].detail

    def test_custom_shapes_warn(self):
        data = {
            "hyper_files": [],
            "custom_shapes": [{"path": "star.png"}],
            "embedded_fonts": [], "custom_geocoding": [],
        }
        cat = _check_extract_and_packaging(data)
        cs = [c for c in cat.checks if "shape" in c.name.lower()]
        assert cs[0].severity == WARN

    def test_embedded_fonts_warn(self):
        data = {
            "hyper_files": [], "custom_shapes": [],
            "embedded_fonts": [{"path": "font.ttf"}],
            "custom_geocoding": [],
        }
        cat = _check_extract_and_packaging(data)
        ef = [c for c in cat.checks if "font" in c.name.lower()]
        assert ef[0].severity == WARN

    def test_custom_geocoding_warn(self):
        data = {
            "hyper_files": [], "custom_shapes": [], "embedded_fonts": [],
            "custom_geocoding": [{"type": "custom_file", "path": "geo.csv"}],
        }
        cat = _check_extract_and_packaging(data)
        cg = [c for c in cat.checks if "geocoding" in c.name.lower()]
        assert len(cg) == 1
        assert cg[0].severity == WARN

    def test_full_packaging_complex(self, complex_extracted):
        cat = _check_extract_and_packaging(complex_extracted)
        assert cat.warn_count >= 3  # shapes, fonts, geocoding


# ═══════════════════════════════════════════════════════════════════
#  Category 8: Migration Scope & Effort
# ═══════════════════════════════════════════════════════════════════

class TestCheckMigrationScope:
    def test_empty_workbook_low(self, empty_extracted):
        cat = _check_migration_scope(empty_extracted)
        score = [c for c in cat.checks if "Complexity" in c.name]
        assert "Low" in score[0].detail

    def test_simple_workbook_low(self, simple_extracted):
        cat = _check_migration_scope(simple_extracted)
        score = [c for c in cat.checks if "Complexity" in c.name]
        assert "Low" in score[0].detail or "Medium" in score[0].detail

    def test_complex_workbook_high(self, complex_extracted):
        cat = _check_migration_scope(complex_extracted)
        score = [c for c in cat.checks if "Complexity" in c.name]
        # complexity ~48 → Medium (large workbooks with many unsupported
        # features push into High/Very High)
        assert any(t in score[0].detail for t in ("Medium", "High", "Very High"))

    def test_object_inventory(self, complex_extracted):
        cat = _check_migration_scope(complex_extracted)
        inv = [c for c in cat.checks if "inventory" in c.name.lower()]
        assert len(inv) == 1
        assert "Datasources:" in inv[0].detail
        assert "Worksheets:" in inv[0].detail

    def test_effort_estimate(self, simple_extracted):
        cat = _check_migration_scope(simple_extracted)
        eff = [c for c in cat.checks if "Estimated" in c.name]
        assert len(eff) == 1
        assert "hour" in eff[0].detail


# ═══════════════════════════════════════════════════════════════════
#  Full assessment orchestrator
# ═══════════════════════════════════════════════════════════════════

class TestRunAssessment:
    def test_returns_report(self, simple_extracted):
        report = run_assessment(simple_extracted, workbook_name="WB")
        assert isinstance(report, AssessmentReport)
        assert report.workbook_name == "WB"
        assert len(report.categories) == 8

    def test_green_score(self, simple_extracted):
        report = run_assessment(simple_extracted)
        assert report.overall_score == "GREEN"

    def test_red_score(self, complex_extracted):
        report = run_assessment(complex_extracted)
        assert report.overall_score == "RED"

    def test_timestamp_set(self, empty_extracted):
        report = run_assessment(empty_extracted)
        assert report.timestamp.endswith("Z")

    def test_eight_categories(self, empty_extracted):
        report = run_assessment(empty_extracted)
        names = [c.name for c in report.categories]
        assert "Datasource Compatibility" in names
        assert "Calculation Readiness" in names
        assert "Visual & Dashboard Coverage" in names
        assert "Filter & Parameter Complexity" in names
        assert "Data Model Complexity" in names
        assert "Interactivity & Actions" in names
        assert "Data Extracts & Packaging" in names
        assert "Migration Scope & Effort" in names


# ═══════════════════════════════════════════════════════════════════
#  Print & Save
# ═══════════════════════════════════════════════════════════════════

class TestPrintAndSave:
    def test_print_no_crash(self, simple_extracted, capsys):
        report = run_assessment(simple_extracted, workbook_name="Test")
        print_assessment_report(report)
        captured = capsys.readouterr()
        assert "PRE-MIGRATION ASSESSMENT" in captured.out
        assert "Test" in captured.out
        assert "GREEN" in captured.out

    def test_print_complex(self, complex_extracted, capsys):
        report = run_assessment(complex_extracted, workbook_name="Complex")
        print_assessment_report(report)
        captured = capsys.readouterr()
        assert "RED" in captured.out
        assert "Complex" in captured.out

    def test_save_creates_file(self, simple_extracted, tmp_path):
        report = run_assessment(simple_extracted, workbook_name="SaveTest")
        out_dir = str(tmp_path / "reports")
        path = save_assessment_report(report, output_dir=out_dir)
        assert os.path.exists(path)
        assert path.endswith(".json")
        with open(path, "r") as f:
            data = json.load(f)
        assert data["workbook_name"] == "SaveTest"
        assert data["overall_score"] == "GREEN"
        assert len(data["categories"]) == 8

    def test_save_roundtrip(self, complex_extracted, tmp_path):
        report = run_assessment(complex_extracted, workbook_name="RTTest")
        path = save_assessment_report(report, output_dir=str(tmp_path))
        with open(path, "r") as f:
            data = json.load(f)
        assert data["overall_score"] == "RED"
        assert data["totals"]["fail"] > 0

    def test_to_dict_serializable(self, complex_extracted):
        report = run_assessment(complex_extracted)
        d = report.to_dict()
        # Must be JSON-serializable
        serialized = json.dumps(d)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed["overall_score"] == "RED"


# ═══════════════════════════════════════════════════════════════════
#  Edge cases
# ═══════════════════════════════════════════════════════════════════

class TestEdgeCases:
    def test_missing_keys_in_extracted(self):
        """Assessment should handle missing keys gracefully."""
        report = run_assessment({}, workbook_name="Sparse")
        assert isinstance(report, AssessmentReport)
        assert report.total_checks > 0

    def test_empty_formulas(self):
        data = {"calculations": [{"name": "c", "formula": ""}]}
        cat = _check_calculations(data)
        assert cat.fail_count == 0

    def test_none_values_in_datasource(self):
        data = {
            "datasources": [{"name": None, "connection": {}}],
            "custom_sql": [], "data_blending": [], "published_datasources": [],
        }
        cat = _check_datasources(data)
        # Should not crash
        assert isinstance(cat, CategoryResult)

    def test_hexbin_unsupported(self):
        data = {"calculations": [
            {"name": "hex", "caption": "HexBin", "formula": "HEXBINX([Lon])"},
        ]}
        cat = _check_calculations(data)
        assert cat.fail_count == 1

    def test_highlight_action_pass(self):
        data = {"actions": [{"type": "highlight"}], "stories": []}
        cat = _check_interactivity(data)
        ha = [c for c in cat.checks if "highlight" in c.name]
        assert ha[0].severity == PASS

    def test_unknown_action_type(self):
        data = {"actions": [{"type": "go_to_url_fancy"}], "stories": []}
        cat = _check_interactivity(data)
        assert cat.warn_count >= 1

    def test_no_custom_geocoding_files(self):
        data = {
            "hyper_files": [], "custom_shapes": [], "embedded_fonts": [],
            "custom_geocoding": [{"role": "State", "field": "State"}],
        }
        cat = _check_extract_and_packaging(data)
        # role-type geocoding entries are not flagged
        assert not any("geocoding" in c.name.lower() for c in cat.checks)
