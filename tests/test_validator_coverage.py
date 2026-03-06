"""Extra tests for fabric_import.validator — targeting uncovered branches.

Existing test_validator.py covers basic validate_artifact/json/tmdl and 
validate_project. This file targets the remaining uncovered lines:
  - validate_tmdl_dax (single-line & multi-line expression scanning)
  - validate_dax_formula (balanced parens, leakage, param refs)
  - _collect_model_symbols / validate_semantic_references
  - validate_notebook (missing keys)
  - validate_lakehouse_definition (missing props/tables)
  - validate_dataflow_definition (missing mashup)
  - validate_pipeline_definition (missing activities/name)
  - validate_project (pipeline, notebook, dataflow, lakehouse sub-dirs)
  - validate_directory (no .pbip projects)
"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.validator import ArtifactValidator


class TestValidateDaxFormula(unittest.TestCase):
    """Cover validate_dax_formula branches."""

    def test_empty_formula_returns_no_issues(self):
        self.assertEqual(ArtifactValidator.validate_dax_formula(""), [])
        self.assertEqual(ArtifactValidator.validate_dax_formula("   "), [])
        self.assertEqual(ArtifactValidator.validate_dax_formula(None), [])

    def test_balanced_parens_ok(self):
        self.assertEqual(ArtifactValidator.validate_dax_formula("SUM(A)"), [])

    def test_unmatched_closing(self):
        issues = ArtifactValidator.validate_dax_formula("SUM(A))")
        self.assertTrue(any("closing parenthesis" in i for i in issues))

    def test_unmatched_opening(self):
        issues = ArtifactValidator.validate_dax_formula("SUM((A)")
        self.assertTrue(any("unclosed" in i for i in issues))

    def test_tableau_function_leak_countd(self):
        issues = ArtifactValidator.validate_dax_formula("COUNTD([Col])")
        self.assertTrue(any("COUNTD" in i for i in issues))

    def test_tableau_function_leak_zn(self):
        issues = ArtifactValidator.validate_dax_formula("ZN([Sales])")
        self.assertTrue(any("ZN" in i for i in issues))

    def test_tableau_function_leak_ifnull(self):
        issues = ArtifactValidator.validate_dax_formula("IFNULL([X], 0)")
        self.assertTrue(any("IFNULL" in i for i in issues))

    def test_tableau_function_leak_attr(self):
        issues = ArtifactValidator.validate_dax_formula("ATTR([Name])")
        self.assertTrue(any("ATTR" in i for i in issues))

    def test_tableau_function_leak_double_equals(self):
        issues = ArtifactValidator.validate_dax_formula("[A] == 5")
        self.assertTrue(any("Double-equals" in i for i in issues))

    def test_no_false_positive_on_gte_lte(self):
        """>=, <=, != should not trigger double-equals."""
        issues = ArtifactValidator.validate_dax_formula("[A] >= 5")
        self.assertFalse(any("Double-equals" in i for i in issues))

    def test_tableau_function_leak_elseif(self):
        issues = ArtifactValidator.validate_dax_formula("IF [A] THEN 1 ELSEIF [B] THEN 2")
        self.assertTrue(any("ELSEIF" in i for i in issues))

    def test_tableau_function_leak_lod(self):
        issues = ArtifactValidator.validate_dax_formula("{FIXED [Region]: SUM([Sales])}")
        self.assertTrue(any("LOD" in i for i in issues))

    def test_tableau_function_leak_datetrunc(self):
        issues = ArtifactValidator.validate_dax_formula("DATETRUNC('month', [Date])")
        self.assertTrue(any("DATETRUNC" in i for i in issues))

    def test_tableau_function_leak_datepart(self):
        issues = ArtifactValidator.validate_dax_formula("DATEPART('year', [Date])")
        self.assertTrue(any("DATEPART" in i for i in issues))

    def test_tableau_function_leak_makepoint(self):
        issues = ArtifactValidator.validate_dax_formula("MAKEPOINT([Lat], [Lon])")
        self.assertTrue(any("MAKEPOINT" in i for i in issues))

    def test_tableau_function_leak_script(self):
        issues = ArtifactValidator.validate_dax_formula("SCRIPT_REAL('.mean(x)', [X])")
        self.assertTrue(any("SCRIPT_" in i for i in issues))

    def test_unresolved_parameter_reference(self):
        issues = ArtifactValidator.validate_dax_formula("[Parameters].[My Param]")
        self.assertTrue(any("Unresolved parameter" in i for i in issues))

    def test_context_label_appears(self):
        issues = ArtifactValidator.validate_dax_formula("COUNTD([X])", context="MyMeasure")
        self.assertTrue(any("MyMeasure" in i for i in issues))


class TestValidateTmdlDax(unittest.TestCase):
    """Test validate_tmdl_dax scanning logic."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_dax_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, content, name="test.tmdl"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def test_single_line_expression(self):
        path = self._write("measure 'Total'\n\texpression = COUNTD([ID])\n")
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertTrue(any("COUNTD" in i for i in issues))

    def test_multi_line_expression(self):
        content = (
            "measure 'Bad Measure'\n"
            "\texpression = ```\n"
            "\t\tCOUNTD([ID])\n"
            "\t\t```\n"
        )
        path = self._write(content)
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertTrue(any("COUNTD" in i for i in issues))

    def test_m_expression_skipped(self):
        """M (Power Query) expressions starting with 'let' should be skipped."""
        content = "column 'Col'\n\texpression = let Source = Table.FromRows({})\n"
        path = self._write(content)
        issues = ArtifactValidator.validate_tmdl_dax(content)
        # File doesn't exist with content as path – use the real path
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertEqual(issues, [])

    def test_nonexistent_file(self):
        issues = ArtifactValidator.validate_tmdl_dax("/nonexistent.tmdl")
        self.assertEqual(issues, [])


class TestCollectModelSymbolsAndSemanticRefs(unittest.TestCase):
    """Test _collect_model_symbols and validate_semantic_references."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_sym_")
        self.sm_dir = os.path.join(self.tmpdir, "Test.SemanticModel")
        self.def_dir = os.path.join(self.sm_dir, "definition")
        self.tables_dir = os.path.join(self.def_dir, "tables")
        os.makedirs(self.tables_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_tmdl(self, content, filename):
        path = os.path.join(self.tables_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def test_collect_symbols(self):
        self._write_tmdl(
            "table Orders\n\tcolumn OrderID\n\tcolumn Amount\n\tmeasure TotalSales\n",
            "Orders.tmdl",
        )
        symbols = ArtifactValidator._collect_model_symbols(self.sm_dir)
        self.assertIn("Orders", symbols["tables"])
        self.assertIn("OrderID", symbols["columns"]["Orders"])
        self.assertIn("TotalSales", symbols["measures"]["Orders"])

    def test_valid_reference(self):
        self._write_tmdl(
            "table Orders\n\tcolumn Amount\n\tmeasure Revenue\n"
            "\t\texpression = SUM('Orders'[Amount])\n",
            "Orders.tmdl",
        )
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertEqual(warnings, [])

    def test_unknown_table_reference(self):
        self._write_tmdl(
            "table Orders\n\tcolumn Amount\n\tmeasure Revenue\n"
            "\t\texpression = SUM('FakeTable'[Amount])\n",
            "Orders.tmdl",
        )
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertTrue(any("Unknown table" in w and "FakeTable" in w for w in warnings))

    def test_unknown_column_reference(self):
        self._write_tmdl(
            "table Orders\n\tcolumn Amount\n\tmeasure Revenue\n"
            "\t\texpression = SUM('Orders'[FakeCol])\n",
            "Orders.tmdl",
        )
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertTrue(any("Unknown column" in w and "FakeCol" in w for w in warnings))


class TestValidateNotebookBranches(unittest.TestCase):
    """Cover notebook validation edge cases."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_nb_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, obj, name="notebook.ipynb"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            json.dump(obj, f)
        return path

    def test_missing_cells(self):
        path = self._write({"metadata": {}, "nbformat": 4})
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)
        self.assertTrue(any("cells" in e for e in errors))

    def test_missing_metadata(self):
        path = self._write({"cells": [], "nbformat": 4})
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)
        self.assertTrue(any("metadata" in e for e in errors))

    def test_missing_nbformat(self):
        path = self._write({"cells": [], "metadata": {}})
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)
        self.assertTrue(any("nbformat" in e for e in errors))

    def test_non_object_notebook(self):
        path = self._write([1, 2, 3])
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)

    def test_invalid_json_notebook(self):
        path = os.path.join(self.tmpdir, "bad.ipynb")
        with open(path, "w") as f:
            f.write("{bad json")
        valid, errors = ArtifactValidator.validate_notebook(path)
        self.assertFalse(valid)


class TestValidateLakehouseBranches(unittest.TestCase):
    """Cover lakehouse_definition edge cases."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_lh_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, obj, name="lakehouse_definition.json"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            json.dump(obj, f)
        return path

    def test_missing_displayname_and_lakehouse_name(self):
        path = self._write({"tables": []})
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)
        self.assertTrue(any("displayName" in e for e in errors))

    def test_missing_tables(self):
        path = self._write({"properties": {"displayName": "lh"}})
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)
        self.assertTrue(any("tables" in e for e in errors))

    def test_non_object(self):
        path = self._write([1, 2])
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)

    def test_invalid_json(self):
        path = os.path.join(self.tmpdir, "bad.json")
        with open(path, "w") as f:
            f.write("not json")
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertFalse(valid)

    def test_lakehouse_name_key_accepted(self):
        path = self._write({"lakehouse_name": "lh", "tables": []})
        valid, errors = ArtifactValidator.validate_lakehouse_definition(path)
        self.assertTrue(valid)


class TestValidateDataflowBranches(unittest.TestCase):
    """Cover dataflow_definition edge cases."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_df_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, obj, name="dataflow_definition.json"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            json.dump(obj, f)
        return path

    def test_missing_mashup(self):
        path = self._write({"name": "df"})
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertFalse(valid)
        self.assertTrue(any("mashup" in e.lower() for e in errors))

    def test_non_object(self):
        path = self._write([])
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertFalse(valid)

    def test_mashup_key_accepted(self):
        path = self._write({"mashup": {}})
        valid, errors = ArtifactValidator.validate_dataflow_definition(path)
        self.assertTrue(valid)


class TestValidatePipelineBranches(unittest.TestCase):
    """Cover pipeline_definition edge cases."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_pl_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write(self, obj, name="pipeline_definition.json"):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            json.dump(obj, f)
        return path

    def test_missing_activities(self):
        path = self._write({"name": "pl", "properties": {}})
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)
        self.assertTrue(any("activities" in e for e in errors))

    def test_activities_not_array(self):
        path = self._write({"name": "pl", "properties": {"activities": "bad"}})
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)

    def test_missing_name(self):
        path = self._write({"properties": {"activities": []}})
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)
        self.assertTrue(any("name" in e.lower() for e in errors))

    def test_non_object(self):
        path = self._write([])
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertFalse(valid)

    def test_valid_pipeline(self):
        path = self._write({"name": "pl", "properties": {"activities": []}})
        valid, errors = ArtifactValidator.validate_pipeline_definition(path)
        self.assertTrue(valid)


class TestValidateProjectExtended(unittest.TestCase):
    """Test validate_project for Pipeline, Notebook, Dataflow, Lakehouse dirs."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_proj_")
        self.project = os.path.join(self.tmpdir, "TestProject")
        os.makedirs(self.project)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_json(self, relpath, obj):
        full = os.path.join(self.project, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            json.dump(obj, f)

    def _write_text(self, relpath, text):
        full = os.path.join(self.project, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)

    def test_project_not_found(self):
        result = ArtifactValidator.validate_project("/nonexistent_dir")
        self.assertFalse(result["valid"])

    def test_minimal_valid_project(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Report/definition.pbir",
            {"version": "4.0"},
        )
        self._write_text(
            "TestProject.SemanticModel/definition/model.tmdl",
            "model Model\n",
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])
        self.assertGreaterEqual(result["files_checked"], 2)

    def test_project_with_pipeline(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Pipeline/pipeline_definition.json",
            {"name": "pl", "properties": {"activities": []}},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertIn("files_checked", result)

    def test_project_with_invalid_pipeline(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Pipeline/pipeline_definition.json",
            {"properties": {}},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertFalse(result["valid"])

    def test_project_with_notebook(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Notebook/notebook.ipynb",
            {"cells": [], "metadata": {}, "nbformat": 4},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])

    def test_project_with_lakehouse(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Lakehouse/lakehouse_definition.json",
            {"lakehouse_name": "lh", "tables": []},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])

    def test_project_with_dataflow(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "TestProject.Dataflow/dataflow_definition.json",
            {"mashupDocument": {}},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])

    def test_standalone_semantic_model_dir(self):
        """Backward compat: 'SemanticModel/' dir without name prefix."""
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_text(
            "SemanticModel/definition/model.tmdl",
            "model Model\n",
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])

    def test_pipeline_fallback_plain_dir(self):
        """Pipeline directory without name prefix."""
        self._write_json("TestProject.pbip", {"version": "1.0"})
        self._write_json(
            "Pipeline/pipeline_definition.json",
            {"name": "pl", "properties": {"activities": []}},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])

    def test_pages_and_visuals_validated(self):
        self._write_json("TestProject.pbip", {"version": "1.0"})
        # Report with pages and visuals
        self._write_json(
            "TestProject.Report/definition.pbir",
            {"version": "4.0"},
        )
        self._write_json(
            "TestProject.Report/definition/pages/page1/page.json",
            {"displayName": "Page 1"},
        )
        self._write_json(
            "TestProject.Report/definition/pages/page1/visuals/v1/visual.json",
            {"visualType": "barChart"},
        )
        result = ArtifactValidator.validate_project(self.project)
        self.assertTrue(result["valid"])
        self.assertGreaterEqual(result["files_checked"], 3)


class TestValidateDirectoryExtended(unittest.TestCase):
    """Test validate_directory scanning."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_val_dir_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_nonexistent_directory(self):
        results = ArtifactValidator.validate_directory("/nonexistent")
        self.assertEqual(results, {})

    def test_empty_directory(self):
        results = ArtifactValidator.validate_directory(self.tmpdir)
        self.assertEqual(results, {})

    def test_json_files_validated(self):
        path = os.path.join(self.tmpdir, "artifact.json")
        with open(path, "w") as f:
            json.dump({"type": "Dataset"}, f)
        results = ArtifactValidator.validate_directory(self.tmpdir)
        self.assertIn("artifact.json", results)
        self.assertTrue(results["artifact.json"]["valid"])

    def test_project_subdirs_validated(self):
        proj = os.path.join(self.tmpdir, "MyProj")
        os.makedirs(proj)
        with open(os.path.join(proj, "MyProj.pbip"), "w") as f:
            json.dump({"version": "1.0"}, f)
        results = ArtifactValidator.validate_directory(self.tmpdir)
        self.assertIn("MyProj", results)


if __name__ == "__main__":
    unittest.main()
