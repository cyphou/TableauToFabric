"""
Tests for Tableau Prep flow integration with Fabric artifact generators.

Verifies that Prep-sourced datasources (with m_query_override) are handled
correctly by Dataflow Gen2, Pipeline, Notebook, and Lakehouse generators.
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from fabric_import.dataflow_generator import DataflowGenerator
from fabric_import.pipeline_generator import PipelineGenerator
from fabric_import.notebook_generator import NotebookGenerator
from fabric_import.lakehouse_generator import LakehouseGenerator
from tableau_export.prep_flow_parser import merge_prep_with_workbook
from tests.conftest import SAMPLE_EXTRACTED, SAMPLE_DATASOURCE


# ── Sample Prep datasource (standalone, not merged with TWB) ───────

SAMPLE_PREP_DATASOURCE = {
    'name': 'prep.CleanedOrders',
    'caption': 'Cleaned Orders',
    'tables': [{
        'name': 'CleanedOrders',
        'columns': [
            {'name': 'OrderID', 'datatype': 'integer'},
            {'name': 'CustomerName', 'datatype': 'string'},
            {'name': 'Revenue', 'datatype': 'real'},
            {'name': 'Region', 'datatype': 'string'},
        ],
    }],
    'connection': {
        'type': 'CSV',
        'details': {'filename': 'orders.csv', 'delimiter': ','},
    },
    'connection_map': {},
    'connections': [{
        'type': 'CSV',
        'details': {'filename': 'orders.csv', 'delimiter': ','},
    }],
    'columns_metadata': [],
    'calculations': [],
    'relationships': [],
    'm_query_override': (
        'let\n'
        '    Source = Csv.Document(File.Contents("orders.csv")),\n'
        '    #"Promoted Headers" = Table.PromoteHeaders(Source),\n'
        '    #"Filtered Rows" = Table.SelectRows(#"Promoted Headers",\n'
        '        each [Revenue] > 0),\n'
        '    #"Renamed" = Table.RenameColumns(#"Filtered Rows",\n'
        '        {{"Cust", "CustomerName"}}),\n'
        '    Result = #"Renamed"\n'
        'in\n'
        '    Result'
    ),
    'is_prep_source': True,
}

# Prep datasource using only 'connections' (legacy format before fix)
SAMPLE_PREP_DATASOURCE_LEGACY = {
    'name': 'prep.SilverProducts',
    'caption': 'Silver Products',
    'tables': [{
        'name': 'SilverProducts',
        'columns': [
            {'name': 'ProductID', 'datatype': 'integer'},
            {'name': 'ProductName', 'datatype': 'string'},
            {'name': 'Category', 'datatype': 'string'},
        ],
    }],
    'connections': [{
        'type': 'PostgreSQL',
        'details': {'server': 'pghost', 'database': 'products', 'port': '5432'},
    }],
    'columns_metadata': [],
    'calculations': [],
    'relationships': [],
    'm_query_override': (
        'let\n'
        '    Source = PostgreSQL.Database("pghost", "products"),\n'
        '    Products = Source{[Schema="public",Item="products"]}[Data],\n'
        '    #"Cleaned" = Table.TransformColumnTypes(Products,\n'
        '        {{"ProductID", Int64.Type}}),\n'
        '    Result = #"Cleaned"\n'
        'in\n'
        '    Result'
    ),
    'is_prep_source': True,
}

SAMPLE_PREP_EXTRACTED = {
    'datasources': [SAMPLE_PREP_DATASOURCE],
    'worksheets': [],
    'dashboards': [],
    'calculations': [],
    'parameters': [],
    'filters': [],
    'stories': [],
    'actions': [],
    'sets': [],
    'groups': [],
    'bins': [],
    'hierarchies': [],
    'sort_orders': [],
    'aliases': {},
    'custom_sql': [],
    'user_filters': [],
}

# Extracted data with a mix of standard + Prep datasources
SAMPLE_MIXED_EXTRACTED = {
    'datasources': [SAMPLE_DATASOURCE, SAMPLE_PREP_DATASOURCE],
    'worksheets': SAMPLE_EXTRACTED['worksheets'],
    'dashboards': SAMPLE_EXTRACTED['dashboards'],
    'calculations': [],
    'parameters': [],
    'filters': [],
    'stories': [],
    'actions': [],
    'sets': [],
    'groups': [],
    'bins': [],
    'hierarchies': [],
    'sort_orders': [],
    'aliases': {},
    'custom_sql': [],
    'user_filters': [],
}

# ═══════════════════════════════════════════════════════════════════
# merge_prep_with_workbook tests
# ═══════════════════════════════════════════════════════════════════


class TestMergePrepWithWorkbook(unittest.TestCase):
    """Tests for merge_prep_with_workbook()."""

    def test_no_matching_tables_appends_prep(self):
        """Unmatched Prep datasources should be appended as standalone."""
        twb = [{'name': 'DS', 'tables': [{'name': 'Accounts'}]}]
        prep = [SAMPLE_PREP_DATASOURCE]
        merged = merge_prep_with_workbook(prep, twb)
        self.assertEqual(len(merged), 2)
        # First is TWB, second is Prep
        self.assertEqual(merged[0]['name'], 'DS')
        self.assertEqual(merged[1]['name'], 'prep.CleanedOrders')

    def test_matching_table_injects_m_query_overrides(self):
        """Matching Prep table → TWB table should inject m_query_overrides."""
        twb = [{
            'name': 'DS',
            'tables': [{'name': 'CleanedOrders'}],
        }]
        prep = [SAMPLE_PREP_DATASOURCE]
        merged = merge_prep_with_workbook(prep, twb)
        # Should have injected m_query_overrides
        self.assertIn('m_query_overrides', merged[0])
        self.assertIn('CleanedOrders', merged[0]['m_query_overrides'])
        self.assertIn('Csv.Document', merged[0]['m_query_overrides']['CleanedOrders'])

    def test_empty_twb_returns_prep_only(self):
        """No TWB → return Prep datasources as-is."""
        merged = merge_prep_with_workbook([SAMPLE_PREP_DATASOURCE], [])
        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]['name'], 'prep.CleanedOrders')

    def test_empty_prep_returns_twb_only(self):
        """No Prep → return TWB datasources as-is."""
        prep_empty = [{'tables': [{'name': 'X'}], 'm_query_override': ''}]
        twb = [{'name': 'DS', 'tables': [{'name': 'Orders'}]}]
        merged = merge_prep_with_workbook(prep_empty, twb)
        # Empty m_query_override → appended as-is (no matching)
        self.assertEqual(len(merged), 2)

    def test_multiple_prep_tables_matched(self):
        """Multiple Prep outputs matching multiple TWB tables."""
        twb = [{
            'name': 'DS',
            'tables': [
                {'name': 'CleanedOrders'},
                {'name': 'SilverProducts'},
            ],
        }]
        prep = [SAMPLE_PREP_DATASOURCE, SAMPLE_PREP_DATASOURCE_LEGACY]
        merged = merge_prep_with_workbook(prep, twb)
        # 1 TWB datasource (with 2 overrides) + 0 unmatched Prep
        self.assertEqual(len(merged), 1)
        overrides = merged[0].get('m_query_overrides', {})
        self.assertIn('CleanedOrders', overrides)
        self.assertIn('SilverProducts', overrides)


# ═══════════════════════════════════════════════════════════════════
# Dataflow Gen2 + Prep flow tests
# ═══════════════════════════════════════════════════════════════════


class TestDataflowGeneratorPrep(unittest.TestCase):
    """Prep flow integration with DataflowGenerator."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_df_prep_')
        self.gen = DataflowGenerator(self.tmpdir, 'PrepProject')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_uses_m_query_override(self):
        """Standalone Prep datasource should use m_query_override as-is."""
        stats = self.gen.generate(SAMPLE_PREP_EXTRACTED)
        self.assertEqual(stats['queries'], 1)

        # Read the mashup and check the Prep M query is used
        mashup = os.path.join(self.tmpdir, 'PrepProject.Dataflow', 'mashup.pq')
        with open(mashup, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertIn('Csv.Document', content)
        self.assertIn('Promoted Headers', content)
        self.assertIn('Filtered Rows', content)

    def test_uses_m_query_overrides_dict(self):
        """TWB datasource with m_query_overrides dict from Prep merge."""
        ds_with_overrides = {
            'name': 'Merged DS',
            'connection': {'type': 'CSV', 'details': {'filename': 'test.csv'}},
            'connection_map': {},
            'tables': [{'name': 'CleanedOrders', 'columns': []}],
            'm_query_overrides': {
                'CleanedOrders': 'let Source = "prep query" in Source',
            },
        }
        extracted = {
            'datasources': [ds_with_overrides],
            'custom_sql': [],
            'calculations': [],
        }
        stats = self.gen.generate(extracted)
        mashup = os.path.join(self.tmpdir, 'PrepProject.Dataflow', 'mashup.pq')
        with open(mashup, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertIn('prep query', content)

    def test_generates_definition_json(self):
        """Prep datasource should produce a valid Dataflow definition."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        def_path = os.path.join(self.tmpdir, 'PrepProject.Dataflow',
                                'dataflow_definition.json')
        self.assertTrue(os.path.exists(def_path))
        with open(def_path, 'r', encoding='utf-8') as f:
            defn = json.load(f)
        self.assertEqual(len(defn['queries']), 1)
        self.assertEqual(defn['queries'][0]['destination']['type'], 'Lakehouse')

    def test_connections_fallback(self):
        """Legacy Prep datasource with only 'connections' (no 'connection')."""
        extracted = {
            'datasources': [SAMPLE_PREP_DATASOURCE_LEGACY],
            'custom_sql': [],
            'calculations': [],
        }
        stats = self.gen.generate(extracted)
        self.assertEqual(stats['queries'], 1)

        # Source type should come from connections[0], not 'Unknown'
        def_path = os.path.join(self.tmpdir, 'PrepProject.Dataflow',
                                'dataflow_definition.json')
        with open(def_path, 'r', encoding='utf-8') as f:
            defn = json.load(f)
        # The query uses m_query_override, but source_type metadata should not be Unknown
        q_file = os.path.join(self.tmpdir, 'PrepProject.Dataflow', 'queries',
                              'SilverProducts.m')
        with open(q_file, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertIn('PostgreSQL.Database', content)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_mixed_prep_and_standard(self, mock_escape, mock_gen_m):
        """Mixed datasources: standard + Prep in the same generation."""
        mock_gen_m.return_value = 'let Source = "regular" in Source'
        stats = self.gen.generate(SAMPLE_MIXED_EXTRACTED)
        # 2 standard tables + 1 Prep table = 3
        self.assertEqual(stats['queries'], 3)

        mashup = os.path.join(self.tmpdir, 'PrepProject.Dataflow', 'mashup.pq')
        with open(mashup, 'r', encoding='utf-8') as f:
            content = f.read()
        # Standard queries used mock
        self.assertIn('regular', content)
        # Prep query used override
        self.assertIn('Csv.Document', content)

    def test_individual_m_files_created(self):
        """Each Prep query should have its own .m file."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        queries_dir = os.path.join(self.tmpdir, 'PrepProject.Dataflow', 'queries')
        m_files = [f for f in os.listdir(queries_dir) if f.endswith('.m')]
        self.assertEqual(len(m_files), 1)
        self.assertEqual(m_files[0], 'CleanedOrders.m')


# ═══════════════════════════════════════════════════════════════════
# Pipeline + Prep flow tests
# ═══════════════════════════════════════════════════════════════════


class TestPipelineGeneratorPrep(unittest.TestCase):
    """Prep flow integration with PipelineGenerator."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_pl_prep_')
        self.gen = PipelineGenerator(self.tmpdir, 'PrepPipeline')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_activities_for_prep_datasources(self):
        """Pipeline should create Dataflow activity for Prep datasource."""
        stats = self.gen.generate(SAMPLE_PREP_EXTRACTED)
        # 1 Prep DS → 1 Dataflow + 1 Notebook + 1 SM = 3
        self.assertEqual(stats['activities'], 3)

    def test_pipeline_three_stages(self):
        """Pipeline should have 3 stages: Dataflow, Notebook, SM refresh."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        def_path = os.path.join(self.tmpdir, 'PrepPipeline.Pipeline',
                                'pipeline_definition.json')
        with open(def_path, 'r', encoding='utf-8') as f:
            defn = json.load(f)
        activities = defn['properties']['activities']
        types = [a['type'] for a in activities]
        self.assertIn('RefreshDataflow', types)
        self.assertIn('TridentNotebook', types)
        self.assertIn('TridentDatasetRefresh', types)

    def test_notebook_depends_on_dataflow(self):
        """Notebook activity should depend on Dataflow refresh."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        def_path = os.path.join(self.tmpdir, 'PrepPipeline.Pipeline',
                                'pipeline_definition.json')
        with open(def_path, 'r', encoding='utf-8') as f:
            defn = json.load(f)
        activities = defn['properties']['activities']
        nb = [a for a in activities if a['type'] == 'TridentNotebook'][0]
        deps = [d['activity'] for d in nb['dependsOn']]
        df_names = [a['name'] for a in activities if a['type'] == 'RefreshDataflow']
        for df_name in df_names:
            self.assertIn(df_name, deps)

    def test_mixed_datasources_pipeline(self):
        """Mixed standard + Prep datasources → correct activity count."""
        stats = self.gen.generate(SAMPLE_MIXED_EXTRACTED)
        # 2 datasources (1 standard + 1 Prep) → 2 Dataflow + 1 NB + 1 SM = 4
        self.assertEqual(stats['activities'], 4)

    def test_dataflow_activity_names_use_ds_name(self):
        """Dataflow activities should use the datasource name."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        def_path = os.path.join(self.tmpdir, 'PrepPipeline.Pipeline',
                                'pipeline_definition.json')
        with open(def_path, 'r', encoding='utf-8') as f:
            defn = json.load(f)
        df_activity = [a for a in defn['properties']['activities']
                       if a['type'] == 'RefreshDataflow'][0]
        self.assertIn('CleanedOrders', df_activity['name'])

    def test_creates_platform_file(self):
        """Pipeline should generate .platform manifest."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        platform_path = os.path.join(self.tmpdir, 'PrepPipeline.Pipeline', '.platform')
        self.assertTrue(os.path.exists(platform_path))

    def test_creates_metadata_file(self):
        """Pipeline should generate metadata JSON."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        meta_path = os.path.join(self.tmpdir, 'PrepPipeline.Pipeline', 'pipeline_metadata.json')
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        self.assertEqual(meta['displayName'], 'PrepPipeline')
        self.assertEqual(meta['activities'], 3)


# ═══════════════════════════════════════════════════════════════════
# Notebook + Prep flow tests
# ═══════════════════════════════════════════════════════════════════


class TestNotebookGeneratorPrep(unittest.TestCase):
    """Prep flow integration with NotebookGenerator."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_nb_prep_')
        self.gen = NotebookGenerator(self.tmpdir, 'PrepNotebook')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generates_notebook(self):
        """Prep datasource should produce a valid notebook."""
        stats = self.gen.generate(SAMPLE_PREP_EXTRACTED)
        self.assertGreater(stats['cells'], 0)
        etl_path = os.path.join(self.tmpdir, 'PrepNotebook.Notebook',
                                'etl_pipeline.ipynb')
        self.assertTrue(os.path.exists(etl_path))
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        self.assertEqual(nb['nbformat'], 4)

    def test_prep_table_reads_from_lakehouse(self):
        """Prep-sourced tables should read from Lakehouse Delta, not JDBC."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        etl_path = os.path.join(self.tmpdir, 'PrepNotebook.Notebook',
                                'etl_pipeline.ipynb')
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        # Collect all code cell sources
        code = '\n'.join(
            ''.join(c['source'])
            for c in nb['cells']
            if c['cell_type'] == 'code'
        )
        # Prep-sourced table should read from Delta, NOT from JDBC
        self.assertIn('spark.read.format("delta").table(', code)
        self.assertIn('Dataflow Gen2 with Prep transforms', code)
        # Should NOT have TODO comment for unknown source
        self.assertNotIn('TODO: Configure data source for Unknown', code)

    def test_standard_table_reads_normally(self):
        """Standard (non-Prep) tables should use JDBC/file read templates."""
        extracted = {
            'datasources': [SAMPLE_DATASOURCE],
            'worksheets': [],
            'dashboards': [],
            'calculations': [],
            'custom_sql': [],
        }
        self.gen.generate(extracted)
        etl_path = os.path.join(self.tmpdir, 'PrepNotebook.Notebook',
                                'etl_pipeline.ipynb')
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        code = '\n'.join(
            ''.join(c['source'])
            for c in nb['cells']
            if c['cell_type'] == 'code'
        )
        # Standard SQL Server table should use JDBC
        self.assertIn('jdbc:sqlserver://', code)

    @patch('fabric_import.notebook_generator.classify_calculations')
    def test_mixed_prep_and_standard_tables(self, mock_classify):
        """Mixed datasources: Prep → Delta read, standard → JDBC read."""
        mock_classify.return_value = ([], [])
        self.gen.generate(SAMPLE_MIXED_EXTRACTED)
        etl_path = os.path.join(self.tmpdir, 'PrepNotebook.Notebook',
                                'etl_pipeline.ipynb')
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        code = '\n'.join(
            ''.join(c['source'])
            for c in nb['cells']
            if c['cell_type'] == 'code'
        )
        # Should have both Lakehouse read (Prep) and JDBC read (standard)
        self.assertIn('spark.read.format("delta").table(', code)
        self.assertIn('jdbc:sqlserver://', code)

    def test_connections_plural_fallback(self):
        """Prep DS with only 'connections' (no 'connection') should not crash."""
        extracted = {
            'datasources': [SAMPLE_PREP_DATASOURCE_LEGACY],
            'worksheets': [],
            'dashboards': [],
            'calculations': [],
            'custom_sql': [],
        }
        stats = self.gen.generate(extracted)
        self.assertGreater(stats['cells'], 0)


# ═══════════════════════════════════════════════════════════════════
# Lakehouse + Prep flow tests
# ═══════════════════════════════════════════════════════════════════


class TestLakehouseGeneratorPrep(unittest.TestCase):
    """Prep flow integration with LakehouseGenerator."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_lh_prep_')
        self.gen = LakehouseGenerator(self.tmpdir, 'PrepLakehouse')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_tables_for_prep_datasource(self):
        """Prep datasource tables should be generated in Lakehouse."""
        stats = self.gen.generate(SAMPLE_PREP_EXTRACTED)
        self.assertEqual(stats['tables'], 1)

    def test_columns_from_prep_datasource(self):
        """Columns from Prep datasource should appear in table metadata."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        meta_path = os.path.join(self.tmpdir, 'PrepLakehouse.Lakehouse',
                                 'table_metadata.json')
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        # Metadata is a dict keyed by table name
        self.assertEqual(len(meta), 1)
        table_meta = list(meta.values())[0]
        col_names = list(table_meta['columns'].keys())
        self.assertIn('OrderID', col_names)
        self.assertIn('Revenue', col_names)

    def test_ddl_file_created_for_prep_table(self):
        """DDL file should be created for Prep table."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        ddl_dir = os.path.join(self.tmpdir, 'PrepLakehouse.Lakehouse', 'ddl')
        self.assertTrue(os.path.isdir(ddl_dir))
        ddl_files = os.listdir(ddl_dir)
        self.assertGreater(len(ddl_files), 0)

    def test_source_type_from_connection(self):
        """Source type should come from Prep connection info, not 'Unknown'."""
        self.gen.generate(SAMPLE_PREP_EXTRACTED)
        meta_path = os.path.join(self.tmpdir, 'PrepLakehouse.Lakehouse',
                                 'table_metadata.json')
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        table_meta = list(meta.values())[0]
        self.assertEqual(table_meta['source_type'], 'CSV')

    def test_connections_plural_fallback(self):
        """Source type from 'connections' (plural) fallback."""
        extracted = {
            'datasources': [SAMPLE_PREP_DATASOURCE_LEGACY],
            'calculations': [],
            'custom_sql': [],
        }
        stats = self.gen.generate(extracted)
        self.assertEqual(stats['tables'], 1)
        meta_path = os.path.join(self.tmpdir, 'PrepLakehouse.Lakehouse',
                                 'table_metadata.json')
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        # Should use 'connections[0].type' → 'PostgreSQL', not 'Unknown'
        table_meta = list(meta.values())[0]
        self.assertEqual(table_meta['source_type'], 'PostgreSQL')


# ═══════════════════════════════════════════════════════════════════
# Prep datasource structure tests
# ═══════════════════════════════════════════════════════════════════


class TestPrepDatasourceStructure(unittest.TestCase):
    """Verify Prep datasource dicts have the expected keys."""

    def test_has_connection_singular(self):
        """Prep datasource should have 'connection' (singular, dict)."""
        self.assertIn('connection', SAMPLE_PREP_DATASOURCE)
        self.assertIsInstance(SAMPLE_PREP_DATASOURCE['connection'], dict)

    def test_has_connection_map(self):
        """Prep datasource should have 'connection_map' (dict)."""
        self.assertIn('connection_map', SAMPLE_PREP_DATASOURCE)
        self.assertIsInstance(SAMPLE_PREP_DATASOURCE['connection_map'], dict)

    def test_has_connections_plural(self):
        """Prep datasource should have 'connections' (list)."""
        self.assertIn('connections', SAMPLE_PREP_DATASOURCE)
        self.assertIsInstance(SAMPLE_PREP_DATASOURCE['connections'], list)

    def test_has_m_query_override(self):
        """Prep datasource should have non-empty 'm_query_override'."""
        self.assertTrue(SAMPLE_PREP_DATASOURCE.get('m_query_override'))

    def test_has_is_prep_source_flag(self):
        """Prep datasource should have 'is_prep_source' = True."""
        self.assertTrue(SAMPLE_PREP_DATASOURCE.get('is_prep_source'))

    def test_has_tables_with_columns(self):
        """Prep datasource tables should have columns."""
        tables = SAMPLE_PREP_DATASOURCE.get('tables', [])
        self.assertEqual(len(tables), 1)
        self.assertGreater(len(tables[0]['columns']), 0)


if __name__ == '__main__':
    unittest.main()
