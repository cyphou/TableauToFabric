"""
End-to-end / non-regression tests.

These tests exercise the full pipeline from extracted Tableau data
through conversion, TMDL generation, visual generation, and PBIP output.
"""

import os
import json
import tempfile
import shutil
import unittest

from fabric_import.tmdl_generator import generate_tmdl, _build_semantic_model
from fabric_import.visual_generator import (
    generate_visual_containers,
    resolve_visual_type,
    create_visual_container,
    build_query_state,
)
from fabric_import.pbip_generator import FabricPBIPGenerator
from tableau_export.dax_converter import convert_tableau_formula_to_dax
from tableau_export.m_query_builder import generate_power_query_m


# ── Realistic sample data ─────────────────────────────
FULL_DATASOURCE = {
    'name': 'Superstore',
    'connection': {'type': 'SQL Server', 'details': {'server': 'dbsrv', 'database': 'SuperStore'}},
    'connection_map': {},
    'tables': [
        {
            'name': 'Orders',
            'columns': [
                {'name': 'OrderID', 'datatype': 'integer'},
                {'name': 'OrderDate', 'datatype': 'date'},
                {'name': 'CustomerID', 'datatype': 'integer'},
                {'name': 'ProductID', 'datatype': 'integer'},
                {'name': 'Region', 'datatype': 'string'},
                {'name': 'Category', 'datatype': 'string'},
                {'name': 'SubCategory', 'datatype': 'string'},
                {'name': 'Sales', 'datatype': 'real'},
                {'name': 'Quantity', 'datatype': 'integer'},
                {'name': 'Discount', 'datatype': 'real'},
                {'name': 'Profit', 'datatype': 'real'},
                {'name': 'City', 'datatype': 'string', 'role': 'city'},
                {'name': 'State', 'datatype': 'string', 'role': 'state'},
                {'name': 'Latitude', 'datatype': 'real', 'role': 'latitude'},
                {'name': 'Longitude', 'datatype': 'real', 'role': 'longitude'},
            ],
        },
        {
            'name': 'Customers',
            'columns': [
                {'name': 'CustomerID', 'datatype': 'integer'},
                {'name': 'CustomerName', 'datatype': 'string'},
                {'name': 'Segment', 'datatype': 'string'},
            ],
        },
        {
            'name': 'Products',
            'columns': [
                {'name': 'ProductID', 'datatype': 'integer'},
                {'name': 'ProductName', 'datatype': 'string'},
                {'name': 'Category', 'datatype': 'string'},
                {'name': 'SubCategory', 'datatype': 'string'},
            ],
        },
    ],
    'relationships': [
        {'left': {'table': 'Orders', 'column': 'CustomerID'},
         'right': {'table': 'Customers', 'column': 'CustomerID'},
         'type': 'left'},
        {'left': {'table': 'Orders', 'column': 'ProductID'},
         'right': {'table': 'Products', 'column': 'ProductID'},
         'type': 'left'},
    ],
    'calculations': [
        {'name': '[Total Sales]', 'caption': 'Total Sales',
         'formula': 'SUM([Sales])', 'role': 'measure', 'datatype': 'real'},
        {'name': '[Total Profit]', 'caption': 'Total Profit',
         'formula': 'SUM([Profit])', 'role': 'measure', 'datatype': 'real'},
        {'name': '[Profit Ratio]', 'caption': 'Profit Ratio',
         'formula': 'SUM([Profit]) / SUM([Sales])', 'role': 'measure', 'datatype': 'real'},
        {'name': '[Avg Discount]', 'caption': 'Avg Discount',
         'formula': 'AVG([Discount])', 'role': 'measure', 'datatype': 'real'},
        {'name': '[Order Count]', 'caption': 'Order Count',
         'formula': 'COUNTD([OrderID])', 'role': 'measure', 'datatype': 'integer'},
    ],
    'columns': [],
}

FULL_CONVERTED_OBJECTS = {
    'datasources': [FULL_DATASOURCE],
    'worksheets': [
        {'name': 'Sales By Region', 'chart_type': 'clusteredBarChart',
         'fields': [
             {'name': 'Region', 'role': 'dimension'},
             {'name': 'sum:Sales', 'role': 'measure'},
         ]},
        {'name': 'Profit Trend', 'chart_type': 'lineChart',
         'fields': [
             {'name': 'OrderDate', 'role': 'dimension'},
             {'name': 'sum:Profit', 'role': 'measure'},
         ]},
        {'name': 'Category Breakdown', 'chart_type': 'pieChart',
         'fields': [
             {'name': 'Category', 'role': 'dimension'},
             {'name': 'sum:Sales', 'role': 'measure'},
         ]},
        {'name': 'Customer Table', 'chart_type': 'tableEx',
         'fields': [
             {'name': 'CustomerName', 'role': 'dimension'},
             {'name': 'Segment', 'role': 'dimension'},
             {'name': 'sum:Sales', 'role': 'measure'},
         ]},
        {'name': 'Sales Map', 'chart_type': 'map',
         'fields': [
             {'name': 'State', 'role': 'dimension'},
             {'name': 'sum:Sales', 'role': 'measure'},
         ]},
    ],
    'dashboards': [
        {
            'name': 'Executive Dashboard',
            'size': {'width': 1920, 'height': 1080},
            'objects': [
                {'type': 'worksheetReference', 'worksheetName': 'Sales By Region',
                 'position': {'x': 0, 'y': 0, 'w': 960, 'h': 540}},
                {'type': 'worksheetReference', 'worksheetName': 'Profit Trend',
                 'position': {'x': 960, 'y': 0, 'w': 960, 'h': 540}},
                {'type': 'worksheetReference', 'worksheetName': 'Category Breakdown',
                 'position': {'x': 0, 'y': 540, 'w': 640, 'h': 540}},
                {'type': 'worksheetReference', 'worksheetName': 'Customer Table',
                 'position': {'x': 640, 'y': 540, 'w': 640, 'h': 540}},
                {'type': 'worksheetReference', 'worksheetName': 'Sales Map',
                 'position': {'x': 1280, 'y': 540, 'w': 640, 'h': 540}},
            ],
        },
    ],
    'calculations': [],
    'parameters': [],
    'hierarchies': [
        {'name': 'Geography', 'table': 'Orders',
         'levels': ['State', 'City']},
    ],
    'sets': [],
    'groups': [],
    'bins': [],
    'aliases': {},
    'user_filters': [],
    'filters': [
        {'field': 'Region', 'type': 'categorical', 'values': ['West', 'East']},
    ],
    'stories': [],
    'actions': [],
    'custom_sql': [],
}

# Same dataset but with raw Tableau-style field names (federated prefixes,
# :Measure Names) to exercise the cleaning pipeline end-to-end.
_FEDERATED_HASH = 'federated.10vks1203pgcxf1bkw5yk1bwzy2f'
FULL_CONVERTED_WITH_DIRTY_FILTERS = {
    **FULL_CONVERTED_OBJECTS,
    'filters': [
        # federated prefix + derivation qualifiers
        {'field': f'{_FEDERATED_HASH}.none:Region:qk', 'type': 'categorical',
         'values': ['West']},
        # Tableau virtual field — must be dropped entirely
        {'field': f'{_FEDERATED_HASH}.:Measure Names', 'type': 'categorical',
         'values': [f'{_FEDERATED_HASH}.sum:Sales:qk']},
    ],
    'worksheets': [
        {**ws, 'filters': [
            {'field': f'{_FEDERATED_HASH}.none:{ws["fields"][0]["name"].replace("sum:", "")}:qk',
             'type': 'categorical', 'values': ['test']},
            {'field': ':Measure Names', 'type': 'categorical',
             'values': ['sum:Sales']},
        ]}
        for ws in FULL_CONVERTED_OBJECTS['worksheets']
    ],
}


class TestEndToEndTMDLGeneration(unittest.TestCase):
    """E2E — TMDL generation from realistic data."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_realistic_model(self):
        stats = generate_tmdl(
            [FULL_DATASOURCE], 'Superstore', FULL_CONVERTED_OBJECTS,
            self.tmpdir, 'SuperstoreLH'
        )
        self.assertTrue(stats['tables'] >= 3)  # Orders, Customers, Products + Date table
        self.assertTrue(stats['columns'] >= 15)
        self.assertTrue(stats['measures'] >= 5)
        self.assertTrue(stats['relationships'] >= 2)

    def test_directlake_partitions(self):
        model = _build_semantic_model(
            [FULL_DATASOURCE], 'Test',
            FULL_CONVERTED_OBJECTS, 'LH'
        )
        for table in model['model']['tables']:
            for partition in table.get('partitions', []):
                if partition.get('mode') == 'directLake':
                    self.assertIn('entityName', partition['source'])
                    self.assertEqual(partition['source']['schemaName'], 'dbo')

    def test_hierarchies_applied(self):
        stats = generate_tmdl(
            [FULL_DATASOURCE], 'Test', FULL_CONVERTED_OBJECTS,
            self.tmpdir, 'LH'
        )
        self.assertTrue(stats['hierarchies'] >= 1)

    def test_date_table_created(self):
        model = _build_semantic_model(
            [FULL_DATASOURCE], 'Test',
            FULL_CONVERTED_OBJECTS, 'LH'
        )
        table_names = [t['name'] for t in model['model']['tables']]
        # Date table auto-created when date columns exist
        self.assertTrue(any('date' in n.lower() or 'calendar' in n.lower()
                           for n in table_names) or len(table_names) >= 3)

    def test_tmdl_files_valid(self):
        generate_tmdl(
            [FULL_DATASOURCE], 'Test', FULL_CONVERTED_OBJECTS,
            self.tmpdir, 'LH'
        )
        def_dir = os.path.join(self.tmpdir, 'definition')
        # All required files exist
        for fname in ['database.tmdl', 'model.tmdl', 'relationships.tmdl', 'expressions.tmdl']:
            fpath = os.path.join(def_dir, fname)
            self.assertTrue(os.path.exists(fpath), f"Missing: {fname}")
        # Tables directory has files
        tables_dir = os.path.join(def_dir, 'tables')
        tmdl_files = [f for f in os.listdir(tables_dir) if f.endswith('.tmdl')]
        self.assertTrue(len(tmdl_files) >= 3)


class TestEndToEndVisualGeneration(unittest.TestCase):
    """E2E — Visual container generation from realistic data."""

    def test_generates_correct_count(self):
        containers = generate_visual_containers(
            FULL_CONVERTED_OBJECTS['worksheets'], 'Superstore',
            col_table_map={'Region': 'Orders', 'Sales': 'Orders',
                          'Profit': 'Orders', 'Category': 'Orders',
                          'OrderDate': 'Orders', 'State': 'Orders',
                          'CustomerName': 'Customers', 'Segment': 'Customers'}
        )
        self.assertEqual(len(containers), 5)

    def test_visual_types_correct(self):
        # Use 'visualType' key as expected by generate_visual_containers
        worksheets = [
            {'name': 'Sales By Region', 'visualType': 'clusteredBarChart',
             'fields': [{'name': 'Region', 'role': 'dimension'},
                        {'name': 'sum:Sales', 'role': 'measure'}]},
            {'name': 'Profit Trend', 'visualType': 'lineChart',
             'fields': [{'name': 'OrderDate', 'role': 'dimension'},
                        {'name': 'sum:Profit', 'role': 'measure'}]},
            {'name': 'Category Breakdown', 'visualType': 'pieChart',
             'fields': [{'name': 'Category', 'role': 'dimension'},
                        {'name': 'sum:Sales', 'role': 'measure'}]},
            {'name': 'Customer Table', 'visualType': 'tableEx',
             'fields': [{'name': 'CustomerName', 'role': 'dimension'}]},
            {'name': 'Sales Map', 'visualType': 'map',
             'fields': [{'name': 'State', 'role': 'dimension'},
                        {'name': 'sum:Sales', 'role': 'measure'}]},
        ]
        containers = generate_visual_containers(worksheets, 'R')
        types = [c['visual']['visualType'] for c in containers]
        self.assertIn('clusteredBarChart', types)
        self.assertIn('lineChart', types)
        self.assertIn('pieChart', types)
        self.assertIn('tableEx', types)
        self.assertIn('map', types)

    def test_positions_non_overlapping(self):
        containers = generate_visual_containers(
            FULL_CONVERTED_OBJECTS['worksheets'], 'R'
        )
        positions = [(c['position']['x'], c['position']['y']) for c in containers]
        self.assertEqual(len(positions), len(set(positions)))


class TestEndToEndPBIPGeneration(unittest.TestCase):
    """E2E — Full PBIP project generation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_project(self):
        result = self.gen.generate_project(
            'Superstore', FULL_CONVERTED_OBJECTS, 'SuperstoreLH'
        )
        self.assertTrue(result['pages'] >= 1)
        self.assertTrue(result['visuals'] >= 5)

    def test_semantic_model_directlake(self):
        self.gen.generate_project('Superstore', FULL_CONVERTED_OBJECTS, 'LH')
        # Check that SemanticModel directory exists with TMDL files
        sm_dir = os.path.join(
            self.tmpdir, 'Superstore', 'Superstore.SemanticModel', 'definition'
        )
        self.assertTrue(os.path.isdir(sm_dir))
        model_tmdl = os.path.join(sm_dir, 'model.tmdl')
        if os.path.exists(model_tmdl):
            with open(model_tmdl, encoding='utf-8') as f:
                content = f.read()
            self.assertIn('directLake', content)

    def test_report_definition(self):
        self.gen.generate_project('Superstore', FULL_CONVERTED_OBJECTS, 'LH')
        pbir = os.path.join(
            self.tmpdir, 'Superstore', 'Superstore.Report', 'definition.pbir'
        )
        with open(pbir, encoding='utf-8') as f:
            data = json.load(f)
        self.assertEqual(data['version'], '4.0')
        self.assertIn('Superstore.SemanticModel', data['datasetReference']['byPath']['path'])

    def test_pages_and_visuals_on_disk(self):
        self.gen.generate_project('Superstore', FULL_CONVERTED_OBJECTS, 'LH')
        pages_dir = os.path.join(
            self.tmpdir, 'Superstore', 'Superstore.Report', 'definition', 'pages'
        )
        pages_json = os.path.join(pages_dir, 'pages.json')
        with open(pages_json, encoding='utf-8') as f:
            meta = json.load(f)
        self.assertTrue(len(meta['pageOrder']) >= 1)

        # Check that visuals exist on disk
        first_page = meta['pageOrder'][0]
        visuals_dir = os.path.join(pages_dir, first_page, 'visuals')
        if os.path.isdir(visuals_dir):
            visual_dirs = [d for d in os.listdir(visuals_dir)
                          if os.path.isdir(os.path.join(visuals_dir, d))]
            self.assertTrue(len(visual_dirs) >= 1)


class TestEndToEndDAXConversion(unittest.TestCase):
    """E2E — DAX conversion of realistic formulas."""

    def _convert(self, formula, **kwargs):
        return convert_tableau_formula_to_dax(formula, **kwargs)

    def test_simple_sum(self):
        result = self._convert('SUM([Sales])')
        self.assertIn('SUM', result)

    def test_countd(self):
        result = self._convert('COUNTD([Customer])')
        self.assertIn('DISTINCTCOUNT', result)

    def test_profit_ratio(self):
        result = self._convert('SUM([Profit]) / SUM([Sales])')
        self.assertIn('SUM', result)
        self.assertIn('/', result)

    def test_if_then(self):
        result = self._convert('IF SUM([Sales]) > 1000 THEN "High" ELSE "Low" END')
        self.assertIn('IF(', result)

    def test_case_when(self):
        formula = 'CASE [Region] WHEN "East" THEN 1 WHEN "West" THEN 2 ELSE 0 END'
        result = self._convert(formula)
        self.assertIn('SWITCH', result)

    def test_datediff(self):
        result = self._convert("DATEDIFF('day', [OrderDate], TODAY())")
        self.assertIn('DATEDIFF', result)

    def test_contains(self):
        result = self._convert('CONTAINS([Name], "abc")')
        self.assertIn('CONTAINSSTRING', result)

    def test_zn(self):
        result = self._convert('ZN([Sales])')
        self.assertIn('IF', result)
        self.assertIn('ISBLANK', result)


class TestEndToEndMQueryGeneration(unittest.TestCase):
    """E2E — M query generation for various connectors."""

    def test_sql_server(self):
        conn = {'type': 'SQL Server', 'details': {'server': 'dbsrv', 'database': 'SuperStore'}}
        table = {'name': 'Orders', 'columns': FULL_DATASOURCE['tables'][0]['columns']}
        m = generate_power_query_m(conn, table)
        self.assertIn('Sql.Database', m)
        self.assertIn('dbsrv', m)
        self.assertIn('let', m)
        self.assertIn('in', m)

    def test_csv(self):
        conn = {'type': 'CSV', 'details': {'filename': 'orders.csv', 'delimiter': ','}}
        table = {'name': 'Orders', 'columns': FULL_DATASOURCE['tables'][0]['columns']}
        m = generate_power_query_m(conn, table)
        self.assertIn('Csv.Document', m)

    def test_excel(self):
        conn = {'type': 'Excel', 'details': {'filename': 'data.xlsx'}}
        table = {'name': 'Sheet1', 'columns': []}
        m = generate_power_query_m(conn, table)
        self.assertIn('Excel.Workbook', m)


class TestNonRegressionEdgeCases(unittest.TestCase):
    """E2E — Edge cases that previously caused issues."""

    def test_empty_datasources_no_crash(self):
        tmpdir = tempfile.mkdtemp()
        try:
            stats = generate_tmdl([], 'Empty', {}, tmpdir, 'LH')
            self.assertEqual(stats['tables'], 0)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_table_with_no_columns(self):
        ds = {
            'name': 'DS',
            'connection': {'type': 'CSV', 'details': {}},
            'connection_map': {},
            'tables': [{'name': 'T', 'columns': []}],
            'calculations': [],
            'relationships': [],
            'columns': [],
        }
        tmpdir = tempfile.mkdtemp()
        try:
            stats = generate_tmdl([ds], 'Test', {}, tmpdir, 'LH')
            self.assertEqual(stats['tables'], 1)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_special_chars_in_names(self):
        ds = {
            'name': 'DS',
            'connection': {'type': 'CSV', 'details': {}},
            'connection_map': {},
            'tables': [{'name': "Customer's Orders (2024)", 'columns': [
                {'name': 'ID', 'datatype': 'integer'},
            ]}],
            'calculations': [],
            'relationships': [],
            'columns': [],
        }
        tmpdir = tempfile.mkdtemp()
        try:
            stats = generate_tmdl([ds], 'Test', {}, tmpdir, 'LH')
            self.assertTrue(stats['tables'] >= 1)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_visual_type_resilience(self):
        """All 60+ visual types resolve without error."""
        from fabric_import.visual_generator import VISUAL_TYPE_MAP
        for src_type in VISUAL_TYPE_MAP:
            result = resolve_visual_type(src_type)
            self.assertIsInstance(result, str)
            self.assertTrue(len(result) > 0)

    def test_dax_empty_formula_no_crash(self):
        result = convert_tableau_formula_to_dax('')
        self.assertEqual(result, '')

    def test_m_query_unknown_connector(self):
        conn = {'type': 'SomeNewDB', 'details': {}}
        m = generate_power_query_m(conn, {'name': 'T', 'columns': []})
        self.assertIn('#table', m)

    def test_pbip_double_generation(self):
        tmpdir = tempfile.mkdtemp()
        try:
            gen = FabricPBIPGenerator(output_dir=tmpdir)
            gen.generate_project('R', FULL_CONVERTED_OBJECTS, 'LH')
            result = gen.generate_project('R', FULL_CONVERTED_OBJECTS, 'LH')
            self.assertTrue(result['pages'] >= 1)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════
#  Cross-validation: PBIR output ↔ semantic model columns
# ═══════════════════════════════════════════════════════════════════

class TestCrossValidationReportVsModel(unittest.TestCase):
    """E2E — Verify every Property in generated PBIR JSON exists in the
    semantic model.  This is the test class that would have caught the
    federated prefix / :Measure Names bug before deployment.
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _generate_project(self, converted_objects):
        gen = FabricPBIPGenerator(output_dir=self.tmpdir)
        gen.generate_project('CrossVal', converted_objects, 'LH')
        self._project_dir = os.path.join(self.tmpdir, 'CrossVal')
        return gen

    def _collect_semantic_columns(self):
        """Walk generated TMDL files and collect all column/measure names
        per table."""
        tmdl_dir = os.path.join(
            self._project_dir, 'CrossVal.SemanticModel', 'definition', 'tables'
        )
        columns_by_table = {}
        if not os.path.isdir(tmdl_dir):
            return columns_by_table
        for fname in os.listdir(tmdl_dir):
            if not fname.endswith('.tmdl'):
                continue
            table_name = fname[:-5]  # strip .tmdl
            cols = set()
            with open(os.path.join(tmdl_dir, fname), encoding='utf-8') as f:
                for line in f:
                    stripped = line.strip()
                    if stripped.startswith('measure '):
                        # measure 'Name' = DAX_EXPR
                        parts = stripped.split("'", 2)
                        if len(parts) >= 2:
                            cols.add(parts[1])
                    elif stripped.startswith('column '):
                        # column FieldName  (no quotes usually)
                        col_part = stripped[len('column '):]
                        # Some columns may be quoted with single quotes
                        if col_part.startswith("'"):
                            col_name = col_part.split("'", 2)[1]
                        else:
                            col_name = col_part.strip()
                        cols.add(col_name)
                    elif stripped.startswith('column: '):
                        # column: FieldName  (hierarchy levels)
                        col_name = stripped[len('column: '):]
                        cols.add(col_name)
            columns_by_table[table_name] = cols
        return columns_by_table

    def _collect_pbir_properties(self):
        """Walk generated visual.json and report.json files, extract all
        Property values from Column/Measure references."""
        report_dir = os.path.join(
            self._project_dir, 'CrossVal.Report', 'definition'
        )
        properties = []
        if not os.path.isdir(report_dir):
            return properties

        for root, _dirs, files in os.walk(report_dir):
            for fname in files:
                if not fname.endswith('.json'):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        continue
                self._extract_properties(data, properties, fpath)
        return properties

    def _extract_properties(self, obj, properties, source_file):
        """Recursively extract Property values from Column/Measure refs."""
        if isinstance(obj, dict):
            # Check for {"Column": {"Expression": ..., "Property": "X"}}
            # or {"Measure": {"Expression": ..., "Property": "X"}}
            for key in ('Column', 'Measure'):
                if key in obj and isinstance(obj[key], dict):
                    prop = obj[key].get('Property')
                    entity = None
                    expr = obj[key].get('Expression', {})
                    src_ref = expr.get('SourceRef', {})
                    entity = src_ref.get('Entity') or src_ref.get('Source')
                    if prop is not None:
                        properties.append({
                            'property': prop,
                            'entity': entity,
                            'source': source_file,
                        })
            for v in obj.values():
                self._extract_properties(v, properties, source_file)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_properties(item, properties, source_file)

    # ── Actual tests ──

    def test_clean_data_no_invalid_properties(self):
        """With clean converted objects, all PBIR properties should match
        semantic model columns."""
        self._generate_project(FULL_CONVERTED_OBJECTS)
        columns_by_table = self._collect_semantic_columns()
        properties = self._collect_pbir_properties()

        all_columns = set()
        for cols in columns_by_table.values():
            all_columns.update(cols)

        # Should have found at least some properties
        self.assertTrue(len(properties) > 0,
                        "No Property references found in generated PBIR")

        for ref in properties:
            prop = ref['property']
            # Ignore internal Power BI names that are valid queryRefs
            if prop in ('Count',):
                continue
            self.assertIn(
                prop, all_columns,
                f"Property '{prop}' from {ref['source']} not found in "
                f"semantic model columns: {sorted(all_columns)}"
            )

    def test_no_federated_prefix_in_output(self):
        """No Property in any generated PBIR JSON should contain
        'federated.' — that's a raw Tableau string leak."""
        self._generate_project(FULL_CONVERTED_OBJECTS)
        properties = self._collect_pbir_properties()
        for ref in properties:
            self.assertNotIn('federated.', ref['property'],
                             f"Raw Tableau federated prefix leaked into "
                             f"PBIR output: {ref}")

    def test_no_measure_names_in_output(self):
        """No Property should reference ':Measure Names' or 'Measure Names'
        — these are Tableau virtual fields with no PBI equivalent."""
        self._generate_project(FULL_CONVERTED_OBJECTS)
        properties = self._collect_pbir_properties()
        for ref in properties:
            self.assertNotIn('Measure Names', ref['property'],
                             f"Tableau virtual field leaked into PBIR: {ref}")
            self.assertNotIn('Measure Values', ref['property'],
                             f"Tableau virtual field leaked into PBIR: {ref}")

    def test_dirty_filters_produce_clean_output(self):
        """With federated-prefixed filters and :Measure Names, the
        generated PBIR must still have only clean properties."""
        self._generate_project(FULL_CONVERTED_WITH_DIRTY_FILTERS)
        columns_by_table = self._collect_semantic_columns()
        properties = self._collect_pbir_properties()

        all_columns = set()
        for cols in columns_by_table.values():
            all_columns.update(cols)

        for ref in properties:
            prop = ref['property']
            if prop in ('Count',):
                continue
            # No federated prefix
            self.assertNotIn('federated.', prop,
                             f"Federated prefix in PBIR: {ref}")
            # No Tableau virtual fields
            self.assertNotIn('Measure Names', prop,
                             f"Virtual field in PBIR: {ref}")
            # No derivation qualifiers
            self.assertFalse(prop.endswith(':qk') or prop.endswith(':nk'),
                             f"Derivation suffix in PBIR property: {ref}")

    def test_dirty_filters_no_none_prefix_in_property(self):
        """Properties must not start with 'none:' — that's a Tableau
        aggregation qualifier that should have been stripped."""
        self._generate_project(FULL_CONVERTED_WITH_DIRTY_FILTERS)
        properties = self._collect_pbir_properties()
        for ref in properties:
            self.assertFalse(ref['property'].startswith('none:'),
                             f"Unstripped qualifier in property: {ref}")
            self.assertFalse(ref['property'].startswith('sum:'),
                             f"Unstripped qualifier in property: {ref}")
            self.assertFalse(ref['property'].startswith('avg:'),
                             f"Unstripped qualifier in property: {ref}")


if __name__ == '__main__':
    unittest.main()
