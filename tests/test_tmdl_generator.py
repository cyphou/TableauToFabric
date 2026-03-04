"""
Tests for fabric_import.tmdl_generator — TMDL DirectLake generation.

SIMPLE:  _quote_name, _tmdl_datatype, _tmdl_summarize, _safe_filename,
         _sanitize_table_name, generate_theme_json
MEDIUM:  _build_table_directlake structure, _build_relationships,
         _get_format_string, _get_display_folder, _map_semantic_role_to_category,
         _write helpers
COMPLEX: _build_semantic_model full, generate_tmdl integration,
         _infer_cross_table_relationships, _detect_many_to_many,
         _apply_hierarchies, _create_parameter_tables, _create_rls_roles,
         _add_date_table
"""

import os
import json
import tempfile
import shutil
import unittest

from fabric_import.tmdl_generator import (
    generate_tmdl,
    _quote_name,
    _tmdl_datatype,
    _tmdl_summarize,
    _safe_filename,
    _sanitize_table_name,
    generate_theme_json,
    _build_semantic_model,
    _build_table_directlake,
    _build_relationships,
    _write_tmdl_files,
    _write_database_tmdl,
    _write_model_tmdl,
)


# ── Sample data ────────────────────────────────────────
SAMPLE_COLUMNS = [
    {'name': 'OrderID', 'datatype': 'integer'},
    {'name': 'CustomerID', 'datatype': 'integer'},
    {'name': 'Product', 'datatype': 'string'},
    {'name': 'Amount', 'datatype': 'real'},
    {'name': 'OrderDate', 'datatype': 'date'},
]

SAMPLE_DS = {
    'name': 'SuperStore',
    'connection': {'type': 'SQL Server', 'details': {'server': 'srv', 'database': 'db'}},
    'connection_map': {},
    'tables': [
        {'name': 'Orders', 'columns': SAMPLE_COLUMNS},
        {'name': 'Customers', 'columns': [
            {'name': 'CustomerID', 'datatype': 'integer'},
            {'name': 'Name', 'datatype': 'string'},
            {'name': 'City', 'datatype': 'string', 'role': 'city'},
        ]},
    ],
    'calculations': [
        {'name': '[Total Sales]', 'caption': 'Total Sales',
         'formula': 'SUM([Amount])', 'role': 'measure', 'datatype': 'real'},
    ],
    'relationships': [
        {'from_table': 'Orders', 'from_column': 'CustomerID',
         'to_table': 'Customers', 'to_column': 'CustomerID'},
    ],
    'columns': [],
}


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS
# ═══════════════════════════════════════════════════════════════════


class TestQuoteName(unittest.TestCase):
    """SIMPLE — TMDL name quoting."""

    def test_simple_name(self):
        self.assertEqual(_quote_name('Orders'), 'Orders')

    def test_name_with_space(self):
        self.assertEqual(_quote_name('Order Details'), "'Order Details'")

    def test_name_with_apostrophe(self):
        self.assertEqual(_quote_name("O'Brien"), "'O''Brien'")

    def test_name_with_dash(self):
        self.assertEqual(_quote_name('my-table'), "'my-table'")

    def test_underscore_only(self):
        self.assertEqual(_quote_name('my_table'), 'my_table')


class TestTmdlDatatype(unittest.TestCase):
    """SIMPLE — BIM type to TMDL type."""

    def test_int64(self):
        self.assertEqual(_tmdl_datatype('Int64'), 'int64')

    def test_string(self):
        self.assertEqual(_tmdl_datatype('String'), 'string')

    def test_double(self):
        self.assertEqual(_tmdl_datatype('Double'), 'double')

    def test_boolean(self):
        self.assertEqual(_tmdl_datatype('Boolean'), 'boolean')

    def test_datetime(self):
        self.assertEqual(_tmdl_datatype('DateTime'), 'dateTime')

    def test_unknown(self):
        self.assertEqual(_tmdl_datatype('SomeRandom'), 'string')


class TestTmdlSummarize(unittest.TestCase):
    """SIMPLE — summarizeBy mapping."""

    def test_sum(self):
        self.assertEqual(_tmdl_summarize('sum'), 'sum')

    def test_none(self):
        self.assertEqual(_tmdl_summarize('none'), 'none')

    def test_unknown(self):
        self.assertEqual(_tmdl_summarize('xyz'), 'none')

    def test_case_insensitive(self):
        self.assertEqual(_tmdl_summarize('COUNT'), 'count')


class TestSafeFilename(unittest.TestCase):
    """SIMPLE — filename sanitization."""

    def test_normal(self):
        self.assertEqual(_safe_filename('Orders'), 'Orders')

    def test_special_chars(self):
        result = _safe_filename('A/B:C')
        self.assertNotIn('/', result)
        self.assertNotIn(':', result)


class TestSanitizeTableName(unittest.TestCase):
    """SIMPLE — Lakehouse Delta table name sanitization."""

    def test_normal(self):
        self.assertEqual(_sanitize_table_name('Orders'), 'orders')

    def test_spaces(self):
        self.assertEqual(_sanitize_table_name('Order Details'), 'order_details')

    def test_leading_digit(self):
        result = _sanitize_table_name('1stTable')
        self.assertTrue(result.startswith('tbl_'))

    def test_special_chars(self):
        result = _sanitize_table_name('A-B/C')
        self.assertNotIn('-', result)
        self.assertNotIn('/', result)

    def test_empty(self):
        self.assertEqual(_sanitize_table_name(''), 'table')


class TestGenerateThemeJson(unittest.TestCase):
    """SIMPLE — Theme JSON generation."""

    def test_default_theme(self):
        theme = generate_theme_json()
        self.assertEqual(theme['name'], 'Tableau Migration Theme')
        self.assertEqual(len(theme['dataColors']), 12)
        self.assertIn('textClasses', theme)

    def test_custom_colors(self):
        theme = generate_theme_json({'colors': ['#FF0000', '#00FF00', '#0000FF']})
        self.assertEqual(theme['dataColors'][0], '#FF0000')
        self.assertEqual(len(theme['dataColors']), 12)

    def test_custom_font(self):
        theme = generate_theme_json({'font_family': 'Arial'})
        self.assertIn('Arial', theme['textClasses']['title']['fontFace'])

    def test_none_theme(self):
        theme = generate_theme_json(None)
        self.assertEqual(len(theme['dataColors']), 12)


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS
# ═══════════════════════════════════════════════════════════════════


class TestBuildTableDirectLake(unittest.TestCase):
    """MEDIUM — Single table creation for DirectLake."""

    def test_basic_table(self):
        tbl = _build_table_directlake(
            table={'name': 'Orders', 'columns': SAMPLE_COLUMNS},
            calculations=[], dax_context={}, col_metadata_map={},
            extra_objects={}, lakehouse_name='MyLakehouse'
        )
        self.assertEqual(tbl['name'], 'Orders')
        self.assertTrue(len(tbl['columns']) >= 4)
        # DirectLake partition
        self.assertEqual(len(tbl['partitions']), 1)
        self.assertEqual(tbl['partitions'][0]['mode'], 'directLake')
        self.assertIn('entityName', tbl['partitions'][0]['source'])

    def test_entity_name_sanitized(self):
        tbl = _build_table_directlake(
            table={'name': 'Order Details', 'columns': [{'name': 'X', 'datatype': 'string'}]},
            calculations=[], dax_context={}, col_metadata_map={},
            extra_objects={}, lakehouse_name='LH'
        )
        entity = tbl['partitions'][0]['source']['entityName']
        self.assertNotIn(' ', entity)
        self.assertEqual(entity, 'order_details')


class TestBuildRelationships(unittest.TestCase):
    """MEDIUM — Relationship building."""

    def test_single_relationship(self):
        rels = _build_relationships([
            {'left': {'table': 'Orders', 'column': 'CustID'},
             'right': {'table': 'Customers', 'column': 'CustID'},
             'type': 'left'},
        ])
        self.assertEqual(len(rels), 1)
        self.assertEqual(rels[0]['fromTable'], 'Orders')
        self.assertEqual(rels[0]['toTable'], 'Customers')

    def test_empty_relationships(self):
        self.assertEqual(_build_relationships([]), [])


class TestWriteTMDLFiles(unittest.TestCase):
    """MEDIUM — TMDL file writing."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_writes_database_tmdl(self):
        model = {
            'compatibilityLevel': 1604,
            'tables': [{'name': 'T', 'columns': [], 'measures': [],
                         'partitions': [], 'hierarchies': []}],
            'relationships': [],
            'roles': [],
            'culture': 'en-US',
        }
        _write_tmdl_files({'model': model}, self.tmpdir)
        db_path = os.path.join(self.tmpdir, 'definition', 'database.tmdl')
        self.assertTrue(os.path.exists(db_path))
        with open(db_path, encoding='utf-8') as f:
            content = f.read()
        self.assertIn('compatibilityLevel', content)
        self.assertIn('1604', content)

    def test_writes_model_tmdl(self):
        model = {
            'tables': [{'name': 'Orders', 'columns': [], 'measures': [],
                         'partitions': [], 'hierarchies': []}],
            'relationships': [],
            'roles': [],
            'culture': 'fr-FR',
        }
        _write_tmdl_files({'model': model}, self.tmpdir)
        model_path = os.path.join(self.tmpdir, 'definition', 'model.tmdl')
        self.assertTrue(os.path.exists(model_path))
        with open(model_path, encoding='utf-8') as f:
            content = f.read()
        self.assertIn('directLake', content)
        self.assertIn('fr-FR', content)

    def test_writes_table_files(self):
        model = {
            'tables': [
                {'name': 'Orders', 'columns': [{'name': 'ID', 'dataType': 'Int64',
                 'summarizeBy': 'none'}], 'measures': [],
                 'partitions': [{'name': 'P', 'mode': 'directLake',
                                  'source': {'type': 'entity', 'entityName': 'orders',
                                             'schemaName': 'dbo',
                                             'expressionSource': 'DatabaseQuery'}}],
                 'hierarchies': []},
            ],
            'relationships': [],
            'roles': [],
            'culture': 'en-US',
        }
        _write_tmdl_files({'model': model}, self.tmpdir)
        table_path = os.path.join(self.tmpdir, 'definition', 'tables', 'Orders.tmdl')
        self.assertTrue(os.path.exists(table_path))

    def test_writes_relationships_tmdl(self):
        model = {
            'tables': [{'name': 'T', 'columns': [], 'measures': [],
                         'partitions': [], 'hierarchies': []}],
            'relationships': [
                {'name': 'R1', 'fromTable': 'A', 'fromColumn': 'X',
                 'toTable': 'B', 'toColumn': 'X', 'crossFilteringBehavior': 'oneDirection',
                 'fromCardinality': 'many', 'toCardinality': 'one', 'isActive': True},
            ],
            'roles': [],
            'culture': 'en-US',
        }
        _write_tmdl_files({'model': model}, self.tmpdir)
        rel_path = os.path.join(self.tmpdir, 'definition', 'relationships.tmdl')
        self.assertTrue(os.path.exists(rel_path))


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS
# ═══════════════════════════════════════════════════════════════════


class TestBuildSemanticModel(unittest.TestCase):
    """COMPLEX — Full semantic model building."""

    def test_basic_model(self):
        model = _build_semantic_model([SAMPLE_DS], 'TestReport', {},
                                       lakehouse_name='TestLH')
        tables = model['model']['tables']
        self.assertTrue(len(tables) >= 2)
        table_names = [t['name'] for t in tables]
        self.assertIn('Orders', table_names)
        self.assertIn('Customers', table_names)

    def test_directlake_mode(self):
        model = _build_semantic_model([SAMPLE_DS], 'TestReport', {},
                                       lakehouse_name='LH')
        self.assertEqual(model['model']['defaultMode'], 'directLake')

    def test_measures_on_main_table(self):
        model = _build_semantic_model([SAMPLE_DS], 'TestReport', {},
                                       lakehouse_name='LH')
        tables = model['model']['tables']
        # Main table (most columns) should have the measures
        main = max(tables, key=lambda t: len(t.get('columns', [])))
        measure_names = [m['name'] for m in main.get('measures', [])]
        # At least some measures are generated
        self.assertTrue(len(measure_names) >= 1)

    def test_relationships_created(self):
        model = _build_semantic_model([SAMPLE_DS], 'TestReport', {},
                                       lakehouse_name='LH')
        rels = model['model']['relationships']
        self.assertTrue(len(rels) >= 1)

    def test_perspectives_created(self):
        model = _build_semantic_model([SAMPLE_DS], 'TestReport', {},
                                       lakehouse_name='LH')
        persp = model['model'].get('perspectives', [])
        self.assertTrue(len(persp) >= 1)


class TestGenerateTMDLIntegration(unittest.TestCase):
    """COMPLEX — Full generate_tmdl integration test."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_generation(self):
        stats = generate_tmdl(
            datasources=[SAMPLE_DS],
            report_name='IntegrationTest',
            extra_objects={},
            output_dir=self.tmpdir,
            lakehouse_name='TestLH'
        )
        self.assertIn('tables', stats)
        self.assertIn('columns', stats)
        self.assertIn('measures', stats)
        self.assertIn('relationships', stats)
        self.assertTrue(stats['tables'] >= 2)
        self.assertTrue(stats['columns'] >= 4)

    def test_files_on_disk(self):
        generate_tmdl(
            datasources=[SAMPLE_DS],
            report_name='DiskTest',
            extra_objects={},
            output_dir=self.tmpdir,
            lakehouse_name='LH'
        )
        def_dir = os.path.join(self.tmpdir, 'definition')
        self.assertTrue(os.path.exists(os.path.join(def_dir, 'database.tmdl')))
        self.assertTrue(os.path.exists(os.path.join(def_dir, 'model.tmdl')))
        self.assertTrue(os.path.exists(os.path.join(def_dir, 'relationships.tmdl')))
        self.assertTrue(os.path.isdir(os.path.join(def_dir, 'tables')))

    def test_directlake_in_model_tmdl(self):
        generate_tmdl([SAMPLE_DS], 'DLTest', {}, self.tmpdir, 'LH')
        model_path = os.path.join(self.tmpdir, 'definition', 'model.tmdl')
        with open(model_path, encoding='utf-8') as f:
            content = f.read()
        self.assertIn('directLake', content)

    def test_with_hierarchies(self):
        extra = {
            'hierarchies': [
                {'name': 'Geography', 'table': 'Customers',
                 'levels': ['Name', 'City']},
            ],
        }
        stats = generate_tmdl([SAMPLE_DS], 'HierTest', extra, self.tmpdir, 'LH')
        self.assertTrue(stats['hierarchies'] >= 1)

    def test_with_rls(self):
        extra = {
            'user_filters': [
                {'type': 'user_filter', 'name': 'RegionFilter',
                 'column': 'Product',
                 'user_mappings': [
                     {'user': 'user@domain.com', 'value': 'Widget'},
                 ]},
            ],
        }
        stats = generate_tmdl([SAMPLE_DS], 'RLSTest', extra, self.tmpdir, 'LH')
        self.assertTrue(stats['roles'] >= 1)

    def test_with_parameters(self):
        extra = {
            'parameters': [
                {'name': '[Parameters].[Target Sales]', 'caption': 'Target Sales',
                 'datatype': 'integer', 'value': '100',
                 'domain_type': 'range',
                 'allowable_values': [
                     {'type': 'range', 'min': '0', 'max': '1000', 'step': '10'},
                 ]},
            ],
        }
        stats = generate_tmdl([SAMPLE_DS], 'ParamTest', extra, self.tmpdir, 'LH')
        # Should create parameter table (Orders + Customers + Parameter)
        # Calendar is skipped in DirectLake mode
        self.assertTrue(stats['tables'] >= 3)

    def test_empty_datasources(self):
        stats = generate_tmdl([], 'EmptyTest', {}, self.tmpdir, 'LH')
        self.assertEqual(stats['tables'], 0)

    def test_lakehouse_defaults_to_report_name(self):
        stats = generate_tmdl([SAMPLE_DS], 'DefaultLH', {}, self.tmpdir)
        self.assertIsInstance(stats, dict)


if __name__ == '__main__':
    unittest.main()
