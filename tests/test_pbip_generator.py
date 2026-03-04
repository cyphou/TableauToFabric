"""
Tests for fabric_import.pbip_generator — PBIP project generation.

SIMPLE:  create_pbip_file, _build_report_json, _make_visual_position,
         _clean_field_name, _is_measure_field
MEDIUM:  create_semantic_model_structure, _build_field_mapping,
         _resolve_field_entity, _find_worksheet, _find_column_table,
         create_report_structure (simple dashboards)
COMPLEX: generate_project full integration, theme application,
         tooltip pages, slicer visuals
"""

import os
import json
import tempfile
import shutil
import unittest

from fabric_import.pbip_generator import FabricPBIPGenerator


# ── Sample data ────────────────────────────────────────
SAMPLE_DS = {
    'name': 'DS1',
    'connection': {'type': 'SQL Server', 'details': {'server': 's', 'database': 'd'}},
    'connection_map': {},
    'tables': [
        {'name': 'Orders', 'columns': [
            {'name': 'OrderID', 'datatype': 'integer'},
            {'name': 'Amount', 'datatype': 'real'},
            {'name': 'Region', 'datatype': 'string'},
            {'name': 'OrderDate', 'datatype': 'date'},
        ]},
    ],
    'calculations': [
        {'name': '[Total Sales]', 'caption': 'Total Sales',
         'formula': 'SUM([Amount])', 'role': 'measure', 'datatype': 'real'},
    ],
    'relationships': [],
    'columns': [],
}

SAMPLE_CONVERTED_OBJECTS = {
    'datasources': [SAMPLE_DS],
    'worksheets': [
        {'name': 'Sales By Region', 'chart_type': 'clusteredBarChart',
         'fields': [
             {'name': 'Region', 'role': 'dimension'},
             {'name': 'sum:Amount', 'role': 'measure'},
         ]},
    ],
    'dashboards': [
        {'name': 'Overview',
         'size': {'width': 1280, 'height': 720},
         'objects': [
             {'type': 'worksheetReference', 'worksheetName': 'Sales By Region',
              'position': {'x': 10, 'y': 10, 'w': 600, 'h': 400}},
         ]},
    ],
    'calculations': [],
    'parameters': [],
    'hierarchies': [],
    'sets': [],
    'groups': [],
    'bins': [],
    'aliases': {},
    'user_filters': [],
    'filters': [],
    'stories': [],
    'actions': [],
    'custom_sql': [],
}


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS
# ═══════════════════════════════════════════════════════════════════


class TestCreatePBIPFile(unittest.TestCase):
    """SIMPLE — .pbip file creation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_pbip(self):
        pbip_path = self.gen.create_pbip_file(self.tmpdir, 'TestReport')
        self.assertTrue(os.path.exists(pbip_path))
        self.assertTrue(pbip_path.endswith('.pbip'))

    def test_pbip_content(self):
        pbip_path = self.gen.create_pbip_file(self.tmpdir, 'TestReport')
        with open(pbip_path, encoding='utf-8') as f:
            data = json.load(f)
        self.assertIn('$schema', data)
        self.assertIn('artifacts', data)
        self.assertEqual(data['artifacts'][0]['report']['path'], 'TestReport.Report')

    def test_gitignore_created(self):
        self.gen.create_pbip_file(self.tmpdir, 'TestReport')
        gi = os.path.join(self.tmpdir, '.gitignore')
        self.assertTrue(os.path.exists(gi))


class TestBuildReportJson(unittest.TestCase):
    """SIMPLE — report.json generation."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()

    def test_no_theme(self):
        rj = self.gen._build_report_json(None)
        self.assertIn('$schema', rj)
        self.assertIn('themeCollection', rj)
        self.assertNotIn('customTheme', rj.get('themeCollection', {}))

    def test_with_theme(self):
        rj = self.gen._build_report_json({'colors': ['#FF0000']})
        tc = rj.get('themeCollection', {})
        self.assertIn('customTheme', tc)


class TestMakeVisualPosition(unittest.TestCase):
    """SIMPLE — visual positioning helper."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()

    def test_basic_position(self):
        pos = {'x': 100, 'y': 50, 'w': 300, 'h': 200}
        result = self.gen._make_visual_position(pos, 1.0, 1.0, 0)
        self.assertEqual(result['x'], 100)
        self.assertEqual(result['y'], 50)
        self.assertEqual(result['width'], 300)
        self.assertEqual(result['height'], 200)

    def test_scaled_position(self):
        pos = {'x': 100, 'y': 100, 'w': 200, 'h': 200}
        result = self.gen._make_visual_position(pos, 2.0, 0.5, 1)
        self.assertEqual(result['x'], 200)
        self.assertEqual(result['y'], 50)
        self.assertEqual(result['width'], 400)
        self.assertEqual(result['height'], 100)


class TestCleanFieldName(unittest.TestCase):
    """SIMPLE — field name cleaning."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()

    def test_sum_prefix(self):
        self.assertEqual(self.gen._clean_field_name('sum:Amount'), 'Amount')

    def test_none_prefix(self):
        self.assertEqual(self.gen._clean_field_name('none:Region'), 'Region')

    def test_no_prefix(self):
        self.assertEqual(self.gen._clean_field_name('Sales'), 'Sales')

    def test_year_prefix(self):
        self.assertEqual(self.gen._clean_field_name('yr:OrderDate'), 'OrderDate')

    def test_suffix_removal(self):
        result = self.gen._clean_field_name('Region:nk')
        self.assertEqual(result, 'Region')


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS
# ═══════════════════════════════════════════════════════════════════


class TestBuildFieldMapping(unittest.TestCase):
    """MEDIUM — field mapping from converted objects."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()

    def test_maps_columns(self):
        self.gen._build_field_mapping(SAMPLE_CONVERTED_OBJECTS)
        self.assertIn('OrderID', self.gen._field_map)
        self.assertIn('Amount', self.gen._field_map)
        entity, prop = self.gen._field_map['OrderID']
        self.assertEqual(entity, 'Orders')

    def test_maps_measures(self):
        self.gen._build_field_mapping(SAMPLE_CONVERTED_OBJECTS)
        self.assertIn('Total Sales', self.gen._measure_names)

    def test_main_table(self):
        self.gen._build_field_mapping(SAMPLE_CONVERTED_OBJECTS)
        self.assertEqual(self.gen._main_table, 'Orders')


class TestIsMeasureField(unittest.TestCase):
    """MEDIUM — measure detection."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()
        self.gen._build_field_mapping(SAMPLE_CONVERTED_OBJECTS)

    def test_measure(self):
        self.assertTrue(self.gen._is_measure_field('Total Sales'))

    def test_dimension(self):
        self.assertFalse(self.gen._is_measure_field('Region'))


class TestFindWorksheet(unittest.TestCase):
    """MEDIUM — worksheet lookup."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()

    def test_found(self):
        worksheets = [{'name': 'A'}, {'name': 'B'}]
        self.assertEqual(self.gen._find_worksheet(worksheets, 'B'), {'name': 'B'})

    def test_not_found(self):
        self.assertIsNone(self.gen._find_worksheet([{'name': 'A'}], 'Z'))


class TestResolveFieldEntity(unittest.TestCase):
    """MEDIUM — field entity resolution."""

    def setUp(self):
        self.gen = FabricPBIPGenerator()
        self.gen._build_field_mapping(SAMPLE_CONVERTED_OBJECTS)

    def test_known_field(self):
        entity, prop = self.gen._resolve_field_entity('Amount')
        self.assertEqual(entity, 'Orders')
        self.assertEqual(prop, 'Amount')

    def test_unknown_field(self):
        entity, prop = self.gen._resolve_field_entity('Unknown')
        self.assertEqual(prop, 'Unknown')


class TestCreateSemanticModelStructure(unittest.TestCase):
    """MEDIUM — SemanticModel + TMDL generation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_creates_structure(self):
        sm_dir = self.gen.create_semantic_model_structure(
            self.tmpdir, 'TestReport', SAMPLE_CONVERTED_OBJECTS, 'TestLH'
        )
        self.assertTrue(os.path.exists(sm_dir))
        self.assertTrue(os.path.exists(os.path.join(sm_dir, '.platform')))
        self.assertTrue(os.path.exists(os.path.join(sm_dir, 'definition.pbism')))

    def test_platform_type(self):
        sm_dir = self.gen.create_semantic_model_structure(
            self.tmpdir, 'TestReport', SAMPLE_CONVERTED_OBJECTS, 'LH'
        )
        with open(os.path.join(sm_dir, '.platform'), encoding='utf-8') as f:
            data = json.load(f)
        self.assertEqual(data['metadata']['type'], 'SemanticModel')


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS
# ═══════════════════════════════════════════════════════════════════


class TestGenerateProjectIntegration(unittest.TestCase):
    """COMPLEX — Full project generation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_full_generation(self):
        result = self.gen.generate_project('TestProject', SAMPLE_CONVERTED_OBJECTS,
                                           lakehouse_name='TestLH')
        self.assertIn('pages', result)
        self.assertIn('visuals', result)
        self.assertTrue(result['pages'] >= 1)

    def test_creates_all_dirs(self):
        self.gen.generate_project('TestProject', SAMPLE_CONVERTED_OBJECTS, 'LH')
        project_dir = os.path.join(self.tmpdir, 'TestProject')
        self.assertTrue(os.path.exists(project_dir))
        self.assertTrue(os.path.exists(os.path.join(project_dir, 'TestProject.pbip')))
        self.assertTrue(os.path.isdir(os.path.join(project_dir, 'TestProject.SemanticModel')))
        self.assertTrue(os.path.isdir(os.path.join(project_dir, 'TestProject.Report')))

    def test_metadata_created(self):
        self.gen.generate_project('TestProject', SAMPLE_CONVERTED_OBJECTS, 'LH')
        meta_path = os.path.join(self.tmpdir, 'TestProject', 'migration_metadata.json')
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, encoding='utf-8') as f:
            meta = json.load(f)
        self.assertEqual(meta['target'], 'Microsoft Fabric (DirectLake)')

    def test_report_structure_pbir(self):
        self.gen.generate_project('TestProject', SAMPLE_CONVERTED_OBJECTS, 'LH')
        pbir_path = os.path.join(self.tmpdir, 'TestProject',
                                  'TestProject.Report', 'definition.pbir')
        self.assertTrue(os.path.exists(pbir_path))
        with open(pbir_path, encoding='utf-8') as f:
            data = json.load(f)
        self.assertEqual(data['version'], '4.0')
        self.assertIn('byPath', data['datasetReference'])

    def test_pages_json_created(self):
        self.gen.generate_project('TestProject', SAMPLE_CONVERTED_OBJECTS, 'LH')
        pages_json = os.path.join(self.tmpdir, 'TestProject',
                                   'TestProject.Report', 'definition', 'pages', 'pages.json')
        self.assertTrue(os.path.exists(pages_json))

    def test_empty_input(self):
        empty = {
            'datasources': [], 'worksheets': [], 'dashboards': [],
            'calculations': [], 'parameters': [], 'hierarchies': [],
            'sets': [], 'groups': [], 'bins': [], 'aliases': {},
            'user_filters': [], 'filters': [], 'stories': [],
            'actions': [], 'custom_sql': [],
        }
        result = self.gen.generate_project('EmptyProject', empty, 'LH')
        self.assertIn('pages', result)


class TestProjectWithTheme(unittest.TestCase):
    """COMPLEX — Theme application."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_custom_theme_applied(self):
        objects = dict(SAMPLE_CONVERTED_OBJECTS)
        objects['dashboards'] = [
            {'name': 'D1', 'size': {'width': 1280, 'height': 720},
             'theme': {'colors': ['#FF0000', '#00FF00']},
             'objects': [
                 {'type': 'worksheetReference', 'worksheetName': 'Sales By Region',
                  'position': {'x': 0, 'y': 0, 'w': 600, 'h': 400}},
             ]},
        ]
        self.gen.generate_project('ThemeTest', objects, 'LH')
        theme_path = os.path.join(
            self.tmpdir, 'ThemeTest', 'ThemeTest.Report', 'definition',
            'RegisteredResources', 'TableauMigrationTheme.json'
        )
        self.assertTrue(os.path.exists(theme_path))


class TestProjectWithTooltip(unittest.TestCase):
    """COMPLEX — Tooltip pages."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_tooltip_page_created(self):
        objects = dict(SAMPLE_CONVERTED_OBJECTS)
        objects['worksheets'] = [
            {'name': 'Details', 'chart_type': 'tableEx',
             'tooltip': {'viz_in_tooltip': True},
             'fields': [{'name': 'Region'}]},
            {'name': 'Main', 'chart_type': 'clusteredBarChart',
             'fields': [{'name': 'Region'}]},
        ]
        self.gen.generate_project('TooltipTest', objects, 'LH')
        pages_json = os.path.join(self.tmpdir, 'TooltipTest',
                                   'TooltipTest.Report', 'definition',
                                   'pages', 'pages.json')
        with open(pages_json, encoding='utf-8') as f:
            pages_meta = json.load(f)
        # Should have at least main page + tooltip page
        self.assertTrue(len(pages_meta['pageOrder']) >= 2)


class TestIdempotentRegeneration(unittest.TestCase):
    """COMPLEX — Re-running generation should clean up old files."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = FabricPBIPGenerator(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_second_run_succeeds(self):
        self.gen.generate_project('R', SAMPLE_CONVERTED_OBJECTS, 'LH')
        result = self.gen.generate_project('R', SAMPLE_CONVERTED_OBJECTS, 'LH')
        self.assertIn('pages', result)


if __name__ == '__main__':
    unittest.main()
