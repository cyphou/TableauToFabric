"""Tests for fabric_import.notebook_generator"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.notebook_generator import (
    NotebookGenerator,
    _make_var_name,
    _make_notebook,
    _markdown_cell,
    _code_cell,
)
from tests.conftest import SAMPLE_EXTRACTED


class TestMakeVarName(unittest.TestCase):
    """Tests for _make_var_name()."""

    def test_removes_brackets(self):
        self.assertEqual(_make_var_name('[dbo].[Orders]'), 'dbo_orders')

    def test_replaces_special_chars(self):
        self.assertEqual(_make_var_name('my-table!'), 'my_table')

    def test_removes_leading_digits(self):
        result = _make_var_name('123table')
        self.assertEqual(result, 'table')

    def test_lowercased(self):
        self.assertEqual(_make_var_name('MyTable'), 'mytable')

    def test_collapses_underscores(self):
        self.assertEqual(_make_var_name('a___b'), 'a_b')

    def test_empty_fallback(self):
        self.assertEqual(_make_var_name('!!!'), 'table')


class TestMakeNotebook(unittest.TestCase):
    """Tests for _make_notebook()."""

    def test_creates_valid_notebook_structure(self):
        cells = [_code_cell('print("hello")')]
        nb = _make_notebook(cells)
        self.assertEqual(nb['nbformat'], 4)
        self.assertEqual(nb['nbformat_minor'], 5)
        self.assertIn('metadata', nb)
        self.assertIn('cells', nb)
        self.assertEqual(len(nb['cells']), 1)

    def test_metadata_includes_kernel_info(self):
        nb = _make_notebook([])
        self.assertEqual(nb['metadata']['kernelspec']['name'], 'synapse_pyspark')

    def test_custom_metadata_merged(self):
        nb = _make_notebook([], metadata={'lakehouse_name': 'MyLH'})
        self.assertEqual(nb['metadata']['lakehouse_name'], 'MyLH')


class TestMarkdownCell(unittest.TestCase):
    """Tests for _markdown_cell()."""

    def test_basic(self):
        cell = _markdown_cell('# Title')
        self.assertEqual(cell['cell_type'], 'markdown')
        self.assertEqual(cell['source'], ['# Title'])

    def test_list_source(self):
        src = ['line1\n', 'line2\n']
        cell = _markdown_cell(src)
        self.assertEqual(cell['source'], src)

    def test_custom_cell_id(self):
        cell = _markdown_cell('text', cell_id='abc123')
        self.assertEqual(cell['id'], 'abc123')


class TestCodeCell(unittest.TestCase):
    """Tests for _code_cell()."""

    def test_basic(self):
        cell = _code_cell('print("hi")')
        self.assertEqual(cell['cell_type'], 'code')
        self.assertEqual(cell['source'], ['print("hi")'])
        self.assertEqual(cell['outputs'], [])
        self.assertIsNone(cell['execution_count'])

    def test_list_source(self):
        src = ['import os\n', 'print(os.getcwd())\n']
        cell = _code_cell(src)
        self.assertEqual(cell['source'], src)


class TestNotebookGenerator(unittest.TestCase):
    """Tests for NotebookGenerator.generate()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_nb_')
        self.project_name = 'TestProject'
        self.gen = NotebookGenerator(self.tmpdir, self.project_name)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_creates_etl_notebook(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        etl_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Notebook',
            'etl_pipeline.ipynb',
        )
        self.assertTrue(os.path.exists(etl_path))
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        self.assertEqual(nb['nbformat'], 4)
        self.assertIn('cells', nb)

    def test_generate_returns_stats(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        self.assertIn('cells', stats)
        self.assertIn('notebooks', stats)
        self.assertIn('calc_columns', stats)
        self.assertGreater(stats['cells'], 0)
        self.assertGreaterEqual(stats['notebooks'], 1)

    def test_creates_transformation_notebook_when_calculations_exist(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # SAMPLE_EXTRACTED has 2 calculations
        self.assertEqual(stats['notebooks'], 2)
        transform_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Notebook',
            'transformations.ipynb',
        )
        self.assertTrue(os.path.exists(transform_path))

    def test_no_transformation_notebook_without_calculations(self):
        extracted = dict(SAMPLE_EXTRACTED)
        extracted['calculations'] = []
        stats = self.gen.generate(extracted)
        self.assertEqual(stats['notebooks'], 1)

    def test_etl_includes_setup_cell(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        etl_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Notebook',
            'etl_pipeline.ipynb',
        )
        with open(etl_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        # Find code cells with spark imports
        code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
        setup_found = any(
            'SparkSession' in ''.join(c['source']) for c in code_cells
        )
        self.assertTrue(setup_found, 'Setup cell with SparkSession import not found')

    def test_empty_datasources(self):
        extracted = {
            'datasources': [],
            'custom_sql': [],
            'calculations': [],
        }
        stats = self.gen.generate(extracted)
        self.assertGreater(stats['cells'], 0)  # still gets header/setup/summary
        self.assertEqual(stats['notebooks'], 1)


class TestCalcColumnMaterialisation(unittest.TestCase):
    """Tests for calculated-column materialisation in transformations notebook."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_nb2_')
        self.gen = NotebookGenerator(self.tmpdir, 'Test')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_calc_columns_in_stats(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # SAMPLE_EXTRACTED has 2 calc columns (Revenue, Status Label)
        self.assertEqual(stats['calc_columns'], 2)

    def test_transformation_notebook_has_withColumn(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        transform_path = os.path.join(
            self.tmpdir, 'Test.Notebook', 'transformations.ipynb',
        )
        with open(transform_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
        all_code = ' '.join(
            ''.join(c['source']) for c in code_cells
        )
        self.assertIn('withColumn', all_code)

    def test_measures_documented_not_materialised(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        transform_path = os.path.join(
            self.tmpdir, 'Test.Notebook', 'transformations.ipynb',
        )
        with open(transform_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        md_cells = [c for c in nb['cells'] if c['cell_type'] == 'markdown']
        all_md = ' '.join(''.join(c['source']) for c in md_cells)
        # Measures section should mention DAX
        self.assertIn('DAX', all_md)

    def test_no_calc_columns_no_withColumn(self):
        extracted = dict(SAMPLE_EXTRACTED)
        extracted['calculations'] = [
            {'name': 'T', 'formula': 'SUM([Amount])',
             'datatype': 'real', 'role': 'measure'},
        ]
        self.gen.generate(extracted)
        transform_path = os.path.join(
            self.tmpdir, 'Test.Notebook', 'transformations.ipynb',
        )
        with open(transform_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
        all_code = ' '.join(''.join(c['source']) for c in code_cells)
        self.assertNotIn('withColumn', all_code)


if __name__ == '__main__':
    unittest.main()
