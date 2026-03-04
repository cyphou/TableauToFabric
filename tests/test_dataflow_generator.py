"""Tests for fabric_import.dataflow_generator"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from fabric_import.dataflow_generator import (
    DataflowGenerator,
    _sanitize_query_name,
    _add_lakehouse_sink,
)
from tests.conftest import SAMPLE_EXTRACTED, SAMPLE_CUSTOM_SQL


class TestSanitizeQueryName(unittest.TestCase):
    """Tests for _sanitize_query_name()."""

    def test_removes_brackets(self):
        self.assertEqual(_sanitize_query_name('[dbo].[Orders]'), 'dbo_Orders')

    def test_replaces_special_chars(self):
        self.assertEqual(_sanitize_query_name('my-query!'), 'my_query')

    def test_collapses_underscores(self):
        self.assertEqual(_sanitize_query_name('a___b'), 'a_b')

    def test_empty_fallback(self):
        self.assertEqual(_sanitize_query_name('!!!'), 'Query')

    def test_preserves_spaces(self):
        # Spaces are allowed in query names
        result = _sanitize_query_name('Order Details')
        self.assertIn('Order', result)


class TestAddLakehouseSink(unittest.TestCase):
    """Tests for _add_lakehouse_sink() — currently a pass-through."""

    def test_returns_query_unchanged(self):
        query = 'let Source = ... in Source'
        result = _add_lakehouse_sink(query, 'my_table')
        self.assertEqual(result, query)


class TestDataflowGenerator(unittest.TestCase):
    """Tests for DataflowGenerator.generate()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_df_')
        self.project_name = 'TestProject'
        self.gen = DataflowGenerator(self.tmpdir, self.project_name)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_generate_creates_definition(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        def_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Dataflow',
            'dataflow_definition.json',
        )
        self.assertTrue(os.path.exists(def_path))
        with open(def_path, 'r', encoding='utf-8') as f:
            definition = json.load(f)
        self.assertIn('queries', definition)
        self.assertIn('mashupDocument', definition)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_generate_query_count(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # 2 tables in SAMPLE_EXTRACTED → 2 queries
        self.assertEqual(stats['queries'], 2)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_generate_creates_m_files(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        self.gen.generate(SAMPLE_EXTRACTED)
        queries_dir = os.path.join(
            self.tmpdir, f'{self.project_name}.Dataflow', 'queries',
        )
        self.assertTrue(os.path.isdir(queries_dir))
        m_files = [f for f in os.listdir(queries_dir) if f.endswith('.m')]
        self.assertGreater(len(m_files), 0)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_generate_creates_mashup(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        self.gen.generate(SAMPLE_EXTRACTED)
        mashup_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Dataflow', 'mashup.pq',
        )
        self.assertTrue(os.path.exists(mashup_path))
        with open(mashup_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertIn('section Section1', content)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_custom_sql_queries(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        extracted = dict(SAMPLE_EXTRACTED)
        extracted['custom_sql'] = SAMPLE_CUSTOM_SQL
        stats = self.gen.generate(extracted)
        # 2 normal + 1 custom SQL = 3
        self.assertEqual(stats['queries'], 3)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_deduplicates_queries(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        extracted = {
            'datasources': [{
                'name': 'DS',
                'connection': {'type': 'SQL Server', 'details': {}},
                'connection_map': {},
                'tables': [
                    {'name': 'Orders', 'columns': []},
                    {'name': 'Orders', 'columns': []},
                ],
            }],
            'custom_sql': [],
        }
        stats = self.gen.generate(extracted)
        self.assertEqual(stats['queries'], 1)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_empty_datasources(self, mock_escape, mock_gen_m):
        stats = self.gen.generate({'datasources': [], 'custom_sql': []})
        self.assertEqual(stats['queries'], 0)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_lakehouse_destination_in_queries(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let Source = "test" in Source'
        self.gen.generate(SAMPLE_EXTRACTED)
        def_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Dataflow',
            'dataflow_definition.json',
        )
        with open(def_path, 'r', encoding='utf-8') as f:
            definition = json.load(f)
        for q in definition['queries']:
            self.assertEqual(q['destination']['type'], 'Lakehouse')

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_calc_columns_in_stats(self, mock_escape, mock_gen_m):
        mock_gen_m.return_value = 'let\n    Source = "test",\n    Result = Source\nin\n    Result'
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # SAMPLE_EXTRACTED now has 2 calc columns
        self.assertIn('calc_columns', stats)
        self.assertEqual(stats['calc_columns'], 2)

    @patch('fabric_import.dataflow_generator.generate_power_query_m')
    @patch('fabric_import.dataflow_generator._reverse_tableau_bracket_escape')
    def test_calc_columns_injected_into_m_query(self, mock_escape, mock_gen_m):
        """Calc columns should add Table.AddColumn steps to the main query."""
        mock_gen_m.return_value = 'let\n    Source = "test",\n    Result = Source\nin\n    Result'
        self.gen.generate(SAMPLE_EXTRACTED)
        # Check the mashup file which has all queries combined
        mashup_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Dataflow', 'mashup.pq',
        )
        with open(mashup_path, 'r', encoding='utf-8') as f:
            content = f.read()
        self.assertIn('Table.AddColumn', content)


if __name__ == '__main__':
    unittest.main()
