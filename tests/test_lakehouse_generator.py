"""Tests for fabric_import.lakehouse_generator"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.lakehouse_generator import (
    LakehouseGenerator,
    _map_to_spark_type,
    _sanitize_column_name,
    _sanitize_table_name,
)
from tests.conftest import SAMPLE_EXTRACTED, SAMPLE_CUSTOM_SQL


class TestMapToSparkType(unittest.TestCase):
    """Tests for _map_to_spark_type()."""

    def test_known_types(self):
        cases = {
            'string': 'STRING',
            'integer': 'INT',
            'int64': 'BIGINT',
            'real': 'DOUBLE',
            'double': 'DOUBLE',
            'number': 'DOUBLE',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'time': 'STRING',
            'spatial': 'STRING',
            'binary': 'BINARY',
            'currency': 'DECIMAL(19,4)',
            'percentage': 'DOUBLE',
        }
        for tableau_type, expected in cases.items():
            with self.subTest(tableau_type=tableau_type):
                self.assertEqual(_map_to_spark_type(tableau_type), expected)

    def test_case_insensitive(self):
        self.assertEqual(_map_to_spark_type('STRING'), 'STRING')
        self.assertEqual(_map_to_spark_type('Integer'), 'INT')

    def test_unknown_type_defaults_to_string(self):
        self.assertEqual(_map_to_spark_type('xml'), 'STRING')
        self.assertEqual(_map_to_spark_type('custom_thing'), 'STRING')


class TestSanitizeTableName(unittest.TestCase):
    """Tests for _sanitize_table_name()."""

    def test_removes_schema_prefix(self):
        self.assertEqual(_sanitize_table_name('[dbo].[Orders]'), 'orders')

    def test_removes_brackets(self):
        self.assertEqual(_sanitize_table_name('[MyTable]'), 'mytable')

    def test_replaces_spaces(self):
        self.assertEqual(_sanitize_table_name('my table'), 'my_table')

    def test_removes_special_chars(self):
        self.assertEqual(_sanitize_table_name('table$name!'), 'table_name')

    def test_removes_leading_digits(self):
        self.assertEqual(_sanitize_table_name('123table'), 'table')

    def test_collapses_underscores(self):
        self.assertEqual(_sanitize_table_name('a___b'), 'a_b')

    def test_empty_string_returns_table(self):
        self.assertEqual(_sanitize_table_name('!!!'), 'table')

    def test_lowercased(self):
        self.assertEqual(_sanitize_table_name('MyTable'), 'mytable')

    def test_schema_dot_table(self):
        self.assertEqual(_sanitize_table_name('dbo.Orders'), 'orders')


class TestSanitizeColumnName(unittest.TestCase):
    """Tests for _sanitize_column_name()."""

    def test_removes_brackets(self):
        self.assertEqual(_sanitize_column_name('[OrderID]'), 'OrderID')

    def test_replaces_special_chars(self):
        self.assertEqual(_sanitize_column_name('col name!'), 'col_name')

    def test_leading_digit_replaced(self):
        result = _sanitize_column_name('1col')
        self.assertFalse(result.startswith('1'))

    def test_empty_fallback(self):
        self.assertEqual(_sanitize_column_name('!!!'), 'column')


class TestLakehouseGenerator(unittest.TestCase):
    """Tests for LakehouseGenerator.generate()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_lh_')
        self.project_name = 'TestProject'
        self.gen = LakehouseGenerator(self.tmpdir, self.project_name)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_creates_definition_file(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        def_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Lakehouse',
            'lakehouse_definition.json',
        )
        self.assertTrue(os.path.exists(def_path))
        with open(def_path, 'r', encoding='utf-8') as f:
            definition = json.load(f)
        self.assertIn('tables', definition)
        self.assertIn('$schema', definition)

    def test_generate_stats(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # SAMPLE_EXTRACTED has 2 tables (Orders, Products)
        self.assertEqual(stats['tables'], 2)
        # Orders has 5 columns + 2 calc columns, Products has 3 => 10 total
        self.assertEqual(stats['columns'], 10)

    def test_generate_creates_ddl_files(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        ddl_dir = os.path.join(
            self.tmpdir, f'{self.project_name}.Lakehouse', 'ddl',
        )
        self.assertTrue(os.path.isdir(ddl_dir))
        # Individual DDL per table + _all_tables.sql
        sql_files = [f for f in os.listdir(ddl_dir) if f.endswith('.sql')]
        self.assertGreaterEqual(len(sql_files), 2)  # at least 2 table DDLs

    def test_ddl_content_uses_delta(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        ddl_dir = os.path.join(
            self.tmpdir, f'{self.project_name}.Lakehouse', 'ddl',
        )
        # Check combined file
        combined = os.path.join(ddl_dir, '_all_tables.sql')
        if os.path.exists(combined):
            with open(combined, 'r', encoding='utf-8') as f:
                content = f.read()
            self.assertIn('USING DELTA', content)
            self.assertIn('CREATE TABLE IF NOT EXISTS', content)

    def test_generate_creates_table_metadata(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        meta_path = os.path.join(
            self.tmpdir, f'{self.project_name}.Lakehouse',
            'table_metadata.json',
        )
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        # Should have entries for each unique table
        self.assertGreater(len(metadata), 0)

    def test_deduplicates_tables(self):
        """Tables with the same sanitized name should be deduplicated."""
        extracted = {
            'datasources': [
                {
                    'name': 'DS1',
                    'connection': {'type': 'SQL Server', 'details': {}},
                    'tables': [
                        {'name': 'orders', 'columns': []},
                        {'name': 'Orders', 'columns': []},  # same after sanitize
                    ],
                }
            ],
            'custom_sql': [],
        }
        stats = self.gen.generate(extracted)
        self.assertEqual(stats['tables'], 1)

    def test_custom_sql_tables(self):
        extracted = dict(SAMPLE_EXTRACTED)
        extracted['custom_sql'] = SAMPLE_CUSTOM_SQL
        stats = self.gen.generate(extracted)
        # 2 normal tables + 1 custom SQL
        self.assertEqual(stats['tables'], 3)

    def test_empty_datasources(self):
        stats = self.gen.generate({'datasources': [], 'custom_sql': []})
        self.assertEqual(stats['tables'], 0)
        self.assertEqual(stats['columns'], 0)

    def test_calc_columns_added_to_main_table(self):
        """Calculated columns (row-level) should appear as physical columns."""
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        # SAMPLE_EXTRACTED has 2 calc columns: Revenue, Status Label
        self.assertIn('calc_columns', stats)
        self.assertEqual(stats['calc_columns'], 2)

    def test_calc_columns_in_ddl(self):
        """DDL should annotate calculated columns."""
        self.gen.generate(SAMPLE_EXTRACTED)
        ddl_dir = os.path.join(
            self.tmpdir, f'{self.project_name}.Lakehouse', 'ddl',
        )
        combined = os.path.join(ddl_dir, '_all_tables.sql')
        with open(combined, 'r', encoding='utf-8') as f:
            content = f.read()
        # Should contain calc column annotation
        self.assertIn('-- calc:', content)


if __name__ == '__main__':
    unittest.main()
