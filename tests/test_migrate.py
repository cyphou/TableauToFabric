"""Tests for migrate.py (CLI / orchestrator)"""

import logging
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Add project root so migrate is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from migrate import (
    MigrationStats,
    ALL_ARTIFACTS,
    print_header,
    print_step,
    setup_logging,
)


class TestMigrationStats(unittest.TestCase):
    """Tests for MigrationStats data class."""

    def test_defaults(self):
        stats = MigrationStats()
        self.assertEqual(stats.datasources, 0)
        self.assertEqual(stats.worksheets, 0)
        self.assertEqual(stats.lakehouse_tables, 0)
        self.assertEqual(stats.dataflow_queries, 0)
        self.assertEqual(stats.notebook_cells, 0)
        self.assertEqual(stats.semanticmodel_tables, 0)
        self.assertEqual(stats.pipeline_activities, 0)
        self.assertEqual(stats.tmdl_tables, 0)
        self.assertEqual(stats.app_name, '')
        self.assertEqual(stats.output_path, '')
        self.assertFalse(stats.theme_applied)
        self.assertEqual(stats.warnings, [])
        self.assertEqual(stats.skipped, [])

    def test_to_dict(self):
        stats = MigrationStats()
        stats.datasources = 5
        stats.app_name = 'Sales'
        d = stats.to_dict()
        self.assertEqual(d['datasources'], 5)
        self.assertEqual(d['app_name'], 'Sales')
        self.assertIsInstance(d, dict)

    def test_mutable_fields(self):
        stats = MigrationStats()
        stats.warnings.append('test warning')
        self.assertEqual(len(stats.warnings), 1)

    def test_all_expected_fields_present(self):
        stats = MigrationStats()
        d = stats.to_dict()
        expected = [
            'datasources', 'worksheets', 'dashboards', 'calculations',
            'parameters', 'filters', 'stories', 'actions',
            'sets', 'groups', 'bins', 'hierarchies', 'user_filters',
            'custom_sql', 'lakehouse_tables', 'dataflow_queries',
            'notebook_cells', 'semanticmodel_tables', 'pipeline_activities',
            'tmdl_tables', 'tmdl_columns', 'tmdl_measures',
            'tmdl_relationships', 'tmdl_hierarchies', 'tmdl_roles',
            'visuals_generated', 'pages_generated', 'theme_applied',
            'output_path', 'warnings', 'skipped', 'app_name',
        ]
        for field in expected:
            with self.subTest(field=field):
                self.assertIn(field, d)


class TestAllArtifacts(unittest.TestCase):
    """Tests for the ALL_ARTIFACTS constant."""

    def test_contains_all_six(self):
        self.assertEqual(len(ALL_ARTIFACTS), 6)
        for artifact in ['lakehouse', 'dataflow', 'notebook',
                         'semanticmodel', 'pipeline', 'pbi']:
            self.assertIn(artifact, ALL_ARTIFACTS)

    def test_order(self):
        self.assertEqual(ALL_ARTIFACTS[0], 'lakehouse')
        self.assertEqual(ALL_ARTIFACTS[-1], 'pbi')


class TestPrintFunctions(unittest.TestCase):
    """Tests for print_header() and print_step()."""

    @patch('builtins.print')
    def test_print_header(self, mock_print):
        print_header('Test Header')
        # Should have been called with separator lines and centered text
        calls = [str(c) for c in mock_print.call_args_list]
        combined = ' '.join(calls)
        self.assertIn('Test Header', combined)

    @patch('builtins.print')
    def test_print_step(self, mock_print):
        print_step(1, 5, 'Do something')
        calls = [str(c) for c in mock_print.call_args_list]
        combined = ' '.join(calls)
        self.assertIn('1', combined)
        self.assertIn('5', combined)
        self.assertIn('Do something', combined)


class TestSetupLogging(unittest.TestCase):
    """Tests for setup_logging()."""

    def setUp(self):
        # Reset root logger between tests so basicConfig can re-apply
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        root.setLevel(logging.WARNING)

    def test_verbose_sets_debug(self):
        setup_logging(verbose=True)
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.DEBUG)

    def test_non_verbose_sets_info(self):
        setup_logging(verbose=False)
        root_logger = logging.getLogger()
        self.assertEqual(root_logger.level, logging.INFO)


if __name__ == '__main__':
    unittest.main()
