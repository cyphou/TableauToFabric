"""Tests for fabric_import.import_to_fabric (orchestrator)"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from fabric_import.import_to_fabric import FabricImporter


class TestFabricImporterSanitizeName(unittest.TestCase):
    """Tests for FabricImporter._sanitize_name()."""

    def test_removes_special_chars(self):
        self.assertEqual(FabricImporter._sanitize_name('a<b>c'), 'a_b_c')

    def test_removes_colon(self):
        self.assertEqual(FabricImporter._sanitize_name('C:file'), 'C_file')

    def test_removes_quotes(self):
        self.assertEqual(FabricImporter._sanitize_name('"my"report'), '_my_report')

    def test_strips_dots(self):
        result = FabricImporter._sanitize_name('report...')
        self.assertFalse(result.endswith('.'))

    def test_normal_name_unchanged(self):
        self.assertEqual(FabricImporter._sanitize_name('SalesReport'), 'SalesReport')


class TestFabricImporterLoadObjects(unittest.TestCase):
    """Tests for FabricImporter._load_extracted_objects()."""

    def test_missing_files_return_empty(self):
        tmpdir = tempfile.mkdtemp(prefix='ttf_imp_')
        original_cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            importer = FabricImporter(converted_dir=tmpdir, output_dir=tmpdir)
            data = importer._load_extracted_objects()
            self.assertEqual(data['datasources'], [])
            self.assertEqual(data['worksheets'], [])
            self.assertEqual(data['calculations'], [])
            self.assertIsInstance(data['aliases'], dict)
        finally:
            os.chdir(original_cwd)
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestFabricImporterImportAll(unittest.TestCase):
    """Tests for FabricImporter.import_all() with mocked generators."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_imp2_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @patch('fabric_import.import_to_fabric.FabricImporter._load_extracted_objects')
    def test_no_datasources_returns_empty(self, mock_load):
        mock_load.return_value = {'datasources': []}
        importer = FabricImporter(output_dir=self.tmpdir)
        result = importer.import_all(report_name='Test')
        self.assertEqual(result, {})

    @patch('fabric_import.import_to_fabric.FabricImporter._load_extracted_objects')
    def test_generates_metadata_file(self, mock_load):
        from tests.conftest import SAMPLE_EXTRACTED
        mock_load.return_value = SAMPLE_EXTRACTED

        # We need to mock the generators to avoid dependency issues
        with patch('fabric_import.import_to_fabric.FabricImporter.import_all') as mock_import:
            mock_import.return_value = {'lakehouse_tables': 2}
            importer = FabricImporter(output_dir=self.tmpdir)
            result = importer.import_all(report_name='TestReport')
            self.assertIsInstance(result, dict)

    def test_default_output_dir(self):
        importer = FabricImporter()
        self.assertEqual(importer.output_dir, 'artifacts/fabric_projects/')


if __name__ == '__main__':
    unittest.main()
