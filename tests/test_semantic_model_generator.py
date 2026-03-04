"""Tests for fabric_import.semantic_model_generator"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from fabric_import.semantic_model_generator import SemanticModelGenerator
from tests.conftest import SAMPLE_EXTRACTED


class TestSemanticModelGenerator(unittest.TestCase):
    """Tests for SemanticModelGenerator.generate()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_sm_')
        self.model_name = 'TestModel'
        self.gen = SemanticModelGenerator(self.tmpdir, self.model_name)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_generate_returns_stats(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {'tables': 2, 'columns': 5, 'measures': 1}
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        self.assertEqual(stats['tables'], 2)
        self.assertEqual(stats['columns'], 5)

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_calls_tmdl_generator(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {'tables': 1}
        self.gen.generate(SAMPLE_EXTRACTED)
        mock_tmdl.generate_tmdl.assert_called_once()
        call_kwargs = mock_tmdl.generate_tmdl.call_args
        # Verify the correct datasources passed
        self.assertEqual(
            call_kwargs.kwargs.get('datasources') or call_kwargs[1].get('datasources'),
            SAMPLE_EXTRACTED['datasources'],
        )

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_creates_platform_file(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {'tables': 1}
        self.gen.generate(SAMPLE_EXTRACTED)
        platform_path = os.path.join(self.tmpdir, f'{self.model_name}.SemanticModel', '.platform')
        self.assertTrue(os.path.exists(platform_path))
        with open(platform_path, 'r', encoding='utf-8') as f:
            platform = json.load(f)
        self.assertEqual(platform['metadata']['type'], 'SemanticModel')
        self.assertEqual(platform['metadata']['displayName'], self.model_name)

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_creates_metadata_file(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {'tables': 2, 'measures': 3}
        self.gen.generate(SAMPLE_EXTRACTED)
        meta_path = os.path.join(
            self.tmpdir, f'{self.model_name}.SemanticModel', 'semantic_model_metadata.json',
        )
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        self.assertEqual(meta['displayName'], self.model_name)
        self.assertEqual(meta['mode'], 'DirectLake')
        self.assertEqual(meta['stats']['tables'], 2)

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_definition_directory_created(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {'tables': 0}
        self.gen.generate(SAMPLE_EXTRACTED)
        def_dir = os.path.join(self.tmpdir, f'{self.model_name}.SemanticModel', 'definition')
        self.assertTrue(os.path.isdir(def_dir))

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_lakehouse_name_default(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {}
        gen = SemanticModelGenerator(self.tmpdir, 'MyModel')
        self.assertEqual(gen.lakehouse_name, 'MyModel')

    @patch('fabric_import.semantic_model_generator.tmdl_generator')
    def test_lakehouse_name_custom(self, mock_tmdl):
        mock_tmdl.generate_tmdl.return_value = {}
        gen = SemanticModelGenerator(self.tmpdir, 'MyModel', lakehouse_name='CustomLH')
        self.assertEqual(gen.lakehouse_name, 'CustomLH')


if __name__ == '__main__':
    unittest.main()
