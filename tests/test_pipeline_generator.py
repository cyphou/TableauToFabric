"""Tests for fabric_import.pipeline_generator"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.pipeline_generator import PipelineGenerator, _sanitize
from tests.conftest import SAMPLE_EXTRACTED


class TestSanitize(unittest.TestCase):
    """Tests for module-level _sanitize()."""

    def test_removes_special_chars(self):
        self.assertEqual(_sanitize('Sales Data!'), 'Sales_Data')

    def test_no_change_for_clean(self):
        self.assertEqual(_sanitize('my_name'), 'my_name')

    def test_strips_trailing_underscores(self):
        self.assertEqual(_sanitize('abc__'), 'abc')


class TestPipelineGenerator(unittest.TestCase):
    """Tests for PipelineGenerator.generate()."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_pl_')
        self.pipeline_name = 'TestPipeline'
        self.gen = PipelineGenerator(self.tmpdir, self.pipeline_name)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_returns_stats(self):
        stats = self.gen.generate(SAMPLE_EXTRACTED)
        self.assertIn('activities', stats)
        self.assertIn('stages', stats)
        # 1 datasource → 1 dataflow + 1 notebook + 1 SM refresh = 3
        self.assertEqual(stats['activities'], 3)
        self.assertGreater(stats['stages'], 0)

    def test_creates_pipeline_definition(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        def_path = os.path.join(self.tmpdir, f'{self.pipeline_name}.Pipeline', 'pipeline_definition.json')
        self.assertTrue(os.path.exists(def_path))
        with open(def_path, 'r', encoding='utf-8') as f:
            definition = json.load(f)
        self.assertIn('properties', definition)
        activities = definition['properties']['activities']
        self.assertEqual(len(activities), 3)

    def test_creates_pipeline_metadata(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        meta_path = os.path.join(self.tmpdir, f'{self.pipeline_name}.Pipeline', 'pipeline_metadata.json')
        self.assertTrue(os.path.exists(meta_path))
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        self.assertEqual(meta['displayName'], self.pipeline_name)
        self.assertEqual(meta['type'], 'Pipeline')

    def test_creates_platform_file(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        platform_path = os.path.join(self.tmpdir, f'{self.pipeline_name}.Pipeline', '.platform')
        self.assertTrue(os.path.exists(platform_path))
        with open(platform_path, 'r', encoding='utf-8') as f:
            platform = json.load(f)
        self.assertEqual(platform['metadata']['type'], 'DataPipeline')

    def test_dataflow_activities_per_datasource(self):
        extracted = {
            'datasources': [
                {'name': 'DS1', 'tables': []},
                {'name': 'DS2', 'tables': []},
            ],
        }
        stats = self.gen.generate(extracted)
        # 2 dataflow + 1 notebook + 1 SM = 4
        self.assertEqual(stats['activities'], 4)

    def test_activity_dependencies(self):
        self.gen.generate(SAMPLE_EXTRACTED)
        def_path = os.path.join(self.tmpdir, f'{self.pipeline_name}.Pipeline', 'pipeline_definition.json')
        with open(def_path, 'r', encoding='utf-8') as f:
            definition = json.load(f)
        activities = definition['properties']['activities']

        # Dataflow activities have no dependencies
        df_activities = [a for a in activities if a['type'] == 'RefreshDataflow']
        for a in df_activities:
            self.assertEqual(a['dependsOn'], [])

        # Notebook depends on dataflows
        nb_activities = [a for a in activities if a['type'] == 'TridentNotebook']
        self.assertEqual(len(nb_activities), 1)
        self.assertGreater(len(nb_activities[0]['dependsOn']), 0)

        # SM refresh depends on notebook
        sm_activities = [a for a in activities if a['type'] == 'TridentDatasetRefresh']
        self.assertEqual(len(sm_activities), 1)
        self.assertGreater(len(sm_activities[0]['dependsOn']), 0)

    def test_empty_datasources(self):
        stats = self.gen.generate({'datasources': []})
        # 0 dataflow + 1 notebook + 1 SM = 2
        self.assertEqual(stats['activities'], 2)

    def test_count_stages_empty(self):
        result = PipelineGenerator._count_stages([])
        self.assertEqual(result, 0)

    def test_count_stages_with_activities(self):
        activities = [
            {'name': 'A', 'dependsOn': []},
            {'name': 'B', 'dependsOn': [{'activity': 'A'}]},
        ]
        result = PipelineGenerator._count_stages(activities)
        self.assertGreater(result, 0)

    def test_lakehouse_name_default(self):
        gen = PipelineGenerator(self.tmpdir, 'MyPipeline')
        self.assertEqual(gen.lakehouse_name, 'MyPipeline')

    def test_lakehouse_name_custom(self):
        gen = PipelineGenerator(self.tmpdir, 'MyPipeline', lakehouse_name='CustomLH')
        self.assertEqual(gen.lakehouse_name, 'CustomLH')


if __name__ == '__main__':
    unittest.main()
