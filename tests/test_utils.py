"""Tests for fabric_import.utils"""

import json
import os
import shutil
import tempfile
import unittest

from fabric_import.utils import DeploymentReport, ArtifactCache


class TestDeploymentReport(unittest.TestCase):
    """Tests for DeploymentReport."""

    def test_empty_report(self):
        report = DeploymentReport()
        self.assertEqual(len(report.entries), 0)
        self.assertEqual(report.succeeded(), [])
        self.assertEqual(report.failed(), [])

    def test_add_entry(self):
        report = DeploymentReport()
        report.add('MyLH', 'Lakehouse', 'success', {'tables': 5})
        self.assertEqual(len(report.entries), 1)
        entry = report.entries[0]
        self.assertEqual(entry['artifact_name'], 'MyLH')
        self.assertEqual(entry['artifact_type'], 'Lakehouse')
        self.assertEqual(entry['status'], 'success')
        self.assertEqual(entry['details']['tables'], 5)
        self.assertIn('timestamp', entry)

    def test_succeeded_filter(self):
        report = DeploymentReport()
        report.add('A', 'Lakehouse', 'success')
        report.add('B', 'Dataflow', 'failed')
        report.add('C', 'Notebook', 'success')
        self.assertEqual(len(report.succeeded()), 2)

    def test_failed_filter(self):
        report = DeploymentReport()
        report.add('A', 'Lakehouse', 'success')
        report.add('B', 'Dataflow', 'failed')
        self.assertEqual(len(report.failed()), 1)
        self.assertEqual(report.failed()[0]['artifact_name'], 'B')

    def test_summary(self):
        report = DeploymentReport()
        report.add('A', 'Lakehouse', 'success')
        report.add('B', 'Dataflow', 'failed')
        report.add('C', 'Notebook', 'success')
        summary = report.summary()
        self.assertEqual(summary['total'], 3)
        self.assertEqual(summary['succeeded'], 2)
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(len(summary['entries']), 3)

    def test_save(self):
        tmpdir = tempfile.mkdtemp(prefix='ttf_dr_')
        try:
            report = DeploymentReport()
            report.add('A', 'Lakehouse', 'success')
            output_path = os.path.join(tmpdir, 'sub', 'report.json')
            report.save(output_path)
            self.assertTrue(os.path.exists(output_path))
            with open(output_path, 'r') as f:
                saved = json.load(f)
            self.assertEqual(saved['total'], 1)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_add_default_details(self):
        report = DeploymentReport()
        report.add('X', 'Report', 'success')
        self.assertEqual(report.entries[0]['details'], {})


class TestArtifactCache(unittest.TestCase):
    """Tests for ArtifactCache."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_cache_')
        self.cache = ArtifactCache(cache_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_get_nonexistent_returns_none(self):
        result = self.cache.get('ws-1', 'NonExistent')
        self.assertIsNone(result)

    def test_set_and_get(self):
        data = {'id': '123', 'name': 'MyArtifact'}
        self.cache.set('ws-1', 'MyArtifact', data)
        result = self.cache.get('ws-1', 'MyArtifact')
        self.assertEqual(result, data)

    def test_overwrite(self):
        self.cache.set('ws-1', 'A', {'v': 1})
        self.cache.set('ws-1', 'A', {'v': 2})
        result = self.cache.get('ws-1', 'A')
        self.assertEqual(result['v'], 2)

    def test_clear_specific_workspace(self):
        self.cache.set('ws-1', 'A', {'v': 1})
        self.cache.set('ws-2', 'B', {'v': 2})
        self.cache.clear('ws-1')
        self.assertIsNone(self.cache.get('ws-1', 'A'))
        self.assertIsNotNone(self.cache.get('ws-2', 'B'))

    def test_clear_all(self):
        self.cache.set('ws-1', 'A', {'v': 1})
        self.cache.set('ws-2', 'B', {'v': 2})
        self.cache.clear()
        self.assertIsNone(self.cache.get('ws-1', 'A'))
        self.assertIsNone(self.cache.get('ws-2', 'B'))

    def test_cache_key_sanitization(self):
        key = self.cache._cache_key('ws-1', 'My Artifact/Name')
        self.assertNotIn(' ', key)
        self.assertNotIn('/', key)
        self.assertTrue(key.endswith('.json'))


if __name__ == '__main__':
    unittest.main()
