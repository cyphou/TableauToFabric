"""Tests for fabric_import.deployer"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from fabric_import.deployer import ArtifactType, FabricDeployer


class TestArtifactType(unittest.TestCase):
    """Tests for ArtifactType constants."""

    def test_constants(self):
        self.assertEqual(ArtifactType.DATASET, 'Dataset')
        self.assertEqual(ArtifactType.DATAFLOW, 'Dataflow')
        self.assertEqual(ArtifactType.REPORT, 'Report')
        self.assertEqual(ArtifactType.NOTEBOOK, 'Notebook')
        self.assertEqual(ArtifactType.LAKEHOUSE, 'Lakehouse')
        self.assertEqual(ArtifactType.WAREHOUSE, 'Warehouse')
        self.assertEqual(ArtifactType.PIPELINE, 'Pipeline')
        self.assertEqual(ArtifactType.SEMANTIC_MODEL, 'SemanticModel')


class TestFabricDeployer(unittest.TestCase):
    """Tests for FabricDeployer with mocked client."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.deployer = FabricDeployer(client=self.mock_client)

    def test_deploy_lakehouse(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'lh-123'}
        result = self.deployer.deploy_lakehouse('ws-1', 'MyLH', {'tables': []})
        self.assertEqual(result['id'], 'lh-123')
        self.mock_client.post.assert_called_once()

    def test_deploy_dataflow(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'df-456'}
        result = self.deployer.deploy_dataflow('ws-1', 'MyDF', {'queries': []})
        self.assertEqual(result['id'], 'df-456')

    def test_deploy_notebook(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'nb-789'}
        result = self.deployer.deploy_notebook('ws-1', 'MyNB', {})
        self.assertEqual(result['id'], 'nb-789')

    def test_deploy_dataset(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'ds-001'}
        result = self.deployer.deploy_dataset('ws-1', 'MyDS', {})
        self.assertEqual(result['id'], 'ds-001')

    def test_deploy_report(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'rpt-002'}
        result = self.deployer.deploy_report('ws-1', 'MyReport', {})
        self.assertEqual(result['id'], 'rpt-002')

    def test_deploy_pipeline(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'pl-003'}
        result = self.deployer.deploy_pipeline('ws-1', 'MyPL', {})
        self.assertEqual(result['id'], 'pl-003')

    def test_deploy_semantic_model(self):
        self.mock_client.list_items.return_value = {'value': []}
        self.mock_client.post.return_value = {'id': 'sm-004'}
        result = self.deployer.deploy_semantic_model('ws-1', 'MySM', {})
        self.assertEqual(result['id'], 'sm-004')

    def test_overwrite_existing_item(self):
        existing_item = {'id': 'existing-123', 'displayName': 'MyLH'}
        self.mock_client.list_items.return_value = {'value': [existing_item]}
        self.mock_client.put.return_value = {'id': 'existing-123'}
        result = self.deployer.deploy_lakehouse(
            'ws-1', 'MyLH', {'tables': []}, overwrite=True,
        )
        self.mock_client.put.assert_called_once()
        self.assertEqual(result['id'], 'existing-123')

    def test_no_overwrite_creates_new(self):
        existing_item = {'id': 'existing-123', 'displayName': 'MyLH'}
        self.mock_client.list_items.return_value = {'value': [existing_item]}
        self.mock_client.post.return_value = {'id': 'new-456'}
        result = self.deployer.deploy_lakehouse(
            'ws-1', 'MyLH', {'tables': []}, overwrite=False,
        )
        # With overwrite=False and existing item, post is NOT called for overwrite
        # But since existing != None and overwrite=False, it should go to else branch
        self.mock_client.post.assert_called_once()

    def test_deploy_from_file(self):
        tmpdir = tempfile.mkdtemp(prefix='ttf_dep_')
        try:
            path = os.path.join(tmpdir, 'artifact.json')
            with open(path, 'w') as f:
                json.dump({'displayName': 'TestArtifact', 'data': 'value'}, f)
            self.mock_client.list_items.return_value = {'value': []}
            self.mock_client.post.return_value = {'id': 'file-123'}
            result = self.deployer.deploy_from_file(
                'ws-1', path, ArtifactType.DATASET,
            )
            self.assertEqual(result['id'], 'file-123')
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_deploy_artifacts_batch(self):
        tmpdir = tempfile.mkdtemp(prefix='ttf_batch_')
        try:
            for i in range(3):
                path = os.path.join(tmpdir, f'artifact_{i}.json')
                with open(path, 'w') as f:
                    json.dump({'type': 'Dataset', 'displayName': f'A{i}'}, f)
            self.mock_client.list_items.return_value = {'value': []}
            self.mock_client.post.return_value = {'id': 'batch'}
            results = self.deployer.deploy_artifacts_batch('ws-1', tmpdir)
            self.assertEqual(len(results), 3)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_get_deployment_status(self):
        self.mock_client.get.return_value = {'id': 'item-1', 'status': 'Active'}
        result = self.deployer.get_deployment_status('ws-1', 'item-1')
        self.assertEqual(result['status'], 'Active')
        self.mock_client.get.assert_called_with('/workspaces/ws-1/items/item-1')

    def test_find_item_returns_none_when_not_found(self):
        self.mock_client.list_items.return_value = {'value': []}
        result = self.deployer._find_item('ws-1', 'Unknown', 'Dataset')
        self.assertIsNone(result)

    def test_find_item_returns_item_when_found(self):
        item = {'id': 'found-123', 'displayName': 'MyItem'}
        self.mock_client.list_items.return_value = {'value': [item]}
        result = self.deployer._find_item('ws-1', 'MyItem', 'Dataset')
        self.assertEqual(result['id'], 'found-123')

    def test_find_item_handles_exception(self):
        self.mock_client.list_items.side_effect = Exception('Network error')
        result = self.deployer._find_item('ws-1', 'MyItem', 'Dataset')
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
