"""
Fabric artifact deployment module.

Deploys generated Fabric artifacts (Lakehouse, Dataflow, Notebook,
SemanticModel, Report) to a Microsoft Fabric workspace via REST API.

Requires:
    - azure-identity (pip install azure-identity)
    - requests (pip install requests) — optional, falls back to urllib

Usage:
    from deployer import FabricDeployer
    deployer = FabricDeployer()
    deployer.deploy_lakehouse(workspace_id, 'MyLakehouse', config)
"""

import os
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ArtifactType:
    """Supported Fabric artifact types."""
    DATASET = 'Dataset'
    DATAFLOW = 'Dataflow'
    REPORT = 'Report'
    NOTEBOOK = 'Notebook'
    LAKEHOUSE = 'Lakehouse'
    WAREHOUSE = 'Warehouse'
    PIPELINE = 'Pipeline'
    SEMANTIC_MODEL = 'SemanticModel'


class FabricDeployer:
    """Deploy Fabric artifacts to a workspace."""

    def __init__(self, client=None):
        if client is None:
            from .client import FabricClient
            client = FabricClient()
        self.client = client

    def _find_item(self, workspace_id, item_name, item_type):
        try:
            items = self.client.list_items(workspace_id, item_type)
            for item in items.get('value', []):
                if item.get('displayName') == item_name:
                    return item
            return None
        except Exception as e:
            logger.warning(f'Failed to search for item: {e}')
            return None

    def _deploy_item(self, workspace_id, item_name, item_type, config,
                     overwrite=True):
        """Generic deploy/update for any artifact type."""
        logger.info(f'Deploying {item_type}: {item_name}')
        existing = self._find_item(workspace_id, item_name, item_type)

        if existing and overwrite:
            logger.info(f'Overwriting existing {item_type}: {existing["id"]}')
            result = self.client.put(
                f'/workspaces/{workspace_id}/items/{existing["id"]}',
                data=config,
            )
        else:
            result = self.client.post(
                f'/workspaces/{workspace_id}/items',
                data={
                    'displayName': item_name,
                    'type': item_type,
                    'definition': config,
                },
            )
        logger.info(f'{item_type} deployed: {result.get("id", "?")}')
        return result

    def deploy_lakehouse(self, workspace_id, lakehouse_name, config,
                         overwrite=True):
        """Deploy a Lakehouse to a workspace."""
        return self._deploy_item(workspace_id, lakehouse_name,
                                  ArtifactType.LAKEHOUSE, config, overwrite)

    def deploy_dataflow(self, workspace_id, dataflow_name, config,
                        overwrite=True):
        """Deploy a Dataflow Gen2 to a workspace."""
        return self._deploy_item(workspace_id, dataflow_name,
                                  ArtifactType.DATAFLOW, config, overwrite)

    def deploy_notebook(self, workspace_id, notebook_name, config,
                        overwrite=True):
        """Deploy a Notebook to a workspace."""
        return self._deploy_item(workspace_id, notebook_name,
                                  ArtifactType.NOTEBOOK, config, overwrite)

    def deploy_dataset(self, workspace_id, dataset_name, config,
                       overwrite=True):
        """Deploy a SemanticModel / dataset to a workspace."""
        return self._deploy_item(workspace_id, dataset_name,
                                  ArtifactType.SEMANTIC_MODEL, config, overwrite)

    def deploy_report(self, workspace_id, report_name, config,
                      overwrite=True):
        """Deploy a Report to a workspace."""
        return self._deploy_item(workspace_id, report_name,
                                  ArtifactType.REPORT, config, overwrite)

    def deploy_pipeline(self, workspace_id, pipeline_name, config,
                        overwrite=True):
        """Deploy a Data Pipeline to a workspace."""
        return self._deploy_item(workspace_id, pipeline_name,
                                  ArtifactType.PIPELINE, config, overwrite)

    def deploy_semantic_model(self, workspace_id, model_name, config,
                              overwrite=True):
        """Deploy a standalone SemanticModel to a workspace."""
        return self._deploy_item(workspace_id, model_name,
                                  ArtifactType.SEMANTIC_MODEL, config, overwrite)

    def deploy_from_file(self, workspace_id, artifact_path, artifact_type,
                         overwrite=True):
        """Deploy an artifact from a JSON file."""
        artifact_path = Path(artifact_path)
        logger.info(f'Loading artifact from: {artifact_path}')
        with open(artifact_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        artifact_name = config.get('displayName') or artifact_path.stem
        return self._deploy_item(workspace_id, artifact_name,
                                  artifact_type, config, overwrite)

    def deploy_artifacts_batch(self, workspace_id, artifacts_dir,
                               overwrite=True):
        """Deploy all artifacts from a directory."""
        artifacts_dir = Path(artifacts_dir)
        results = []
        for artifact_file in sorted(artifacts_dir.glob('*.json')):
            try:
                logger.info(f'Processing: {artifact_file.name}')
                with open(artifact_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                artifact_type = config.get('type', ArtifactType.DATASET)
                result = self.deploy_from_file(
                    workspace_id, artifact_file, artifact_type, overwrite,
                )
                results.append({'file': str(artifact_file), 'result': result})
            except Exception as e:
                logger.error(f'Failed to deploy {artifact_file.name}: {e}')
                results.append({'file': str(artifact_file), 'error': str(e)})
        return results

    def get_deployment_status(self, workspace_id, item_id):
        """Get deployment or item status."""
        return self.client.get(
            f'/workspaces/{workspace_id}/items/{item_id}'
        )
