"""
Standalone Semantic Model generator for Microsoft Fabric.

Generates a Fabric SemanticModel item definition that can be deployed
independently of a Power BI Report (.pbip).  The model uses DirectLake
mode with entity partitions referencing a Lakehouse.

Output structure:
    {model_name}.SemanticModel/
    ├── definition/
    │   ├── model.tmdl
    │   ├── database.tmdl
    │   ├── expressions.tmdl
    │   ├── relationships.tmdl
    │   ├── roles/
    │   │   └── *.tmdl
    │   └── tables/
    │       └── *.tmdl
    └── .platform
"""

import os
import json

from . import tmdl_generator


class SemanticModelGenerator:
    """Generate a standalone Fabric SemanticModel artifact."""

    def __init__(self, project_dir, model_name, lakehouse_name=None):
        self.project_dir = project_dir
        self.model_name = model_name
        self.lakehouse_name = lakehouse_name or model_name
        self.sm_dir = os.path.join(project_dir, f'{model_name}.SemanticModel')
        os.makedirs(self.sm_dir, exist_ok=True)

    def generate(self, extracted):
        """Generate semantic-model files from extracted Tableau objects.

        Args:
            extracted: dict with keys like 'datasources', 'calculations',
                       'hierarchies', 'parameters', 'user_filters', etc.

        Returns:
            dict with generation statistics.
        """
        datasources = extracted.get('datasources', [])
        extra_objects = {
            'sets': extracted.get('sets', []),
            'groups': extracted.get('groups', []),
            'bins': extracted.get('bins', []),
            'hierarchies': extracted.get('hierarchies', []),
            'parameters': extracted.get('parameters', []),
            'user_filters': extracted.get('user_filters', []),
            'sort_orders': extracted.get('sort_orders', []),
            'aliases': extracted.get('aliases', {}),
        }
        calculations = extracted.get('calculations', [])

        # Output TMDL inside SemanticModel/definition
        definition_dir = os.path.join(self.sm_dir, 'definition')
        os.makedirs(definition_dir, exist_ok=True)

        stats = tmdl_generator.generate_tmdl(
            datasources=datasources,
            report_name=self.model_name,
            extra_objects=extra_objects,
            output_dir=definition_dir,
            lakehouse_name=self.lakehouse_name,
        )

        # Create .platform manifest
        self._write_platform_file()

        # Create item metadata
        self._write_item_metadata(stats)

        return stats

    def _write_platform_file(self):
        """Write the .platform manifest for the SemanticModel item."""
        platform = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "SemanticModel",
                "displayName": self.model_name,
            },
            "config": {
                "version": "2.0",
                "logicalId": f"semantic-model-{self.model_name.lower().replace(' ', '-')}",
            },
        }
        path = os.path.join(self.sm_dir, '.platform')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(platform, f, indent=2)

    def _write_item_metadata(self, stats):
        """Write a metadata JSON for the semantic model."""
        meta = {
            "displayName": self.model_name,
            "type": "SemanticModel",
            "mode": "DirectLake",
            "lakehouse": self.lakehouse_name,
            "stats": stats,
        }
        path = os.path.join(self.sm_dir, 'semantic_model_metadata.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, indent=2)
