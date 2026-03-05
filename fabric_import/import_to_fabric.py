"""
Import orchestrator for Microsoft Fabric artifact generation.

Loads extracted Tableau JSON files and drives the Fabric artifact
generation pipeline:
- Lakehouse definitions (table schemas from Tableau datasources)
- Dataflow Gen2 (Power Query M mashup documents)
- PySpark Notebooks (.ipynb for ETL pipeline)
- Semantic Model (standalone DirectLake TMDL model)
- Data Pipeline (orchestration: Dataflow → Notebook → Model refresh)
- Power BI Reports (.pbip with TMDL semantic model connected to Lakehouse)
"""

import os
import json
import logging
from datetime import datetime

from .naming import sanitize_filesystem_name

logger = logging.getLogger(__name__)


class FabricImporter:
    """Microsoft Fabric artifact importer/generator."""

    def __init__(self, converted_dir='artifacts/fabric_objects/',
                 output_dir='artifacts/fabric_projects/'):
        self.converted_dir = converted_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def import_all(self, report_name=None, output_dir=None,
                   artifacts=None):
        """
        Import all extracted objects and generate Fabric artifacts.

        Args:
            report_name: Override report name (defaults to dashboard name)
            output_dir: Custom output directory
            artifacts: List of artifact types to generate
                       (subset of ['lakehouse', 'dataflow', 'notebook',
                        'semanticmodel', 'pipeline', 'pbi'])
        """
        from .constants import ALL_ARTIFACTS
        if artifacts is None:
            artifacts = ALL_ARTIFACTS

        print("=" * 80)
        print("MICROSOFT FABRIC ARTIFACT GENERATION")
        print("=" * 80)
        print()

        # Load extracted objects from tableau_export/
        extracted = self._load_extracted_objects()

        if not extracted.get('datasources'):
            print("  [ERROR] No datasources found in tableau_export/datasources.json")
            print("     Run extraction first: python tableau_export/extract_tableau_data.py <file>")
            return {}

        # Determine report name
        if not report_name:
            dashboards = extracted.get('dashboards', [])
            if dashboards:
                report_name = dashboards[0].get('name', 'FabricReport')
            else:
                report_name = 'FabricReport'

        # Sanitize name for filesystem
        safe_name = self._sanitize_name(report_name)

        print(f"  Project:      {safe_name}")
        print(f"  Datasources:  {len(extracted.get('datasources', []))}")
        print(f"  Worksheets:   {len(extracted.get('worksheets', []))}")
        print(f"  Calculations: {len(extracted.get('calculations', []))}")
        print(f"  Artifacts:    {', '.join(artifacts)}")
        print()

        # Determine project directory
        if output_dir:
            project_dir = os.path.abspath(output_dir)
        else:
            project_dir = os.path.join(os.path.abspath(self.output_dir), safe_name)
        os.makedirs(project_dir, exist_ok=True)

        stats = {
            'lakehouse_tables': 0,
            'dataflow_queries': 0,
            'notebook_cells': 0,
            'semanticmodel_tables': 0,
            'pipeline_activities': 0,
            'pbi_pages': 0,
            'pbi_visuals': 0,
        }

        total_steps = sum(1 for a in ['lakehouse', 'dataflow', 'notebook',
                                       'semanticmodel', 'pipeline', 'pbi']
                          if a in artifacts)
        step = 0

        # ── 1. Lakehouse ──────────────────────────────────────────
        if 'lakehouse' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating Lakehouse definition...")
            try:
                from .lakehouse_generator import LakehouseGenerator
                lh_gen = LakehouseGenerator(project_dir, safe_name)
                lh_stats = lh_gen.generate(extracted)
                stats['lakehouse_tables'] = lh_stats.get('tables', 0)
                print(f"        ✓ {stats['lakehouse_tables']} table(s)")
            except Exception as e:
                print(f"        ✗ Lakehouse error: {e}")

        # ── 2. Dataflow Gen2 ─────────────────────────────────────
        if 'dataflow' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating Dataflow Gen2...")
            try:
                from .dataflow_generator import DataflowGenerator
                df_gen = DataflowGenerator(project_dir, safe_name)
                df_stats = df_gen.generate(extracted)
                stats['dataflow_queries'] = df_stats.get('queries', 0)
                print(f"        ✓ {stats['dataflow_queries']} query/queries")
            except Exception as e:
                print(f"        ✗ Dataflow error: {e}")

        # ── 3. PySpark Notebook ───────────────────────────────────
        if 'notebook' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating PySpark Notebook...")
            try:
                from .notebook_generator import NotebookGenerator
                nb_gen = NotebookGenerator(project_dir, safe_name)
                nb_stats = nb_gen.generate(extracted)
                stats['notebook_cells'] = nb_stats.get('cells', 0)
                print(f"        ✓ {stats['notebook_cells']} cell(s)")
            except Exception as e:
                print(f"        ✗ Notebook error: {e}")

        # ── 4. Standalone Semantic Model ──────────────────────────
        if 'semanticmodel' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating Semantic Model...")
            try:
                from .semantic_model_generator import SemanticModelGenerator
                sm_gen = SemanticModelGenerator(project_dir, safe_name)
                sm_stats = sm_gen.generate(extracted)
                stats['semanticmodel_tables'] = sm_stats.get('tables', 0)
                print(f"        ✓ {stats['semanticmodel_tables']} table(s)")
            except Exception as e:
                print(f"        ✗ SemanticModel error: {e}")

        # ── 5. Data Pipeline ──────────────────────────────────────
        if 'pipeline' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating Data Pipeline...")
            try:
                from .pipeline_generator import PipelineGenerator
                pl_gen = PipelineGenerator(project_dir, safe_name)
                pl_stats = pl_gen.generate(extracted)
                stats['pipeline_activities'] = pl_stats.get('activities', 0)
                print(f"        ✓ {stats['pipeline_activities']} activity/activities")
            except Exception as e:
                print(f"        ✗ Pipeline error: {e}")

        # ── 6. Power BI Report (.pbip) ────────────────────────────
        if 'pbi' in artifacts:
            step += 1
            print(f"  [{step}/{total_steps}] Generating Power BI Report (.pbip)...")
            try:
                from .pbip_generator import FabricPBIPGenerator
                pbi_gen = FabricPBIPGenerator(project_dir, safe_name)
                pbi_stats = pbi_gen.generate(extracted)
                stats['pbi_pages'] = pbi_stats.get('pages', 0)
                stats['pbi_visuals'] = pbi_stats.get('visuals', 0)
                print(f"        ✓ {stats['pbi_pages']} page(s), "
                      f"{stats['pbi_visuals']} visual(s)")
            except Exception as e:
                print(f"        ✗ PBI error: {e}")

        # ── Save metadata ─────────────────────────────────────────
        metadata = {
            'project_name': safe_name,
            'source': 'TableauToFabric',
            'generated_at': datetime.now().isoformat(),
            'artifacts_generated': artifacts,
            'stats': stats,
        }
        meta_path = os.path.join(project_dir, 'migration_metadata.json')
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        print()
        print("=" * 80)
        print("GENERATION COMPLETE")
        print("=" * 80)
        print(f"  Output: {project_dir}")
        for k, v in stats.items():
            if v > 0:
                print(f"    {k}: {v}")
        print()

        return stats

    def _load_extracted_objects(self):
        """Load all extracted JSON files from tableau_export/."""
        data = {}
        files_map = {
            'datasources': 'tableau_export/datasources.json',
            'worksheets': 'tableau_export/worksheets.json',
            'dashboards': 'tableau_export/dashboards.json',
            'calculations': 'tableau_export/calculations.json',
            'parameters': 'tableau_export/parameters.json',
            'filters': 'tableau_export/filters.json',
            'stories': 'tableau_export/stories.json',
            'actions': 'tableau_export/actions.json',
            'sets': 'tableau_export/sets.json',
            'groups': 'tableau_export/groups.json',
            'bins': 'tableau_export/bins.json',
            'hierarchies': 'tableau_export/hierarchies.json',
            'sort_orders': 'tableau_export/sort_orders.json',
            'aliases': 'tableau_export/aliases.json',
            'custom_sql': 'tableau_export/custom_sql.json',
            'user_filters': 'tableau_export/user_filters.json',
        }

        for key, filepath in files_map.items():
            try:
                if os.path.exists(filepath):
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data[key] = json.load(f)
                else:
                    data[key] = [] if key != 'aliases' else {}
            except Exception as e:
                logger.warning(f"Failed to load {key} from {filepath}: {e}")
                data[key] = [] if key != 'aliases' else {}

        return data

    @staticmethod
    def _sanitize_name(name):
        """Sanitize a name for filesystem use."""
        return sanitize_filesystem_name(name)


def main():
    """Main entry point."""
    import sys
    importer = FabricImporter()
    importer.import_all()


if __name__ == '__main__':
    main()
