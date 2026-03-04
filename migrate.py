"""
Main script for Tableau to Microsoft Fabric migration

Pipeline:
1. Extract datasources from the Tableau file (.twb/.twbx)
1b. (Optional) Parse Tableau Prep flow (.tfl/.tflx) and merge transforms
2. Generate Fabric artifacts:
   a. Lakehouse definition (table schemas from Tableau datasources)
   b. Dataflow Gen2 (Power Query M from Tableau connections + transforms)
   c. PySpark Notebook (ETL pipeline for complex transformations)
   d. Power BI report (.pbip) with TMDL semantic model

Supports:
- Single workbook migration:  python migrate.py workbook.twbx
- Batch migration:            python migrate.py --batch folder/
- Custom output directory:    python migrate.py workbook.twbx --output-dir out/
- Select artifacts:           python migrate.py workbook.twbx --artifacts lakehouse,dataflow,notebook,semanticmodel,pipeline,pbi
- Verbose logging:            python migrate.py workbook.twbx --verbose
"""

import os
import sys
import glob
import json
import logging
import argparse
from datetime import datetime

# Ensure sub-packages are importable
_base = os.path.dirname(os.path.abspath(__file__))
if _base not in sys.path:
    sys.path.insert(0, _base)


# ── Structured logging setup ────────────────────────────────────────

logger = logging.getLogger('tableau_to_fabric')


def setup_logging(verbose=False, log_file=None):
    """Configure structured logging.

    Args:
        verbose: If True, set DEBUG level; otherwise INFO.
        log_file: Optional path to a log file.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        os.makedirs(os.path.dirname(log_file) or '.', exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)
    if not verbose:
        logging.getLogger('tableau_to_fabric').setLevel(logging.INFO)


# ── Migration statistics tracker ────────────────────────────────────

class MigrationStats:
    """Tracks statistics across all pipeline steps."""

    def __init__(self):
        # Extraction
        self.app_name = ""
        self.datasources = 0
        self.worksheets = 0
        self.dashboards = 0
        self.calculations = 0
        self.parameters = 0
        self.filters = 0
        self.stories = 0
        self.actions = 0
        self.sets = 0
        self.groups = 0
        self.bins = 0
        self.hierarchies = 0
        self.user_filters = 0
        self.custom_sql = 0
        # Fabric artifacts
        self.lakehouse_tables = 0
        self.dataflow_queries = 0
        self.notebook_cells = 0
        self.semanticmodel_tables = 0
        self.pipeline_activities = 0
        # PBI generation
        self.tmdl_tables = 0
        self.tmdl_columns = 0
        self.tmdl_measures = 0
        self.tmdl_relationships = 0
        self.tmdl_hierarchies = 0
        self.tmdl_roles = 0
        self.visuals_generated = 0
        self.pages_generated = 0
        self.theme_applied = False
        self.output_path = ""
        # Diagnostics
        self.warnings = []
        self.skipped = []

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items()}


_stats = MigrationStats()

ALL_ARTIFACTS = ['lakehouse', 'dataflow', 'notebook', 'semanticmodel', 'pipeline', 'pbi']


def print_header(text):
    """Print a formatted header"""
    print()
    print("=" * 80)
    print(text.center(80))
    print("=" * 80)
    print()


def print_step(step_num, total_steps, text):
    """Print a step indicator"""
    print(f"\n[Step {step_num}/{total_steps}] {text}")
    print("-" * 80)


def run_extraction(tableau_file):
    """Run Tableau extraction"""
    global _stats
    print_step(1, 2, "TABLEAU OBJECTS EXTRACTION")

    if not os.path.exists(tableau_file):
        print(f"Error: Tableau file not found: {tableau_file}")
        return False

    print(f"Source file: {tableau_file}")
    _stats.app_name = os.path.splitext(os.path.basename(tableau_file))[0]

    try:
        from tableau_export.extract_tableau_data import TableauExtractor

        extractor = TableauExtractor(tableau_file)
        success = extractor.extract_all()

        if success:
            json_dir = os.path.join(os.path.dirname(__file__), 'tableau_export')
            for attr, fname in [
                ('datasources', 'datasources.json'),
                ('worksheets', 'worksheets.json'),
                ('dashboards', 'dashboards.json'),
                ('calculations', 'calculations.json'),
                ('parameters', 'parameters.json'),
                ('filters', 'filters.json'),
                ('stories', 'stories.json'),
                ('actions', 'actions.json'),
                ('sets', 'sets.json'),
                ('groups', 'groups.json'),
                ('bins', 'bins.json'),
                ('hierarchies', 'hierarchies.json'),
                ('user_filters', 'user_filters.json'),
                ('custom_sql', 'custom_sql.json'),
            ]:
                fpath = os.path.join(json_dir, fname)
                if os.path.exists(fpath):
                    try:
                        with open(fpath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        setattr(_stats, attr, len(data) if isinstance(data, list) else 0)
                    except Exception as e:
                        logger.debug(f"Could not read stats from {fname}: {e}")

            print("\n✓ Extraction completed successfully")
            return True
        else:
            print("\nError during extraction")
            return False

    except Exception as e:
        print(f"\nError during extraction: {str(e)}")
        return False


def run_generation(report_name=None, output_dir=None, artifacts=None):
    """Generate Fabric artifacts from extracted data

    Args:
        report_name: Override report name (defaults to dashboard name or 'Report')
        output_dir: Custom output directory (default: artifacts/fabric_projects/)
        artifacts: List of artifacts to generate (default: all)
    """
    global _stats
    if artifacts is None:
        artifacts = ALL_ARTIFACTS

    total_sub = len(artifacts)
    print_step(2, 2, "MICROSOFT FABRIC ARTIFACTS GENERATION")
    print(f"  Artifacts to generate: {', '.join(artifacts)}")

    try:
        from fabric_import.import_to_fabric import FabricImporter

        importer = FabricImporter()
        importer.import_all(
            artifacts=artifacts,
            report_name=report_name,
            output_dir=output_dir,
        )

        # Collect stats
        base_dir = output_dir or os.path.join('artifacts', 'fabric_projects')
        project_name = report_name or 'Report'
        project_dir = os.path.join(base_dir, project_name)
        if os.path.exists(project_dir):
            _stats.output_path = project_dir

            # Lakehouse stats
            lh_def = os.path.join(project_dir, f'{project_name}.Lakehouse', 'lakehouse_definition.json')
            if os.path.exists(lh_def):
                try:
                    with open(lh_def, 'r', encoding='utf-8') as f:
                        lh = json.load(f)
                    _stats.lakehouse_tables = len(lh.get('tables', []))
                except Exception as e:
                    logger.debug(f"Could not read lakehouse stats: {e}")

            # Dataflow stats
            df_def = os.path.join(project_dir, f'{project_name}.Dataflow', 'dataflow_definition.json')
            if os.path.exists(df_def):
                try:
                    with open(df_def, 'r', encoding='utf-8') as f:
                        df = json.load(f)
                    _stats.dataflow_queries = len(df.get('queries', []))
                except Exception as e:
                    logger.debug(f"Could not read dataflow stats: {e}")

            # Notebook stats
            nb_path = os.path.join(project_dir, f'{project_name}.Notebook', 'etl_pipeline.ipynb')
            if os.path.exists(nb_path):
                try:
                    with open(nb_path, 'r', encoding='utf-8') as f:
                        nb = json.load(f)
                    _stats.notebook_cells = len(nb.get('cells', []))
                except Exception as e:
                    logger.debug(f"Could not read notebook stats: {e}")

            # Semantic Model stats
            sm_meta = os.path.join(project_dir, f'{project_name}.SemanticModel', 'semantic_model_metadata.json')
            if os.path.exists(sm_meta):
                try:
                    with open(sm_meta, 'r', encoding='utf-8') as f:
                        sm = json.load(f)
                    _stats.semanticmodel_tables = sm.get('stats', {}).get('tables', 0)
                except Exception as e:
                    logger.debug(f"Could not read semantic model stats: {e}")

            # Pipeline stats
            pl_meta = os.path.join(project_dir, f'{project_name}.Pipeline', 'pipeline_metadata.json')
            if os.path.exists(pl_meta):
                try:
                    with open(pl_meta, 'r', encoding='utf-8') as f:
                        pl = json.load(f)
                    _stats.pipeline_activities = pl.get('activities', 0)
                except Exception as e:
                    logger.debug(f"Could not read pipeline stats: {e}")

            # PBI stats
            for root, dirs, files in os.walk(project_dir):
                if os.path.basename(root) == 'tables':
                    _stats.tmdl_tables = len([f for f in files if f.endswith('.tmdl')])
                if os.path.basename(root) == 'pages':
                    _stats.pages_generated = len([d for d in dirs if d.startswith('ReportSection')])
                if os.path.basename(root) == 'visuals':
                    _stats.visuals_generated += len(dirs)
                if 'TableauMigrationTheme.json' in files:
                    _stats.theme_applied = True

            meta_path = os.path.join(project_dir, 'migration_metadata.json')
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, 'r', encoding='utf-8') as f:
                        meta = json.load(f)
                    tmdl = meta.get('tmdl_stats', {})
                    _stats.tmdl_columns = tmdl.get('columns', 0)
                    _stats.tmdl_measures = tmdl.get('measures', 0)
                    _stats.tmdl_relationships = tmdl.get('relationships', 0)
                    _stats.tmdl_hierarchies = tmdl.get('hierarchies', 0)
                    _stats.tmdl_roles = tmdl.get('roles', 0)
                except Exception as e:
                    logger.debug(f"Could not read migration metadata: {e}")

        print("\n✓ Fabric artifacts generated successfully")
        return True

    except Exception as e:
        print(f"\nError during generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def run_prep_flow(prep_file, datasources_json='tableau_export/datasources.json'):
    """Parse Tableau Prep flow and merge transforms into extracted datasources."""
    import json as _json

    print_step("1b", 2, "TABLEAU PREP FLOW PARSING")

    if not os.path.exists(prep_file):
        print(f"Error: Prep flow file not found: {prep_file}")
        return False

    print(f"Prep flow: {prep_file}")

    try:
        from tableau_export.prep_flow_parser import parse_prep_flow, merge_prep_with_workbook

        prep_datasources = parse_prep_flow(prep_file)
        print(f"\n  [OK] {len(prep_datasources)} Prep output(s) parsed")

        if os.path.exists(datasources_json):
            with open(datasources_json, 'r', encoding='utf-8') as f:
                twb_datasources = _json.load(f)
            print(f"  [OK] {len(twb_datasources)} TWB datasource(s) loaded")
        else:
            twb_datasources = []
            print("  [WARN] No TWB datasources found -- using Prep flow only")

        merged = merge_prep_with_workbook(prep_datasources, twb_datasources)

        with open(datasources_json, 'w', encoding='utf-8') as f:
            _json.dump(merged, f, indent=2, ensure_ascii=False)
        print(f"  [OK] {len(merged)} merged datasource(s) saved to {datasources_json}")

        print("\n[OK] Prep flow parsing completed successfully")
        return True

    except Exception as e:
        print(f"\nError during Prep flow parsing: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def run_batch_migration(batch_dir, output_dir=None, prep_file=None,
                        skip_extraction=False, artifacts=None):
    """Batch migrate all .twb/.twbx files in a directory."""
    if not os.path.isdir(batch_dir):
        print(f"Error: Batch directory not found: {batch_dir}")
        return 1

    patterns = ['*.twb', '*.twbx']
    tableau_files = []
    for pattern in patterns:
        tableau_files.extend(glob.glob(os.path.join(batch_dir, pattern)))

    if not tableau_files:
        print(f"Error: No .twb/.twbx files found in {batch_dir}")
        return 1

    tableau_files.sort()

    print_header("TABLEAU TO MICROSOFT FABRIC BATCH MIGRATION")
    print(f"  Directory: {batch_dir}")
    print(f"  Workbooks found: {len(tableau_files)}")
    if output_dir:
        print(f"  Output dir: {output_dir}")
    if artifacts:
        print(f"  Artifacts: {', '.join(artifacts)}")
    print()

    batch_start = datetime.now()
    batch_results = {}

    for i, tableau_file in enumerate(tableau_files, 1):
        basename = os.path.splitext(os.path.basename(tableau_file))[0]
        print(f"\n{'=' * 80}")
        print(f"  [{i}/{len(tableau_files)}] Migrating: {basename}")
        print(f"{'=' * 80}")

        global _stats
        _stats = MigrationStats()

        file_results = {}

        if not skip_extraction:
            file_results['extraction'] = run_extraction(tableau_file)
            if not file_results['extraction']:
                logger.warning(f"Extraction failed for {basename}, skipping")
                batch_results[basename] = {'success': False, 'error': 'extraction'}
                continue
        else:
            file_results['extraction'] = True

        if prep_file:
            file_results['prep'] = run_prep_flow(prep_file)

        file_results['generation'] = run_generation(
            report_name=basename,
            output_dir=output_dir,
            artifacts=artifacts,
        )

        all_ok = all(v for v in file_results.values() if v is not None)
        batch_results[basename] = {
            'success': all_ok,
            'stats': _stats.to_dict(),
        }

    batch_duration = datetime.now() - batch_start
    succeeded = sum(1 for r in batch_results.values() if r['success'])
    failed = len(batch_results) - succeeded

    print_header("BATCH MIGRATION SUMMARY")
    print(f"  Total workbooks: {len(batch_results)}")
    print(f"  Succeeded:       {succeeded}")
    print(f"  Failed:          {failed}")
    print(f"  Duration:        {batch_duration}")
    print()

    for name, result in batch_results.items():
        status = "[OK]" if result['success'] else "[FAIL]"
        print(f"  {status} {name}")

    return 0 if failed == 0 else 1


def main():
    """Main entry point"""

    parser = argparse.ArgumentParser(
        description='Migrate a Tableau workbook to Microsoft Fabric artifacts '
                    '(Lakehouse, Dataflow Gen2, Notebook, Power BI)'
    )

    parser.add_argument(
        'tableau_file',
        nargs='?',
        default=None,
        help='Path to the Tableau file (.twb or .twbx)'
    )

    parser.add_argument(
        '--prep',
        metavar='PREP_FILE',
        help='Path to a Tableau Prep flow file (.tfl or .tflx) to merge transforms'
    )

    parser.add_argument(
        '--skip-extraction',
        action='store_true',
        help='Skip extraction (use existing JSON files)'
    )

    parser.add_argument(
        '--output-dir',
        metavar='DIR',
        default=None,
        help='Custom output directory (default: artifacts/fabric_projects/)'
    )

    parser.add_argument(
        '--artifacts',
        metavar='LIST',
        default=None,
        help='Comma-separated list of artifacts to generate: '
             'lakehouse,dataflow,notebook,semanticmodel,pipeline,pbi (default: all)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose (DEBUG) logging'
    )

    parser.add_argument(
        '--log-file',
        metavar='FILE',
        default=None,
        help='Write logs to a file in addition to console'
    )

    parser.add_argument(
        '--batch',
        metavar='DIR',
        default=None,
        help='Batch migrate all .twb/.twbx files in the specified directory'
    )

    args = parser.parse_args()

    setup_logging(verbose=args.verbose, log_file=args.log_file)

    # Parse artifact list
    artifacts = None
    if args.artifacts:
        artifacts = [a.strip().lower() for a in args.artifacts.split(',')]
        invalid = [a for a in artifacts if a not in ALL_ARTIFACTS]
        if invalid:
            parser.error(f"Invalid artifact(s): {', '.join(invalid)}. "
                         f"Valid: {', '.join(ALL_ARTIFACTS)}")

    # ── Batch mode ──
    if args.batch:
        return run_batch_migration(
            batch_dir=args.batch,
            output_dir=args.output_dir,
            prep_file=args.prep,
            skip_extraction=args.skip_extraction,
            artifacts=artifacts,
        )

    # ── Single file ──
    if not args.tableau_file:
        parser.error('tableau_file is required (or use --batch DIR)')

    print_header("TABLEAU TO MICROSOFT FABRIC MIGRATION")
    print(f"Source file: {args.tableau_file}")
    if args.prep:
        print(f"Prep flow:   {args.prep}")
    if args.output_dir:
        print(f"Output dir:  {args.output_dir}")
    artifact_list = artifacts or ALL_ARTIFACTS
    print(f"Artifacts:   {', '.join(artifact_list)}")
    print()

    start_time = datetime.now()
    results = {}

    # Step 1: Extraction
    if not args.skip_extraction:
        results['extraction'] = run_extraction(args.tableau_file)
        if not results['extraction']:
            print("\nMigration aborted due to extraction failure")
            return 1
    else:
        print("\nExtraction skipped (using existing JSON files)")
        results['extraction'] = True

    # Step 1b: Prep flow
    if args.prep:
        results['prep'] = run_prep_flow(args.prep)
        if not results['prep']:
            print("\n⚠ Prep flow parsing failed — continuing with TWB data only")

    # Step 2: Generate Fabric artifacts
    source_basename = os.path.splitext(os.path.basename(args.tableau_file))[0]
    results['generation'] = run_generation(
        report_name=source_basename,
        output_dir=args.output_dir,
        artifacts=artifacts,
    )

    # ── Final report ──
    duration = datetime.now() - start_time
    print_header("MIGRATION SUMMARY")

    print("  Step Results:")
    for step_name, success in [
        ("Tableau Extraction", results.get('extraction', False)),
        ("Prep Flow Parsing", results.get('prep', None)),
        ("Fabric Generation", results.get('generation', False)),
    ]:
        if success is None:
            continue
        status = "✓ Success" if success else "✗ Failed"
        print(f"    {step_name:<30} {status}")

    # Extraction summary
    if results.get('extraction'):
        print(f"\n  Extraction Summary ({_stats.app_name}):")
        extraction_items = [
            ("Datasources", _stats.datasources),
            ("Worksheets", _stats.worksheets),
            ("Dashboards", _stats.dashboards),
            ("Calculations", _stats.calculations),
            ("Parameters", _stats.parameters),
            ("Filters", _stats.filters),
            ("Stories", _stats.stories),
            ("Actions", _stats.actions),
            ("Sets", _stats.sets),
            ("Groups", _stats.groups),
            ("Bins", _stats.bins),
            ("Hierarchies", _stats.hierarchies),
            ("User Filters / RLS", _stats.user_filters),
            ("Custom SQL", _stats.custom_sql),
        ]
        for label, count in extraction_items:
            if count > 0:
                print(f"    {label:<30} {count}")

    # Fabric artifacts summary
    if results.get('generation'):
        print(f"\n  Fabric Artifacts Summary:")
        fabric_items = [
            ("Lakehouse Tables", _stats.lakehouse_tables),
            ("Dataflow Queries", _stats.dataflow_queries),
            ("Notebook Cells", _stats.notebook_cells),
            ("SemanticModel Tables", _stats.semanticmodel_tables),
            ("Pipeline Activities", _stats.pipeline_activities),
            ("TMDL Tables", _stats.tmdl_tables),
            ("TMDL Columns", _stats.tmdl_columns),
            ("DAX Measures", _stats.tmdl_measures),
            ("Relationships", _stats.tmdl_relationships),
            ("Hierarchies", _stats.tmdl_hierarchies),
            ("RLS Roles", _stats.tmdl_roles),
            ("Report Pages", _stats.pages_generated),
            ("Visuals", _stats.visuals_generated),
        ]
        for label, count in fabric_items:
            if count > 0:
                print(f"    {label:<30} {count}")
        if _stats.theme_applied:
            print(f"    {'Custom Theme':<30} ✓ Applied")

    if _stats.warnings:
        print(f"\n  Warnings ({len(_stats.warnings)}):")
        for w in _stats.warnings[:10]:
            print(f"    ⚠ {w}")
        if len(_stats.warnings) > 10:
            print(f"    ... and {len(_stats.warnings) - 10} more")

    if _stats.skipped:
        print(f"\n  Skipped ({len(_stats.skipped)}):")
        for s in _stats.skipped[:5]:
            print(f"    ⊘ {s}")

    print(f"\n  Duration: {duration}")

    all_success = all(v for v in results.values() if v is not None)

    if all_success:
        print("\n✓ Migration completed successfully!")
        if _stats.output_path:
            print(f"\n  Output: {_stats.output_path}")
        print("\n  Next steps:")
        print("    1. Import the Lakehouse definition into your Fabric workspace")
        print("    2. Upload and run the Dataflow Gen2 to ingest data")
        print("    3. Import and execute the PySpark Notebook for transformations")
        print("    4. Deploy the Semantic Model to the workspace")
        print("    5. Deploy the Data Pipeline and trigger a run")
        print("    6. Open the .pbip file in Power BI Desktop (Developer Mode)")
        print("    7. Configure Lakehouse connection in Power Query Editor")
        print("    8. Verify DAX measures and relationships")
        print("    9. Publish to your Fabric workspace")
    else:
        print("\n✗ Migration completed with errors")

    return 0 if all_success else 1


if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {str(e)}")
        sys.exit(1)
