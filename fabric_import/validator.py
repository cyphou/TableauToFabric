"""
Artifact validator for generated Fabric projects.

Validates generated PBIR report files, TMDL semantic model files,
Lakehouse definitions, Dataflow definitions, and Notebook files
against required schemas and structure rules.

Usage:
    from validator import ArtifactValidator
    results = ArtifactValidator.validate_directory(Path('artifacts/fabric_projects/MyReport'))
"""

import os
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ArtifactValidator:
    """Validate generated Fabric project artifacts."""

    # Valid Fabric artifact types
    VALID_ARTIFACT_TYPES = {
        'Dataset', 'Dataflow', 'Report', 'Notebook',
        'Lakehouse', 'Warehouse', 'Pipeline', 'SemanticModel',
    }

    # Valid PBIR schemas
    VALID_REPORT_SCHEMAS = [
        'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json',
    ]
    VALID_PAGE_SCHEMAS = [
        'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json',
    ]
    VALID_VISUAL_SCHEMAS = [
        'https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json',
    ]

    @staticmethod
    def validate_artifact(artifact_path):
        """Validate a single artifact JSON file."""
        artifact_path = Path(artifact_path)
        errors = []
        try:
            if not artifact_path.exists():
                return False, [f'File not found: {artifact_path}']
            with open(artifact_path, 'r', encoding='utf-8') as f:
                artifact = json.load(f)
            if not isinstance(artifact, dict):
                errors.append('Artifact must be a JSON object')
                return False, errors
            artifact_type = artifact.get('type')
            if artifact_type and artifact_type not in ArtifactValidator.VALID_ARTIFACT_TYPES:
                errors.append(f'Invalid artifact type: {artifact_type}')
            return len(errors) == 0, errors
        except json.JSONDecodeError as e:
            return False, [f'Invalid JSON: {str(e)}']
        except Exception as e:
            return False, [f'Validation error: {str(e)}']

    @staticmethod
    def validate_json_file(filepath):
        """Validate that a file contains valid JSON."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                json.load(f)
            return True, None
        except json.JSONDecodeError as e:
            return False, f'Invalid JSON in {filepath}: {e}'
        except Exception as e:
            return False, f'Error reading {filepath}: {e}'

    @staticmethod
    def validate_tmdl_file(filepath):
        """Validate a TMDL file has valid structure."""
        errors = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            if not content.strip():
                errors.append(f'Empty TMDL file: {filepath}')
                return False, errors
            basename = os.path.basename(filepath)
            if basename == 'model.tmdl':
                if not content.strip().startswith('model Model'):
                    errors.append(f'model.tmdl must start with "model Model"')
            return len(errors) == 0, errors
        except Exception as e:
            return False, [f'Error reading {filepath}: {e}']

    @staticmethod
    def validate_notebook(filepath):
        """Validate a Jupyter notebook (.ipynb) file."""
        errors = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                nb = json.load(f)
            if not isinstance(nb, dict):
                errors.append('Notebook must be a JSON object')
                return False, errors
            if 'cells' not in nb:
                errors.append('Notebook missing "cells" array')
            if 'metadata' not in nb:
                errors.append('Notebook missing "metadata"')
            if 'nbformat' not in nb:
                errors.append('Notebook missing "nbformat"')
            return len(errors) == 0, errors
        except json.JSONDecodeError as e:
            return False, [f'Invalid JSON in notebook: {e}']
        except Exception as e:
            return False, [f'Error reading notebook: {e}']

    @staticmethod
    def validate_lakehouse_definition(filepath):
        """Validate a lakehouse_definition.json file."""
        errors = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lh = json.load(f)
            if not isinstance(lh, dict):
                errors.append('Lakehouse definition must be a JSON object')
                return False, errors
            # Check for displayName in properties (generator output format)
            props = lh.get('properties', {})
            if not props.get('displayName') and 'lakehouse_name' not in lh:
                errors.append('Missing "properties.displayName" or "lakehouse_name"')
            if 'tables' not in lh:
                errors.append('Missing "tables" array')
            return len(errors) == 0, errors
        except json.JSONDecodeError as e:
            return False, [f'Invalid JSON: {e}']
        except Exception as e:
            return False, [f'Error: {e}']

    @staticmethod
    def validate_dataflow_definition(filepath):
        """Validate a dataflow_definition.json file."""
        errors = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                df = json.load(f)
            if not isinstance(df, dict):
                errors.append('Dataflow definition must be a JSON object')
                return False, errors
            if 'mashupDocument' not in df and 'mashup' not in df:
                errors.append('Missing "mashupDocument" (or "mashup") section')
            return len(errors) == 0, errors
        except json.JSONDecodeError as e:
            return False, [f'Invalid JSON: {e}']
        except Exception as e:
            return False, [f'Error: {e}']

    @staticmethod
    def validate_pipeline_definition(filepath):
        """Validate a pipeline_definition.json file."""
        errors = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                pl = json.load(f)
            if not isinstance(pl, dict):
                errors.append('Pipeline definition must be a JSON object')
                return False, errors
            props = pl.get('properties', {})
            if 'activities' not in props:
                errors.append('Missing "properties.activities" array')
            elif not isinstance(props['activities'], list):
                errors.append('"properties.activities" must be an array')
            if 'name' not in pl:
                errors.append('Missing "name"')
            return len(errors) == 0, errors
        except json.JSONDecodeError as e:
            return False, [f'Invalid JSON: {e}']
        except Exception as e:
            return False, [f'Error: {e}']

    @classmethod
    def validate_project(cls, project_dir):
        """Validate a complete Fabric project directory."""
        project_dir = Path(project_dir)
        errors = []
        warnings = []
        files_checked = 0

        if not project_dir.exists():
            return {'valid': False, 'errors': [f'Not found: {project_dir}'],
                    'warnings': [], 'files_checked': 0}

        report_name = project_dir.name

        # .pbip file
        pbip_file = project_dir / f'{report_name}.pbip'
        if pbip_file.exists():
            files_checked += 1
            valid, err = cls.validate_json_file(pbip_file)
            if not valid:
                errors.append(err)
        else:
            errors.append(f'Missing .pbip file: {pbip_file.name}')

        # Report directory
        report_dir = project_dir / f'{report_name}.Report'
        if report_dir.exists():
            for jf in ['definition.pbir']:
                fp = report_dir / jf
                if fp.exists():
                    files_checked += 1
                    valid, err = cls.validate_json_file(fp)
                    if not valid:
                        errors.append(err)

            # Pages & visuals
            pages_dir = report_dir / 'definition' / 'pages'
            if pages_dir.exists():
                for page_dir in pages_dir.iterdir():
                    if page_dir.is_dir():
                        pj = page_dir / 'page.json'
                        if pj.exists():
                            files_checked += 1
                            valid, err = cls.validate_json_file(pj)
                            if not valid:
                                errors.append(err)
                        vd = page_dir / 'visuals'
                        if vd.exists():
                            for vdir in vd.iterdir():
                                if vdir.is_dir():
                                    vj = vdir / 'visual.json'
                                    if vj.exists():
                                        files_checked += 1
                                        valid, err = cls.validate_json_file(vj)
                                        if not valid:
                                            errors.append(err)
        else:
            warnings.append(f'Missing Report directory (may not have been generated)')

        # SemanticModel directory
        sm_dir = project_dir / f'{report_name}.SemanticModel'
        if sm_dir.exists():
            model_tmdl = sm_dir / 'definition' / 'model.tmdl'
            if model_tmdl.exists():
                files_checked += 1
                valid, errs = cls.validate_tmdl_file(model_tmdl)
                if not valid:
                    errors.extend(errs)
            else:
                errors.append('Missing model.tmdl')
            tables_dir = sm_dir / 'definition' / 'tables'
            if tables_dir.exists():
                for tf in tables_dir.glob('*.tmdl'):
                    files_checked += 1
                    valid, errs = cls.validate_tmdl_file(tf)
                    if not valid:
                        errors.extend(errs)
        else:
            warnings.append(f'Missing SemanticModel directory')

        # Lakehouse directory
        lh_dir = project_dir / f'{report_name}.Lakehouse'
        if lh_dir.exists():
            lh_def = lh_dir / 'lakehouse_definition.json'
            if lh_def.exists():
                files_checked += 1
                valid, errs = cls.validate_lakehouse_definition(lh_def)
                if not valid:
                    errors.extend(errs)

        # Dataflow directory
        df_dir = project_dir / f'{report_name}.Dataflow'
        if df_dir.exists():
            df_def = df_dir / 'dataflow_definition.json'
            if df_def.exists():
                files_checked += 1
                valid, errs = cls.validate_dataflow_definition(df_def)
                if not valid:
                    errors.extend(errs)

        # Notebook directory
        nb_dir = project_dir / f'{report_name}.Notebook'
        if nb_dir.exists():
            for nb_file in nb_dir.glob('*.ipynb'):
                files_checked += 1
                valid, errs = cls.validate_notebook(nb_file)
                if not valid:
                    errors.extend(errs)

        # Backward-compat: plain 'SemanticModel' directory (no name prefix)
        # Only check if the named {name}.SemanticModel was NOT already validated above
        if not sm_dir.exists():
            standalone_sm = project_dir / 'SemanticModel'
            if standalone_sm.exists():
                model_tmdl = standalone_sm / 'definition' / 'model.tmdl'
                if model_tmdl.exists():
                    files_checked += 1
                    valid, errs = cls.validate_tmdl_file(model_tmdl)
                    if not valid:
                        errors.extend(errs)
                tables_dir = standalone_sm / 'definition' / 'tables'
                if tables_dir.exists():
                    for tf in tables_dir.glob('*.tmdl'):
                        files_checked += 1
                        valid, errs = cls.validate_tmdl_file(tf)
                        if not valid:
                            errors.extend(errs)

        # Pipeline directory ({name}.Pipeline)
        pl_dir = project_dir / f'{report_name}.Pipeline'
        if not pl_dir.exists():
            # Fallback: also check plain 'Pipeline' for backward compat
            pl_dir = project_dir / 'Pipeline'
        if pl_dir.exists():
            pl_def = pl_dir / 'pipeline_definition.json'
            if pl_def.exists():
                files_checked += 1
                valid, errs = cls.validate_pipeline_definition(pl_def)
                if not valid:
                    errors.extend(errs)

        is_valid = len(errors) == 0
        result = {'valid': is_valid, 'errors': errors,
                  'warnings': warnings, 'files_checked': files_checked}

        status = '[OK]' if is_valid else '[FAIL]'
        logger.info(f'{status} {report_name}: {files_checked} files, '
                     f'{len(errors)} errors, {len(warnings)} warnings')
        for e in errors:
            logger.warning(f'  ERROR: {e}')
        for w in warnings:
            logger.info(f'  WARN: {w}')
        return result

    @classmethod
    def validate_directory(cls, artifacts_dir):
        """Validate all projects in a directory."""
        artifacts_dir = Path(artifacts_dir)
        results = {}
        if not artifacts_dir.exists():
            logger.error(f'Directory not found: {artifacts_dir}')
            return results
        for item in sorted(artifacts_dir.iterdir()):
            if item.is_dir():
                pbip_files = list(item.glob('*.pbip'))
                if pbip_files:
                    results[item.name] = cls.validate_project(item)
        for json_file in sorted(artifacts_dir.glob('*.json')):
            is_valid, errs = cls.validate_artifact(json_file)
            results[json_file.name] = {'valid': is_valid, 'errors': errs,
                                       'warnings': [], 'files_checked': 1}
        return results
