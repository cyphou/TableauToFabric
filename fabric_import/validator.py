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
import re
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

    # ── DAX validation ─────────────────────────────────────────────

    # Tableau functions that should never appear in valid DAX
    _TABLEAU_FUNCTION_LEAK_PATTERNS = [
        (r'\bCOUNTD\s*\(', 'COUNTD (use DISTINCTCOUNT)'),
        (r'\bZN\s*\(', 'ZN (use IF(ISBLANK(...)))'),
        (r'\bIFNULL\s*\(', 'IFNULL (use IF(ISBLANK(...)))'),
        (r'\bATTR\s*\(', 'ATTR (use VALUES)'),
        (r'(?<![<>!])={2}(?!=)', 'Double-equals == (use single =)'),
        (r'\bELSEIF\b', 'ELSEIF (use nested IF)'),
        (r'(?<!\{)\{(?:FIXED|INCLUDE|EXCLUDE)\s', 'LOD expression {FIXED/INCLUDE/EXCLUDE}'),
        (r'\bDATETRUNC\s*\(', 'DATETRUNC (use STARTOF*)'),
        (r'\bDATEPART\s*\(', 'DATEPART (use YEAR/MONTH/DAY)'),
        (r'\bMAKEPOINT\s*\(', 'MAKEPOINT (spatial — no DAX equivalent)'),
        (r'\bSCRIPT_(?:BOOL|INT|REAL|STR)\s*\(', 'SCRIPT_* analytics extension'),
    ]

    # Regex helpers for semantic reference validation
    _RE_TABLE_DEF = re.compile(r"^table\s+'?([^']+?)'?\s*$")
    _RE_COL_DEF = re.compile(r"^column\s+'?([^']+?)'?\s*$")
    _RE_MEASURE_DEF = re.compile(r"^measure\s+'?([^']+?)'?\s*$")
    _RE_DAX_REF = re.compile(r"'([^']+?)'\[([^\]]+)\]")

    @classmethod
    def validate_dax_formula(cls, formula, context=''):
        """Validate a single DAX formula for common issues.

        Checks:
        - Balanced parentheses
        - Tableau function leakage
        - Unresolved [Parameters].[X] references

        Args:
            formula: DAX formula string
            context: Optional context label (measure/column name) for error messages

        Returns:
            list of error/warning strings (empty = valid)
        """
        issues = []
        if not formula or not formula.strip():
            return issues

        ctx = f' in {context}' if context else ''

        # 1. Balanced parentheses
        depth = 0
        for ch in formula:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth < 0:
                    issues.append(f'Unmatched closing parenthesis{ctx}')
                    break
        if depth > 0:
            issues.append(f'Unmatched opening parenthesis ({depth} unclosed){ctx}')

        # 2. Tableau function leakage
        for pattern, description in cls._TABLEAU_FUNCTION_LEAK_PATTERNS:
            if re.search(pattern, formula):
                issues.append(f'Tableau function leak: {description}{ctx}')

        # 3. Unresolved parameter references [Parameters].[X]
        if re.search(r'\[Parameters\]\s*\.\s*\[', formula):
            issues.append(f'Unresolved parameter reference [Parameters].[...]{ctx}')

        return issues

    @classmethod
    def validate_tmdl_dax(cls, filepath):
        """Validate all DAX formulas inside a TMDL file.

        Scans for 'expression =' and 'expression =\\n' patterns to extract
        DAX from table/measure/column definitions.

        Args:
            filepath: Path to .tmdl file

        Returns:
            list of issue strings
        """
        issues = []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception:
            return issues

        basename = os.path.basename(filepath)
        current_object = basename

        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Track current object name
            for prefix in ('measure ', 'column ', 'table '):
                if stripped.startswith(prefix):
                    current_object = stripped

            # Single-line expression
            if stripped.startswith('expression =') and not stripped.endswith('```'):
                formula = stripped[len('expression ='):].strip()
                # Skip M expressions (Power Query)
                if not formula.lstrip().startswith('let') and not formula.lstrip().startswith('//'):
                    issues.extend(cls.validate_dax_formula(formula, current_object))

            # Multi-line expression block (``` delimited)
            if stripped.startswith('expression =') and stripped.endswith('```'):
                formula_lines = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('```'):
                    formula_lines.append(lines[i])
                    i += 1
                formula = '\n'.join(formula_lines)
                # Skip M expressions
                if not formula.lstrip().startswith('let') and not formula.lstrip().startswith('//'):
                    issues.extend(cls.validate_dax_formula(formula, current_object))

            i += 1

        return issues

    @classmethod
    def _collect_model_symbols(cls, sm_dir):
        """Collect all table names, column names, and measure names
        from the SemanticModel TMDL files.

        Args:
            sm_dir: Path to ``{name}.SemanticModel`` directory.

        Returns:
            dict with keys ``tables`` (set), ``columns`` (dict), ``measures`` (dict).
        """
        tables = set()
        columns = {}
        measures = {}

        def _scan_tmdl(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    file_lines = f.readlines()
            except Exception:
                return
            current_table = None
            for file_line in file_lines:
                stripped_line = file_line.strip()
                tm = cls._RE_TABLE_DEF.match(stripped_line)
                if tm:
                    current_table = tm.group(1)
                    tables.add(current_table)
                    columns.setdefault(current_table, set())
                    measures.setdefault(current_table, set())
                    continue
                if current_table:
                    cm = cls._RE_COL_DEF.match(stripped_line)
                    if cm:
                        columns[current_table].add(cm.group(1))
                        continue
                    mm = cls._RE_MEASURE_DEF.match(stripped_line)
                    if mm:
                        measures[current_table].add(mm.group(1))
                        continue

        sm_path = Path(sm_dir)
        def_dir = sm_path / 'definition'

        model_tmdl = def_dir / 'model.tmdl'
        if model_tmdl.exists():
            _scan_tmdl(str(model_tmdl))

        tables_dir = def_dir / 'tables'
        if tables_dir.exists():
            for tmdl_f in tables_dir.glob('*.tmdl'):
                _scan_tmdl(str(tmdl_f))

        return {'tables': tables, 'columns': columns, 'measures': measures}

    @classmethod
    def validate_semantic_references(cls, sm_dir):
        """Validate that DAX column references (``'Table'[Column]``) in TMDL
        files actually exist in the model.

        Args:
            sm_dir: Path to ``{name}.SemanticModel`` directory.

        Returns:
            list of warning strings for unresolved references.
        """
        symbols = cls._collect_model_symbols(sm_dir)
        known_tables = symbols['tables']
        known_cols = symbols['columns']
        known_measures = symbols['measures']
        warnings_list = []

        sm_path = Path(sm_dir)
        def_dir = sm_path / 'definition'

        tmdl_files = []
        model_tmdl = def_dir / 'model.tmdl'
        if model_tmdl.exists():
            tmdl_files.append(model_tmdl)
        tables_dir = def_dir / 'tables'
        if tables_dir.exists():
            tmdl_files.extend(tables_dir.glob('*.tmdl'))
        roles_file = def_dir / 'roles.tmdl'
        if roles_file.exists():
            tmdl_files.append(roles_file)

        for tmdl_file in tmdl_files:
            try:
                content = tmdl_file.read_text(encoding='utf-8')
            except Exception:
                continue
            basename = tmdl_file.name
            for match in cls._RE_DAX_REF.finditer(content):
                table_ref = match.group(1)
                col_ref = match.group(2)
                if table_ref not in known_tables:
                    warnings_list.append(
                        f'Unknown table reference \'{table_ref}\' in {basename}'
                    )
                else:
                    all_fields = known_cols.get(table_ref, set()) | known_measures.get(table_ref, set())
                    if col_ref not in all_fields:
                        warnings_list.append(
                            f'Unknown column/measure [{col_ref}] in table \'{table_ref}\' ({basename})'
                        )

        return warnings_list

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
