"""
TMDL (Tabular Model Definition Language) Generator — Fabric Edition

Converts extracted Tableau data directly into TMDL files
for the Power BI SemanticModel using DirectLake mode
connected to a Fabric Lakehouse.

Key difference from the standard PBI version:
- Partitions use DirectLake entity expressions referencing
  Lakehouse Delta tables instead of M query import partitions
- Model uses DirectLake defaultMode
- No M query expressions needed (data lives in Lakehouse)

Handles:
- Physical tables with DirectLake partitions
- DAX measures and calculated columns
- Relationships (manyToOne, manyToMany)
- Hierarchies, sets, groups, bins
- Parameter tables (What-If)
- Date table with time intelligence
- Geographic data categories
- RLS roles from Tableau user filters

Generated structure:
  definition/
    database.tmdl
    model.tmdl
    relationships.tmdl
    expressions.tmdl
    roles.tmdl (if RLS)
    tables/
      {TableName}.tmdl
"""

import sys
import os
import re
import uuid
import json

# Import from tableau_export package
from tableau_export.datasource_extractor import (
    convert_tableau_formula_to_dax,
    map_tableau_to_powerbi_type
)

from .calc_column_utils import classify_calculations, sanitize_calc_col_name


# ════════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ════════════════════════════════════════════════════════════════════

def generate_tmdl(datasources, report_name, extra_objects, output_dir,
                  lakehouse_name=None, calendar_start=None, calendar_end=None,
                  culture=None):
    """
    Main entry point: convert extracted Tableau data to TMDL files
    using DirectLake mode for Fabric Lakehouse.

    Args:
        datasources: List of datasources with connections, tables, calculations
        report_name: Name of the report
        extra_objects: Dict with hierarchies, sets, groups, bins, aliases,
                       parameters, user_filters, _datasources
        output_dir: Path to the SemanticModel folder
        lakehouse_name: Name of the target Lakehouse (defaults to report_name)
        calendar_start: Start year for Calendar table (default: 2020)
        calendar_end: End year for Calendar table (default: 2030)
        culture: Override culture/locale for semantic model (e.g., fr-FR)

    Returns:
        dict: Statistics about the generated model
    """
    if extra_objects is None:
        extra_objects = {}

    lh_name = lakehouse_name or report_name

    # Step 1: Build the semantic model (DirectLake mode)
    model = _build_semantic_model(datasources, report_name, extra_objects,
                                   lakehouse_name=lh_name,
                                   calendar_start=calendar_start,
                                   calendar_end=calendar_end,
                                   culture=culture)

    # Step 2: Write TMDL files
    _write_tmdl_files(model, output_dir)

    # Step 3: Compute and return stats
    tables = model.get('model', {}).get('tables', [])
    rels = model.get('model', {}).get('relationships', [])
    stats = {
        'tables': len(tables),
        'columns': sum(len(t.get('columns', [])) for t in tables),
        'measures': sum(len(t.get('measures', [])) for t in tables),
        'relationships': len(rels),
        'hierarchies': sum(len(t.get('hierarchies', [])) for t in tables),
        'roles': len(model.get('model', {}).get('roles', []))
    }
    return stats


# ════════════════════════════════════════════════════════════════════
#  SEMANTIC MODEL BUILDING (DirectLake)
# ════════════════════════════════════════════════════════════════════

from .naming import sanitize_tmdl_table_name as _sanitize_table_name  # noqa: E402


def _build_semantic_model(datasources, report_name="Report", extra_objects=None,
                          lakehouse_name=None, calendar_start=None,
                          calendar_end=None, culture=None):
    """
    Build a complete semantic model from extracted Tableau datasources
    using DirectLake mode for Fabric Lakehouse.

    Tables reference Lakehouse Delta tables via entity expressions
    instead of M query import partitions.
    """
    if extra_objects is None:
        extra_objects = {}

    effective_culture = culture or 'en-US'

    model = {
        "name": report_name,
        "compatibilityLevel": 1604,
        "model": {
            "culture": effective_culture,
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "defaultMode": "directLake",
            "tables": [],
            "relationships": [],
            "roles": [],
            "expressions": []
        }
    }

    # Phase 1: Collect all physical tables and deduplicate
    best_tables = {}
    all_calculations = []
    all_columns_metadata = []

    for ds in datasources:
        ds_connection = ds.get('connection', {})
        connection_map = ds.get('connection_map', {})
        calculations = ds.get('calculations', [])
        all_calculations.extend(calculations)
        all_columns_metadata.extend(ds.get('columns', []))

        for table in ds.get('tables', []):
            table_name = table.get('name', 'Table1')
            if not table_name or table_name == 'Unknown':
                continue

            col_count = len(table.get('columns', []))
            table_conn = table.get('connection_details', {})
            if not table_conn:
                conn_ref = table.get('connection', '')
                table_conn = connection_map.get(conn_ref, ds_connection)

            if table_name not in best_tables or col_count > len(best_tables[table_name][0].get('columns', [])):
                best_tables[table_name] = (table, table_conn)

    # Phase 2: Identify the main table (fact table = most columns)
    main_table_name = None
    max_cols = 0
    for tname, (table, conn) in best_tables.items():
        ncols = len(table.get('columns', []))
        if ncols > max_cols:
            max_cols = ncols
            main_table_name = tname

    # Phase 2a: Build column metadata mapping
    col_metadata_map = {}
    for cm in all_columns_metadata:
        raw = cm.get('name', '').replace('[', '').replace(']', '')
        caption = cm.get('caption', raw)
        key = caption if caption else raw
        col_metadata_map[key] = cm
        col_metadata_map[raw] = cm

    # Phase 2b: Build context mappings for DAX conversion
    calc_map = {}
    for calc in all_calculations:
        raw = calc.get('name', '').replace('[', '').replace(']', '')
        caption = calc.get('caption', raw)
        if raw and raw != caption:
            calc_map[raw] = caption

    param_map = {}
    for ds in datasources:
        if ds.get('name', '') == 'Parameters':
            for calc in ds.get('calculations', []):
                raw = calc.get('name', '').replace('[', '').replace(']', '')
                caption = calc.get('caption', raw)
                if raw:
                    param_map[raw] = caption
    for param in extra_objects.get('parameters', []):
        raw_name = param.get('name', '')
        caption = param.get('caption', '')
        if raw_name and caption:
            match = re.match(r'\[Parameters\]\.\[([^\]]+)\]', raw_name)
            if match:
                param_map[match.group(1)] = caption
            else:
                clean = raw_name.replace('[', '').replace(']', '')
                if clean and clean not in param_map:
                    param_map[clean] = caption

    column_table_map = {}
    for tname, (table, conn) in best_tables.items():
        for col in table.get('columns', []):
            cname = col.get('name', '')
            if cname and cname not in column_table_map:
                column_table_map[cname] = tname

    measure_names = set()
    for calc in all_calculations:
        caption = calc.get('caption', calc.get('name', '').replace('[', '').replace(']', ''))
        if caption:
            measure_names.add(caption)
    measure_names.update(param_map.values())

    param_values = {}
    for calc in all_calculations:
        caption = calc.get('caption', calc.get('name', '').replace('[', '').replace(']', ''))
        formula = calc.get('formula', '').strip()
        if caption and formula and '[' not in formula:
            param_values[caption] = formula
    for param in extra_objects.get('parameters', []):
        caption = param.get('caption', '')
        value = param.get('value', '').strip('"')
        if caption and value and caption not in param_values:
            datatype = param.get('datatype', 'string')
            if datatype == 'string':
                param_values[caption] = f'"{value}"'
            elif datatype in ('date', 'datetime'):
                date_m = re.match(r'#(\d{4})-(\d{2})-(\d{2})#', value)
                if date_m:
                    param_values[caption] = f'DATE({int(date_m.group(1))}, {int(date_m.group(2))}, {int(date_m.group(3))})'
                else:
                    param_values[caption] = value
            else:
                param_values[caption] = value

    for param in extra_objects.get('parameters', []):
        caption = param.get('caption', '')
        if caption:
            measure_names.add(caption)

    dax_context = {
        'calc_map': calc_map,
        'param_map': param_map,
        'column_table_map': column_table_map,
        'measure_names': measure_names,
        'param_values': param_values
    }

    # Phase 3: Create tables with DirectLake partitions
    for table_name, (table, table_conn) in best_tables.items():
        table_calculations = all_calculations if table_name == main_table_name else []

        tbl = _build_table_directlake(
            table=table,
            calculations=table_calculations,
            dax_context=dax_context,
            col_metadata_map=col_metadata_map,
            extra_objects=extra_objects,
            lakehouse_name=lakehouse_name
        )
        model["model"]["tables"].append(tbl)

    # Phase 4: Create relationships
    seen_rels = set()
    for ds in datasources:
        relationships = ds.get('relationships', [])
        rels = _build_relationships(relationships)
        for rel in rels:
            key = (rel.get('fromTable'), rel.get('fromColumn'),
                   rel.get('toTable'), rel.get('toColumn'))
            if key not in seen_rels:
                seen_rels.add(key)
                model["model"]["relationships"].append(rel)

    # Validate relationships
    valid_relationships = []
    table_columns = {}
    for table in model["model"]["tables"]:
        tname = table.get("name", "")
        table_columns[tname] = {col.get("name", "") for col in table.get("columns", [])}

    for rel in model["model"]["relationships"]:
        from_table = rel.get("fromTable", "")
        to_table = rel.get("toTable", "")
        from_col = rel.get("fromColumn", "")
        to_col = rel.get("toColumn", "")

        if (from_table in table_columns and to_table in table_columns
                and from_col in table_columns[from_table]
                and to_col in table_columns[to_table]
                and from_table != to_table):
            valid_relationships.append(rel)

    model["model"]["relationships"] = valid_relationships

    # Phase 4b: Fix type mismatches
    _fix_relationship_type_mismatches(model)

    # Phase 4c: Data blending relationships
    blending = extra_objects.get('data_blending', [])
    for blend in blending:
        primary_ds = blend.get('primary_datasource', '')
        secondary_ds = blend.get('secondary_datasource', '')
        for link in blend.get('link_fields', []):
            p_field = link.get('primary_field', '')
            s_field = link.get('secondary_field', '')
            if p_field and s_field and primary_ds and secondary_ds:
                key = (primary_ds, p_field, secondary_ds, s_field)
                if key not in seen_rels:
                    seen_rels.add(key)
                    model["model"]["relationships"].append({
                        "name": f"Blend-{len(model['model']['relationships'])+1}",
                        "fromTable": secondary_ds,
                        "fromColumn": s_field,
                        "toTable": primary_ds,
                        "toColumn": p_field,
                        "joinType": "left",
                        "crossFilteringBehavior": "oneDirection",
                    })

    # Phase 5: Sets, groups, bins as calculated columns
    _process_sets_groups_bins(model, extra_objects, main_table_name, column_table_map)

    # Phase 6: Date table if date columns detected (skip for DirectLake — import
    # partitions are not allowed alongside DirectLake partitions)
    is_direct_lake = model["model"].get("defaultMode") == "directLake"
    has_date_columns = False
    for table in model["model"]["tables"]:
        for col in table.get("columns", []):
            if col.get("dataType") == "DateTime" or col.get("dataCategory") == "DateTime":
                has_date_columns = True
                break
        if has_date_columns:
            break
    if has_date_columns and not is_direct_lake:
        _add_date_table(model, calendar_start=calendar_start,
                        calendar_end=calendar_end)

    # Phase 7: Hierarchies
    _apply_hierarchies(model, extra_objects.get('hierarchies', []), column_table_map)

    # Phase 7b: Auto-generate date hierarchies for date columns
    _auto_date_hierarchies(model)

    # Phase 8: Parameter tables
    _create_parameter_tables(model, extra_objects.get('parameters', []), main_table_name)

    # Phase 9: RLS roles
    _create_rls_roles(model, extra_objects.get('user_filters', []),
                      main_table_name, column_table_map)

    # Phase 10: Infer missing relationships
    _infer_cross_table_relationships(model)

    # Phase 10b: Detect cardinality
    _detect_many_to_many(model, datasources)

    # Phase 10c: Fix RELATED for manyToMany
    _fix_related_for_many_to_many(model)

    # Phase 11: Deactivate ambiguous paths
    _deactivate_ambiguous_paths(model)

    # Deduplicate measures globally
    global_measure_names = set()
    for table in model["model"]["tables"]:
        unique_measures = []
        for measure in table.get("measures", []):
            mname = measure.get("name", "")
            if mname not in global_measure_names:
                global_measure_names.add(mname)
                unique_measures.append(measure)
        table["measures"] = unique_measures

    # Phase 12: Perspectives
    all_table_names = [t.get('name', '') for t in model["model"]["tables"]]
    model["model"]["perspectives"] = [{
        "name": "Full Model",
        "tables": all_table_names
    }]

    # Phase 13: Calculation groups from parameter actions
    _create_calculation_groups(model, extra_objects.get('parameters', []),
                                main_table_name)

    # Phase 14: Field parameters from dynamic dimension/measure swaps
    _create_field_parameters(model, extra_objects.get('parameters', []),
                              main_table_name, column_table_map)

    return model


def _build_table_directlake(table, calculations, dax_context=None,
                             col_metadata_map=None, extra_objects=None,
                             lakehouse_name=None):
    """
    Create a semantic model table using DirectLake mode.

    Instead of M query partitions, uses entity expressions that reference
    Lakehouse Delta tables directly.
    """
    if dax_context is None:
        dax_context = {}
    if col_metadata_map is None:
        col_metadata_map = {}
    if extra_objects is None:
        extra_objects = {}

    table_name = table.get('name', 'Table1')
    columns = table.get('columns', [])

    # DirectLake entity name = sanitized table name in Lakehouse
    lakehouse_table = _sanitize_table_name(table_name)

    result_table = {
        "name": table_name,
        "columns": [],
        "partitions": [
            {
                "name": f"Partition-{table_name}",
                "mode": "directLake",
                "source": {
                    "type": "entity",
                    "entityName": lakehouse_table,
                    "schemaName": "dbo",
                    "expressionSource": "DatabaseQuery"
                }
            }
        ],
        "measures": []
    }

    # Track column names
    column_name_counts = {}

    for col in columns:
        original_col_name = col.get('name', 'Column')

        if original_col_name in column_name_counts:
            column_name_counts[original_col_name] += 1
            unique_col_name = f"{original_col_name}_{column_name_counts[original_col_name]}"
        else:
            column_name_counts[original_col_name] = 0
            unique_col_name = original_col_name

        bim_column = {
            "name": unique_col_name,
            "dataType": map_tableau_to_powerbi_type(col.get('datatype', 'string')),
            "sourceColumn": col.get('name', 'Column'),
            "summarizeBy": "none"
        }

        col_meta = col_metadata_map.get(unique_col_name, col_metadata_map.get(col.get('name', ''), {}))
        if col_meta.get('hidden', False):
            bim_column["isHidden"] = True
        if col_meta.get('description', ''):
            bim_column["description"] = col_meta['description']

        semantic_role = col_meta.get('semantic_role', '')
        geo_category = _map_semantic_role_to_category(semantic_role, unique_col_name)
        if geo_category:
            bim_column["dataCategory"] = geo_category

        if col.get('datatype') == 'date' or col.get('datatype') == 'datetime':
            bim_column["dataCategory"] = "DateTime"
            bim_column["formatString"] = "General Date"
        elif col.get('datatype') in ['integer', 'real']:
            bim_column["summarizeBy"] = "sum"
            if col.get('datatype') == 'real':
                bim_column["formatString"] = "#,0.00"

        # Apply Tableau number format if available (overrides default)
        tableau_fmt = col_meta.get('default_format', '') or col.get('default_format', '')
        if tableau_fmt:
            pbi_fmt = _convert_tableau_format_to_pbi(tableau_fmt)
            if pbi_fmt:
                bim_column["formatString"] = pbi_fmt

        result_table["columns"].append(bim_column)

    # Process calculations (measures + calculated columns)
    column_table_map = dax_context.get('column_table_map', {})
    calc_map_ctx = dax_context.get('calc_map', {})
    param_values = dax_context.get('param_values', {})
    measure_names_ctx = dax_context.get('measure_names', set())

    from .constants import AGG_PATTERN as _agg_pattern  # noqa: E402

    # Pre-classification pass
    prelim_calc_col_captions = set()
    prelim_calc_col_raws = set()
    for _pc in calculations:
        _pc_name = _pc.get('name', '').replace('[', '').replace(']', '')
        _pc_caption = _pc.get('caption', _pc_name)
        _pc_formula = _pc.get('formula', '').strip()
        _pc_role = _pc.get('role', 'measure')
        _pc_is_literal = _pc_formula and '[' not in _pc_formula
        _pc_has_agg = bool(_agg_pattern.search(_pc_formula))
        _pc_refs = re.findall(r'\[([^\]]+)\]', _pc_formula)
        _pc_has_col = False
        for _r in _pc_refs:
            if _r == _pc_caption or _r.startswith('Parameters'):
                continue
            if not (_r in measure_names_ctx or _r in calc_map_ctx.values() or _r in calc_map_ctx):
                _pc_has_col = True
                break
        _pc_is_cc = (not _pc_is_literal) and (
            _pc_role == 'dimension' or
            (_pc_role == 'measure' and not _pc_has_agg and _pc_has_col)
        )
        if _pc_is_cc:
            prelim_calc_col_captions.add(_pc_caption)
            prelim_calc_col_raws.add(_pc_name)

    for calc in calculations:
        calc_name = calc.get('name', '').replace('[', '').replace(']', '')
        caption = calc.get('caption', calc_name)
        formula = calc.get('formula', '').strip()
        role = calc.get('role', 'measure')
        datatype = calc.get('datatype', 'string')

        is_literal = formula and '[' not in formula

        has_aggregation = bool(_agg_pattern.search(formula))
        refs_in_formula = re.findall(r'\[([^\]]+)\]', formula)
        has_column_refs = False
        references_only_measures = True
        for ref in refs_in_formula:
            if ref == caption:
                continue
            if ref.startswith('Parameters'):
                continue
            is_known_calc = (ref in measure_names_ctx or
                             ref in calc_map_ctx.values() or
                             ref in calc_map_ctx)
            is_calc_col_ref = (ref in prelim_calc_col_captions or
                               ref in prelim_calc_col_raws)
            is_measure_ref = is_known_calc and not is_calc_col_ref
            if not is_measure_ref:
                has_column_refs = True
                references_only_measures = False
                break

        is_calc_col = (not is_literal) and (
            role == 'dimension' or
            (role == 'measure' and not has_aggregation and has_column_refs)
        )

        if is_calc_col and not has_column_refs and references_only_measures:
            is_calc_col = False

        has_security_func = bool(re.search(
            r'\b(USERPRINCIPALNAME|USERNAME|CUSTOMDATA|USERCULTURE)\s*\(',
            dax_context.get('_preview_dax', formula), re.IGNORECASE
        )) or bool(re.search(
            r'\b(USERNAME|FULLNAME|USERDOMAIN|ISMEMBEROF)\s*\(',
            formula, re.IGNORECASE
        ))
        if has_security_func:
            is_calc_col = False

        if re.search(r'\bMAKEPOINT\b', formula, re.IGNORECASE):
            continue

        dax_formula = convert_tableau_formula_to_dax(
            formula,
            column_name=calc_name,
            table_name=table_name,
            calc_map=dax_context.get('calc_map'),
            param_map=dax_context.get('param_map'),
            column_table_map=column_table_map,
            measure_names=dax_context.get('measure_names'),
            is_calc_column=is_calc_col,
            param_values=param_values,
            calc_datatype=datatype
        )

        if is_calc_col:
            # ── Materialised calculated column ──────────────────
            # The column is physically computed and stored in the
            # Lakehouse by a Dataflow or Notebook, so we reference
            # it as a regular sourceColumn (not a DAX expression).
            bim_calc_col = {
                "name": caption,
                "dataType": map_tableau_to_powerbi_type(datatype),
                "sourceColumn": sanitize_calc_col_name(caption),
                "summarizeBy": "none",
            }
            if datatype == 'real':
                bim_calc_col["formatString"] = "#,0.00"

            calc_meta = col_metadata_map.get(caption, col_metadata_map.get(calc_name, {}))
            if calc_meta.get('hidden', False):
                bim_calc_col["isHidden"] = True
            if calc_meta.get('description', ''):
                bim_calc_col["description"] = calc_meta['description']
            sr = calc_meta.get('semantic_role', '')
            geo_cat = _map_semantic_role_to_category(sr, caption)
            if geo_cat:
                bim_calc_col["dataCategory"] = geo_cat

            # Store the original Tableau formula as an annotation
            # for documentation purposes.
            bim_calc_col["annotations"] = [{
                "name": "TableauFormula",
                "value": formula,
            }]

            result_table["columns"].append(bim_calc_col)
        else:
            bim_measure = {
                "name": caption,
                "expression": dax_formula,
                "formatString": _get_format_string(datatype),
                "displayFolder": _get_display_folder(datatype, role)
            }
            result_table["measures"].append(bim_measure)

    return result_table


# ════════════════════════════════════════════════════════════════════
#  RELATIONSHIPS
# ════════════════════════════════════════════════════════════════════

def _build_relationships(relationships):
    """Create relationships from Tableau joins."""
    result = []
    for rel in relationships:
        # Support both formats:
        #   Format A (Tableau extractor): from_table, from_column, to_table, to_column
        #   Format B (join XML):          left.table, left.column, right.table, right.column
        left = rel.get('left', {})
        right = rel.get('right', {})
        from_table = rel.get('from_table') or left.get('table', '')
        from_column = rel.get('from_column') or left.get('column', '')
        to_table = rel.get('to_table') or right.get('table', '')
        to_column = rel.get('to_column') or right.get('column', '')
        if not from_table or not to_table or not from_column or not to_column:
            continue
        join_type = rel.get('type', 'left')
        result.append({
            "name": f"Relationship-{len(result)+1}",
            "fromTable": from_table,
            "fromColumn": from_column,
            "toTable": to_table,
            "toColumn": to_column,
            "joinType": join_type,
            "crossFilteringBehavior": "bothDirections" if join_type == 'full' else "oneDirection"
        })
    return result


def _infer_cross_table_relationships(model):
    """Infer relationships between unconnected tables from DAX cross-references."""
    tables = model["model"]["tables"]
    relationships = model["model"]["relationships"]

    connected_pairs = set()
    for rel in relationships:
        ft = rel.get("fromTable", "")
        tt = rel.get("toTable", "")
        connected_pairs.add((ft, tt))
        connected_pairs.add((tt, ft))

    table_columns = {}
    for table in tables:
        tname = table.get("name", "")
        table_columns[tname] = {col.get("name", "") for col in table.get("columns", [])}

    cross_ref_pattern = re.compile(r"'([^']+)'\[([^\]]+)\]")
    needed_pairs = set()

    for table in tables:
        tname = table.get("name", "")
        for measure in table.get("measures", []):
            expr = measure.get("expression", "")
            for match in cross_ref_pattern.finditer(expr):
                ref_table = match.group(1)
                if ref_table != tname and ref_table in table_columns:
                    needed_pairs.add((tname, ref_table))
        for col in table.get("columns", []):
            if col.get("isCalculated"):
                expr = col.get("expression", "")
                for match in cross_ref_pattern.finditer(expr):
                    ref_table = match.group(1)
                    if ref_table != tname and ref_table in table_columns:
                        needed_pairs.add((tname, ref_table))

    for role in model["model"].get("roles", []):
        for tp in role.get("tablePermissions", []):
            perm_table = tp.get("name", "")
            expr = tp.get("filterExpression", "")
            for match in cross_ref_pattern.finditer(expr):
                ref_table = match.group(1)
                if ref_table != perm_table and ref_table in table_columns:
                    needed_pairs.add((perm_table, ref_table))

    for (source_table, ref_table) in needed_pairs:
        if (source_table, ref_table) in connected_pairs:
            continue

        source_cols = table_columns.get(source_table, set())
        ref_cols = table_columns.get(ref_table, set())
        best_match = None
        best_score = 0

        for sc in source_cols:
            for rc in ref_cols:
                sc_lower = sc.lower()
                rc_lower = rc.lower()
                score = 0
                if sc_lower == rc_lower:
                    score = 100
                elif sc_lower in rc_lower and len(sc_lower) >= 3:
                    score = 50 - (len(rc) - len(sc))
                elif rc_lower in sc_lower and len(rc_lower) >= 3:
                    score = 50 - (len(sc) - len(rc))
                elif len(sc_lower) >= 3 and len(rc_lower) >= 3:
                    common = 0
                    for a, b in zip(sc_lower, rc_lower):
                        if a == b:
                            common += 1
                        else:
                            break
                    if common >= 3:
                        score = common * 5
                if score > best_score:
                    best_score = score
                    best_match = (sc, rc)

        if best_match and best_score >= 15:
            from_col, to_col = best_match
            if len(source_cols) >= len(ref_cols):
                fact_table, dim_table = source_table, ref_table
                fk_col, pk_col = from_col, to_col
            else:
                fact_table, dim_table = ref_table, source_table
                fk_col, pk_col = to_col, from_col

            relationships.append({
                "name": f"inferred_{fact_table}_{dim_table}",
                "fromTable": fact_table,
                "fromColumn": fk_col,
                "toTable": dim_table,
                "toColumn": pk_col,
                "crossFilteringBehavior": "oneDirection"
            })
            connected_pairs.add((source_table, ref_table))
            connected_pairs.add((ref_table, source_table))


def _detect_many_to_many(model, datasources):
    """Determine cardinality for each relationship."""
    for rel in model['model']['relationships']:
        to_table = rel.get('toTable', '')
        to_col = rel.get('toColumn', '')
        join_type = rel.get('joinType', 'left')

        if join_type == 'full':
            rel['fromCardinality'] = 'many'
            rel['toCardinality'] = 'many'
            rel['crossFilteringBehavior'] = 'bothDirections'
            print(f"  \u26a0\ufe0f  Relation \u2192 '{to_table}.{to_col}' set to manyToMany (full join).")
        else:
            rel['fromCardinality'] = 'many'
            rel['toCardinality'] = 'one'
            rel['crossFilteringBehavior'] = 'oneDirection'
            print(f"  \u2713  Relation \u2192 '{to_table}.{to_col}' set to manyToOne (lookup table).")


def _fix_related_for_many_to_many(model):
    """Replace RELATED() with LOOKUPVALUE() for manyToMany relationships."""
    m2m_tables = {}
    for rel in model['model']['relationships']:
        if rel.get('fromCardinality') == 'many' and rel.get('toCardinality') == 'many':
            to_table = rel.get('toTable', '')
            to_col = rel.get('toColumn', '')
            from_table = rel.get('fromTable', '')
            from_col = rel.get('fromColumn', '')
            if to_table and to_table not in m2m_tables:
                m2m_tables[to_table] = (to_col, from_table, from_col)

    if not m2m_tables:
        return

    for table in model['model']['tables']:
        for col in table.get('columns', []):
            expr = col.get('expression', '')
            if expr and 'RELATED(' in expr:
                col['expression'] = _replace_related_with_lookupvalue(expr, m2m_tables)
        for measure in table.get('measures', []):
            expr = measure.get('expression', '')
            if expr and 'RELATED(' in expr:
                measure['expression'] = _replace_related_with_lookupvalue(expr, m2m_tables)


def _replace_related_with_lookupvalue(expr, m2m_tables):
    """Replace RELATED('table'[col]) with LOOKUPVALUE() for m2m tables."""
    pattern = r"RELATED\(('([^']+)'|([A-Za-z0-9_][A-Za-z0-9_ .-]*))\[([^\]]*(?:\]\][^\]]*)*)\]\)"

    def replacer(match):
        table_name = match.group(2) if match.group(2) else match.group(3)
        col_name = match.group(4)
        if table_name not in m2m_tables:
            return match.group(0)
        join_to_col, from_table, from_col = m2m_tables[table_name]
        t_ref = f"'{table_name}'" if not table_name.isidentifier() else table_name
        ft_ref = f"'{from_table}'" if not from_table.isidentifier() else from_table
        return f"LOOKUPVALUE({t_ref}[{col_name}], {t_ref}[{join_to_col}], {ft_ref}[{from_col}])"

    return re.sub(pattern, replacer, expr)


def _fix_relationship_type_mismatches(model):
    """Fix type mismatches between relationship key columns."""
    tables = {t.get('name', ''): t for t in model['model']['tables']}

    for rel in model['model']['relationships']:
        from_table = tables.get(rel.get('fromTable', ''))
        to_table = tables.get(rel.get('toTable', ''))
        if not from_table or not to_table:
            continue

        from_col_name = rel.get('fromColumn', '')
        to_col_name = rel.get('toColumn', '')
        from_col = next((c for c in from_table.get('columns', []) if c.get('name') == from_col_name), None)
        to_col = next((c for c in to_table.get('columns', []) if c.get('name') == to_col_name), None)
        if not from_col or not to_col:
            continue

        from_type = from_col.get('dataType', 'string')
        to_type = to_col.get('dataType', 'string')
        if from_type == to_type:
            continue

        print(f"  \u26a0\ufe0f  Type mismatch: {rel.get('fromTable')}.{from_col_name} ({from_type}) "
              f"-> {rel.get('toTable')}.{to_col_name} ({to_type}). Aligning to {from_type}.")
        to_col['dataType'] = from_type
        if from_type.lower() == 'string':
            to_col['summarizeBy'] = 'none'
            if 'formatString' in to_col:
                del to_col['formatString']


# ════════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════

def _map_semantic_role_to_category(semantic_role, col_name=''):
    """Map a Tableau semantic-role to a Power BI dataCategory."""
    role_map = {
        '[Country].[Name]': 'Country',
        '[Country].[ISO3166_2]': 'Country',
        '[State].[Name]': 'StateOrProvince',
        '[State].[Abbreviation]': 'StateOrProvince',
        '[County].[Name]': 'County',
        '[City].[Name]': 'City',
        '[ZipCode].[Name]': 'PostalCode',
        '[Latitude]': 'Latitude',
        '[Longitude]': 'Longitude',
        '[Geographical].[Latitude]': 'Latitude',
        '[Geographical].[Longitude]': 'Longitude',
        '[Address]': 'Address',
        '[Continent].[Name]': 'Continent',
    }
    if semantic_role in role_map:
        return role_map[semantic_role]

    if not semantic_role:
        name_lower = col_name.lower()
        if 'latitude' in name_lower or name_lower in ('lat', 'lat_upgrade'):
            return 'Latitude'
        if 'longitude' in name_lower or name_lower in ('lon', 'lng', 'long', 'long_upgrade'):
            return 'Longitude'
        if name_lower in ('city', 'ville', 'commune', 'label') and 'code' not in name_lower:
            return 'City'
        if name_lower in ('country', 'pays'):
            return 'Country'
        if any(x in name_lower for x in ['region', '\u00e9tat', 'state', 'province', 'd\u00e9partement']):
            return 'StateOrProvince'
        if 'postal' in name_lower or 'zip' in name_lower or 'code_postal' in name_lower:
            return 'PostalCode'

    return None


def _get_display_folder(datatype, role):
    """Determine the display folder based on type and role."""
    if role == 'dimension':
        return 'Dimensions'
    if datatype in ('real', 'integer', 'number'):
        return 'Measures'
    if datatype in ('date', 'datetime'):
        return 'Time Intelligence'
    if datatype == 'boolean':
        return 'Flags'
    return 'Calculations'


def _get_format_string(datatype):
    """Return the Power BI format string for a given type."""
    format_map = {
        'integer': '0',
        'real': '#,0.00',
        'currency': '$#,0.00',
        'percentage': '0.00%',
        'date': 'Short Date',
        'datetime': 'General Date',
        'boolean': 'True/False'
    }
    return format_map.get(datatype.lower(), '0')


def _convert_tableau_format_to_pbi(tableau_format):
    """Convert a Tableau number format string to Power BI format string.

    Tableau formats:  #,##0.00  |  0.0%  |  $#,##0  |  0.000  |  #,##0
    PBI formats:      #,0.00   |  0.0%  |  $#,0    |  0.000  |  #,0

    Args:
        tableau_format: Tableau format string (from default-format attribute)

    Returns:
        str: Power BI format string, or empty string if no conversion needed
    """
    if not tableau_format:
        return ''

    fmt = tableau_format.strip()

    # Already a PBI-compatible format
    if fmt in ('0', '#,0', '#,0.00', '0.00%', '$#,0.00', 'General Date', 'Short Date'):
        return fmt

    # Percentage formats
    if '%' in fmt:
        return fmt

    # Currency with symbol
    for symbol in ('$', '\u20ac', '\u00a3', '\u00a5'):
        if symbol in fmt:
            cleaned = fmt.replace('##0', '#0').replace('###', '#').replace(',,', ',')
            if '0' not in cleaned:
                cleaned = cleaned + '0'
            return cleaned

    # Numeric formats — convert Tableau's #,##0 → #,0 pattern
    result = fmt
    result = result.replace('#,##0', '#,0')
    result = result.replace('#,###', '#,#')
    if result and result[0] == '0':
        return result

    return result if result != fmt else fmt


def _process_sets_groups_bins(model, extra_objects, main_table_name, column_table_map):
    """Add sets, groups and bins as calculated columns."""
    if not main_table_name:
        return
    main_table = None
    for table in model["model"]["tables"]:
        if table.get("name") == main_table_name:
            main_table = table
            break
    if not main_table:
        return

    existing_cols = {col.get("name", "") for col in main_table.get("columns", [])}

    for s in extra_objects.get('sets', []):
        set_name = s.get('name', '')
        if not set_name or set_name in existing_cols:
            continue
        members = s.get('members', [])
        formula = s.get('formula', '')
        if formula:
            dax_expr = formula
        elif members:
            escaped = [f'"{m}"' for m in members[:50]]
            dax_expr = f"'{main_table_name}'[{set_name}] IN {{{', '.join(escaped)}}}"
        else:
            dax_expr = 'TRUE()'
        main_table["columns"].append({
            "name": set_name, "dataType": "Boolean",
            "expression": dax_expr, "summarizeBy": "none",
            "isCalculated": True, "displayFolder": "Sets"
        })
        existing_cols.add(set_name)

    for g in extra_objects.get('groups', []):
        group_name = g.get('name', '')
        if not group_name or group_name in existing_cols:
            continue
        group_type = g.get('group_type', 'values')
        members = g.get('members', {})
        source_field = g.get('source_field', '').replace('[', '').replace(']', '')
        source_fields = g.get('source_fields', [])

        if group_type == 'combined' and source_fields:
            calc_map_lookup = {}
            for ds in extra_objects.get('_datasources', []):
                for calc in ds.get('calculations', []):
                    raw = calc.get('name', '').replace('[', '').replace(']', '')
                    cap = calc.get('caption', raw)
                    calc_map_lookup[raw] = cap
            for table_obj in model.get('model', {}).get('tables', []):
                for col in table_obj.get('columns', []):
                    if col.get('isCalculated'):
                        col_name = col.get('name', '')
                        if col_name and col_name not in column_table_map:
                            column_table_map[col_name] = table_obj.get('name', main_table_name)
                for meas in table_obj.get('measures', []):
                    meas_name = meas.get('name', '')
                    if meas_name and meas_name not in column_table_map:
                        column_table_map[meas_name] = table_obj.get('name', main_table_name)
            parts = []
            for sf in source_fields:
                resolved = calc_map_lookup.get(sf, sf)
                table_ref = column_table_map.get(resolved, column_table_map.get(sf, main_table_name))
                escaped_col = resolved.replace(']', ']]')
                ref = f"'{table_ref}'[{escaped_col}]"
                if table_ref != main_table_name:
                    ref = f"RELATED({ref})"
                parts.append(ref)
            dax_expr = ' & " | " & '.join(parts) if len(parts) > 1 else (parts[0] if parts else '""')
        elif members and source_field:
            table_ref = column_table_map.get(source_field, main_table_name)
            cases = []
            for label, values in members.items():
                for val in values:
                    cases.append(f'"{val}", "{label}"')
            dax_expr = f"SWITCH('{table_ref}'[{source_field}], {', '.join(cases)}, \"Other\")" if cases else f"'{table_ref}'[{source_field}]"
        else:
            dax_expr = '""'

        main_table["columns"].append({
            "name": group_name, "dataType": "String",
            "expression": dax_expr, "summarizeBy": "none",
            "isCalculated": True, "displayFolder": "Groups"
        })
        existing_cols.add(group_name)

    for b in extra_objects.get('bins', []):
        bin_name = b.get('name', '')
        if not bin_name or bin_name in existing_cols:
            continue
        source_field = b.get('source_field', '').replace('[', '').replace(']', '')
        bin_size = b.get('size', '10')
        if source_field:
            table_ref = column_table_map.get(source_field, main_table_name)
            dax_expr = f"FLOOR('{table_ref}'[{source_field}], {bin_size})"
        else:
            dax_expr = '0'
        main_table["columns"].append({
            "name": bin_name, "dataType": "Double",
            "expression": dax_expr, "summarizeBy": "none",
            "isCalculated": True, "displayFolder": "Bins"
        })
        existing_cols.add(bin_name)


def _apply_hierarchies(model, hierarchies, column_table_map):
    """Apply Tableau hierarchies to the model."""
    if not hierarchies:
        return
    for h in hierarchies:
        h_name = h.get('name', '')
        levels = h.get('levels', [])
        if not h_name or not levels:
            continue
        first_level = levels[0]
        target_table_name = column_table_map.get(first_level, '')
        if not target_table_name:
            continue
        for table in model["model"]["tables"]:
            if table.get("name") == target_table_name:
                table_col_names = {col.get("name", "") for col in table.get("columns", [])}
                valid_levels = [l for l in levels if l in table_col_names]
                if valid_levels:
                    if "hierarchies" not in table:
                        table["hierarchies"] = []
                    hierarchy = {
                        "name": h_name,
                        "levels": [
                            {"name": lvl, "ordinal": idx, "column": lvl}
                            for idx, lvl in enumerate(valid_levels)
                        ]
                    }
                    table["hierarchies"].append(hierarchy)
                break


def _auto_date_hierarchies(model):
    """Auto-generate Year > Quarter > Month > Day hierarchies for date columns.

    For every date/dateTime column that does not already belong to a
    user-defined hierarchy, we create a synthetic 'Date Hierarchy'
    composed of calculated columns (Year, Quarter, Month, Day)
    and a hierarchy definition on the same table.
    """
    DATE_TYPES = {'dateTime', 'date'}
    PARTS = [
        ('Year', 'YEAR', 'int64', 0),
        ('Quarter', 'QUARTER', 'int64', 1),         # 1-4
        ('Month', 'MONTH', 'int64', 2),
        ('Day', 'DAY', 'int64', 3),
    ]

    for table in model.get('model', {}).get('tables', []):
        columns = table.get('columns', [])
        existing_hierarchies = table.get('hierarchies', [])

        # Collect columns already used in a hierarchy
        hier_cols = set()
        for h in existing_hierarchies:
            for lvl in h.get('levels', []):
                hier_cols.add(lvl.get('column', ''))

        existing_col_names = {c.get('name', '') for c in columns}

        for col in list(columns):  # iterate copy – we may append
            col_type = col.get('dataType', '')
            col_name = col.get('name', '')
            if col_type not in DATE_TYPES:
                continue
            if col_name in hier_cols:
                continue  # already in a user-defined hierarchy

            # Build hierarchy name scoped to the column
            hier_name = f"{col_name} Hierarchy"

            # Skip if we already auto-generated this one (idempotency)
            if any(h.get('name') == hier_name for h in existing_hierarchies):
                continue

            # Add calculated columns for the parts (skip if name clashes)
            calc_col_names = []
            for part_label, dax_fn, dt, _ in PARTS:
                calc_name = f"{col_name} {part_label}"
                if calc_name in existing_col_names:
                    calc_col_names.append(calc_name)
                    continue  # already exists (e.g. from Tableau extraction)
                calc_col = {
                    'name': calc_name,
                    'dataType': dt,
                    'sourceColumn': '',
                    'expression': f"{dax_fn}([{col_name}])",
                    'isHidden': True,
                    'type': 'calculated',
                }
                columns.append(calc_col)
                existing_col_names.add(calc_name)
                calc_col_names.append(calc_name)

            # Create the hierarchy
            hierarchy = {
                'name': hier_name,
                'levels': [
                    {'name': PARTS[i][0], 'ordinal': i, 'column': calc_col_names[i]}
                    for i in range(len(calc_col_names))
                ],
            }
            if 'hierarchies' not in table:
                table['hierarchies'] = []
            table['hierarchies'].append(hierarchy)


def _create_parameter_tables(model, parameters, main_table_name):
    """Create What-If parameter tables for Tableau parameters."""
    if not parameters:
        return

    type_map = {
        'integer': ('int64', 'INTEGER'),
        'real': ('double', 'DOUBLE'),
        'date': ('dateTime', 'DATETIME'),
        'datetime': ('dateTime', 'DATETIME'),
        'boolean': ('boolean', 'BOOLEAN'),
        'string': ('string', 'STRING'),
    }

    for param in parameters:
        caption = param.get('caption', '')
        if not caption:
            continue
        datatype = param.get('datatype', 'string')
        default_value = param.get('value', '').strip('"')
        domain_type = param.get('domain_type', 'any')
        allowable_values = param.get('allowable_values', [])
        pbi_type, dax_type = type_map.get(datatype, ('string', 'STRING'))

        if datatype == 'string':
            default_expr = f'"{default_value}"'
        elif datatype == 'boolean':
            default_expr = default_value.upper() if default_value else 'TRUE'
        elif datatype in ('date', 'datetime'):
            date_m = re.match(r'#(\d{4})-(\d{2})-(\d{2})#', default_value)
            if date_m:
                default_expr = f'DATE({int(date_m.group(1))}, {int(date_m.group(2))}, {int(date_m.group(3))})'
            else:
                default_expr = default_value if default_value else 'DATE(2024, 1, 1)'
        else:
            default_expr = default_value if default_value else '0'

        if domain_type == 'any' or not allowable_values:
            for table in model["model"]["tables"]:
                if table.get("name") == main_table_name:
                    if "measures" not in table:
                        table["measures"] = []
                    table["measures"].append({
                        "name": caption,
                        "expression": default_expr,
                        "annotations": [
                            {"name": "displayFolder", "value": "Parameters"}
                        ]
                    })
                    break
            continue

        table_expr = None
        col_name = caption

        if domain_type == 'range':
            range_info = next((v for v in allowable_values if v.get('type') == 'range'), None)
            if range_info:
                min_val = range_info.get('min', '0')
                max_val = range_info.get('max', '100')
                step = range_info.get('step', '') or '1'
                table_expr = f"GENERATESERIES({min_val}, {max_val}, {step})"
                col_name = "Value"
        elif domain_type == 'list':
            list_values = [v for v in allowable_values if v.get('type') != 'range']
            if list_values:
                if datatype == 'string':
                    rows = ', '.join(f'{{"{v.get("value", "")}"}}' for v in list_values)
                elif datatype == 'boolean':
                    rows = ', '.join(f'{{{v.get("value", "TRUE").upper()}}}' for v in list_values)
                else:
                    rows = ', '.join(f'{{{v.get("value", "0")}}}' for v in list_values)
                col_name = "Value"
                table_expr = f'DATATABLE("Value", {dax_type}, {{{rows}}})'

        if not table_expr:
            continue

        param_table = {
            "name": caption,
            "columns": [{
                "name": col_name, "dataType": pbi_type,
                "sourceColumn": col_name,
                "annotations": [{"name": "displayFolder", "value": "Parameters"}]
            }],
            "measures": [{
                "name": caption,
                "expression": f"SELECTEDVALUE('{caption}'[{col_name}], {default_expr})",
                "annotations": [{"name": "displayFolder", "value": "Parameters"}]
            }],
            "partitions": [{
                "name": caption,
                "mode": "import",
                "source": {"type": "calculated", "expression": table_expr}
            }]
        }
        model["model"]["tables"].append(param_table)

    # Deduplicate parameter measures
    param_table_names = set()
    for param in parameters:
        caption = param.get('caption', '')
        domain_type = param.get('domain_type', 'any')
        if caption and domain_type in ('range', 'list') and param.get('allowable_values'):
            param_table_names.add(caption)
    if param_table_names:
        for table in model["model"]["tables"]:
            table_name = table.get("name", "")
            if table_name in param_table_names:
                continue
            if "measures" in table:
                table["measures"] = [
                    m for m in table["measures"]
                    if m.get("name", "") not in param_table_names
                ]


def _create_rls_roles(model, user_filters, main_table_name, column_table_map):
    """Create RLS roles from Tableau user filters."""
    if not user_filters:
        return
    if not main_table_name:
        tables = model.get('model', {}).get('tables', [])
        main_table_name = tables[0].get('name', 'Table') if tables else 'Table'

    roles = []
    role_names = set()

    for uf in user_filters:
        uf_type = uf.get('type', '')

        if uf_type == 'user_filter':
            filter_name = uf.get('name', 'UserFilter')
            column = uf.get('column', '')
            user_mappings = uf.get('user_mappings', [])
            table_name = column_table_map.get(column, main_table_name)
            col_clean = column.split(':')[-1] if ':' in column else column

            if user_mappings:
                user_values = {}
                for mapping in user_mappings:
                    user = mapping.get('user', '')
                    val = mapping.get('value', '')
                    if user and val:
                        user_values.setdefault(user, []).append(val)
                or_clauses = []
                for user_email, values in user_values.items():
                    if len(values) == 1:
                        val_expr = f'[{col_clean}] = "{values[0]}"'
                    else:
                        val_list = ', '.join(f'"{v}"' for v in values)
                        val_expr = f'[{col_clean}] IN {{{val_list}}}'
                    or_clauses.append(f'(USERPRINCIPALNAME() = "{user_email}" && {val_expr})')
                filter_dax = ' || '.join(or_clauses) if or_clauses else 'FALSE()'
                role_name = _unique_role_name(filter_name, role_names)
                role_names.add(role_name)
                roles.append({
                    "name": role_name, "modelPermission": "read",
                    "tablePermissions": [{"name": table_name, "filterExpression": filter_dax}],
                    "_migration_note": f"Migrated from Tableau user filter '{filter_name}'.",
                    "_user_mappings": user_mappings
                })
            elif column:
                filter_dax = f"[{col_clean}] = USERPRINCIPALNAME()"
                role_name = _unique_role_name(filter_name, role_names)
                role_names.add(role_name)
                roles.append({
                    "name": role_name, "modelPermission": "read",
                    "tablePermissions": [{"name": table_name, "filterExpression": filter_dax}]
                })

        elif uf_type == 'calculated_security':
            calc_name = uf.get('name', 'SecurityCalc')
            formula = uf.get('formula', '')
            functions_used = uf.get('functions_used', [])
            ismemberof_groups = uf.get('ismemberof_groups', [])

            if ismemberof_groups:
                for group in ismemberof_groups:
                    role_name = _unique_role_name(group, role_names)
                    role_names.add(role_name)
                    roles.append({
                        "name": role_name, "modelPermission": "read",
                        "tablePermissions": [{"name": main_table_name,
                                              "filterExpression": f"TRUE()  /* Members of role '{group}' have access */"}],
                        "_migration_note": f"Migrated from Tableau ISMEMBEROF(\"{group}\")."
                    })
            elif 'USERNAME' in functions_used or 'FULLNAME' in functions_used:
                dax_filter = convert_tableau_formula_to_dax(
                    formula, table_name=main_table_name,
                    column_table_map=column_table_map
                )
                role_name = _unique_role_name(calc_name, role_names)
                role_names.add(role_name)
                cross_ref = re.search(r"'([^']+)'\[", dax_filter)
                perm_table = main_table_name
                if cross_ref:
                    ref_table = cross_ref.group(1)
                    model_table_names = {t.get("name", "") for t in model["model"]["tables"]}
                    if ref_table in model_table_names and ref_table != main_table_name:
                        perm_table = ref_table
                        dax_filter = dax_filter.replace(f"'{ref_table}'[", "[")
                roles.append({
                    "name": role_name, "modelPermission": "read",
                    "tablePermissions": [{"name": perm_table, "filterExpression": dax_filter}],
                    "_migration_note": f"Migrated from Tableau '{calc_name}'. Original: {formula}"
                })

    if roles:
        model["model"]["roles"] = roles
        print(f"    \u2713 {len(roles)} RLS role(s) created")


def _unique_role_name(base_name, existing_names):
    """Generate a unique role name."""
    clean = re.sub(r'[^\w\s-]', '', base_name).strip()
    if not clean:
        clean = 'Role'
    if clean not in existing_names:
        return clean
    counter = 2
    while f"{clean}_{counter}" in existing_names:
        counter += 1
    return f"{clean}_{counter}"


def _create_calculation_groups(model, parameters, main_table_name):
    """Create calculation group tables from parameter actions that swap measures.

    A Tableau parameter that selects between different measures maps to
    a PBI calculation group with one calculation item per parameter value.
    """
    if not parameters:
        return

    existing_tables = {t.get('name', '') for t in model['model']['tables']}

    for param in parameters:
        caption = param.get('caption', '')
        domain_type = param.get('domain_type', '')
        datatype = param.get('datatype', 'string')
        allowable_values = param.get('allowable_values', [])

        # Only string list parameters are candidates for calc groups
        if datatype != 'string' or domain_type != 'list' or not allowable_values:
            continue

        # Check if values look like measure names (heuristic: no spaces, or matches known measure names)
        measure_names = set()
        for table in model['model']['tables']:
            for m in table.get('measures', []):
                measure_names.add(m.get('name', ''))

        matching_values = [v for v in allowable_values
                           if v.get('type') != 'range' and v.get('value', '') in measure_names]
        if len(matching_values) < 2:
            continue

        cg_name = f"{caption} CalcGroup"
        if cg_name in existing_tables:
            continue

        calc_items = []
        for val in matching_values:
            measure_ref = val.get('value', '')
            calc_items.append({
                "name": measure_ref,
                "expression": f"CALCULATE(SELECTEDMEASURE())",
                "ordinal": len(calc_items),
            })

        cg_table = {
            "name": cg_name,
            "calculationGroup": {
                "columns": [{"name": caption, "dataType": "string", "sourceColumn": caption}],
                "calculationItems": calc_items,
            },
            "columns": [{"name": caption, "dataType": "string", "sourceColumn": caption}],
            "partitions": [{
                "name": cg_name,
                "mode": "import",
                "source": {"type": "calculationGroup"}
            }],
            "annotations": [{"name": "displayFolder", "value": "Calculation Groups"}],
        }
        model['model']['tables'].append(cg_table)
        existing_tables.add(cg_name)


def _create_field_parameters(model, parameters, main_table_name, column_table_map):
    """Create field parameter tables from Tableau parameters that swap dimensions/measures.

    Field parameters in PBI allow users to dynamically switch which field
    appears on an axis or in a slicer. This maps to Tableau parameter actions
    that switch between columns.
    """
    if not parameters:
        return

    existing_tables = {t.get('name', '') for t in model['model']['tables']}

    # Collect all known columns
    all_columns = set()
    for table in model['model']['tables']:
        for col in table.get('columns', []):
            all_columns.add(col.get('name', ''))

    for param in parameters:
        caption = param.get('caption', '')
        domain_type = param.get('domain_type', '')
        datatype = param.get('datatype', 'string')
        allowable_values = param.get('allowable_values', [])

        # Only string list parameters with column-like values
        if datatype != 'string' or domain_type != 'list' or not allowable_values:
            continue

        matching_cols = [v for v in allowable_values
                         if v.get('type') != 'range' and v.get('value', '') in all_columns]

        # Need at least 2 matching column names and NOT all measures (those go to calc groups)
        measure_names = set()
        for table in model['model']['tables']:
            for m in table.get('measures', []):
                measure_names.add(m.get('name', ''))

        if len(matching_cols) < 2:
            continue
        # Skip if already handled as calc group (all values are measures)
        if all(v.get('value', '') in measure_names for v in matching_cols):
            continue

        fp_name = f"{caption} FieldParam"
        if fp_name in existing_tables:
            continue

        # Build NAMEOF references for field parameter DAX
        rows = []
        for idx, val in enumerate(matching_cols):
            col_name = val.get('value', '')
            col_table = column_table_map.get(col_name, main_table_name)
            rows.append(f'(NAMEOF(\'{col_table}\'[{col_name}]), {idx}, "{col_name}")')

        fp_expr = "{\n" + ",\n".join(rows) + "\n}"

        fp_table = {
            "name": fp_name,
            "columns": [
                {"name": caption, "dataType": "string", "sourceColumn": caption,
                 "annotations": [{"name": "displayFolder", "value": "Field Parameters"}]},
                {"name": f"{caption}_Order", "dataType": "int64",
                 "sourceColumn": f"{caption}_Order", "isHidden": True},
                {"name": f"{caption}_Fields", "dataType": "string",
                 "sourceColumn": f"{caption}_Fields", "isHidden": True},
            ],
            "partitions": [{
                "name": fp_name,
                "mode": "import",
                "source": {"type": "calculated", "expression": fp_expr}
            }],
            "annotations": [
                {"name": "displayFolder", "value": "Field Parameters"},
                {"name": "PBI_NavigationStepName", "value": "Navigation"},
                {"name": "ParameterMetadata", "value": json.dumps({"version": 3, "kind": 2})},
            ],
        }
        model['model']['tables'].append(fp_table)
        existing_tables.add(fp_name)


def _add_date_table(model, calendar_start=None, calendar_end=None):
    """Add a Calendar date table using M partition (compatible with DirectLake)."""
    start_year = calendar_start or 2020
    end_year = calendar_end or 2030
    calendar_m = (
        'let\n'
        f'    StartDate = #date({start_year}, 1, 1),\n'
        f'    EndDate = #date({end_year}, 12, 31),\n'
        '    DayCount = Duration.Days(EndDate - StartDate) + 1,\n'
        '    DateList = List.Dates(StartDate, DayCount, #duration(1, 0, 0, 0)),\n'
        '    #"Date Table" = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),\n'
        '    #"Changed Type" = Table.TransformColumnTypes(#"Date Table", {{"Date", type date}}),\n'
        '    #"Added Year" = Table.AddColumn(#"Changed Type", "Year", each Date.Year([Date]), Int64.Type),\n'
        '    #"Added Quarter" = Table.AddColumn(#"Added Year", "Quarter", each "Q" & Text.From(Date.QuarterOfYear([Date]))),\n'
        '    #"Added Month" = Table.AddColumn(#"Added Quarter", "Month", each Date.Month([Date]), Int64.Type),\n'
        '    #"Added MonthName" = Table.AddColumn(#"Added Month", "MonthName", each Date.MonthName([Date])),\n'
        '    #"Added Day" = Table.AddColumn(#"Added MonthName", "Day", each Date.Day([Date]), Int64.Type),\n'
        '    #"Added DayOfWeek" = Table.AddColumn(#"Added Day", "DayOfWeek", each Date.DayOfWeek([Date], Day.Monday) + 1, Int64.Type),\n'
        '    #"Added DayName" = Table.AddColumn(#"Added DayOfWeek", "DayName", each Date.DayOfWeekName([Date]))\n'
        'in\n'
        '    #"Added DayName"'
    )

    date_table = {
        "name": "Calendar",
        "isHidden": False,
        "columns": [
            {"name": "Date", "dataType": "DateTime", "isKey": True,
             "dataCategory": "DateTime", "formatString": "dd/mm/yyyy",
             "sourceColumn": "Date", "summarizeBy": "none"},
            {"name": "Year", "dataType": "int64", "dataCategory": "Years",
             "sourceColumn": "Year", "summarizeBy": "none"},
            {"name": "Quarter", "dataType": "string",
             "sourceColumn": "Quarter", "summarizeBy": "none"},
            {"name": "Month", "dataType": "int64", "dataCategory": "Months",
             "sourceColumn": "Month", "summarizeBy": "none"},
            {"name": "MonthName", "dataType": "string",
             "sourceColumn": "MonthName", "sortByColumn": "Month", "summarizeBy": "none"},
            {"name": "Day", "dataType": "int64", "dataCategory": "Days",
             "sourceColumn": "Day", "summarizeBy": "none"},
            {"name": "DayOfWeek", "dataType": "int64",
             "sourceColumn": "DayOfWeek", "summarizeBy": "none"},
            {"name": "DayName", "dataType": "string",
             "sourceColumn": "DayName", "sortByColumn": "DayOfWeek", "summarizeBy": "none"},
        ],
        "partitions": [{
            "name": "Calendar-Partition",
            "mode": "import",
            "source": {"type": "m", "expression": calendar_m}
        }],
        "measures": []
    }

    # Time intelligence measures
    value_expr = None
    for t in model["model"]["tables"]:
        if t["name"] == "Calendar":
            continue
        for ms in t.get("measures", []):
            expr = ms.get("expression", "")
            if re.match(r'^SUM\b', expr, re.IGNORECASE):
                value_expr = f'[{ms["name"]}]'
                break
        if value_expr:
            break

    if value_expr:
        date_table["measures"].extend([
            {"name": "Year To Date",
             "expression": f"TOTALYTD({value_expr}, 'Calendar'[Date])",
             "formatString": "#,0.00", "displayFolder": "Time Intelligence"},
            {"name": "Previous Year",
             "expression": f"CALCULATE({value_expr}, SAMEPERIODLASTYEAR('Calendar'[Date]))",
             "formatString": "#,0.00", "displayFolder": "Time Intelligence"},
            {"name": "Year Over Year %",
             "expression": "DIVIDE([Year To Date] - [Previous Year], [Previous Year], 0)",
             "formatString": "0.00%", "displayFolder": "Time Intelligence"}
        ])

    model["model"]["tables"].append(date_table)

    # Link Calendar to fact tables with date columns
    for t in model["model"]["tables"]:
        tname = t.get("name", "")
        if tname == "Calendar":
            continue
        for col in t.get("columns", []):
            if col.get("dataType") == "DateTime" or col.get("dataCategory") == "DateTime":
                date_col_name = col.get("name", "")
                if date_col_name and not col.get("isCalculated", False):
                    model["model"]["relationships"].append({
                        "name": f"Calendar_{tname}_{date_col_name}",
                        "fromTable": tname, "fromColumn": date_col_name,
                        "toTable": "Calendar", "toColumn": "Date",
                        "crossFilteringBehavior": "oneDirection"
                    })
                    break


def _deactivate_ambiguous_paths(model):
    """Deactivate relationships that create cycles (ambiguous paths)."""
    relationships = model["model"]["relationships"]
    if not relationships:
        return

    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra == rb:
            return False
        parent[ra] = rb
        return True

    def _deactivation_priority(rel):
        name = rel.get('name', '')
        if name.startswith('Calendar_'):
            return 2
        if name.startswith('inferred_'):
            return 1
        return 0

    sorted_rels = sorted(relationships, key=_deactivation_priority)
    deactivated = []

    for rel in sorted_rels:
        if rel.get('isActive') == False:
            continue
        from_t = rel.get('fromTable', '')
        to_t = rel.get('toTable', '')
        if not from_t or not to_t:
            continue
        if not union(from_t, to_t):
            rel['isActive'] = False
            deactivated.append(f"{from_t}.{rel.get('fromColumn','')} \u2192 {to_t}.{rel.get('toColumn','')}")

    for d in deactivated:
        print(f"  \u26a0 Deactivated relationship (ambiguous path): {d}")


# ════════════════════════════════════════════════════════════════════
#  THEME GENERATION
# ════════════════════════════════════════════════════════════════════

_DEFAULT_PBI_COLORS = [
    "#4E79A7", "#F28E2B", "#E15759", "#76B7B2",
    "#59A14F", "#EDC948", "#B07AA1", "#FF9DA7",
    "#9C755F", "#BAB0AC", "#86BCB6", "#8CD17D"
]


def generate_theme_json(theme_data=None):
    """Generate a Power BI theme.json from Tableau theme data."""
    colors = _DEFAULT_PBI_COLORS
    font_family = "Segoe UI"

    if theme_data:
        t_colors = theme_data.get('colors', [])
        if t_colors:
            valid = [c for c in t_colors if isinstance(c, str) and c.startswith('#')]
            if valid:
                colors = valid[:12]
                while len(colors) < 12:
                    colors.append(_DEFAULT_PBI_COLORS[len(colors) % len(_DEFAULT_PBI_COLORS)])
        t_font = theme_data.get('font_family', '')
        if t_font:
            font_family = t_font

    return {
        "name": "Tableau Migration Theme",
        "dataColors": colors,
        "background": "#FFFFFF",
        "foreground": "#252423",
        "tableAccent": colors[0] if colors else "#4E79A7",
        "textClasses": {
            "callout": {"fontSize": 28, "fontFace": font_family, "color": "#252423"},
            "title": {"fontSize": 12, "fontFace": font_family, "color": "#252423"},
            "header": {"fontSize": 12, "fontFace": font_family, "color": "#252423"},
            "label": {"fontSize": 10, "fontFace": font_family, "color": "#666666"}
        },
        "visualStyles": {
            "*": {"*": {"*": [{"fontFamily": font_family, "wordWrap": True}]}}
        }
    }


# ════════════════════════════════════════════════════════════════════
#  TMDL FILE WRITERS
# ════════════════════════════════════════════════════════════════════

def _quote_name(name):
    """Quote a TMDL name if needed."""
    if re.search(r'[^a-zA-Z0-9_]', name):
        escaped = name.replace("'", "''")
        return f"'{escaped}'"
    return name


def _tmdl_datatype(bim_type):
    """Convert a type to TMDL type."""
    mapping = {
        'Int64': 'int64', 'int64': 'int64',
        'String': 'string', 'string': 'string',
        'Double': 'double', 'double': 'double',
        'Decimal': 'decimal', 'decimal': 'decimal',
        'Boolean': 'boolean', 'boolean': 'boolean',
        'DateTime': 'dateTime', 'dateTime': 'dateTime',
        'Binary': 'binary', 'binary': 'binary',
    }
    return mapping.get(bim_type, 'string')


def _tmdl_summarize(summarize_by):
    """Convert summarizeBy to TMDL."""
    mapping = {
        'sum': 'sum', 'none': 'none', 'count': 'count',
        'average': 'average', 'min': 'min', 'max': 'max',
    }
    return mapping.get(str(summarize_by).lower(), 'none')


def _safe_filename(name):
    """Create a safe filename for a table."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)


def _write_tmdl_files(model_data, output_dir):
    """Write the complete TMDL file structure."""
    model = model_data.get('model', model_data)

    def_dir = os.path.join(output_dir, 'definition')
    os.makedirs(def_dir, exist_ok=True)

    tables = model.get('tables', [])
    relationships = model.get('relationships', [])
    roles = model.get('roles', [])
    culture = model.get('culture', 'en-US')

    _write_database_tmdl(def_dir, model)
    _write_model_tmdl(def_dir, model, tables, roles)
    _write_relationships_tmdl(def_dir, relationships)
    _write_expressions_tmdl(def_dir, tables)

    if roles:
        _write_roles_tmdl(def_dir, roles)

    tables_dir = os.path.join(def_dir, 'tables')
    os.makedirs(tables_dir, exist_ok=True)

    expected_files = set()
    for table in tables:
        tname = table.get('name', 'Table')
        expected_files.add(tname + '.tmdl')
    for existing in os.listdir(tables_dir):
        if existing.endswith('.tmdl') and existing not in expected_files:
            os.remove(os.path.join(tables_dir, existing))

    for table in tables:
        _write_table_tmdl(tables_dir, table)

    diagram_path = os.path.join(def_dir, 'diagramLayout.json')
    with open(diagram_path, 'w', encoding='utf-8') as f:
        json.dump({}, f)

    perspectives = model.get('perspectives', [])
    if not perspectives and len(tables) > 2:
        perspectives = [{"name": "Full Model", "tables": [t.get('name', '') for t in tables]}]
    if perspectives:
        _write_perspectives_tmdl(def_dir, perspectives)

    if culture and culture != 'en-US':
        cultures_dir = os.path.join(def_dir, 'cultures')
        os.makedirs(cultures_dir, exist_ok=True)
        _write_culture_tmdl(cultures_dir, culture, tables)

    return def_dir


def _write_perspectives_tmdl(def_dir, perspectives):
    """Write perspectives.tmdl."""
    lines = []
    for persp in perspectives:
        p_name = persp.get('name', 'Default')
        lines.append(f"perspective {_quote_name(p_name)}")
        for table_ref in persp.get('tables', []):
            tbl_name = table_ref if isinstance(table_ref, str) else table_ref.get('name', '')
            if tbl_name:
                lines.append(f"\tperspectiveTable {_quote_name(tbl_name)}")
        lines.append("")
    filepath = os.path.join(def_dir, 'perspectives.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


def _write_culture_tmdl(cultures_dir, culture_name, tables):
    """Write a culture TMDL file."""
    lines = [f"culture {_quote_name(culture_name)}"]
    lines.append("\tlinguisticMetadata =")
    lines.append('\t\t```')
    metadata = {"Version": "1.0.0", "Language": culture_name, "DynamicImprovement": "HighConfidence"}
    lines.append(f'\t\t\t{json.dumps(metadata, ensure_ascii=False)}')
    lines.append('\t\t\t```')
    lines.append("")
    filepath = os.path.join(cultures_dir, f'{culture_name}.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


def _write_database_tmdl(def_dir, model):
    """Generate database.tmdl."""
    compat = model.get('compatibilityLevel', 1604)
    if compat < 1604:
        compat = 1604
    content = f"database\n\tcompatibilityLevel: {compat}\n\n"
    filepath = os.path.join(def_dir, 'database.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_model_tmdl(def_dir, model, tables, roles=None):
    """Generate model.tmdl with DirectLake defaultMode."""
    culture = model.get('culture', 'en-US')
    perspectives = model.get('perspectives', [])

    lines = []
    lines.append("model Model")
    lines.append(f"\tculture: {culture}")
    lines.append("\tdefaultPowerBIDataSourceVersion: powerBI_V3")
    lines.append("\tdefaultMode: directLake")
    lines.append("\tsourceQueryCulture: en-US")
    lines.append("\tdataAccessOptions")
    lines.append("\t\tlegacyRedirects")
    lines.append("\t\treturnErrorValuesAsNull")
    lines.append("")

    table_names = [t.get('name', '') for t in tables]
    table_names_json = '["' + '","'.join(table_names) + '"]'
    lines.append(f"annotation PBI_QueryOrder = {table_names_json}")
    lines.append("")

    for table in tables:
        tname = _quote_name(table.get('name', ''))
        lines.append(f"ref table {tname}")
    lines.append("")

    lines.append("ref expression DatabaseQuery")
    lines.append("")

    if roles:
        for role in roles:
            rname = _quote_name(role.get('name', ''))
            lines.append(f"ref role {rname}")
        lines.append("")

    if perspectives:
        for persp in perspectives:
            pname = _quote_name(persp.get('name', 'Default'))
            lines.append(f"ref perspective {pname}")
        lines.append("")

    if culture and culture != 'en-US':
        lines.append(f"ref culture {_quote_name(culture)}")
        lines.append("")

    content = '\n'.join(lines) + '\n'
    filepath = os.path.join(def_dir, 'model.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_expressions_tmdl(def_dir, tables):
    """Generate expressions.tmdl with DatabaseQuery for DirectLake."""
    lines = []
    # DirectLake uses a DatabaseQuery expression that references the Lakehouse
    lines.append('expression DatabaseQuery =')
    lines.append('\tlet')
    lines.append('\t\tdatabase = Sql.Database("{{YOUR_LAKEHOUSE_SQL_ENDPOINT}}", "{{YOUR_LAKEHOUSE_NAME}}")')
    lines.append('\tin')
    lines.append('\t\tdatabase')
    lines.append('\tmeta [IsParameterQuery=false]')
    lines.append("")

    content = '\n'.join(lines) + '\n'
    filepath = os.path.join(def_dir, 'expressions.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_roles_tmdl(def_dir, roles):
    """Generate roles.tmdl with RLS role definitions."""
    if not roles:
        return
    lines = []
    for role in roles:
        role_name = _quote_name(role.get('name', 'DefaultRole'))
        model_permission = role.get('modelPermission', 'read')
        lines.append(f"role {role_name}")
        lines.append(f"\tmodelPermission: {model_permission}")
        migration_note = role.get('_migration_note', '')
        if migration_note:
            note_escaped = migration_note.replace('"', '\\"')
            lines.append(f'\tannotation MigrationNote = "{note_escaped}"')
        lines.append("")
        for tp in role.get('tablePermissions', []):
            tp_name = tp.get('name', '') or ''
            if not tp_name:
                continue
            table_name = _quote_name(tp_name)
            filter_expr = tp.get('filterExpression', '')
            lines.append(f"\ttablePermission {table_name}")
            if filter_expr:
                filter_clean = filter_expr.replace('\n', ' ').replace('\r', ' ').strip()
                lines.append(f"\t\tfilterExpression = {filter_clean}")
            lines.append("")
    content = '\n'.join(lines) + '\n'
    filepath = os.path.join(def_dir, 'roles.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_relationships_tmdl(def_dir, relationships):
    """Generate relationships.tmdl."""
    if not relationships:
        filepath = os.path.join(def_dir, 'relationships.tmdl')
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("")
        return

    lines = []
    for rel in relationships:
        rel_id = rel.get('name', str(uuid.uuid4()))
        try:
            uuid.UUID(rel_id)
        except ValueError:
            rel_id = str(uuid.uuid4())

        from_table = _quote_name(rel.get('fromTable', ''))
        from_col = _quote_name(rel.get('fromColumn', ''))
        to_table = _quote_name(rel.get('toTable', ''))
        to_col = _quote_name(rel.get('toColumn', ''))

        lines.append(f"relationship {rel_id}")
        lines.append(f"\tfromColumn: {from_table}.{from_col}")
        lines.append(f"\ttoColumn: {to_table}.{to_col}")

        from_card = rel.get('fromCardinality', '')
        to_card = rel.get('toCardinality', '')
        if from_card == 'many' and to_card == 'many':
            lines.append("\tfromCardinality: many")
            lines.append("\ttoCardinality: many")

        cfb = rel.get('crossFilteringBehavior', 'oneDirection')
        lines.append(f"\tcrossFilteringBehavior: {cfb}")

        if rel.get('isActive') == False:
            lines.append("\tisActive: false")

        lines.append("")

    content = '\n'.join(lines) + '\n'
    filepath = os.path.join(def_dir, 'relationships.tmdl')
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_table_tmdl(tables_dir, table):
    """Generate a {table_name}.tmdl file."""
    table_name = table.get('name', 'Table')
    tname_quoted = _quote_name(table_name)

    lines = []
    lines.append(f"table {tname_quoted}")
    lines.append(f"\tlineageTag: {uuid.uuid4()}")
    lines.append("")

    for measure in table.get('measures', []):
        _write_measure(lines, measure)

    for column in table.get('columns', []):
        _write_column(lines, column)

    for hierarchy in table.get('hierarchies', []):
        _write_hierarchy(lines, hierarchy)

    for partition in table.get('partitions', []):
        _write_partition(lines, table_name, partition)

    lines.append("\tannotation PBI_ResultType = Table")
    lines.append("")

    content = '\n'.join(lines) + '\n'
    filename = _safe_filename(table_name) + '.tmdl'
    filepath = os.path.join(tables_dir, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def _write_measure(lines, measure):
    """Write a measure in TMDL."""
    mname = _quote_name(measure.get('name', 'Measure'))
    expression = measure.get('expression', '0')
    if '\n' in expression:
        lines.append(f"\tmeasure {mname} = ```")
        for expr_line in expression.split('\n'):
            lines.append(f"\t\t\t{expr_line}")
        lines.append("\t\t\t```")
    else:
        lines.append(f"\tmeasure {mname} = {expression}")
    fmt = measure.get('formatString', '')
    if fmt and fmt != '0':
        lines.append(f"\t\tformatString: {fmt}")
    folder = measure.get('displayFolder', '')
    if folder:
        lines.append(f"\t\tdisplayFolder: {folder}")
    desc = measure.get('description', '')
    if desc:
        lines.append(f"\t\tdescription: {desc}")
    if measure.get('isHidden', False):
        lines.append("\t\tisHidden")
    lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
    lines.append("")


def _write_column(lines, column):
    """Write a column in TMDL (physical or calculated)."""
    col_name = column.get('name', 'Column')
    cname_quoted = _quote_name(col_name)
    data_type = _tmdl_datatype(column.get('dataType', 'string'))
    expression = column.get('expression', '')
    is_calculated = column.get('isCalculated', False)
    is_hidden = column.get('isHidden', False)
    data_category = column.get('dataCategory', '')
    description = column.get('description', '')
    display_folder = column.get('displayFolder', '')

    if is_calculated and expression:
        if '\n' in expression:
            lines.append(f"\tcolumn {cname_quoted} = ```")
            for expr_line in expression.split('\n'):
                lines.append(f"\t\t\t{expr_line}")
            lines.append("\t\t\t```")
        else:
            lines.append(f"\tcolumn {cname_quoted} = {expression}")
        lines.append(f"\t\tdataType: {data_type}")
        fmt = column.get('formatString', '')
        if fmt:
            lines.append(f"\t\tformatString: {fmt}")
        lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
        summarize = _tmdl_summarize(column.get('summarizeBy', 'none'))
        lines.append(f"\t\tsummarizeBy: {summarize}")
        if is_hidden:
            lines.append("\t\tisHidden")
        if column.get('isKey', False):
            lines.append("\t\tisKey")
        if data_category:
            lines.append(f"\t\tdataCategory: {data_category}")
        if description:
            lines.append(f"\t\tdescription: {description}")
        if display_folder:
            lines.append(f"\t\tdisplayFolder: {display_folder}")
        sort_by = column.get('sortByColumn', '')
        if sort_by:
            lines.append(f"\t\tsortByColumn: {_quote_name(sort_by)}")
        lines.append("")
        lines.append("\t\tannotation SummarizationSetBy = Automatic")
        lines.append("")
    else:
        lines.append(f"\tcolumn {cname_quoted}")
        lines.append(f"\t\tdataType: {data_type}")
        fmt = column.get('formatString', '')
        if fmt:
            lines.append(f"\t\tformatString: {fmt}")
        lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
        summarize = _tmdl_summarize(column.get('summarizeBy', 'none'))
        lines.append(f"\t\tsummarizeBy: {summarize}")
        source_col = column.get('sourceColumn', col_name)
        source_col_quoted = _quote_name(source_col) if re.search(r'[^a-zA-Z0-9_]', source_col) else source_col
        lines.append(f"\t\tsourceColumn: {source_col_quoted}")
        if is_hidden:
            lines.append("\t\tisHidden")
        if column.get('isKey', False):
            lines.append("\t\tisKey")
        if data_category:
            lines.append(f"\t\tdataCategory: {data_category}")
        if description:
            lines.append(f"\t\tdescription: {description}")
        if display_folder:
            lines.append(f"\t\tdisplayFolder: {display_folder}")
        sort_by = column.get('sortByColumn', '')
        if sort_by:
            lines.append(f"\t\tsortByColumn: {_quote_name(sort_by)}")
        lines.append("")
        lines.append("\t\tannotation SummarizationSetBy = Automatic")
        lines.append("")


def _write_hierarchy(lines, hierarchy):
    """Write a hierarchy in TMDL."""
    h_name = _quote_name(hierarchy.get('name', 'Hierarchy'))
    levels = hierarchy.get('levels', [])
    lines.append(f"\thierarchy {h_name}")
    lines.append(f"\t\tlineageTag: {uuid.uuid4()}")
    lines.append("")
    for level in levels:
        level_name = _quote_name(level.get('name', 'Level'))
        col_name = _quote_name(level.get('column', level.get('name', '')))
        ordinal = level.get('ordinal', 0)
        lines.append(f"\t\tlevel {level_name}")
        lines.append(f"\t\t\tordinal: {ordinal}")
        lines.append(f"\t\t\tcolumn: {col_name}")
        lines.append(f"\t\t\tlineageTag: {uuid.uuid4()}")
        lines.append("")
    lines.append("")


def _write_partition(lines, table_name, partition):
    """Write a partition in TMDL — supports DirectLake entity partitions."""
    part_name = f"{table_name}-{uuid.uuid4()}"
    mode = partition.get('mode', 'directLake')
    source = partition.get('source', {})
    source_type = source.get('type', 'entity')
    expression = source.get('expression', '')

    if source_type == 'entity':
        # DirectLake entity partition
        entity_name = source.get('entityName', _sanitize_table_name(table_name))
        schema_name = source.get('schemaName', 'dbo')
        expr_source = source.get('expressionSource', 'DatabaseQuery')

        lines.append(f"\tpartition {_quote_name(part_name)} = entity")
        lines.append(f"\t\tmode: {mode}")
        lines.append(f"\t\tentityName: {entity_name}")
        lines.append(f"\t\tschemaName: {schema_name}")
        lines.append(f"\t\texpressionSource: {expr_source}")
    elif source_type == 'calculated':
        lines.append(f"\tpartition {_quote_name(part_name)} = {source_type}")
        lines.append(f"\t\tmode: {mode}")
        expr_clean = expression.replace('\r\n', '\n').replace('\r', '\n')
        if '\n' in expr_clean:
            lines.append("\t\tsource = ```")
            for expr_line in expr_clean.split('\n'):
                lines.append(f"\t\t\t\t{expr_line}")
            lines.append("\t\t\t\t```")
        else:
            lines.append(f"\t\tsource = {expr_clean}")
    else:
        # M query partition (for Calendar table etc.)
        lines.append(f"\tpartition {_quote_name(part_name)} = {source_type}")
        lines.append(f"\t\tmode: {mode}")
        if expression:
            lines.append(f"\t\tsource =")
            for expr_line in expression.split('\n'):
                lines.append(f"\t\t\t\t{expr_line}")
        else:
            lines.append(f"\t\tsource =")
            lines.append("\t\t\t\tlet")
            lines.append("\t\t\t\t\tSource = null // TODO: Configure data source")
            lines.append("\t\t\t\tin")
            lines.append("\t\t\t\t\tSource")
    lines.append("")
