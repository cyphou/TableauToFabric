"""
Dataflow Gen2 Generator for Microsoft Fabric.

Generates Dataflow Gen2 definitions from extracted Tableau datasources.
Dataflow Gen2 uses Power Query M language to define data transformations.

Output:
- dataflow_definition.json: Dataflow Gen2 mashup document
- Individual .m files for each query (for readability)

Dataflows handle data ingestion from original Tableau sources
(SQL Server, PostgreSQL, Excel, CSV, etc.) into the Fabric Lakehouse.

Calculated columns (row-level Tableau formulas) are added as
Power Query M ``Table.AddColumn`` steps so they are materialized
as physical columns in the Lakehouse Delta tables.
"""

import os
import json
import re
import sys
from datetime import datetime

# Add parent directory for imports (guarded to avoid duplicates)
_parent = os.path.join(os.path.dirname(__file__), '..')
if _parent not in sys.path:
    sys.path.insert(0, _parent)
from tableau_export.m_query_builder import generate_power_query_m
from tableau_export.datasource_extractor import _reverse_tableau_bracket_escape

from .calc_column_utils import (
    classify_calculations,
    make_m_add_column_step,
    sanitize_calc_col_name,
)


def _sanitize_query_name(name):
    """Sanitize a table/query name for Dataflow Gen2."""
    name = name.replace('[', '').replace(']', '')
    name = re.sub(r'[^a-zA-Z0-9_ ]', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name or 'Query'


def _add_lakehouse_sink(m_query, lakehouse_table_name):
    """Append a Lakehouse sink step to an M query.

    In Dataflow Gen2, queries can have a staging step that writes
    the result to a Lakehouse table.
    """
    # The sink is metadata, not part of the M query itself.
    # Dataflow Gen2 handles this via the destination configuration.
    return m_query


class DataflowGenerator:
    """Generates Dataflow Gen2 definitions from Tableau datasources."""

    def __init__(self, project_dir, project_name):
        self.project_dir = project_dir
        self.project_name = project_name
        self.dataflow_dir = os.path.join(project_dir, f'{project_name}.Dataflow')
        os.makedirs(self.dataflow_dir, exist_ok=True)

    def generate(self, extracted_data):
        """
        Generate Dataflow Gen2 definition from extracted Tableau data.

        Calculated columns are appended as M ``Table.AddColumn`` steps
        to the query for the main (fact) table, so they are computed
        during the Dataflow refresh and materialized in the Lakehouse.

        Args:
            extracted_data: Dict with 'datasources', 'custom_sql',
                            'calculations', etc.

        Returns:
            Dict with generation stats {'queries': int, 'calc_columns': int}
        """
        datasources = extracted_data.get('datasources', [])
        custom_sql = extracted_data.get('custom_sql', [])
        calculations = extracted_data.get('calculations', [])

        # Classify calculations
        calc_columns, _measures = classify_calculations(calculations)

        queries = []
        seen_queries = set()

        for ds in datasources:
            connection = ds.get('connection', {})
            # Fallback: Prep flow outputs use 'connections' (plural, list)
            if not connection and ds.get('connections'):
                connection = ds['connections'][0]
            connection_map = ds.get('connection_map', {})

            for table in ds.get('tables', []):
                table_name = table.get('name', '')
                query_name = _sanitize_query_name(table_name)

                if query_name in seen_queries:
                    continue
                seen_queries.add(query_name)

                # Use per-table connection if available
                table_conn = table.get('connection_details', {})
                if table_conn and table_conn.get('type'):
                    conn = table_conn
                elif table.get('connection') and table.get('connection') in connection_map:
                    conn = connection_map[table['connection']]
                else:
                    conn = connection

                # Check for M query override (from Prep flow)
                m_query_overrides = ds.get('m_query_overrides', {})
                m_override = ds.get('m_query_override', '')

                if table_name in m_query_overrides:
                    m_query = m_query_overrides[table_name]
                elif m_override:
                    m_query = m_override
                else:
                    # Generate M query from connection info
                    m_query = generate_power_query_m(conn, table)

                # Determine Lakehouse target table name
                lh_table = re.sub(r'[^a-zA-Z0-9_]', '_', table_name).lower()

                queries.append({
                    'name': query_name,
                    'description': f'Ingests data from {table_name}',
                    'm_query': m_query,
                    'lakehouse_table': lh_table,
                    'source_type': conn.get('type', 'Unknown'),
                    'source_details': conn.get('details', {}),
                    'result_type': 'Table',
                    'load_enabled': True,
                })

        # Custom SQL queries
        for sql_entry in custom_sql:
            sql_name = _sanitize_query_name(sql_entry.get('name', 'Custom_SQL'))
            if sql_name not in seen_queries:
                seen_queries.add(sql_name)

                # Find connection details for this custom SQL
                ds_name = sql_entry.get('datasource', '')
                conn = {'type': 'SQL Server', 'details': {'server': 'localhost', 'database': 'MyDB'}}
                for ds in datasources:
                    if ds.get('name', '') == ds_name:
                        conn = ds.get('connection', conn)
                        break

                sql_query = sql_entry.get('query', '')
                server = conn.get('details', {}).get('server', 'localhost')
                database = conn.get('details', {}).get('database', 'MyDB')
                sql_escaped = sql_query.replace('"', '""')

                m_query = (
                    'let\n'
                    '    // Custom SQL Query\n'
                    f'    Source = Sql.Database("{server}", "{database}", '
                    f'[Query="{sql_escaped}"]),\n'
                    '    Result = Source\n'
                    'in\n'
                    '    Result'
                )

                lh_table = re.sub(r'[^a-zA-Z0-9_]', '_', sql_name).lower()
                queries.append({
                    'name': sql_name,
                    'description': f'Custom SQL: {sql_name}',
                    'm_query': m_query,
                    'lakehouse_table': lh_table,
                    'source_type': 'Custom SQL',
                    'result_type': 'Table',
                    'load_enabled': True,
                })

        # ── Inject calculated columns into the main table query ──
        if calc_columns and queries:
            # Main query = first query (typically the fact table)
            main_q = queries[0]
            main_q['m_query'] = self._inject_calc_column_steps(
                main_q['m_query'], calc_columns,
            )
            main_q['description'] += (
                f' + {len(calc_columns)} calculated column(s)'
            )

        # Generate Dataflow Gen2 definition
        dataflow_def = self._build_dataflow_definition(queries, calc_columns)

        # Write dataflow definition
        def_path = os.path.join(self.dataflow_dir, 'dataflow_definition.json')
        with open(def_path, 'w', encoding='utf-8') as f:
            json.dump(dataflow_def, f, indent=2, ensure_ascii=False)

        # Write individual M query files for readability
        self._write_m_query_files(queries)

        # Write mashup document (Power Query M combined)
        self._write_mashup_document(queries)

        return {'queries': len(queries), 'calc_columns': len(calc_columns)}

    def _inject_calc_column_steps(self, m_query, calc_columns):
        """Inject Table.AddColumn steps for calculated columns into an M query.

        Rewrites 'let … in Result' to include additional steps that compute
        each calculated column.
        """
        # Parse the M query: look for 'in\n    <final_step>'
        in_match = re.search(r'\bin\s*\n\s*(\w+)\s*$', m_query, re.MULTILINE)
        if not in_match:
            # Can't parse — append as comment
            comment = '\n// Calculated columns (manual conversion needed):\n'
            for cc in calc_columns:
                name = cc.get('caption', cc.get('name', ''))
                comment += f'// - {name}: {cc.get("formula", "")}\n'
            return m_query + comment

        final_step = in_match.group(1)
        before_in = m_query[:in_match.start()]

        # Build AddColumn steps
        prev_step = final_step
        extra_lines = []
        for cc in calc_columns:
            col_name = cc.get('caption', cc.get('name', ''))
            formula = cc.get('formula', '')
            line, prev_step = make_m_add_column_step(formula, col_name, prev_step)
            extra_lines.append(line)

        # Reassemble
        steps_block = ',\n'.join(extra_lines)
        return f'{before_in},\n{steps_block}\nin\n    {prev_step}'

    def _build_dataflow_definition(self, queries, calc_columns=None):
        """Build the Dataflow Gen2 JSON definition.

        This follows the Fabric Dataflow Gen2 API format with
        Power Query M mashup documents and destination configurations.
        """
        # Build mashup document (all queries combined)
        mashup_sections = []
        for q in queries:
            mashup_sections.append(f'shared {q["name"]} = {q["m_query"]};')

        mashup_document = '\nsection Section1;\n\n' + '\n\n'.join(mashup_sections)

        # Build query metadata
        query_groups = []
        for q in queries:
            query_groups.append({
                'name': q['name'],
                'description': q.get('description', ''),
                'queryId': q['name'].lower().replace(' ', '_'),
                'resultType': q.get('result_type', 'Table'),
                'loadEnabled': q.get('load_enabled', True),
                'destination': {
                    'type': 'Lakehouse',
                    'tableName': q.get('lakehouse_table', q['name'].lower()),
                    'updateMethod': 'Replace',  # or 'Append'
                    'schemaMapping': 'Auto',
                },
            })

        return {
            '$schema': 'https://developer.microsoft.com/json-schemas/fabric/item/dataflow/definition/dataflowGen2/1.0.0/schema.json',
            'properties': {
                'displayName': f'{self.project_name}_Dataflow',
                'description': f'Dataflow Gen2 generated from Tableau workbook: {self.project_name}',
                'type': 'DataflowGen2',
                'created': datetime.now().isoformat(),
            },
            'mashupDocument': mashup_document,
            'queries': query_groups,
        }

    def _write_m_query_files(self, queries):
        """Write individual .m files for each query."""
        queries_dir = os.path.join(self.dataflow_dir, 'queries')
        os.makedirs(queries_dir, exist_ok=True)

        for q in queries:
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', q['name'])
            m_path = os.path.join(queries_dir, f'{safe_name}.m')
            with open(m_path, 'w', encoding='utf-8') as f:
                f.write(f'// Query: {q["name"]}\n')
                f.write(f'// Source: {q.get("source_type", "Unknown")}\n')
                f.write(f'// Destination: Lakehouse → {q.get("lakehouse_table", "")}\n')
                f.write(f'// Generated: {datetime.now().isoformat()}\n\n')
                f.write(q['m_query'])

    def _write_mashup_document(self, queries):
        """Write the combined Power Query M mashup document."""
        mashup_path = os.path.join(self.dataflow_dir, 'mashup.pq')
        with open(mashup_path, 'w', encoding='utf-8') as f:
            f.write('// Dataflow Gen2 Mashup Document\n')
            f.write(f'// Project: {self.project_name}\n')
            f.write(f'// Generated: {datetime.now().isoformat()}\n\n')
            f.write('section Section1;\n\n')

            for q in queries:
                f.write(f'// ── {q["name"]} {"─" * max(1, 60 - len(q["name"]))}──\n')
                f.write(f'// Source: {q.get("source_type", "Unknown")}\n')
                f.write(f'// → Lakehouse.{q.get("lakehouse_table", "")}\n')
                f.write(f'shared {q["name"]} = {q["m_query"]};\n\n')
