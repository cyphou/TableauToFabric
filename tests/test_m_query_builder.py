"""
Tests for tableau_export.m_query_builder — Power Query M generation.

Organized by complexity:
  SIMPLE:  Type mapping, column escaping, type change builders
  MEDIUM:  Per-connector generators (SQL, PostgreSQL, CSV, Excel, BigQuery,
           MySQL, Oracle, Snowflake, etc.), transform step generators,
           inject_m_steps chaining
  COMPLEX: Join/union/aggregate transforms, wildcard union, custom SQL,
           conditional columns, full generators with columns
"""

import unittest
from tableau_export.m_query_builder import (
    map_tableau_to_m_type,
    _m_escape_col_name,
    _build_type_changes,
    generate_power_query_m,
    inject_m_steps,
    # Column operations
    m_transform_rename,
    m_transform_remove_columns,
    m_transform_select_columns,
    m_transform_duplicate_column,
    m_transform_reorder_columns,
    m_transform_split_by_delimiter,
    m_transform_merge_columns,
    # Value operations
    m_transform_replace_value,
    m_transform_replace_nulls,
    m_transform_trim,
    m_transform_clean,
    m_transform_upper,
    m_transform_lower,
    m_transform_proper_case,
    m_transform_fill_down,
    m_transform_fill_up,
    # Filter operations
    m_transform_filter_values,
    m_transform_exclude_values,
    m_transform_filter_range,
    m_transform_filter_nulls,
    m_transform_filter_contains,
    m_transform_distinct,
    m_transform_top_n,
    # Aggregate
    m_transform_aggregate,
    # Pivot / Unpivot
    m_transform_unpivot,
    m_transform_unpivot_other,
    m_transform_pivot,
    # Join / Union
    m_transform_join,
    m_transform_union,
    m_transform_wildcard_union,
    # Reshape
    m_transform_sort,
    m_transform_transpose,
    m_transform_add_index,
    m_transform_skip_rows,
    m_transform_remove_last_rows,
    m_transform_remove_errors,
    m_transform_promote_headers,
    m_transform_demote_headers,
    # Calculated column
    m_transform_add_column,
    m_transform_conditional_column,
)


# ── Sample helpers ────────────────────────────────────────────────

SAMPLE_COLUMNS = [
    {'name': 'OrderID', 'datatype': 'integer'},
    {'name': 'Product Name', 'datatype': 'string'},
    {'name': 'Price', 'datatype': 'real'},
    {'name': 'OrderDate', 'datatype': 'date'},
    {'name': 'IsActive', 'datatype': 'boolean'},
]

SQL_CONNECTION = {
    'type': 'SQL Server',
    'details': {'server': 'myserver', 'database': 'mydb'},
}

PG_CONNECTION = {
    'type': 'PostgreSQL',
    'details': {'server': 'pghost', 'port': '5432', 'database': 'pgdb'},
}

CSV_CONNECTION = {
    'type': 'CSV',
    'details': {'filename': 'data.csv', 'delimiter': ','},
}

EXCEL_CONNECTION = {
    'type': 'Excel',
    'details': {'filename': 'report.xlsx'},
}

BQ_CONNECTION = {
    'type': 'BigQuery',
    'details': {'project': 'my-gcp-project', 'dataset': 'analytics'},
}

MYSQL_CONNECTION = {
    'type': 'MySQL',
    'details': {'server': 'mysqlhost', 'port': '3306', 'database': 'mydb'},
}

ORACLE_CONNECTION = {
    'type': 'Oracle',
    'details': {'server': 'orahost', 'service': 'ORCL', 'port': '1521'},
}

SNOWFLAKE_CONNECTION = {
    'type': 'Snowflake',
    'details': {'server': 'acme.snowflakecomputing.com', 'database': 'DB1',
                'warehouse': 'WH1', 'schema': 'PUBLIC'},
}


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS
# ═══════════════════════════════════════════════════════════════════


class TestMapTableauToMType(unittest.TestCase):
    """SIMPLE — type mapping."""

    def test_integer(self):
        self.assertEqual(map_tableau_to_m_type('integer'), 'Int64.Type')

    def test_real(self):
        self.assertEqual(map_tableau_to_m_type('real'), 'type number')

    def test_string(self):
        self.assertEqual(map_tableau_to_m_type('string'), 'type text')

    def test_boolean(self):
        self.assertEqual(map_tableau_to_m_type('boolean'), 'type logical')

    def test_date(self):
        self.assertEqual(map_tableau_to_m_type('date'), 'type date')

    def test_datetime(self):
        self.assertEqual(map_tableau_to_m_type('datetime'), 'type datetime')

    def test_unknown(self):
        self.assertEqual(map_tableau_to_m_type('blob'), 'type text')

    def test_case_insensitive(self):
        self.assertEqual(map_tableau_to_m_type('INTEGER'), 'Int64.Type')

    def test_binary(self):
        self.assertEqual(map_tableau_to_m_type('binary'), 'type binary')

    def test_currency(self):
        self.assertEqual(map_tableau_to_m_type('currency'), 'Currency.Type')


class TestColumnEscape(unittest.TestCase):
    """SIMPLE — M column name escaping."""

    def test_no_escape_needed(self):
        self.assertEqual(_m_escape_col_name('OrderID'), 'OrderID')

    def test_double_quote_escaped(self):
        self.assertEqual(_m_escape_col_name('Col"Name'), 'Col""Name')


class TestBuildTypeChanges(unittest.TestCase):
    """SIMPLE — Building M type-change entries."""

    def test_produces_entries(self):
        entries = _build_type_changes(SAMPLE_COLUMNS)
        self.assertEqual(len(entries), 5)

    def test_entry_format(self):
        entries = _build_type_changes([{'name': 'X', 'datatype': 'integer'}])
        self.assertEqual(entries[0], '{"X", Int64.Type}')

    def test_empty_columns(self):
        self.assertEqual(_build_type_changes([]), [])


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS — Per-connector generators
# ═══════════════════════════════════════════════════════════════════


class TestSQLServerGenerator(unittest.TestCase):
    """MEDIUM — SQL Server M query."""

    def test_contains_sql_database(self):
        m = generate_power_query_m(SQL_CONNECTION,
                                   {'name': 'Orders', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Sql.Database', m)
        self.assertIn('myserver', m)
        self.assertIn('mydb', m)

    def test_contains_let_in(self):
        m = generate_power_query_m(SQL_CONNECTION,
                                   {'name': 'Orders', 'columns': SAMPLE_COLUMNS})
        self.assertIn('let', m)
        self.assertIn('in', m)

    def test_references_table(self):
        m = generate_power_query_m(SQL_CONNECTION,
                                   {'name': 'Orders', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Orders', m)


class TestPostgreSQLGenerator(unittest.TestCase):
    """MEDIUM — PostgreSQL M query."""

    def test_contains_postgresql_database(self):
        m = generate_power_query_m(PG_CONNECTION,
                                   {'name': 'Customers', 'columns': SAMPLE_COLUMNS})
        self.assertIn('PostgreSQL.Database', m)
        self.assertIn('pghost', m)
        self.assertIn('5432', m)
        self.assertIn('pgdb', m)


class TestCSVGenerator(unittest.TestCase):
    """MEDIUM — CSV M query."""

    def test_contains_csv_document(self):
        m = generate_power_query_m(CSV_CONNECTION,
                                   {'name': 'Data', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Csv.Document', m)
        self.assertIn('data.csv', m)

    def test_has_type_changes(self):
        m = generate_power_query_m(CSV_CONNECTION,
                                   {'name': 'Data', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Changed Types', m)

    def test_empty_columns(self):
        m = generate_power_query_m(CSV_CONNECTION,
                                   {'name': 'Data', 'columns': []})
        self.assertIn('Csv.Document', m)


class TestExcelGenerator(unittest.TestCase):
    """MEDIUM — Excel M query."""

    def test_contains_excel_workbook(self):
        m = generate_power_query_m(EXCEL_CONNECTION,
                                   {'name': 'Sheet1', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Excel.Workbook', m)
        self.assertIn('report.xlsx', m)


class TestBigQueryGenerator(unittest.TestCase):
    """MEDIUM — BigQuery M query."""

    def test_contains_google_bigquery(self):
        m = generate_power_query_m(BQ_CONNECTION,
                                   {'name': 'Events', 'columns': SAMPLE_COLUMNS})
        self.assertIn('GoogleBigQuery.Database', m)
        self.assertIn('my-gcp-project', m)
        self.assertIn('analytics', m)


class TestMySQLGenerator(unittest.TestCase):
    """MEDIUM — MySQL M query."""

    def test_contains_mysql_database(self):
        m = generate_power_query_m(MYSQL_CONNECTION,
                                   {'name': 'Users', 'columns': SAMPLE_COLUMNS})
        self.assertIn('MySQL.Database', m)


class TestOracleGenerator(unittest.TestCase):
    """MEDIUM — Oracle M query."""

    def test_contains_oracle_database(self):
        m = generate_power_query_m(ORACLE_CONNECTION,
                                   {'name': 'HR', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Oracle.Database', m)
        self.assertIn('ORCL', m)


class TestSnowflakeGenerator(unittest.TestCase):
    """MEDIUM — Snowflake M query."""

    def test_contains_snowflake(self):
        m = generate_power_query_m(SNOWFLAKE_CONNECTION,
                                   {'name': 'Sales', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Snowflake.Databases', m)
        self.assertIn('WH1', m)


class TestFallbackGenerator(unittest.TestCase):
    """MEDIUM — Unknown connector type falls back to #table."""

    def test_fallback_produces_table(self):
        conn = {'type': 'SomeUnknownDB', 'details': {}}
        m = generate_power_query_m(conn,
                                   {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('#table', m)
        self.assertIn('TODO', m)


class TestAdditionalConnectors(unittest.TestCase):
    """MEDIUM — Additional connectors: Teradata, SAP, Redshift, etc."""

    def test_teradata(self):
        m = generate_power_query_m(
            {'type': 'Teradata', 'details': {'server': 'td1', 'database': 'DB1'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Teradata.Database', m)

    def test_sap_hana(self):
        m = generate_power_query_m(
            {'type': 'SAP HANA', 'details': {'server': 'hana1', 'port': '30015'}},
            {'name': 'T', 'columns': []})
        self.assertIn('SapHana.Database', m)

    def test_redshift(self):
        m = generate_power_query_m(
            {'type': 'Amazon Redshift', 'details': {'server': 'rs1', 'database': 'db'}},
            {'name': 'T', 'columns': []})
        self.assertIn('AmazonRedshift.Database', m)

    def test_databricks(self):
        m = generate_power_query_m(
            {'type': 'Databricks', 'details': {'server': 'adb-1'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Databricks.Catalogs', m)

    def test_spark_sql(self):
        m = generate_power_query_m(
            {'type': 'Spark SQL', 'details': {'server': 'spark1'}},
            {'name': 'T', 'columns': []})
        self.assertIn('SparkSql.Database', m)

    def test_azure_sql(self):
        m = generate_power_query_m(
            {'type': 'Azure SQL', 'details': {'server': 'az.database.windows.net'}},
            {'name': 'T', 'columns': []})
        self.assertIn('AzureSQL.Database', m)

    def test_synapse(self):
        m = generate_power_query_m(
            {'type': 'Azure Synapse', 'details': {'server': 'syn1'}},
            {'name': 'T', 'columns': []})
        self.assertIn('AzureSQL.Database', m)

    def test_google_sheets(self):
        m = generate_power_query_m(
            {'type': 'Google Sheets', 'details': {'spreadsheet_id': 'abc123'}},
            {'name': 'Sheet1', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Web.Contents', m)
        self.assertIn('abc123', m)

    def test_sharepoint(self):
        m = generate_power_query_m(
            {'type': 'SharePoint', 'details': {'site_url': 'https://sp.com/sites/x'}},
            {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('SharePoint.Files', m)

    def test_json(self):
        m = generate_power_query_m(
            {'type': 'JSON', 'details': {'filename': 'data.json'}},
            {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Json.Document', m)

    def test_xml(self):
        m = generate_power_query_m(
            {'type': 'XML', 'details': {'filename': 'data.xml'}},
            {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Xml.Tables', m)

    def test_pdf(self):
        m = generate_power_query_m(
            {'type': 'PDF', 'details': {'filename': 'report.pdf'}},
            {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Pdf.Tables', m)

    def test_salesforce(self):
        m = generate_power_query_m(
            {'type': 'Salesforce', 'details': {}},
            {'name': 'Account', 'columns': []})
        self.assertIn('Salesforce.Data', m)

    def test_web(self):
        m = generate_power_query_m(
            {'type': 'Web', 'details': {'url': 'https://api.example.com'}},
            {'name': 'T', 'columns': SAMPLE_COLUMNS})
        self.assertIn('Web.Contents', m)

    def test_vertica(self):
        m = generate_power_query_m(
            {'type': 'Vertica', 'details': {'server': 'vrt1', 'port': '5433', 'database': 'mydb'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Odbc.DataSource', m)
        self.assertIn('Vertica', m)
        self.assertIn('vrt1', m)

    def test_impala(self):
        m = generate_power_query_m(
            {'type': 'Impala', 'details': {'server': 'imp1', 'port': '21050'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Impala.Database', m)
        self.assertIn('imp1', m)

    def test_hadoop_hive_odbc(self):
        m = generate_power_query_m(
            {'type': 'Hadoop Hive', 'details': {'server': 'hive1', 'port': '10000', 'database': 'default'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Odbc.DataSource', m)
        self.assertIn('hive1', m)

    def test_hadoop_hive_hdinsight(self):
        m = generate_power_query_m(
            {'type': 'Hadoop Hive', 'details': {'server': 'mycluster.azurehdinsight.net', 'database': 'default'}},
            {'name': 'T', 'columns': []})
        self.assertIn('HdInsight', m)
        self.assertIn('azurehdinsight', m)

    def test_presto(self):
        m = generate_power_query_m(
            {'type': 'Presto', 'details': {'server': 'presto1', 'port': '8080', 'catalog': 'hive', 'schema': 'default'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Odbc.DataSource', m)
        self.assertIn('Presto', m)

    def test_trino_alias(self):
        m = generate_power_query_m(
            {'type': 'Trino', 'details': {'server': 'trino1'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Odbc.DataSource', m)
        self.assertIn('Presto', m)

    def test_hive_alias(self):
        m = generate_power_query_m(
            {'type': 'Hive', 'details': {'server': 'hive2'}},
            {'name': 'T', 'columns': []})
        self.assertIn('Odbc.DataSource', m)


class TestAllConnectorMSyntax(unittest.TestCase):
    """Exhaustive validation: every connector in _M_GENERATORS produces valid M."""

    # All connector type keys that should be in the dispatch table
    ALL_CONNECTOR_TYPES = [
        'Excel', 'SQL Server', 'PostgreSQL', 'CSV', 'BigQuery', 'MySQL',
        'Oracle', 'Snowflake', 'GeoJSON', 'Teradata', 'SAP HANA', 'SAP BW',
        'Amazon Redshift', 'Redshift', 'Databricks', 'Spark SQL', 'Spark',
        'Azure SQL', 'Azure Synapse', 'Synapse', 'Google Sheets', 'SharePoint',
        'JSON', 'XML', 'PDF', 'Salesforce', 'Web', 'Custom SQL',
        'OData', 'Google Analytics', 'Azure Blob', 'Azure Blob Storage',
        'ADLS', 'Azure Data Lake',
        'Vertica', 'Impala', 'Hadoop Hive', 'Hive', 'HDInsight',
        'Presto', 'Trino',
    ]

    def test_every_connector_produces_let_in(self):
        """Every connector M output must start with 'let' and contain 'in'."""
        for conn_type in self.ALL_CONNECTOR_TYPES:
            with self.subTest(connector=conn_type):
                details = {
                    'server': 'localhost', 'port': '1234',
                    'database': 'testdb', 'schema': 'dbo',
                    'filename': 'test.csv', 'directory': '/data',
                    'warehouse': 'WH', 'project': 'proj',
                    'dataset': 'ds', 'spreadsheet_id': 'abc',
                    'site_url': 'https://sp.com', 'url': 'https://api.com',
                    'sql_query': 'SELECT 1', 'http_path': '/sql/1.0',
                    'catalog': 'main', 'account': 'storage1',
                    'container': 'data', 'path': 'test.csv',
                }
                table = {'name': 'TestTable', 'columns': SAMPLE_COLUMNS}
                m = generate_power_query_m(
                    {'type': conn_type, 'details': details}, table)
                self.assertTrue(m.strip().startswith('let'),
                                f"M for {conn_type} doesn't start with 'let': {m[:80]}")
                self.assertIn('\nin', m,
                              f"M for {conn_type} missing 'in' keyword")

    def test_every_connector_no_python_artifacts(self):
        """M output should not contain Python f-string artifacts like {details}."""
        for conn_type in self.ALL_CONNECTOR_TYPES:
            with self.subTest(connector=conn_type):
                details = {'server': 'host', 'database': 'db'}
                m = generate_power_query_m(
                    {'type': conn_type, 'details': details},
                    {'name': 'T', 'columns': []})
                self.assertNotIn('{details}', m)
                self.assertNotIn('{columns}', m)
                self.assertNotIn('{table_name}', m)
    """MEDIUM — Custom SQL M query."""

    def test_custom_sql(self):
        conn = {
            'type': 'Custom SQL',
            'details': {'server': 'srv', 'database': 'db',
                        'sql_query': 'SELECT * FROM orders WHERE year = 2024'},
        }
        m = generate_power_query_m(conn, {'name': 'Q', 'columns': []})
        self.assertIn('Sql.Database', m)
        self.assertIn('Query=', m)
        self.assertIn('SELECT * FROM orders', m)


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS — Transform steps
# ═══════════════════════════════════════════════════════════════════


class TestColumnTransforms(unittest.TestCase):
    """MEDIUM — Column operation step generators."""

    def test_rename(self):
        name, expr = m_transform_rename({'Old': 'New', 'X': 'Y'})
        self.assertIn('Renamed Columns', name)
        self.assertIn('Table.RenameColumns', expr)
        self.assertIn('"Old"', expr)
        self.assertIn('"New"', expr)

    def test_remove_columns(self):
        name, expr = m_transform_remove_columns(['A', 'B'])
        self.assertIn('Table.RemoveColumns', expr)

    def test_select_columns(self):
        name, expr = m_transform_select_columns(['A', 'B'])
        self.assertIn('Table.SelectColumns', expr)

    def test_duplicate_column(self):
        name, expr = m_transform_duplicate_column('Src', 'Copy')
        self.assertIn('Table.DuplicateColumn', expr)
        self.assertIn('"Src"', expr)
        self.assertIn('"Copy"', expr)

    def test_reorder(self):
        name, expr = m_transform_reorder_columns(['C', 'A', 'B'])
        self.assertIn('Table.ReorderColumns', expr)

    def test_split(self):
        name, expr = m_transform_split_by_delimiter('Address', '-', 3)
        self.assertIn('Table.SplitColumn', expr)
        self.assertIn('SplitTextByDelimiter', expr)

    def test_merge(self):
        name, expr = m_transform_merge_columns(['First', 'Last'], 'FullName', ' ')
        self.assertIn('Table.CombineColumns', expr)
        self.assertIn('"FullName"', expr)


class TestValueTransforms(unittest.TestCase):
    """MEDIUM — Value operation step generators."""

    def test_replace_value(self):
        name, expr = m_transform_replace_value('Status', 'old', 'new')
        self.assertIn('Table.ReplaceValue', expr)

    def test_replace_nulls(self):
        name, expr = m_transform_replace_nulls('Revenue', 0)
        self.assertIn('null', expr)
        self.assertIn('Replacer.ReplaceValue', expr)

    def test_trim(self):
        name, expr = m_transform_trim(['A', 'B'])
        self.assertIn('Text.Trim', expr)

    def test_clean(self):
        name, expr = m_transform_clean(['A'])
        self.assertIn('Text.Clean', expr)

    def test_upper(self):
        name, expr = m_transform_upper(['Col'])
        self.assertIn('Text.Upper', expr)

    def test_lower(self):
        name, expr = m_transform_lower(['Col'])
        self.assertIn('Text.Lower', expr)

    def test_proper_case(self):
        name, expr = m_transform_proper_case(['Col'])
        self.assertIn('Text.Proper', expr)

    def test_fill_down(self):
        name, expr = m_transform_fill_down(['A'])
        self.assertIn('Table.FillDown', expr)

    def test_fill_up(self):
        name, expr = m_transform_fill_up(['A'])
        self.assertIn('Table.FillUp', expr)


class TestFilterTransforms(unittest.TestCase):
    """MEDIUM — Filter operation step generators."""

    def test_filter_single_value(self):
        name, expr = m_transform_filter_values('Status', ['Active'])
        self.assertIn('Table.SelectRows', expr)
        self.assertIn('"Active"', expr)

    def test_filter_multi_value(self):
        name, expr = m_transform_filter_values('Status', ['Active', 'Pending'])
        self.assertIn('List.Contains', expr)

    def test_exclude_values(self):
        name, expr = m_transform_exclude_values('Region', ['Unknown'])
        self.assertIn('Table.SelectRows', expr)
        self.assertIn('<>', expr)

    def test_filter_range(self):
        name, expr = m_transform_filter_range('Price', min_val=10, max_val=100)
        self.assertIn('>=', expr)
        self.assertIn('<=', expr)

    def test_filter_nulls_exclude(self):
        name, expr = m_transform_filter_nulls('Col')
        self.assertIn('<> null', expr)

    def test_filter_nulls_keep(self):
        name, expr = m_transform_filter_nulls('Col', keep_nulls=True)
        self.assertIn('= null', expr)

    def test_filter_contains(self):
        name, expr = m_transform_filter_contains('Name', 'abc')
        self.assertIn('Text.Contains', expr)

    def test_distinct_all(self):
        name, expr = m_transform_distinct()
        self.assertIn('Table.Distinct', expr)

    def test_distinct_columns(self):
        name, expr = m_transform_distinct(['A', 'B'])
        self.assertIn('Table.Distinct', expr)
        self.assertIn('"A"', expr)

    def test_top_n(self):
        name, expr = m_transform_top_n(10, 'Sales', descending=True)
        self.assertIn('Table.FirstN', expr)
        self.assertIn('Order.Descending', expr)


class TestInjectMSteps(unittest.TestCase):
    """MEDIUM — inject_m_steps chaining."""

    def _base_query(self):
        return '''let
    Source = Sql.Database("server", "db"),
    Result = Source
in
    Result'''

    def test_inject_one_step(self):
        q = inject_m_steps(self._base_query(), [
            m_transform_rename({'Old': 'New'})
        ])
        self.assertIn('Renamed Columns', q)
        self.assertIn('let', q)
        self.assertIn('in', q)

    def test_inject_multiple_steps(self):
        q = inject_m_steps(self._base_query(), [
            m_transform_rename({'A': 'B'}),
            m_transform_trim(['B']),
        ])
        self.assertIn('Renamed Columns', q)
        self.assertIn('Trimmed Text', q)

    def test_empty_steps(self):
        q = inject_m_steps(self._base_query(), [])
        self.assertEqual(q, self._base_query())

    def test_chaining_preserves_prev(self):
        q = inject_m_steps(self._base_query(), [
            m_transform_filter_values('Status', ['Active']),
            m_transform_trim(['Name']),
        ])
        # Second step should reference first
        self.assertIn('Filtered Rows', q)
        self.assertIn('Trimmed Text', q)

    def test_idempotent_double_inject(self):
        q1 = inject_m_steps(self._base_query(), [
            m_transform_rename({'A': 'B'}),
        ])
        q2 = inject_m_steps(q1, [
            m_transform_trim(['B']),
        ])
        self.assertIn('Renamed Columns', q2)
        self.assertIn('Trimmed Text', q2)
        self.assertIn('Result =', q2)


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS
# ═══════════════════════════════════════════════════════════════════


class TestAggregateTransform(unittest.TestCase):
    """COMPLEX — Group By / Aggregate."""

    def test_sum_aggregate(self):
        name, expr = m_transform_aggregate(
            ['Region'],
            [{'name': 'Total Sales', 'column': 'Sales', 'agg': 'sum'}]
        )
        self.assertIn('Table.Group', expr)
        self.assertIn('"Region"', expr)
        self.assertIn('List.Sum', expr)

    def test_count_aggregate(self):
        _, expr = m_transform_aggregate(
            ['Region'],
            [{'name': 'Order Count', 'column': 'ID', 'agg': 'count'}]
        )
        self.assertIn('Table.RowCount', expr)

    def test_countd_aggregate(self):
        _, expr = m_transform_aggregate(
            ['Region'],
            [{'name': 'Unique Customers', 'column': 'Customer', 'agg': 'countd'}]
        )
        self.assertIn('List.Distinct', expr)

    def test_multi_group_multi_agg(self):
        _, expr = m_transform_aggregate(
            ['Region', 'Category'],
            [
                {'name': 'Sum Sales', 'column': 'Sales', 'agg': 'sum'},
                {'name': 'Avg Profit', 'column': 'Profit', 'agg': 'avg'},
            ]
        )
        self.assertIn('"Region"', expr)
        self.assertIn('"Category"', expr)
        self.assertIn('List.Sum', expr)
        self.assertIn('List.Average', expr)


class TestPivotUnpivot(unittest.TestCase):
    """COMPLEX — Pivot/Unpivot."""

    def test_unpivot(self):
        name, expr = m_transform_unpivot(['Q1', 'Q2', 'Q3', 'Q4'])
        self.assertIn('Table.Unpivot', expr)
        self.assertIn('"Q1"', expr)

    def test_unpivot_other(self):
        name, expr = m_transform_unpivot_other(['ID', 'Name'])
        self.assertIn('Table.UnpivotOtherColumns', expr)

    def test_pivot(self):
        name, expr = m_transform_pivot('Quarter', 'Sales')
        self.assertIn('Table.Pivot', expr)


class TestJoinUnion(unittest.TestCase):
    """COMPLEX — Join and Union operations."""

    def test_join_inner(self):
        steps = m_transform_join(
            'OtherTable', ['ID'], ['ID'],
            join_type='inner', expand_columns=['Name']
        )
        self.assertEqual(len(steps), 2)  # join + expand
        self.assertIn('Table.NestedJoin', steps[0][1])
        self.assertIn('JoinKind.Inner', steps[0][1])
        self.assertIn('Table.ExpandTableColumn', steps[1][1])

    def test_join_left(self):
        steps = m_transform_join('T2', ['A'], ['A'], join_type='left')
        self.assertEqual(len(steps), 1)
        self.assertIn('JoinKind.LeftOuter', steps[0][1])

    def test_join_multi_key(self):
        steps = m_transform_join('T2', ['A', 'B'], ['X', 'Y'], join_type='inner')
        self.assertIn('"A"', steps[0][1])
        self.assertIn('"B"', steps[0][1])

    def test_union(self):
        name, expr = m_transform_union(['Table1', 'Table2', 'Table3'])
        self.assertIn('Table.Combine', expr)

    def test_wildcard_union(self):
        result = m_transform_wildcard_union('C:\\Data', '.csv', ',')
        self.assertIn('Folder.Files', result)
        self.assertIn('Csv.Document', result)


class TestReshapeTransforms(unittest.TestCase):
    """COMPLEX — Sort, transpose, index, skip, etc."""

    def test_sort(self):
        name, expr = m_transform_sort([('Sales', True), ('Name', False)])
        self.assertIn('Table.Sort', expr)
        self.assertIn('Order.Descending', expr)
        self.assertIn('Order.Ascending', expr)

    def test_transpose(self):
        name, expr = m_transform_transpose()
        self.assertIn('Table.Transpose', expr)

    def test_add_index(self):
        name, expr = m_transform_add_index('RowNum', 0, 1)
        self.assertIn('Table.AddIndexColumn', expr)

    def test_skip_rows(self):
        name, expr = m_transform_skip_rows(5)
        self.assertIn('Table.Skip', expr)
        self.assertIn('5', expr)

    def test_remove_last_rows(self):
        name, expr = m_transform_remove_last_rows(3)
        self.assertIn('Table.RemoveLastN', expr)

    def test_remove_errors(self):
        name, expr = m_transform_remove_errors()
        self.assertIn('Table.RemoveRowsWithErrors', expr)

    def test_promote_headers(self):
        name, expr = m_transform_promote_headers()
        self.assertIn('Table.PromoteHeaders', expr)

    def test_demote_headers(self):
        name, expr = m_transform_demote_headers()
        self.assertIn('Table.DemoteHeaders', expr)


class TestCalculatedColumn(unittest.TestCase):
    """COMPLEX — Add column / Conditional column."""

    def test_add_column(self):
        name, expr = m_transform_add_column(
            'Revenue', 'each [Price] * [Qty]', 'type number'
        )
        self.assertIn('Table.AddColumn', expr)
        self.assertIn('"Revenue"', expr)
        self.assertIn('type number', expr)

    def test_add_column_no_type(self):
        name, expr = m_transform_add_column('Flag', 'each "Yes"')
        self.assertIn('Table.AddColumn', expr)
        self.assertNotIn('type ', expr)

    def test_conditional_column(self):
        name, expr = m_transform_conditional_column(
            'Tier',
            [('[Sales] > 1000', '"High"'), ('[Sales] > 500', '"Medium"')],
            default_value='"Low"'
        )
        self.assertIn('Table.AddColumn', expr)
        self.assertIn('if [Sales] > 1000 then "High"', expr)
        self.assertIn('"Low"', expr)

    def test_conditional_no_default(self):
        name, expr = m_transform_conditional_column(
            'Flag',
            [('[Active] = true', '"Yes"')]
        )
        self.assertIn('null', expr)


class TestGeoJSONGenerator(unittest.TestCase):
    """COMPLEX — GeoJSON M query with geometry handling."""

    def test_geojson_with_geometry(self):
        cols = [
            {'name': 'Name', 'datatype': 'string'},
            {'name': 'Population', 'datatype': 'integer'},
            {'name': 'Geometry', 'datatype': 'spatial'},
        ]
        conn = {'type': 'GeoJSON', 'details': {'filename': 'regions.geojson'}}
        m = generate_power_query_m(conn, {'name': 'Regions', 'columns': cols})
        self.assertIn('Json.Document', m)
        self.assertIn('features', m)
        self.assertIn('Geometry', m)

    def test_geojson_no_geometry(self):
        cols = [
            {'name': 'Name', 'datatype': 'string'},
            {'name': 'Value', 'datatype': 'real'},
        ]
        conn = {'type': 'GeoJSON', 'details': {'filename': 'data.geojson'}}
        m = generate_power_query_m(conn, {'name': 'D', 'columns': cols})
        self.assertIn('Json.Document', m)


class TestIntegrationInjectOnRealQuery(unittest.TestCase):
    """COMPLEX — inject_m_steps on real connector queries."""

    def test_inject_on_sql_query(self):
        m = generate_power_query_m(SQL_CONNECTION,
                                   {'name': 'Orders', 'columns': SAMPLE_COLUMNS})
        enriched = inject_m_steps(m, [
            m_transform_filter_values('IsActive', ['true']),
            m_transform_rename({'Product Name': 'ProductName'}),
            m_transform_add_column('Revenue', 'each [Price] * 1.1', 'type number'),
        ])
        self.assertIn('Sql.Database', enriched)
        self.assertIn('Filtered Rows', enriched)
        self.assertIn('Renamed Columns', enriched)
        self.assertIn('Added Revenue', enriched)
        self.assertIn('Result =', enriched)

    def test_inject_on_csv_query(self):
        m = generate_power_query_m(CSV_CONNECTION,
                                   {'name': 'Data', 'columns': SAMPLE_COLUMNS})
        enriched = inject_m_steps(m, [
            m_transform_distinct(),
            m_transform_sort([('OrderDate', True)]),
        ])
        self.assertIn('Csv.Document', enriched)
        self.assertIn('Removed Duplicates', enriched)
        self.assertIn('Sorted Rows', enriched)


if __name__ == '__main__':
    unittest.main()
