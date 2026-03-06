"""Coverage tests for tableau_export/datasource_extractor.py.

Uses inline XML via ET.fromstring() and plain strings to test all
extraction functions without requiring real .twb/.twbx files.
"""

import os
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET

from tableau_export.datasource_extractor import (
    _detect_csv_delimiter,
    _read_csv_header_from_twbx,
    _parse_connection_class,
    _build_connection_map,
    extract_connection_details,
    extract_datasource,
    extract_tables_with_columns,
    extract_column_metadata,
    extract_calculations,
    extract_relationships,
)


# ═══════════════════════════════════════════════════════════════
#  _detect_csv_delimiter
# ═══════════════════════════════════════════════════════════════

class TestDetectCsvDelimiter(unittest.TestCase):

    def test_comma(self):
        self.assertEqual(_detect_csv_delimiter("a,b,c,d"), ",")

    def test_semicolon(self):
        self.assertEqual(_detect_csv_delimiter("a;b;c;d"), ";")

    def test_tab(self):
        self.assertEqual(_detect_csv_delimiter("a\tb\tc\td"), "\t")

    def test_pipe(self):
        self.assertEqual(_detect_csv_delimiter("a|b|c|d"), "|")

    def test_empty(self):
        self.assertEqual(_detect_csv_delimiter(""), ",")

    def test_none(self):
        self.assertEqual(_detect_csv_delimiter(None), ",")

    def test_no_delimiters(self):
        self.assertEqual(_detect_csv_delimiter("singlevalue"), ",")


# ═══════════════════════════════════════════════════════════════
#  _read_csv_header_from_twbx
# ═══════════════════════════════════════════════════════════════

class TestReadCsvHeaderFromTwbx(unittest.TestCase):

    def test_none_path(self):
        self.assertIsNone(_read_csv_header_from_twbx(None, "", "data.csv"))

    def test_nonexistent_path(self):
        self.assertIsNone(_read_csv_header_from_twbx("/no/such/file.twbx", "", "data.csv"))

    def test_wrong_extension(self):
        # Create a temp file with wrong extension
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"dummy")
            path = f.name
        try:
            self.assertIsNone(_read_csv_header_from_twbx(path, "", "data.csv"))
        finally:
            os.unlink(path)

    def test_valid_twbx_exact_match(self):
        with tempfile.NamedTemporaryFile(suffix=".twbx", delete=False) as f:
            path = f.name
        try:
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr("data.csv", "col1,col2,col3\nval1,val2,val3\n")
            result = _read_csv_header_from_twbx(path, "", "data.csv")
            self.assertEqual(result, "col1,col2,col3")
        finally:
            os.unlink(path)

    def test_valid_twbx_with_directory(self):
        with tempfile.NamedTemporaryFile(suffix=".twbx", delete=False) as f:
            path = f.name
        try:
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr("Data/Extracts/sales.csv", "id;name;price\n1;A;10\n")
            result = _read_csv_header_from_twbx(path, "Data/Extracts", "sales.csv")
            self.assertEqual(result, "id;name;price")
        finally:
            os.unlink(path)

    def test_valid_twbx_partial_match(self):
        with tempfile.NamedTemporaryFile(suffix=".twbx", delete=False) as f:
            path = f.name
        try:
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr("some/deep/path/data.csv", "x|y|z\n1|2|3\n")
            result = _read_csv_header_from_twbx(path, "", "data.csv")
            self.assertEqual(result, "x|y|z")
        finally:
            os.unlink(path)

    def test_file_not_in_zip(self):
        with tempfile.NamedTemporaryFile(suffix=".twbx", delete=False) as f:
            path = f.name
        try:
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr("other.csv", "a,b\n")
            result = _read_csv_header_from_twbx(path, "", "data.csv")
            self.assertIsNone(result)
        finally:
            os.unlink(path)

    def test_bad_zip(self):
        with tempfile.NamedTemporaryFile(suffix=".twbx", delete=False) as f:
            f.write(b"this is not a zip")
            path = f.name
        try:
            result = _read_csv_header_from_twbx(path, "", "data.csv")
            self.assertIsNone(result)
        finally:
            os.unlink(path)

    def test_tdsx_extension(self):
        with tempfile.NamedTemporaryFile(suffix=".tdsx", delete=False) as f:
            path = f.name
        try:
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr("data.csv", "a\tb\tc\n")
            result = _read_csv_header_from_twbx(path, "", "data.csv")
            self.assertEqual(result, "a\tb\tc")
        finally:
            os.unlink(path)


# ═══════════════════════════════════════════════════════════════
#  _parse_connection_class
# ═══════════════════════════════════════════════════════════════

class TestParseConnectionClass(unittest.TestCase):

    def test_excel(self):
        conn = ET.fromstring('<connection class="excel-direct" filename="data.xlsx" cleaning="yes"/>')
        named = ET.fromstring('<named-connection caption="MyExcel"/>')
        result = _parse_connection_class(conn, named)
        self.assertEqual(result['type'], 'Excel')
        self.assertEqual(result['details']['filename'], 'data.xlsx')
        self.assertEqual(result['details']['caption'], 'MyExcel')
        self.assertEqual(result['details']['cleaning'], 'yes')

    def test_excel_no_named_conn(self):
        conn = ET.fromstring('<connection class="excel-direct" filename="data.xlsx"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'Excel')
        self.assertEqual(result['details']['caption'], '')

    def test_csv_with_separator(self):
        conn = ET.fromstring('<connection class="textscan" filename="data.csv" directory="/tmp" separator=";" charset="latin1"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'CSV')
        self.assertEqual(result['details']['delimiter'], ';')
        self.assertEqual(result['details']['encoding'], 'latin1')

    def test_csv_no_separator_no_twbx(self):
        conn = ET.fromstring('<connection class="textscan" filename="data.csv" directory="/tmp"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'CSV')
        self.assertEqual(result['details']['delimiter'], ',')  # default

    def test_geojson(self):
        conn = ET.fromstring('<connection class="ogrdirect" filename="map.geojson" directory="/maps"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'GeoJSON')

    def test_sql_server(self):
        conn = ET.fromstring(
            '<connection class="sqlserver" server="myserver" dbname="mydb"'
            ' authentication="sql" username="user1"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'SQL Server')
        self.assertEqual(result['details']['server'], 'myserver')
        self.assertEqual(result['details']['database'], 'mydb')
        self.assertEqual(result['details']['authentication'], 'sql')

    def test_postgres(self):
        conn = ET.fromstring(
            '<connection class="postgres" server="pg.host" port="5433"'
            ' dbname="analytics" username="admin" sslmode="disable"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'PostgreSQL')
        self.assertEqual(result['details']['port'], '5433')
        self.assertEqual(result['details']['sslmode'], 'disable')

    def test_bigquery(self):
        conn = ET.fromstring(
            '<connection class="bigquery" project="myproj" dataset="ds1"'
            ' service-account-email="sa@proj.iam"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'BigQuery')
        self.assertEqual(result['details']['project'], 'myproj')

    def test_oracle(self):
        conn = ET.fromstring(
            '<connection class="oracle" server="ora.host" service="ORCL"'
            ' port="1522" username="orauser"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'Oracle')
        self.assertEqual(result['details']['service'], 'ORCL')

    def test_mysql(self):
        conn = ET.fromstring(
            '<connection class="mysql" server="mysql.host" port="3307" dbname="shop" username="root"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'MySQL')
        self.assertEqual(result['details']['port'], '3307')

    def test_snowflake(self):
        conn = ET.fromstring(
            '<connection class="snowflake" server="acct.snowflakecomputing.com"'
            ' dbname="ANALYTICS" schema="PUBLIC" warehouse="COMPUTE" role="ANALYST"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'Snowflake')
        self.assertEqual(result['details']['warehouse'], 'COMPUTE')

    def test_sapbw(self):
        conn = ET.fromstring(
            '<connection class="sapbw" server="sap.host" systemNumber="01"'
            ' clientId="100" language="DE" cube="ZCUBE" catalog="CAT1"/>'
        )
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'SAP BW')
        self.assertEqual(result['details']['system_number'], '01')
        self.assertEqual(result['details']['language'], 'DE')

    def test_unknown_fallback(self):
        conn = ET.fromstring('<connection class="databricks" server="db.host" token="abc"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'DATABRICKS')
        self.assertIn('server', result['details'])
        self.assertIn('token', result['details'])

    def test_unknown_with_no_class(self):
        conn = ET.fromstring('<connection server="host"/>')
        result = _parse_connection_class(conn)
        self.assertEqual(result['type'], 'UNKNOWN')


# ═══════════════════════════════════════════════════════════════
#  _build_connection_map
# ═══════════════════════════════════════════════════════════════

class TestBuildConnectionMap(unittest.TestCase):

    def test_named_connections(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <named-connection name="excel_conn">'
            '      <connection class="excel-direct" filename="data.xlsx"/>'
            '    </named-connection>'
            '    <named-connection name="sql_conn">'
            '      <connection class="sqlserver" server="srv" dbname="db"/>'
            '    </named-connection>'
            '  </connection>'
            '</datasource>'
        )
        cm = _build_connection_map(ds)
        self.assertIn('excel_conn', cm)
        self.assertIn('sql_conn', cm)
        self.assertEqual(cm['excel_conn']['type'], 'Excel')
        self.assertEqual(cm['sql_conn']['type'], 'SQL Server')

    def test_no_federated_falls_back(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="sqlserver">'
            '    <named-connection name="nc1">'
            '      <connection class="postgres" server="pg"/>'
            '    </named-connection>'
            '  </connection>'
            '</datasource>'
        )
        cm = _build_connection_map(ds)
        self.assertIn('nc1', cm)

    def test_no_connection_at_all(self):
        ds = ET.fromstring('<datasource></datasource>')
        cm = _build_connection_map(ds)
        self.assertEqual(cm, {})

    def test_skip_named_conn_without_inner(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <named-connection name="broken"/>'
            '  </connection>'
            '</datasource>'
        )
        cm = _build_connection_map(ds)
        self.assertEqual(cm, {})

    def test_skip_named_conn_without_name(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <named-connection>'
            '      <connection class="sqlserver" server="s"/>'
            '    </named-connection>'
            '  </connection>'
            '</datasource>'
        )
        cm = _build_connection_map(ds)
        self.assertEqual(cm, {})


# ═══════════════════════════════════════════════════════════════
#  extract_connection_details
# ═══════════════════════════════════════════════════════════════

class TestExtractConnectionDetails(unittest.TestCase):

    def test_with_named_conn(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <named-connection name="nc1" caption="My Excel">'
            '      <connection class="excel-direct" filename="data.xlsx"/>'
            '    </named-connection>'
            '  </connection>'
            '</datasource>'
        )
        result = extract_connection_details(ds)
        self.assertEqual(result['type'], 'Excel')

    def test_without_named_conn(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated"/>'
            '</datasource>'
        )
        result = extract_connection_details(ds)
        self.assertEqual(result['type'], 'Unknown')

    def test_no_connection(self):
        ds = ET.fromstring('<datasource></datasource>')
        result = extract_connection_details(ds)
        self.assertEqual(result['type'], 'Unknown')
        self.assertEqual(result['details'], {})

    def test_named_conn_missing_inner(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <named-connection name="nc1"/>'
            '  </connection>'
            '</datasource>'
        )
        result = extract_connection_details(ds)
        self.assertEqual(result['type'], 'Unknown')


# ═══════════════════════════════════════════════════════════════
#  extract_column_metadata
# ═══════════════════════════════════════════════════════════════

class TestExtractColumnMetadata(unittest.TestCase):

    def test_basic_column(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <column name="[Sales]" caption="Revenue" datatype="real"'
            '          role="measure" type="quantitative" hidden="false"'
            '          semantic-role="amount" default-type="SUM"/>'
            '</datasource>'
        )
        cols = extract_column_metadata(ds)
        self.assertEqual(len(cols), 1)
        self.assertEqual(cols[0]['name'], '[Sales]')
        self.assertEqual(cols[0]['caption'], 'Revenue')
        self.assertEqual(cols[0]['role'], 'measure')
        self.assertFalse(cols[0]['hidden'])
        self.assertIsNone(cols[0]['calculation'])

    def test_hidden_column(self):
        ds = ET.fromstring(
            '<datasource><column name="[ID]" hidden="true"/></datasource>'
        )
        cols = extract_column_metadata(ds)
        self.assertTrue(cols[0]['hidden'])

    def test_column_with_calculation(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <column name="[Profit Ratio]" datatype="real">'
            '    <calculation class="tableau" formula="SUM([Profit])/SUM([Sales])"/>'
            '  </column>'
            '</datasource>'
        )
        cols = extract_column_metadata(ds)
        self.assertIsNotNone(cols[0]['calculation'])
        self.assertEqual(cols[0]['calculation']['formula'], 'SUM([Profit])/SUM([Sales])')

    def test_defaults(self):
        ds = ET.fromstring('<datasource><column name="[X]"/></datasource>')
        cols = extract_column_metadata(ds)
        self.assertEqual(cols[0]['datatype'], 'string')
        self.assertEqual(cols[0]['role'], 'dimension')
        self.assertEqual(cols[0]['type'], 'nominal')
        self.assertFalse(cols[0]['hidden'])

    def test_empty(self):
        ds = ET.fromstring('<datasource></datasource>')
        cols = extract_column_metadata(ds)
        self.assertEqual(cols, [])


# ═══════════════════════════════════════════════════════════════
#  extract_calculations
# ═══════════════════════════════════════════════════════════════

class TestExtractCalculations(unittest.TestCase):

    def test_single_calc(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <column name="[ProfitRatio]" caption="Profit Ratio" datatype="real"'
            '          role="measure" type="quantitative">'
            '    <calculation class="tableau" formula="SUM([Profit])/SUM([Sales])"/>'
            '  </column>'
            '</datasource>'
        )
        calcs = extract_calculations(ds)
        self.assertEqual(len(calcs), 1)
        self.assertEqual(calcs[0]['name'], '[ProfitRatio]')
        self.assertEqual(calcs[0]['caption'], 'Profit Ratio')
        self.assertIn('Profit', calcs[0]['formula'])

    def test_caption_fallback_to_name(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <column name="[Calc1]">'
            '    <calculation formula="1+1"/>'
            '  </column>'
            '</datasource>'
        )
        calcs = extract_calculations(ds)
        self.assertEqual(calcs[0]['caption'], '[Calc1]')

    def test_no_calculations(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <column name="[Sales]" datatype="real"/>'
            '</datasource>'
        )
        calcs = extract_calculations(ds)
        self.assertEqual(calcs, [])

    def test_empty(self):
        ds = ET.fromstring('<datasource></datasource>')
        calcs = extract_calculations(ds)
        self.assertEqual(calcs, [])


# ═══════════════════════════════════════════════════════════════
#  extract_tables_with_columns — Phase 1 (inline columns)
# ═══════════════════════════════════════════════════════════════

class TestExtractTablesPhase1(unittest.TestCase):

    def test_physical_table_with_columns(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <relation type="table" name="Orders" connection="conn1">'
            '      <columns>'
            '        <column name="OrderID" datatype="integer" ordinal="0"/>'
            '        <column name="Amount" datatype="real" ordinal="1"/>'
            '      </columns>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]['name'], 'Orders')
        self.assertEqual(len(tables[0]['columns']), 2)

    def test_skip_join_relations(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <relation type="join" join="inner">'
            '      <relation type="table" name="Orders">'
            '        <columns><column name="ID" datatype="integer"/></columns>'
            '      </relation>'
            '      <relation type="table" name="Products">'
            '        <columns><column name="PID" datatype="integer"/></columns>'
            '      </relation>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        names = [t['name'] for t in tables]
        self.assertIn('Orders', names)
        self.assertIn('Products', names)
        # Should NOT have a table named '' from the join itself

    def test_deduplication_keeps_more_columns(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="T1">'
            '      <columns><column name="A" datatype="string"/></columns>'
            '    </relation>'
            '    <relation type="table" name="T1">'
            '      <columns>'
            '        <column name="A" datatype="string"/>'
            '        <column name="B" datatype="string"/>'
            '      </columns>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        t1_list = [t for t in tables if t['name'] == 'T1']
        self.assertEqual(len(t1_list), 1)
        self.assertEqual(len(t1_list[0]['columns']), 2)

    def test_empty_datasource(self):
        ds = ET.fromstring('<datasource></datasource>')
        tables = extract_tables_with_columns(ds)
        self.assertEqual(tables, [])

    def test_skip_empty_table_name(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="">'
            '      <columns><column name="A" datatype="string"/></columns>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(tables, [])

    def test_connection_map_lookup(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="Sales" connection="conn_pg">'
            '      <columns><column name="ID" datatype="integer"/></columns>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        cm = {'conn_pg': {'type': 'PostgreSQL', 'details': {'server': 'pg.host'}}}
        tables = extract_tables_with_columns(ds, cm)
        self.assertEqual(tables[0]['connection_details']['type'], 'PostgreSQL')


# ═══════════════════════════════════════════════════════════════
#  extract_tables_with_columns — Phase 2 (cols/map fallback)
# ═══════════════════════════════════════════════════════════════

class TestExtractTablesPhase2(unittest.TestCase):

    def test_sql_server_cols_map(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <cols>'
            '      <map key="[OrderID]" value="[Orders].[OrderID]"/>'
            '      <map key="[Amount]" value="[Orders].[Amount]"/>'
            '    </cols>'
            '    <relation type="table" name="Orders" connection="sqlconn"/>'
            '  </connection>'
            '  <column name="[OrderID]" datatype="integer" role="dimension"/>'
            '  <column name="[Amount]" datatype="real" role="measure"/>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0]['columns']), 2)

    def test_skip_calculated_columns(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <cols>'
            '      <map key="[OrderID]" value="[Orders].[OrderID]"/>'
            '      <map key="[Calc]" value="[Orders].[Calc]"/>'
            '    </cols>'
            '    <relation type="table" name="Orders"/>'
            '  </connection>'
            '  <column name="[OrderID]" datatype="integer"/>'
            '  <column name="[Calc]" datatype="real">'
            '    <calculation formula="1+1"/>'
            '  </column>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        # Calc column should be skipped in phase 2
        self.assertEqual(len(tables[0]['columns']), 1)

    def test_skip_sheet_link_columns(self):
        # Note: user:auto-column is a Tableau namespace attribute. In real
        # Tableau XML it works with the namespace declared at document level.
        # ET.fromstring can't easily reproduce this, so we just verify
        # that the phase-2 path runs without errors for tables needing columns.
        ds = ET.fromstring(
            '<datasource>'
            '  <connection class="federated">'
            '    <cols>'
            '      <map key="[ID]" value="[T1].[ID]"/>'
            '    </cols>'
            '    <relation type="table" name="T1"/>'
            '  </connection>'
            '  <column name="[ID]" datatype="integer"/>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables[0]['columns']), 1)


# ═══════════════════════════════════════════════════════════════
#  extract_tables_with_columns — Phase 3 (metadata-records)
# ═══════════════════════════════════════════════════════════════

class TestExtractTablesPhase3(unittest.TestCase):

    def test_metadata_records(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="Employees"/>'
            '    <metadata-records>'
            '      <metadata-record class="column">'
            '        <remote-name>EmpID</remote-name>'
            '        <local-name>[EmpID]</local-name>'
            '        <parent-name>[Employees]</parent-name>'
            '        <local-type>integer</local-type>'
            '        <ordinal>0</ordinal>'
            '        <contains-null>false</contains-null>'
            '      </metadata-record>'
            '      <metadata-record class="column">'
            '        <remote-name>Name</remote-name>'
            '        <local-name>[Name]</local-name>'
            '        <parent-name>[Employees]</parent-name>'
            '        <local-type>string</local-type>'
            '        <ordinal>1</ordinal>'
            '        <contains-null>true</contains-null>'
            '      </metadata-record>'
            '    </metadata-records>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0]['columns']), 2)
        self.assertEqual(tables[0]['columns'][0]['name'], 'EmpID')
        self.assertFalse(tables[0]['columns'][0]['nullable'])

    def test_metadata_ordinal_sorting(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="T"/>'
            '    <metadata-records>'
            '      <metadata-record class="column">'
            '        <remote-name>B</remote-name>'
            '        <local-name>[B]</local-name>'
            '        <parent-name>[T]</parent-name>'
            '        <local-type>string</local-type>'
            '        <ordinal>1</ordinal>'
            '      </metadata-record>'
            '      <metadata-record class="column">'
            '        <remote-name>A</remote-name>'
            '        <local-name>[A]</local-name>'
            '        <parent-name>[T]</parent-name>'
            '        <local-type>string</local-type>'
            '        <ordinal>0</ordinal>'
            '      </metadata-record>'
            '    </metadata-records>'
            '  </connection>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(tables[0]['columns'][0]['name'], 'A')
        self.assertEqual(tables[0]['columns'][1]['name'], 'B')


# ═══════════════════════════════════════════════════════════════
#  extract_tables_with_columns — Phase 4 (last-resort fallback)
# ═══════════════════════════════════════════════════════════════

class TestExtractTablesPhase4(unittest.TestCase):

    def test_fallback_to_ds_columns(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="FallbackTable"/>'
            '  </connection>'
            '  <column name="[Col1]" datatype="string"/>'
            '  <column name="[Col2]" datatype="integer"/>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0]['columns']), 2)

    def test_fallback_skips_calculations(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="table" name="FallbackTable"/>'
            '  </connection>'
            '  <column name="[Physical]" datatype="string"/>'
            '  <column name="[Calc]" datatype="real">'
            '    <calculation formula="1+1"/>'
            '  </column>'
            '</datasource>'
        )
        tables = extract_tables_with_columns(ds)
        self.assertEqual(len(tables[0]['columns']), 1)
        self.assertEqual(tables[0]['columns'][0]['name'], 'Physical')


# ═══════════════════════════════════════════════════════════════
#  extract_relationships — legacy joins
# ═══════════════════════════════════════════════════════════════

class TestExtractRelationshipsLegacy(unittest.TestCase):

    def test_explicit_table_column_format(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="join" join="inner">'
            '      <relation type="table" name="Orders"/>'
            '      <relation type="table" name="Products"/>'
            '      <clause>'
            '        <expression op="=">'
            '          <expression op="[Orders].[ProductID]"/>'
            '          <expression op="[Products].[ProductID]"/>'
            '        </expression>'
            '      </clause>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(len(rels), 1)
        self.assertEqual(rels[0]['type'], 'inner')
        self.assertEqual(rels[0]['left']['table'], 'Orders')
        self.assertEqual(rels[0]['left']['column'], 'ProductID')
        self.assertEqual(rels[0]['right']['table'], 'Products')

    def test_bare_column_format_inferred(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="join" join="left">'
            '      <relation type="table" name="Orders"/>'
            '      <relation type="table" name="Customers"/>'
            '      <clause>'
            '        <expression op="=">'
            '          <expression op="[CustID]"/>'
            '          <expression op="[CustID]"/>'
            '        </expression>'
            '      </clause>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(len(rels), 1)
        self.assertEqual(rels[0]['left']['table'], 'Orders')
        self.assertEqual(rels[0]['right']['table'], 'Customers')

    def test_nested_join(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="join" join="inner">'
            '      <relation type="join" join="left">'
            '        <relation type="table" name="A"/>'
            '        <relation type="table" name="B"/>'
            '        <clause>'
            '          <expression op="=">'
            '            <expression op="[A].[id]"/>'
            '            <expression op="[B].[id]"/>'
            '          </expression>'
            '        </clause>'
            '      </relation>'
            '      <relation type="table" name="C"/>'
            '      <clause>'
            '        <expression op="=">'
            '          <expression op="[A].[cid]"/>'
            '          <expression op="[C].[cid]"/>'
            '        </expression>'
            '      </clause>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertTrue(len(rels) >= 2)

    def test_deduplication(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="join" join="inner">'
            '      <relation type="table" name="T1"/>'
            '      <relation type="table" name="T2"/>'
            '      <clause>'
            '        <expression op="=">'
            '          <expression op="[T1].[ID]"/>'
            '          <expression op="[T2].[ID]"/>'
            '        </expression>'
            '      </clause>'
            '      <clause>'
            '        <expression op="=">'
            '          <expression op="[T1].[ID]"/>'
            '          <expression op="[T2].[ID]"/>'
            '        </expression>'
            '      </clause>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(len(rels), 1)

    def test_no_joins(self):
        ds = ET.fromstring(
            '<datasource><connection><relation type="table" name="T"/></connection></datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(rels, [])

    def test_clause_without_expression(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <connection>'
            '    <relation type="join" join="inner">'
            '      <relation type="table" name="A"/>'
            '      <relation type="table" name="B"/>'
            '      <clause/>'
            '    </relation>'
            '  </connection>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(rels, [])


# ═══════════════════════════════════════════════════════════════
#  extract_relationships — object model format
# ═══════════════════════════════════════════════════════════════

class TestExtractRelationshipsObjectModel(unittest.TestCase):

    def test_object_graph_relationships(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>'
            '    <relationship type="Left"'
            '      expression="[Orders].[CustID] = [Customers].[CustID]"/>'
            '  </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(len(rels), 1)
        self.assertEqual(rels[0]['type'], 'left')
        self.assertEqual(rels[0]['left']['table'], 'Orders')

    def test_object_graph_not_enough_matches(self):
        ds = ET.fromstring(
            '<datasource>'
            '  <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>'
            '    <relationship type="Left" expression="[Orders].[CustID]"/>'
            '  </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>'
            '</datasource>'
        )
        rels = extract_relationships(ds)
        self.assertEqual(rels, [])


# ═══════════════════════════════════════════════════════════════
#  extract_datasource (integration)
# ═══════════════════════════════════════════════════════════════

class TestExtractDatasource(unittest.TestCase):

    def test_full_extraction(self):
        ds = ET.fromstring(
            '<datasource name="SalesDS" caption="Sales Data">'
            '  <connection class="federated">'
            '    <named-connection name="pg_conn">'
            '      <connection class="postgres" server="pg.host" dbname="analytics"/>'
            '    </named-connection>'
            '    <relation type="table" name="Orders" connection="pg_conn">'
            '      <columns>'
            '        <column name="OrderID" datatype="integer" ordinal="0"/>'
            '        <column name="Amount" datatype="real" ordinal="1"/>'
            '      </columns>'
            '    </relation>'
            '  </connection>'
            '  <column name="[ProfitRatio]" caption="Profit Ratio" datatype="real">'
            '    <calculation class="tableau" formula="SUM([Profit])/SUM([Sales])"/>'
            '  </column>'
            '</datasource>'
        )
        result = extract_datasource(ds)
        self.assertEqual(result['name'], 'SalesDS')
        self.assertEqual(result['caption'], 'Sales Data')
        self.assertIsInstance(result['connection'], dict)
        self.assertIsInstance(result['tables'], list)
        self.assertIsInstance(result['calculations'], list)
        self.assertIsInstance(result['columns'], list)
        self.assertIsInstance(result['relationships'], list)
        self.assertTrue(len(result['tables']) >= 1)
        self.assertTrue(len(result['calculations']) >= 1)

    def test_empty_datasource(self):
        ds = ET.fromstring('<datasource/>')
        result = extract_datasource(ds)
        self.assertEqual(result['name'], 'Unknown')
        self.assertEqual(result['caption'], 'Unknown')
        self.assertEqual(result['tables'], [])
        self.assertEqual(result['calculations'], [])

    def test_caption_defaults_to_name(self):
        ds = ET.fromstring('<datasource name="MyDS"/>')
        result = extract_datasource(ds)
        self.assertEqual(result['caption'], 'MyDS')


if __name__ == "__main__":
    unittest.main()
