"""Extra tests for fabric_import.tmdl_generator — targeting uncovered code paths.

Focuses on:
  - _map_semantic_role_to_category (geo-inference by name)
  - _convert_tableau_format_to_pbi (currency, %, numeric)
  - _process_sets_groups_bins (groups with members, combined, bins)
  - _write_measure (multi-line, formatString, displayFolder)
  - _write_column (calculated with expression, isHidden, isKey, dataCategory, sortByColumn)
  - _write_partition (calculated with multi-line, M with expression, M empty)
  - _build_semantic_model (data blending / Unknown table skip / param_map from Parameters DS /
     param values date/string/numeric / extra_objects=None)
  - _build_table_directlake (duplicate column, hidden/description metadata, date/real columns, format override)
  - generate_tmdl with extra_objects=None
"""

import os
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

from fabric_import.tmdl_generator import (
    _build_relationships,
    _build_semantic_model,
    _build_table_directlake,
    _convert_tableau_format_to_pbi,
    _map_semantic_role_to_category,
    _process_sets_groups_bins,
    _write_column,
    _write_measure,
    _write_partition,
    generate_tmdl,
)


# ────────────────────────────────────────────────────────────────────
#  _map_semantic_role_to_category
# ────────────────────────────────────────────────────────────────────

class TestMapSemanticRoleToCategory(unittest.TestCase):

    def test_known_role_latitude(self):
        self.assertEqual(_map_semantic_role_to_category('[Latitude]', ''), 'Latitude')

    def test_known_role_country(self):
        self.assertEqual(_map_semantic_role_to_category('[Country].[Name]', ''), 'Country')

    def test_known_role_state(self):
        self.assertEqual(_map_semantic_role_to_category('[State].[Name]', ''), 'StateOrProvince')

    def test_known_role_city(self):
        self.assertEqual(_map_semantic_role_to_category('[City].[Name]', ''), 'City')

    def test_known_role_zipcode(self):
        self.assertEqual(_map_semantic_role_to_category('[ZipCode].[Name]', ''), 'PostalCode')

    def test_name_latitude(self):
        self.assertEqual(_map_semantic_role_to_category('', 'Store Latitude'), 'Latitude')

    def test_name_lat(self):
        self.assertEqual(_map_semantic_role_to_category('', 'lat'), 'Latitude')

    def test_name_longitude(self):
        self.assertEqual(_map_semantic_role_to_category('', 'Store Longitude'), 'Longitude')

    def test_name_lon(self):
        self.assertEqual(_map_semantic_role_to_category('', 'lon'), 'Longitude')

    def test_name_lng(self):
        self.assertEqual(_map_semantic_role_to_category('', 'lng'), 'Longitude')

    def test_name_city(self):
        self.assertEqual(_map_semantic_role_to_category('', 'city'), 'City')

    def test_name_ville(self):
        self.assertEqual(_map_semantic_role_to_category('', 'ville'), 'City')

    def test_name_country(self):
        self.assertEqual(_map_semantic_role_to_category('', 'country'), 'Country')

    def test_name_pays(self):
        self.assertEqual(_map_semantic_role_to_category('', 'pays'), 'Country')

    def test_name_region(self):
        self.assertEqual(_map_semantic_role_to_category('', 'Sales Region'), 'StateOrProvince')

    def test_name_state(self):
        self.assertEqual(_map_semantic_role_to_category('', 'state'), 'StateOrProvince')

    def test_name_province(self):
        self.assertEqual(_map_semantic_role_to_category('', 'province'), 'StateOrProvince')

    def test_name_postal(self):
        self.assertEqual(_map_semantic_role_to_category('', 'postal_code'), 'PostalCode')

    def test_name_zip(self):
        self.assertEqual(_map_semantic_role_to_category('', 'zip'), 'PostalCode')

    def test_none_unrecognised(self):
        self.assertIsNone(_map_semantic_role_to_category('', 'revenue'))


# ────────────────────────────────────────────────────────────────────
#  _convert_tableau_format_to_pbi
# ────────────────────────────────────────────────────────────────────

class TestConvertTableauFormatToPbi(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(_convert_tableau_format_to_pbi(''), '')
        self.assertEqual(_convert_tableau_format_to_pbi(None), '')

    def test_already_pbi_format(self):
        self.assertEqual(_convert_tableau_format_to_pbi('#,0'), '#,0')
        self.assertEqual(_convert_tableau_format_to_pbi('0'), '0')

    def test_percentage(self):
        self.assertEqual(_convert_tableau_format_to_pbi('0.0%'), '0.0%')

    def test_dollar_currency(self):
        result = _convert_tableau_format_to_pbi('$#,##0.00')
        self.assertIn('$', result)
        self.assertNotIn('##0', result)

    def test_euro_currency(self):
        result = _convert_tableau_format_to_pbi('\u20ac#,##0')
        self.assertIn('\u20ac', result)

    def test_pound_currency(self):
        result = _convert_tableau_format_to_pbi('\u00a3#,##0.00')
        self.assertIn('\u00a3', result)

    def test_yen_currency(self):
        result = _convert_tableau_format_to_pbi('\u00a5#,###')
        self.assertIn('\u00a5', result)

    def test_numeric_hash_format(self):
        result = _convert_tableau_format_to_pbi('#,##0.00')
        self.assertEqual(result, '#,0.00')

    def test_numeric_zero_format(self):
        result = _convert_tableau_format_to_pbi('0.000')
        self.assertEqual(result, '0.000')


# ────────────────────────────────────────────────────────────────────
#  _write_measure
# ────────────────────────────────────────────────────────────────────

class TestWriteMeasure(unittest.TestCase):

    def test_single_line_measure(self):
        lines = []
        m = {'name': 'Total', 'expression': 'SUM(A)', 'formatString': '#,0.00',
             'displayFolder': 'Measures'}
        _write_measure(lines, m)
        joined = '\n'.join(lines)
        self.assertIn("measure Total = SUM(A)", joined)
        self.assertIn('formatString: #,0.00', joined)
        self.assertIn('displayFolder: Measures', joined)

    def test_multi_line_measure(self):
        lines = []
        m = {'name': 'Complex', 'expression': 'VAR x = 1\nRETURN x'}
        _write_measure(lines, m)
        joined = '\n'.join(lines)
        self.assertIn('```', joined)
        self.assertIn('VAR x = 1', joined)
        self.assertIn('RETURN x', joined)

    def test_default_format_not_emitted(self):
        lines = []
        m = {'name': 'Simple', 'expression': '42', 'formatString': '0'}
        _write_measure(lines, m)
        joined = '\n'.join(lines)
        self.assertNotIn('formatString', joined)


# ────────────────────────────────────────────────────────────────────
#  _write_column
# ────────────────────────────────────────────────────────────────────

class TestWriteColumn(unittest.TestCase):

    def test_physical_column(self):
        lines = []
        col = {'name': 'OrderID', 'dataType': 'Int64', 'sourceColumn': 'OrderID',
               'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn("column OrderID", joined)
        self.assertIn('sourceColumn: OrderID', joined)

    def test_calculated_column_single_line(self):
        lines = []
        col = {'name': 'FullName', 'dataType': 'String', 'isCalculated': True,
               'expression': '[First] & " " & [Last]', 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn("column FullName = [First]", joined)

    def test_calculated_column_multi_line(self):
        lines = []
        col = {'name': 'Status', 'dataType': 'String', 'isCalculated': True,
               'expression': 'IF(\n  [A]=1,\n  "X",\n  "Y"\n)', 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('```', joined)

    def test_hidden_column(self):
        lines = []
        col = {'name': 'Hidden', 'dataType': 'String', 'sourceColumn': 'Hidden',
               'isHidden': True, 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('isHidden', joined)

    def test_key_column(self):
        lines = []
        col = {'name': 'PK', 'dataType': 'Int64', 'sourceColumn': 'PK',
               'isKey': True, 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('isKey', joined)

    def test_data_category_column(self):
        lines = []
        col = {'name': 'City', 'dataType': 'String', 'sourceColumn': 'City',
               'dataCategory': 'City', 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('dataCategory: City', joined)

    def test_description_column(self):
        lines = []
        col = {'name': 'Desc', 'dataType': 'String', 'sourceColumn': 'Desc',
               'description': 'A description', 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('description: A description', joined)

    def test_sort_by_column(self):
        lines = []
        col = {'name': 'Month', 'dataType': 'String', 'sourceColumn': 'Month',
               'sortByColumn': 'MonthNum', 'summarizeBy': 'none'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('sortByColumn:', joined)

    def test_format_string_emitted(self):
        lines = []
        col = {'name': 'Amt', 'dataType': 'Double', 'sourceColumn': 'Amt',
               'formatString': '#,0.00', 'summarizeBy': 'sum'}
        _write_column(lines, col)
        joined = '\n'.join(lines)
        self.assertIn('formatString: #,0.00', joined)


# ────────────────────────────────────────────────────────────────────
#  _write_partition
# ────────────────────────────────────────────────────────────────────

class TestWritePartition(unittest.TestCase):

    def test_entity_partition(self):
        lines = []
        p = {'mode': 'directLake', 'source': {
            'type': 'entity', 'entityName': 'Orders', 'schemaName': 'dbo',
            'expressionSource': 'DatabaseQuery'}}
        _write_partition(lines, 'Orders', p)
        joined = '\n'.join(lines)
        self.assertIn('entity', joined)
        self.assertIn('entityName: Orders', joined)

    def test_calculated_partition_single_line(self):
        lines = []
        p = {'mode': 'import', 'source': {
            'type': 'calculated', 'expression': '{("A", 1), ("B", 2)}'}}
        _write_partition(lines, 'Params', p)
        joined = '\n'.join(lines)
        self.assertIn('calculated', joined)
        self.assertIn('source = {("A"', joined)

    def test_calculated_partition_multi_line(self):
        lines = []
        p = {'mode': 'import', 'source': {
            'type': 'calculated', 'expression': '{"A", 1},\n{"B", 2}'}}
        _write_partition(lines, 'Params', p)
        joined = '\n'.join(lines)
        self.assertIn('```', joined)

    def test_m_partition_with_expression(self):
        lines = []
        p = {'mode': 'import', 'source': {
            'type': 'm', 'expression': 'let\n  Source = null\nin\n  Source'}}
        _write_partition(lines, 'Calendar', p)
        joined = '\n'.join(lines)
        self.assertIn('let', joined)
        self.assertIn('Source = null', joined)

    def test_m_partition_empty_expression(self):
        lines = []
        p = {'mode': 'import', 'source': {'type': 'm', 'expression': ''}}
        _write_partition(lines, 'Empty', p)
        joined = '\n'.join(lines)
        self.assertIn('TODO: Configure data source', joined)


# ────────────────────────────────────────────────────────────────────
#  _build_relationships (Format B)
# ────────────────────────────────────────────────────────────────────

class TestBuildRelationshipsFormatB(unittest.TestCase):

    def test_format_b_join_xml(self):
        rels = [{'left': {'table': 'Orders', 'column': 'ProductID'},
                 'right': {'table': 'Products', 'column': 'ProductID'},
                 'joinType': 'inner'}]
        result = _build_relationships(rels)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['fromTable'], 'Orders')
        self.assertEqual(result[0]['toTable'], 'Products')

    def test_empty_keys_skipped(self):
        rels = [{'from_table': '', 'from_column': 'X', 'to_table': 'T', 'to_column': 'Y'}]
        result = _build_relationships(rels)
        self.assertEqual(len(result), 0)


# ────────────────────────────────────────────────────────────────────
#  _process_sets_groups_bins
# ────────────────────────────────────────────────────────────────────

class TestProcessSetsGroupsBins(unittest.TestCase):

    def _make_model(self):
        return {
            "model": {
                "tables": [
                    {
                        "name": "Orders",
                        "columns": [
                            {"name": "Status", "dataType": "String"},
                            {"name": "Amount", "dataType": "Double"},
                        ],
                        "measures": [],
                    }
                ],
                "relationships": [],
            }
        }

    def test_groups_with_members(self):
        model = self._make_model()
        extra = {
            'groups': [{
                'name': 'StatusGroup',
                'group_type': 'values',
                'source_field': 'Status',
                'members': {'Active': ['Open', 'In Progress'], 'Done': ['Closed']},
            }],
            'sets': [], 'bins': [],
        }
        _process_sets_groups_bins(model, extra, 'Orders', {'Status': 'Orders'})
        cols = model["model"]["tables"][0]["columns"]
        group_col = next(c for c in cols if c["name"] == "StatusGroup")
        self.assertIn("SWITCH", group_col["expression"])

    def test_groups_combined(self):
        model = self._make_model()
        extra = {
            'groups': [{
                'name': 'CombinedGroup',
                'group_type': 'combined',
                'source_fields': ['Status', 'Amount'],
            }],
            'sets': [], 'bins': [], '_datasources': [],
        }
        _process_sets_groups_bins(model, extra, 'Orders',
                                  {'Status': 'Orders', 'Amount': 'Orders'})
        cols = model["model"]["tables"][0]["columns"]
        group_col = next(c for c in cols if c["name"] == "CombinedGroup")
        self.assertIn("&", group_col["expression"])

    def test_bins(self):
        model = self._make_model()
        extra = {
            'bins': [{'name': 'Amount Bin', 'source_field': 'Amount', 'size': '25'}],
            'sets': [], 'groups': [],
        }
        _process_sets_groups_bins(model, extra, 'Orders', {'Amount': 'Orders'})
        cols = model["model"]["tables"][0]["columns"]
        bin_col = next(c for c in cols if c["name"] == "Amount Bin")
        self.assertIn("FLOOR", bin_col["expression"])
        self.assertIn("25", bin_col["expression"])


# ────────────────────────────────────────────────────────────────────
#  _build_semantic_model (high-level integration paths)
# ────────────────────────────────────────────────────────────────────

class TestBuildSemanticModel(unittest.TestCase):

    def _sample_ds(self, **overrides):
        ds = {
            'name': 'Sales',
            'connection': {'type': 'SQL Server', 'details': {}},
            'connection_map': {},
            'tables': [
                {
                    'name': 'Orders',
                    'columns': [
                        {'name': 'OrderID', 'datatype': 'integer'},
                        {'name': 'Amount', 'datatype': 'real'},
                    ],
                }
            ],
            'calculations': [],
            'columns': [],
            'relationships': [],
        }
        ds.update(overrides)
        return ds

    def test_unknown_table_skipped(self):
        ds = self._sample_ds()
        ds['tables'].append({'name': 'Unknown', 'columns': [{'name': 'X', 'datatype': 'string'}]})
        model = _build_semantic_model([ds], 'Test')
        table_names = [t['name'] for t in model['model']['tables']]
        self.assertNotIn('Unknown', table_names)
        self.assertIn('Orders', table_names)

    def test_extra_objects_none(self):
        ds = self._sample_ds()
        model = _build_semantic_model([ds], 'Test', extra_objects=None)
        self.assertGreaterEqual(len(model['model']['tables']), 1)

    def test_data_blending_relationships(self):
        ds1 = self._sample_ds()
        ds2 = self._sample_ds(
            name='Products',
            tables=[{'name': 'Products', 'columns': [
                {'name': 'ProductID', 'datatype': 'integer'},
                {'name': 'Name', 'datatype': 'string'},
            ]}],
        )
        extra = {
            'data_blending': [{
                'primary_datasource': 'Orders',
                'secondary_datasource': 'Products',
                'link_fields': [{'primary_field': 'OrderID', 'secondary_field': 'ProductID'}],
            }],
        }
        model = _build_semantic_model([ds1, ds2], 'Test', extra_objects=extra)
        rels = model['model']['relationships']
        blend_rels = [r for r in rels if r.get('name', '').startswith('Blend')]
        self.assertGreaterEqual(len(blend_rels), 1)

    def test_parameters_datasource_creates_param_map(self):
        ds = self._sample_ds()
        param_ds = {
            'name': 'Parameters',
            'connection': {'type': 'None', 'details': {}},
            'connection_map': {},
            'tables': [],
            'calculations': [
                {'name': '[TopN]', 'caption': 'Top N', 'formula': '10',
                 'datatype': 'integer', 'role': 'measure'},
            ],
            'columns': [],
            'relationships': [],
        }
        model = _build_semantic_model([ds, param_ds], 'Test')
        # Should not crash, and the TopN param should be accessible as measure
        self.assertIsNotNone(model)

    def test_param_values_date_format(self):
        ds = self._sample_ds()
        extra = {
            'parameters': [
                {'name': '[Parameters].[Start Date]', 'caption': 'Start Date',
                 'value': '#2024-01-15#', 'datatype': 'date'},
            ],
        }
        model = _build_semantic_model([ds], 'Test', extra_objects=extra)
        self.assertIsNotNone(model)

    def test_param_values_string(self):
        ds = self._sample_ds()
        extra = {
            'parameters': [
                {'name': '[Parameters].[Region]', 'caption': 'Region',
                 'value': 'West', 'datatype': 'string'},
            ],
        }
        model = _build_semantic_model([ds], 'Test', extra_objects=extra)
        self.assertIsNotNone(model)

    def test_column_metadata_caption_mapping(self):
        ds = self._sample_ds()
        ds['columns'] = [
            {'name': 'OrderID', 'caption': 'Order #', 'hidden': True, 'description': 'Unique ID'},
        ]
        model = _build_semantic_model([ds], 'Test')
        self.assertIsNotNone(model)

    def test_culture_override(self):
        ds = self._sample_ds()
        model = _build_semantic_model([ds], 'Test', culture='fr-FR')
        self.assertEqual(model['model']['culture'], 'fr-FR')


# ────────────────────────────────────────────────────────────────────
#  _build_table_directlake (edge cases)
# ────────────────────────────────────────────────────────────────────

class TestBuildTableDirectlake(unittest.TestCase):

    def test_duplicate_column_names(self):
        table = {
            'name': 'T1',
            'columns': [
                {'name': 'Col', 'datatype': 'string'},
                {'name': 'Col', 'datatype': 'integer'},
            ],
        }
        tbl = _build_table_directlake(table, [])
        col_names = [c['name'] for c in tbl['columns']]
        self.assertEqual(len(set(col_names)), 2)
        self.assertIn('Col_1', col_names)

    def test_hidden_and_description_metadata(self):
        table = {
            'name': 'T1',
            'columns': [{'name': 'Secret', 'datatype': 'string'}],
        }
        meta = {'Secret': {'hidden': True, 'description': 'Hidden column'}}
        tbl = _build_table_directlake(table, [], col_metadata_map=meta)
        col = tbl['columns'][0]
        self.assertTrue(col.get('isHidden'))
        self.assertEqual(col.get('description'), 'Hidden column')

    def test_date_column_gets_category(self):
        table = {
            'name': 'T1',
            'columns': [{'name': 'OrderDate', 'datatype': 'date'}],
        }
        tbl = _build_table_directlake(table, [])
        col = tbl['columns'][0]
        self.assertEqual(col.get('dataCategory'), 'DateTime')

    def test_real_column_format_and_summarize(self):
        table = {
            'name': 'T1',
            'columns': [{'name': 'amount', 'datatype': 'real'}],
        }
        tbl = _build_table_directlake(table, [])
        col = tbl['columns'][0]
        self.assertEqual(col['summarizeBy'], 'sum')
        self.assertEqual(col['formatString'], '#,0.00')

    def test_format_override_from_metadata(self):
        table = {
            'name': 'T1',
            'columns': [{'name': 'Price', 'datatype': 'real'}],
        }
        meta = {'Price': {'default_format': '$#,##0.00'}}
        tbl = _build_table_directlake(table, [], col_metadata_map=meta)
        col = tbl['columns'][0]
        self.assertIn('$', col['formatString'])

    def test_none_args_default_to_empty(self):
        table = {'name': 'T1', 'columns': [{'name': 'X', 'datatype': 'string'}]}
        tbl = _build_table_directlake(table, [], dax_context=None,
                                       col_metadata_map=None, extra_objects=None)
        self.assertEqual(len(tbl['columns']), 1)

    def test_security_function_forces_measure(self):
        """Calculations with USERPRINCIPALNAME should never become calc columns."""
        table = {
            'name': 'T1',
            'columns': [{'name': 'Region', 'datatype': 'string'}],
        }
        calcs = [{
            'name': 'UserRegion',
            'caption': 'UserRegion',
            'formula': 'IF [Region] = USERNAME() THEN "Y" ELSE "N" END',
            'datatype': 'string',
            'role': 'dimension',
        }]
        tbl = _build_table_directlake(table, calcs)
        # Should be a measure, not a calculated column
        col_names = [c['name'] for c in tbl['columns']]
        measure_names = [m['name'] for m in tbl['measures']]
        self.assertIn('UserRegion', measure_names)
        self.assertNotIn('UserRegion', col_names)


# ────────────────────────────────────────────────────────────────────
#  generate_tmdl (integration)
# ────────────────────────────────────────────────────────────────────

class TestGenerateTmdl(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ttf_tmdl_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_with_none_extra_objects(self):
        ds = {
            'name': 'Sales',
            'connection': {'type': 'SQL Server', 'details': {}},
            'connection_map': {},
            'tables': [{'name': 'Orders', 'columns': [
                {'name': 'Amt', 'datatype': 'real'},
            ]}],
            'calculations': [],
            'columns': [],
            'relationships': [],
        }
        stats = generate_tmdl([ds], 'Test', None, self.tmpdir)
        self.assertIn('tables', stats)
        self.assertGreaterEqual(stats['tables'], 1)
        # model.tmdl should exist
        model_path = os.path.join(self.tmpdir, 'definition', 'model.tmdl')
        self.assertTrue(os.path.isfile(model_path))


if __name__ == "__main__":
    unittest.main()
