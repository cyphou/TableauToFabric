"""
Unit tests for fabric_import.naming — field-name cleaning & sanitisation.

Coverage targets:
  - clean_field_name: derivation prefix/suffix stripping, federated prefix
    removal, combined prefix+suffix, Tableau virtual fields
  - sanitize_table_name, sanitize_column_name, sanitize_query_name,
    sanitize_tmdl_table_name, sanitize_pipeline_name,
    sanitize_filesystem_name, make_python_var
"""

import unittest

from fabric_import.naming import (
    clean_field_name,
    sanitize_table_name,
    sanitize_column_name,
    sanitize_query_name,
    sanitize_tmdl_table_name,
    sanitize_pipeline_name,
    sanitize_filesystem_name,
    make_python_var,
)


# ═══════════════════════════════════════════════════════════════════
#  clean_field_name — derivation prefixes / suffixes
# ═══════════════════════════════════════════════════════════════════

class TestCleanFieldNameDerivation(unittest.TestCase):
    """Derivation prefix and suffix stripping."""

    # — Prefix only —

    def test_sum_prefix(self):
        self.assertEqual(clean_field_name('sum:Sales'), 'Sales')

    def test_none_prefix(self):
        self.assertEqual(clean_field_name('none:Region'), 'Region')

    def test_avg_prefix(self):
        self.assertEqual(clean_field_name('avg:Discount'), 'Discount')

    def test_count_prefix(self):
        self.assertEqual(clean_field_name('count:OrderID'), 'OrderID')

    def test_min_prefix(self):
        self.assertEqual(clean_field_name('min:Price'), 'Price')

    def test_max_prefix(self):
        self.assertEqual(clean_field_name('max:Price'), 'Price')

    def test_yr_prefix(self):
        self.assertEqual(clean_field_name('yr:OrderDate'), 'OrderDate')

    def test_attr_prefix(self):
        self.assertEqual(clean_field_name('attr:City'), 'City')

    def test_trunc_prefix(self):
        self.assertEqual(clean_field_name('trunc:Amount'), 'Amount')

    def test_usr_prefix(self):
        self.assertEqual(clean_field_name('usr:Calc'), 'Calc')

    def test_mn_prefix(self):
        self.assertEqual(clean_field_name('mn:OrderDate'), 'OrderDate')

    def test_dy_prefix(self):
        self.assertEqual(clean_field_name('dy:OrderDate'), 'OrderDate')

    def test_qr_prefix(self):
        self.assertEqual(clean_field_name('qr:OrderDate'), 'OrderDate')

    def test_wk_prefix(self):
        self.assertEqual(clean_field_name('wk:OrderDate'), 'OrderDate')

    def test_md_prefix(self):
        self.assertEqual(clean_field_name('md:OrderDate'), 'OrderDate')

    def test_mdy_prefix(self):
        self.assertEqual(clean_field_name('mdy:OrderDate'), 'OrderDate')

    def test_hms_prefix(self):
        self.assertEqual(clean_field_name('hms:Timestamp'), 'Timestamp')

    def test_hr_prefix(self):
        self.assertEqual(clean_field_name('hr:Timestamp'), 'Timestamp')

    def test_mt_prefix(self):
        self.assertEqual(clean_field_name('mt:Timestamp'), 'Timestamp')

    def test_sc_prefix(self):
        self.assertEqual(clean_field_name('sc:Timestamp'), 'Timestamp')

    def test_thr_prefix(self):
        self.assertEqual(clean_field_name('thr:OrderDate'), 'OrderDate')

    def test_tmn_prefix(self):
        self.assertEqual(clean_field_name('tmn:OrderDate'), 'OrderDate')

    # — Suffix only —

    def test_nk_suffix(self):
        self.assertEqual(clean_field_name('Region:nk'), 'Region')

    def test_qk_suffix(self):
        self.assertEqual(clean_field_name('YR:qk'), 'YR')

    def test_ok_suffix(self):
        self.assertEqual(clean_field_name('Sales:ok'), 'Sales')

    def test_fn_suffix(self):
        self.assertEqual(clean_field_name('Category:fn'), 'Category')

    def test_tn_suffix(self):
        self.assertEqual(clean_field_name('Region:tn'), 'Region')

    # — Both prefix + suffix —

    def test_prefix_and_suffix(self):
        self.assertEqual(clean_field_name('none:YR:qk'), 'YR')

    def test_sum_prefix_and_nk_suffix(self):
        self.assertEqual(clean_field_name('sum:Sales:nk'), 'Sales')

    def test_avg_prefix_and_qk_suffix(self):
        self.assertEqual(clean_field_name('avg:Discount:qk'), 'Discount')

    # — No prefix/suffix —

    def test_no_derivation(self):
        self.assertEqual(clean_field_name('Sales'), 'Sales')

    def test_no_derivation_spaces(self):
        self.assertEqual(clean_field_name('Order Date'), 'Order Date')

    def test_empty_string(self):
        self.assertEqual(clean_field_name(''), '')


# ═══════════════════════════════════════════════════════════════════
#  clean_field_name — federated prefix stripping
# ═══════════════════════════════════════════════════════════════════

class TestCleanFieldNameFederated(unittest.TestCase):
    """Federated datasource path prefix stripping (the root cause bug)."""

    def test_federated_prefix_simple(self):
        """federated.HASH.FieldName → FieldName"""
        self.assertEqual(
            clean_field_name('federated.10vks1203pgcxf1bkw5yk1bwzy2f.YR'),
            'YR',
        )

    def test_federated_prefix_with_derivation(self):
        """federated.HASH.none:YR:qk → YR"""
        self.assertEqual(
            clean_field_name('federated.10vks1203pgcxf1bkw5yk1bwzy2f.none:YR:qk'),
            'YR',
        )

    def test_federated_prefix_with_sum(self):
        """federated.HASH.sum:Sales:nk → Sales"""
        self.assertEqual(
            clean_field_name('federated.abc123def456.sum:Sales:nk'),
            'Sales',
        )

    def test_federated_prefix_with_avg(self):
        """federated.HASH.avg:ASST:qk → ASST"""
        self.assertEqual(
            clean_field_name('federated.10vks1203pgcxf1bkw5yk1bwzy2f.avg:ASST:qk'),
            'ASST',
        )

    def test_federated_prefix_spaces_in_name(self):
        """federated.HASH.Action (Player) → Action (Player)"""
        self.assertEqual(
            clean_field_name('federated.xyz789.Action (Player)'),
            'Action (Player)',
        )

    def test_federated_prefix_no_derivation(self):
        """federated.HASH.City → City"""
        self.assertEqual(
            clean_field_name('federated.abc.City'),
            'City',
        )

    def test_federated_prefix_with_attr(self):
        """federated.HASH.attr:Region → Region"""
        self.assertEqual(
            clean_field_name('federated.abc123.attr:Region'),
            'Region',
        )

    def test_no_federated_prefix_unchanged(self):
        """A name that starts with 'federated' but not the pattern."""
        self.assertEqual(clean_field_name('federated_field'), 'federated_field')

    def test_federated_measure_names(self):
        """federated.HASH.:Measure Names → :Measure Names"""
        result = clean_field_name(
            'federated.10vks1203pgcxf1bkw5yk1bwzy2f.:Measure Names'
        )
        self.assertEqual(result, ':Measure Names')


# ═══════════════════════════════════════════════════════════════════
#  sanitize_table_name
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeTableName(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(sanitize_table_name('Orders'), 'orders')

    def test_schema_prefix(self):
        self.assertEqual(sanitize_table_name('[dbo].[Orders]'), 'orders')

    def test_leading_digits_stripped(self):
        self.assertEqual(sanitize_table_name('123_data'), 'data')

    def test_spaces_replaced(self):
        self.assertEqual(sanitize_table_name('Order Details'), 'order_details')

    def test_empty_fallback(self):
        self.assertEqual(sanitize_table_name(''), 'table')

    def test_special_chars(self):
        result = sanitize_table_name('My$Table!')
        self.assertNotIn('$', result)

    def test_lowercase(self):
        self.assertEqual(sanitize_table_name('MyTable'), 'mytable')


# ═══════════════════════════════════════════════════════════════════
#  sanitize_column_name
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeColumnName(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(sanitize_column_name('Sales'), 'Sales')

    def test_brackets(self):
        self.assertEqual(sanitize_column_name('[Sales]'), 'Sales')

    def test_leading_digits(self):
        result = sanitize_column_name('123Col')
        self.assertFalse(result[0].isdigit())

    def test_empty_fallback(self):
        self.assertEqual(sanitize_column_name(''), 'column')

    def test_special_chars(self):
        result = sanitize_column_name('Sale$Amount')
        self.assertNotIn('$', result)


# ═══════════════════════════════════════════════════════════════════
#  sanitize_query_name (spaces allowed)
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeQueryName(unittest.TestCase):

    def test_spaces_preserved(self):
        self.assertEqual(sanitize_query_name('Order Details'), 'Order Details')

    def test_brackets_removed(self):
        self.assertEqual(sanitize_query_name('[Orders]'), 'Orders')

    def test_empty_fallback(self):
        self.assertEqual(sanitize_query_name(''), 'Query')


# ═══════════════════════════════════════════════════════════════════
#  sanitize_tmdl_table_name
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeTmdlTableName(unittest.TestCase):

    def test_lowercased(self):
        self.assertEqual(sanitize_tmdl_table_name('Orders'), 'orders')

    def test_digit_prefix(self):
        result = sanitize_tmdl_table_name('123Table')
        self.assertTrue(result.startswith('tbl_'))

    def test_empty_fallback(self):
        self.assertEqual(sanitize_tmdl_table_name(''), 'table')


# ═══════════════════════════════════════════════════════════════════
#  sanitize_pipeline_name
# ═══════════════════════════════════════════════════════════════════

class TestSanitizePipelineName(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(sanitize_pipeline_name('LoadData'), 'LoadData')

    def test_special_chars(self):
        result = sanitize_pipeline_name('Load!Data')
        self.assertNotIn('!', result)

    def test_empty_fallback(self):
        self.assertEqual(sanitize_pipeline_name(''), 'activity')


# ═══════════════════════════════════════════════════════════════════
#  sanitize_filesystem_name
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeFilesystemName(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(sanitize_filesystem_name('report.json'), 'report.json')

    def test_angle_brackets(self):
        result = sanitize_filesystem_name('my<report>')
        self.assertNotIn('<', result)
        self.assertNotIn('>', result)

    def test_slashes_replaced(self):
        result = sanitize_filesystem_name('path/to\\file')
        self.assertNotIn('/', result)
        self.assertNotIn('\\', result)

    def test_leading_dots_stripped(self):
        result = sanitize_filesystem_name('..hidden')
        self.assertFalse(result.startswith('.'))


# ═══════════════════════════════════════════════════════════════════
#  make_python_var
# ═══════════════════════════════════════════════════════════════════

class TestMakePythonVar(unittest.TestCase):

    def test_simple(self):
        self.assertEqual(make_python_var('Orders'), 'orders')

    def test_spaces_replaced(self):
        result = make_python_var('Order Details')
        self.assertNotIn(' ', result)

    def test_empty_fallback(self):
        self.assertEqual(make_python_var(''), 'table')

    def test_leading_digit_stripped(self):
        result = make_python_var('123var')
        self.assertFalse(result[0].isdigit())


if __name__ == '__main__':
    unittest.main()
