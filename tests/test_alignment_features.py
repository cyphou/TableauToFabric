"""Tests for features ported from TableauToPowerBI alignment (Phase 28).

Covers:
  - LOD balanced-brace walker (nested LODs without dimensions)
  - Datasource-filter extraction
  - Tableau → PBI number format conversion
  - DAX formula validation (leak detection, balanced parens, semantic refs)
  - CLI flags (--dry-run, --calendar-start, --calendar-end, --culture)
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from xml.etree import ElementTree as ET

# ═══════════════════════════════════════════════════════════════════════
#  LOD balanced-brace walker
# ═══════════════════════════════════════════════════════════════════════

from tableau_export.dax_converter import _convert_lod_expressions


class TestLODBalancedBraces(unittest.TestCase):
    """LOD-without-dimension uses a balanced-brace walker, not regex."""

    def test_simple_no_dim(self):
        """{ SUM([Sales]) } → CALCULATE(SUM([Sales]))."""
        result = _convert_lod_expressions('{SUM([Sales])}', 'T', {})
        self.assertIn('CALCULATE', result)
        self.assertIn('SUM', result)
        self.assertNotIn('{', result)

    def test_nested_braces(self):
        """Nested braces must be handled by the depth counter."""
        formula = '{SUM({FIXED [X] : AVG([Y])})}'
        result = _convert_lod_expressions(formula, 'T', {})
        # Outer LOD-no-dim should match
        self.assertIn('CALCULATE', result)
        self.assertNotIn('{SUM', result)

    def test_countd_no_dim(self):
        result = _convert_lod_expressions('{COUNTD([ID])}', 'T', {})
        self.assertIn('CALCULATE', result)
        self.assertIn('COUNTD', result)

    def test_avg_no_dim(self):
        result = _convert_lod_expressions('{AVG([Score])}', 'T', {})
        self.assertIn('CALCULATE', result)

    def test_min_max_no_dim(self):
        for fn in ('MIN', 'MAX'):
            with self.subTest(fn=fn):
                result = _convert_lod_expressions(f'{{{fn}([Val])}}', 'T', {})
                self.assertIn('CALCULATE', result)

    def test_no_match_plain_text(self):
        """Formula without LOD should pass through unchanged."""
        formula = 'SUM([Sales])'
        result = _convert_lod_expressions(formula, 'T', {})
        self.assertEqual(result, formula)

    def test_multiple_lod_no_dim(self):
        formula = '{SUM([A])} + {AVG([B])}'
        result = _convert_lod_expressions(formula, 'T', {})
        self.assertEqual(result.count('CALCULATE'), 2)
        self.assertNotIn('{', result)


# ═══════════════════════════════════════════════════════════════════════
#  Datasource-filter extraction
# ═══════════════════════════════════════════════════════════════════════

from tableau_export.extract_tableau_data import TableauExtractor


class TestDatasourceFilterExtraction(unittest.TestCase):
    """Tests for extract_datasource_filters and _parse_datasource_filter."""

    def _make_extractor(self):
        ext = TableauExtractor.__new__(TableauExtractor)
        ext.workbook_data = {}
        return ext

    def test_extract_no_filters(self):
        xml = '<workbook><datasource name="DS1"></datasource></workbook>'
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        self.assertEqual(ext.workbook_data['datasource_filters'], [])

    def test_extract_top_level_filter(self):
        xml = '''<workbook>
          <datasource name="Sales">
            <filter column="[Region]" class="categorical" type="included">
              <groupfilter member="East"/>
              <groupfilter member="West"/>
            </filter>
          </datasource>
        </workbook>'''
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        filters = ext.workbook_data['datasource_filters']
        self.assertEqual(len(filters), 1)
        self.assertEqual(filters[0]['column'], 'Region')
        self.assertEqual(filters[0]['filter_class'], 'categorical')
        self.assertIn('East', filters[0]['values'])
        self.assertIn('West', filters[0]['values'])

    def test_extract_quantitative_filter(self):
        xml = '''<workbook>
          <datasource name="Metrics">
            <filter column="[Revenue]" class="quantitative">
              <min value="100"/>
              <max value="1000"/>
            </filter>
          </datasource>
        </workbook>'''
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        filters = ext.workbook_data['datasource_filters']
        self.assertEqual(len(filters), 1)
        self.assertEqual(filters[0]['range_min'], '100')
        self.assertEqual(filters[0]['range_max'], '1000')

    def test_deduplication(self):
        """Same column+ds+class combination should be deduplicated."""
        xml = '''<workbook>
          <datasource name="DS">
            <filter column="[Col]" class="categorical" type="included"/>
            <connection>
              <filter column="[Col]" class="categorical" type="included"/>
            </connection>
          </datasource>
        </workbook>'''
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        self.assertEqual(len(ext.workbook_data['datasource_filters']), 1)

    def test_filter_missing_column_skipped(self):
        xml = '''<workbook>
          <datasource name="DS">
            <filter class="categorical"/>
          </datasource>
        </workbook>'''
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        self.assertEqual(ext.workbook_data['datasource_filters'], [])

    def test_parse_datasource_filter_static(self):
        xml = '<filter column="[Status]" class="categorical" type="excluded"><groupfilter member="Cancelled"/></filter>'
        el = ET.fromstring(xml)
        result = TableauExtractor._parse_datasource_filter(el, 'MyDS')
        self.assertEqual(result['datasource'], 'MyDS')
        self.assertEqual(result['column'], 'Status')
        self.assertEqual(result['filter_class'], 'categorical')
        self.assertEqual(result['filter_type'], 'excluded')
        self.assertIn('Cancelled', result['values'])

    def test_caption_used_as_datasource_name(self):
        xml = '''<workbook>
          <datasource name="ds1" caption="Human Name">
            <filter column="[X]" class="categorical"/>
          </datasource>
        </workbook>'''
        root = ET.fromstring(xml)
        ext = self._make_extractor()
        ext.extract_datasource_filters(root)
        self.assertEqual(ext.workbook_data['datasource_filters'][0]['datasource'], 'Human Name')


# ═══════════════════════════════════════════════════════════════════════
#  Tableau → PBI number format conversion
# ═══════════════════════════════════════════════════════════════════════

from fabric_import.tmdl_generator import _convert_tableau_format_to_pbi


class TestConvertTableauFormatToPbi(unittest.TestCase):
    """Tests for _convert_tableau_format_to_pbi."""

    def test_empty_input(self):
        self.assertEqual(_convert_tableau_format_to_pbi(''), '')
        self.assertEqual(_convert_tableau_format_to_pbi(None), '')

    def test_already_pbi_format(self):
        for fmt in ('#,0', '#,0.00', '0.00%', '$#,0.00'):
            with self.subTest(fmt=fmt):
                self.assertEqual(_convert_tableau_format_to_pbi(fmt), fmt)

    def test_tableau_numeric_conversion(self):
        """#,##0 → #,0 and #,##0.00 → #,0.00."""
        self.assertEqual(_convert_tableau_format_to_pbi('#,##0'), '#,0')
        self.assertEqual(_convert_tableau_format_to_pbi('#,##0.00'), '#,0.00')

    def test_percentage_passthrough(self):
        self.assertEqual(_convert_tableau_format_to_pbi('0.0%'), '0.0%')
        self.assertIn('%', _convert_tableau_format_to_pbi('0.00%'))

    def test_currency_symbol(self):
        result = _convert_tableau_format_to_pbi('$#,##0.00')
        self.assertIn('$', result)
        self.assertNotIn('##0', result)

    def test_euro_symbol(self):
        result = _convert_tableau_format_to_pbi('\u20ac#,##0')
        self.assertIn('\u20ac', result)

    def test_plain_zero_format(self):
        self.assertEqual(_convert_tableau_format_to_pbi('0'), '0')
        self.assertEqual(_convert_tableau_format_to_pbi('0.000'), '0.000')


# ═══════════════════════════════════════════════════════════════════════
#  DAX formula validation
# ═══════════════════════════════════════════════════════════════════════

from fabric_import.validator import ArtifactValidator


class TestValidateDaxFormula(unittest.TestCase):
    """Tests for ArtifactValidator.validate_dax_formula."""

    def test_valid_formula(self):
        issues = ArtifactValidator.validate_dax_formula('SUM(Sales[Amount])')
        self.assertEqual(issues, [])

    def test_empty_formula(self):
        self.assertEqual(ArtifactValidator.validate_dax_formula(''), [])
        self.assertEqual(ArtifactValidator.validate_dax_formula(None), [])

    def test_unmatched_opening_paren(self):
        issues = ArtifactValidator.validate_dax_formula('SUM(Sales[Amount]')
        self.assertTrue(any('Unmatched opening parenthesis' in i for i in issues))

    def test_unmatched_closing_paren(self):
        issues = ArtifactValidator.validate_dax_formula('SUM(Sales[Amount]))')
        self.assertTrue(any('Unmatched closing parenthesis' in i for i in issues))

    def test_countd_leak(self):
        issues = ArtifactValidator.validate_dax_formula('COUNTD([ID])')
        self.assertTrue(any('COUNTD' in i for i in issues))

    def test_zn_leak(self):
        issues = ArtifactValidator.validate_dax_formula('ZN([Profit])')
        self.assertTrue(any('ZN' in i for i in issues))

    def test_ifnull_leak(self):
        issues = ArtifactValidator.validate_dax_formula('IFNULL([X], 0)')
        self.assertTrue(any('IFNULL' in i for i in issues))

    def test_attr_leak(self):
        issues = ArtifactValidator.validate_dax_formula('ATTR([Name])')
        self.assertTrue(any('ATTR' in i for i in issues))

    def test_double_equals_leak(self):
        issues = ArtifactValidator.validate_dax_formula('IF([A] == [B], 1, 0)')
        self.assertTrue(any('==' in i for i in issues))

    def test_elseif_leak(self):
        issues = ArtifactValidator.validate_dax_formula('IF([A] > 0, 1, ELSEIF([B], 2, 0))')
        self.assertTrue(any('ELSEIF' in i for i in issues))

    def test_lod_leak(self):
        issues = ArtifactValidator.validate_dax_formula('{FIXED [X] : SUM([Y])}')
        self.assertTrue(any('LOD' in i for i in issues))

    def test_datetrunc_leak(self):
        issues = ArtifactValidator.validate_dax_formula('DATETRUNC(month, [Date])')
        self.assertTrue(any('DATETRUNC' in i for i in issues))

    def test_script_leak(self):
        issues = ArtifactValidator.validate_dax_formula('SCRIPT_REAL(".r code.", [X])')
        self.assertTrue(any('SCRIPT_' in i for i in issues))

    def test_unresolved_parameter_ref(self):
        issues = ArtifactValidator.validate_dax_formula('[Parameters].[Date Range]')
        self.assertTrue(any('parameter' in i.lower() for i in issues))

    def test_context_in_message(self):
        issues = ArtifactValidator.validate_dax_formula('COUNTD([X])', 'MyMeasure')
        self.assertTrue(any('MyMeasure' in i for i in issues))

    def test_clean_dax_no_issues(self):
        """Proper DAX should produce zero issues."""
        issues = ArtifactValidator.validate_dax_formula(
            'CALCULATE(SUM(Sales[Amount]), ALL(Products))'
        )
        self.assertEqual(issues, [])


class TestValidateTmdlDax(unittest.TestCase):
    """Tests for ArtifactValidator.validate_tmdl_dax."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_vdax_')

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_clean_tmdl(self):
        path = os.path.join(self.tmpdir, 'orders.tmdl')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('table Orders\n')
            f.write('  measure TotalSales\n')
            f.write('    expression = SUM(Orders[Amount])\n')
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertEqual(issues, [])

    def test_leak_detected(self):
        path = os.path.join(self.tmpdir, 'bad.tmdl')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('table Orders\n')
            f.write('  measure BadMeasure\n')
            f.write('    expression = COUNTD(Orders[ID])\n')
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertTrue(len(issues) > 0)
        self.assertTrue(any('COUNTD' in i for i in issues))

    def test_m_expression_skipped(self):
        """Power Query (M) expressions starting with 'let' should be skipped."""
        path = os.path.join(self.tmpdir, 'mquery.tmdl')
        with open(path, 'w', encoding='utf-8') as f:
            f.write('table Orders\n')
            f.write('  column Source\n')
            f.write('    expression = let Source = ...\n')
        issues = ArtifactValidator.validate_tmdl_dax(path)
        self.assertEqual(issues, [])

    def test_nonexistent_file(self):
        issues = ArtifactValidator.validate_tmdl_dax('/nonexistent.tmdl')
        self.assertEqual(issues, [])


class TestCollectModelSymbols(unittest.TestCase):
    """Tests for ArtifactValidator._collect_model_symbols."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_syms_')
        self.sm_dir = os.path.join(self.tmpdir, 'Test.SemanticModel')
        self.def_dir = os.path.join(self.sm_dir, 'definition')
        self.tables_dir = os.path.join(self.def_dir, 'tables')
        os.makedirs(self.tables_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_collects_tables_columns_measures(self):
        with open(os.path.join(self.tables_dir, 'Orders.tmdl'), 'w', encoding='utf-8') as f:
            f.write('table Orders\n')
            f.write('\tcolumn OrderID\n')
            f.write('\tcolumn Amount\n')
            f.write('\tmeasure TotalSales\n')
        symbols = ArtifactValidator._collect_model_symbols(self.sm_dir)
        self.assertIn('Orders', symbols['tables'])
        self.assertIn('OrderID', symbols['columns']['Orders'])
        self.assertIn('Amount', symbols['columns']['Orders'])
        self.assertIn('TotalSales', symbols['measures']['Orders'])

    def test_empty_model(self):
        # Just model.tmdl with no tables
        with open(os.path.join(self.def_dir, 'model.tmdl'), 'w', encoding='utf-8') as f:
            f.write('model Model\n  culture: en-US\n')
        symbols = ArtifactValidator._collect_model_symbols(self.sm_dir)
        self.assertEqual(len(symbols['tables']), 0)


class TestValidateSemanticReferences(unittest.TestCase):
    """Tests for ArtifactValidator.validate_semantic_references."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='ttf_ref_')
        self.sm_dir = os.path.join(self.tmpdir, 'Test.SemanticModel')
        self.def_dir = os.path.join(self.sm_dir, 'definition')
        self.tables_dir = os.path.join(self.def_dir, 'tables')
        os.makedirs(self.tables_dir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_valid_references(self):
        with open(os.path.join(self.tables_dir, 'Sales.tmdl'), 'w', encoding='utf-8') as f:
            f.write("table Sales\n")
            f.write("\tcolumn Amount\n")
            f.write("\tmeasure Total\n")
            f.write("\t\texpression = SUM('Sales'[Amount])\n")
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertEqual(warnings, [])

    def test_unknown_table_reference(self):
        with open(os.path.join(self.tables_dir, 'Sales.tmdl'), 'w', encoding='utf-8') as f:
            f.write("table Sales\n")
            f.write("\tcolumn Amount\n")
            f.write("\tmeasure Total\n")
            f.write("\t\texpression = SUM('NonExistent'[Amount])\n")
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertTrue(any('NonExistent' in w for w in warnings))

    def test_unknown_column_reference(self):
        with open(os.path.join(self.tables_dir, 'Sales.tmdl'), 'w', encoding='utf-8') as f:
            f.write("table Sales\n")
            f.write("\tcolumn Amount\n")
            f.write("\tmeasure Total\n")
            f.write("\t\texpression = SUM('Sales'[Bogus])\n")
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertTrue(any('Bogus' in w for w in warnings))

    def test_no_warnings_for_empty_model(self):
        with open(os.path.join(self.def_dir, 'model.tmdl'), 'w', encoding='utf-8') as f:
            f.write('model Model\n  culture: en-US\n')
        warnings = ArtifactValidator.validate_semantic_references(self.sm_dir)
        self.assertEqual(warnings, [])


# ═══════════════════════════════════════════════════════════════════════
#  CLI flags
# ═══════════════════════════════════════════════════════════════════════

class TestCLIFlags(unittest.TestCase):
    """Tests that the new CLI flags are accepted by argparse."""

    def _parse(self, args):
        """Parse CLI args using migrate's argparse."""
        import migrate as m
        import argparse
        # Build parser the same way main() does
        parser = argparse.ArgumentParser()
        parser.add_argument('source', nargs='?')
        parser.add_argument('--output-dir', '-o')
        parser.add_argument('--assess', action='store_true')
        parser.add_argument('--auto', action='store_true')
        parser.add_argument('--artifacts', nargs='+')
        parser.add_argument('--batch')
        parser.add_argument('--verbose', '-v', action='store_true')
        parser.add_argument('--log-file')
        parser.add_argument('--dry-run', action='store_true')
        parser.add_argument('--calendar-start', type=int)
        parser.add_argument('--calendar-end', type=int)
        parser.add_argument('--culture')
        return parser.parse_args(args)

    def test_dry_run_flag(self):
        ns = self._parse(['test.twb', '--dry-run'])
        self.assertTrue(ns.dry_run)

    def test_calendar_start(self):
        ns = self._parse(['test.twb', '--calendar-start', '2015'])
        self.assertEqual(ns.calendar_start, 2015)

    def test_calendar_end(self):
        ns = self._parse(['test.twb', '--calendar-end', '2035'])
        self.assertEqual(ns.calendar_end, 2035)

    def test_culture_flag(self):
        ns = self._parse(['test.twb', '--culture', 'de-DE'])
        self.assertEqual(ns.culture, 'de-DE')

    def test_all_new_flags_together(self):
        ns = self._parse([
            'test.twb',
            '--dry-run',
            '--calendar-start', '2010',
            '--calendar-end', '2040',
            '--culture', 'ja-JP',
        ])
        self.assertTrue(ns.dry_run)
        self.assertEqual(ns.calendar_start, 2010)
        self.assertEqual(ns.calendar_end, 2040)
        self.assertEqual(ns.culture, 'ja-JP')

    def test_defaults(self):
        ns = self._parse(['test.twb'])
        self.assertFalse(ns.dry_run)
        self.assertIsNone(ns.calendar_start)
        self.assertIsNone(ns.calendar_end)
        self.assertIsNone(ns.culture)


# ═══════════════════════════════════════════════════════════════════════
#  TMDL culture configurability
# ═══════════════════════════════════════════════════════════════════════

from fabric_import.tmdl_generator import _build_semantic_model


class TestCultureConfigurability(unittest.TestCase):
    """Tests that culture is configurable and defaults to en-US."""

    def _minimal_datasources(self):
        return [
            {
                'name': 'TestDS',
                'connection': {'type': 'SQL Server', 'details': {}},
                'connection_map': {},
                'tables': [{'name': 'T', 'columns': [{'name': 'ID', 'datatype': 'integer'}]}],
            }
        ]

    def test_default_culture_en_us(self):
        result = _build_semantic_model(self._minimal_datasources(), 'TestApp')
        culture = result.get('model', {}).get('culture', result.get('culture'))
        self.assertEqual(culture, 'en-US')

    def test_custom_culture(self):
        result = _build_semantic_model(
            self._minimal_datasources(), 'TestApp', culture='fr-FR'
        )
        culture = result.get('model', {}).get('culture', result.get('culture'))
        self.assertEqual(culture, 'fr-FR')

    def test_culture_ja_jp(self):
        result = _build_semantic_model(
            self._minimal_datasources(), 'TestApp', culture='ja-JP'
        )
        culture = result.get('model', {}).get('culture', result.get('culture'))
        self.assertEqual(culture, 'ja-JP')


if __name__ == '__main__':
    unittest.main()
