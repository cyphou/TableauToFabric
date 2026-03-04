"""Tests for fabric_import.calc_column_utils"""

import unittest

from fabric_import.calc_column_utils import (
    classify_calculations,
    sanitize_calc_col_name,
    tableau_formula_to_m,
    make_m_add_column_step,
    tableau_formula_to_pyspark,
)


# ═══════════════════════════════════════════════════════════════════
#  CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════

class TestClassifyCalculations(unittest.TestCase):
    """Tests for classify_calculations()."""

    def test_measure_with_sum(self):
        calcs = [{'name': 'Total', 'formula': 'SUM([Amount])',
                  'datatype': 'real', 'role': 'measure'}]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 0)
        self.assertEqual(len(ms), 1)

    def test_measure_with_count(self):
        calcs = [{'name': 'Cnt', 'formula': 'COUNT([ID])',
                  'datatype': 'integer', 'role': 'measure'}]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 0)
        self.assertEqual(len(ms), 1)

    def test_calc_column_dimension_role(self):
        calcs = [{'name': 'Revenue', 'formula': '[Amount] * [Price]',
                  'datatype': 'real', 'role': 'dimension'}]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 1)
        self.assertEqual(len(ms), 0)
        self.assertEqual(cc[0]['spark_type'], 'DOUBLE')

    def test_calc_column_no_aggregation(self):
        calcs = [{'name': 'R', 'formula': '[A] + [B]',
                  'datatype': 'integer', 'role': 'measure'}]
        cc, ms = classify_calculations(calcs)
        # No aggregation → classified as calc column even with role=measure
        self.assertEqual(len(cc), 1)

    def test_literal_is_measure(self):
        calcs = [{'name': 'Const', 'formula': '42',
                  'datatype': 'integer', 'role': 'dimension'}]
        cc, ms = classify_calculations(calcs)
        # Literal (no column refs) → measure
        self.assertEqual(len(cc), 0)
        self.assertEqual(len(ms), 1)

    def test_empty_formula_skipped(self):
        calcs = [{'name': 'Empty', 'formula': '',
                  'datatype': 'string', 'role': 'measure'}]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 0)
        self.assertEqual(len(ms), 0)

    def test_empty_list(self):
        cc, ms = classify_calculations([])
        self.assertEqual(cc, [])
        self.assertEqual(ms, [])

    def test_mixed_calculations(self):
        calcs = [
            {'name': 'Total', 'formula': 'SUM([Amt])',
             'datatype': 'real', 'role': 'measure'},
            {'name': 'Rev', 'formula': '[Amt] * [Qty]',
             'datatype': 'real', 'role': 'dimension'},
            {'name': 'Cnt', 'formula': 'COUNT([ID])',
             'datatype': 'integer', 'role': 'measure'},
            {'name': 'Status', 'formula': 'IF [Active] THEN "Y" ELSE "N" END',
             'datatype': 'string', 'role': 'dimension'},
        ]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 2)  # Rev, Status
        self.assertEqual(len(ms), 2)  # Total, Cnt

    def test_spark_type_mapping(self):
        calcs = [
            {'name': 'A', 'formula': '[X]', 'datatype': 'string', 'role': 'dimension'},
            {'name': 'B', 'formula': '[X]', 'datatype': 'real', 'role': 'dimension'},
            {'name': 'C', 'formula': '[X]', 'datatype': 'boolean', 'role': 'dimension'},
            {'name': 'D', 'formula': '[X]', 'datatype': 'unknown', 'role': 'dimension'},
        ]
        cc, _ = classify_calculations(calcs)
        types = [c['spark_type'] for c in cc]
        self.assertEqual(types, ['STRING', 'DOUBLE', 'BOOLEAN', 'STRING'])

    def test_window_function_is_measure(self):
        calcs = [{'name': 'W', 'formula': 'RUNNING_SUM(SUM([Amount]))',
                  'datatype': 'real', 'role': 'measure'}]
        cc, ms = classify_calculations(calcs)
        self.assertEqual(len(cc), 0)
        self.assertEqual(len(ms), 1)


# ═══════════════════════════════════════════════════════════════════
#  SANITISATION
# ═══════════════════════════════════════════════════════════════════

class TestSanitizeCalcColName(unittest.TestCase):
    """Tests for sanitize_calc_col_name()."""

    def test_basic(self):
        self.assertEqual(sanitize_calc_col_name('Revenue'), 'revenue')

    def test_spaces(self):
        self.assertEqual(sanitize_calc_col_name('Total Sales'), 'total_sales')

    def test_special_chars(self):
        self.assertEqual(sanitize_calc_col_name('col$!name'), 'col_name')

    def test_leading_digit(self):
        result = sanitize_calc_col_name('1col')
        self.assertFalse(result[0].isdigit())

    def test_brackets_stripped(self):
        self.assertEqual(sanitize_calc_col_name('[Amount]'), 'amount')

    def test_empty_fallback(self):
        self.assertEqual(sanitize_calc_col_name('!!!'), 'calc_col')


# ═══════════════════════════════════════════════════════════════════
#  FORMULA CONVERSION — M
# ═══════════════════════════════════════════════════════════════════

class TestTableauFormulaToM(unittest.TestCase):
    """Tests for tableau_formula_to_m()."""

    def test_arithmetic(self):
        result = tableau_formula_to_m('[Amount] * [Price]')
        self.assertIn('[Amount]', result)
        self.assertIn('*', result)

    def test_if_then_else(self):
        result = tableau_formula_to_m(
            'IF [IsActive] THEN "Yes" ELSE "No" END'
        )
        self.assertIn('if', result)
        self.assertIn('then', result)
        self.assertIn('else', result)
        self.assertNotIn('END', result)

    def test_boolean_operators(self):
        result = tableau_formula_to_m('[A] AND [B] OR NOT [C]')
        self.assertIn('and', result)
        self.assertIn('or', result)
        self.assertIn('not', result)

    def test_string_functions(self):
        result = tableau_formula_to_m('UPPER([Name])')
        self.assertIn('Text.Upper', result)

    def test_round(self):
        result = tableau_formula_to_m('ROUND([Price], 2)')
        self.assertIn('Number.Round', result)


class TestMakeMAddColumnStep(unittest.TestCase):
    """Tests for make_m_add_column_step()."""

    def test_basic(self):
        line, step = make_m_add_column_step('[A] * [B]', 'Revenue', 'Source')
        self.assertIn('Table.AddColumn', line)
        self.assertIn('Source', line)
        self.assertIn('"Revenue"', line)
        self.assertIn('each', line)
        self.assertTrue(step.startswith('CalcCol_'))

    def test_chaining(self):
        _, step1 = make_m_add_column_step('[A]', 'Col1', 'Source')
        line2, step2 = make_m_add_column_step('[B]', 'Col2', step1)
        self.assertIn(step1, line2)
        self.assertNotEqual(step1, step2)

    def test_escapes_quotes_in_name(self):
        line, _ = make_m_add_column_step('[X]', 'My "Col"', 'Source')
        self.assertIn('""', line)


# ═══════════════════════════════════════════════════════════════════
#  FORMULA CONVERSION — PySpark
# ═══════════════════════════════════════════════════════════════════

class TestTableauFormulaToPySpark(unittest.TestCase):
    """Tests for tableau_formula_to_pyspark()."""

    def test_simple_arithmetic(self):
        result = tableau_formula_to_pyspark('[Amount] * [Price]', 'Revenue')
        self.assertIn('withColumn', result)
        self.assertIn('F.col("Amount")', result)
        self.assertIn('F.col("Price")', result)

    def test_if_then_else(self):
        result = tableau_formula_to_pyspark(
            'IF [IsActive] THEN "Active" ELSE "Inactive" END',
            'Status',
        )
        self.assertIn('F.when', result)
        self.assertIn('otherwise', result)
        self.assertIn('"Status"', result)

    def test_column_reference(self):
        result = tableau_formula_to_pyspark('[Amount]', 'Amt')
        self.assertIn('F.col("Amount")', result)
        self.assertIn('withColumn', result)

    def test_returns_string(self):
        result = tableau_formula_to_pyspark('[X] + 1', 'Y')
        self.assertIsInstance(result, str)


if __name__ == '__main__':
    unittest.main()
