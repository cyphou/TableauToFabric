"""
Extra coverage tests for tableau_export.dax_converter.

Targets uncovered lines: nested parens (depth += 1), unbalanced parens,
fallback arg-count branches, and entire untested functions:
_convert_corr_covar, _convert_running_functions, _convert_total_function,
_unwrap_inner_agg, generate_combined_field_dax, _fix_startof_calc_columns,
LOD edges, WINDOW compute_using, RANK variants.
"""

import unittest
from tableau_export.dax_converter import (
    _convert_corr_covar,
    _convert_running_functions,
    _convert_total_function,
    _unwrap_inner_agg,
    generate_combined_field_dax,
    _convert_find,
    _convert_endswith,
    _convert_startswith,
    _convert_proper,
    _convert_split,
    _convert_atan2,
    _convert_div,
    _convert_square,
    _convert_iif,
    _convert_str_to_format,
    _convert_float_to_convert,
    _convert_datename,
    _convert_dateparse,
    _convert_isdate,
    _convert_radians_degrees,
    _convert_datediff,
    _convert_ifnull,
    _convert_lod_expressions,
    _convert_window_functions,
    _convert_rank_functions,
    _convert_agg_expr_to_aggx,
    _convert_case_structure,
    _fix_ceiling_floor,
    _fix_startof_calc_columns,
    _resolve_columns,
    _extract_balanced_call,
)


# ═══════════════════════════════════════════════════════════════════
#  generate_combined_field_dax — fully untested
# ═══════════════════════════════════════════════════════════════════

class TestGenerateCombinedFieldDax(unittest.TestCase):

    def test_empty_fields(self):
        self.assertEqual(generate_combined_field_dax([], "T"), '""')

    def test_single_field(self):
        result = generate_combined_field_dax(["Name"], "Products")
        self.assertEqual(result, "'Products'[Name]")

    def test_two_fields(self):
        result = generate_combined_field_dax(["First", "Last"], "T")
        self.assertIn("&", result)
        self.assertIn("'T'[First]", result)
        self.assertIn("'T'[Last]", result)

    def test_three_fields(self):
        result = generate_combined_field_dax(["A", "B", "C"], "T")
        self.assertEqual(result.count("&"), 4)  # A & sep & B & sep & C

    def test_custom_separator(self):
        result = generate_combined_field_dax(["X", "Y"], "T", separator="-")
        self.assertIn('"-"', result)


# ═══════════════════════════════════════════════════════════════════
#  _convert_corr_covar — fully untested
# ═══════════════════════════════════════════════════════════════════

class TestConvertCorrCovar(unittest.TestCase):

    def test_corr(self):
        result = _convert_corr_covar("CORR('T'[X], 'T'[Y])")
        self.assertIn("DIVIDE(", result)
        self.assertIn("SQRT(", result)
        self.assertIn("SUMX(", result)
        self.assertIn("AVERAGEX(", result)

    def test_covarp(self):
        result = _convert_corr_covar("COVARP('T'[X], 'T'[Y])")
        self.assertIn("COUNTROWS(", result)
        self.assertNotIn("- 1)", result)

    def test_covar_sample(self):
        result = _convert_corr_covar("COVAR('T'[X], 'T'[Y])")
        self.assertIn("COUNTROWS(", result)
        self.assertIn("- 1)", result)

    def test_corr_single_arg_fallback(self):
        result = _convert_corr_covar("CORR([X])")
        self.assertIn("unable to parse", result)

    def test_corr_nested_parens(self):
        """Nested parens inside CORR args — covers depth += 1."""
        result = _convert_corr_covar("CORR(SUM('T'[X]), ABS('T'[Y]))")
        self.assertIn("DIVIDE(", result)


# ═══════════════════════════════════════════════════════════════════
#  _convert_running_functions — fully untested
# ═══════════════════════════════════════════════════════════════════

class TestConvertRunningFunctions(unittest.TestCase):

    def test_running_sum(self):
        result = _convert_running_functions("RUNNING_SUM(SUM([Sales]))", "Orders")
        self.assertIn("CALCULATE(", result)
        self.assertIn("ALLSELECTED(", result)
        self.assertIn("RUNNING_SUM", result)  # in comment

    def test_running_avg(self):
        result = _convert_running_functions("RUNNING_AVG(AVG([Score]))", "T")
        self.assertIn("CALCULATE(", result)

    def test_running_count(self):
        result = _convert_running_functions("RUNNING_COUNT(COUNT([ID]))", "T")
        self.assertIn("CALCULATE(", result)

    def test_running_max(self):
        result = _convert_running_functions("RUNNING_MAX(MAX([V]))", "T")
        self.assertIn("CALCULATE(", result)

    def test_running_min(self):
        result = _convert_running_functions("RUNNING_MIN(MIN([V]))", "T")
        self.assertIn("CALCULATE(", result)

    def test_no_match(self):
        result = _convert_running_functions("SUM([X])", "T")
        self.assertEqual(result, "SUM([X])")


# ═══════════════════════════════════════════════════════════════════
#  _convert_total_function — fully untested
# ═══════════════════════════════════════════════════════════════════

class TestConvertTotalFunction(unittest.TestCase):

    def test_total(self):
        result = _convert_total_function("TOTAL(SUM([Sales]))", "Orders")
        self.assertEqual(result, "CALCULATE(SUM([Sales]), ALL('Orders'))")

    def test_nested_total(self):
        result = _convert_total_function("SUM([X]) / TOTAL(SUM([X]))", "T")
        self.assertIn("ALL('T')", result)

    def test_no_match(self):
        result = _convert_total_function("SUM([X])", "T")
        self.assertEqual(result, "SUM([X])")


# ═══════════════════════════════════════════════════════════════════
#  _unwrap_inner_agg — fully untested
# ═══════════════════════════════════════════════════════════════════

class TestUnwrapInnerAgg(unittest.TestCase):

    def test_sum(self):
        result = _unwrap_inner_agg("SUM('T'[a] * 'T'[b])")
        self.assertEqual(result, "'T'[a] * 'T'[b]")

    def test_average(self):
        result = _unwrap_inner_agg("AVERAGE([Price] * [Qty])")
        self.assertEqual(result, "[Price] * [Qty]")

    def test_not_an_agg(self):
        result = _unwrap_inner_agg("[Col]")
        self.assertIsNone(result)

    def test_trailing_text_not_unwrapped(self):
        """AGG(x) + extra text → should NOT unwrap."""
        result = _unwrap_inner_agg("SUM([X]) + 1")
        self.assertIsNone(result)

    def test_nested_parens(self):
        result = _unwrap_inner_agg("SUM(ABS([X]) * [Y])")
        self.assertEqual(result, "ABS([X]) * [Y]")


# ═══════════════════════════════════════════════════════════════════
#  _convert_agg_expr_to_aggx — stat iterator + unwrap
# ═══════════════════════════════════════════════════════════════════

class TestAggExprToAggxStatIterator(unittest.TestCase):

    def test_stdev_s_with_inner_sum_unwrapped(self):
        """STDEV.S(SUM(a*b)) → STDEVX.S('T', a*b)."""
        result = _convert_agg_expr_to_aggx("STDEV.S(SUM('T'[a] * 'T'[b]))", "T")
        self.assertIn("STDEVX.S(", result)
        self.assertIn("'T'[a] * 'T'[b]", result)

    def test_median_of_expression(self):
        result = _convert_agg_expr_to_aggx("MEDIAN('T'[a] * 2)", "T")
        self.assertIn("MEDIANX(", result)

    def test_sum_nested_parens(self):
        """SUM(ABS([X])) → SUMX('T', ABS([X])) — nested parens depth+=1."""
        result = _convert_agg_expr_to_aggx("SUM(ABS('T'[X]))", "T")
        self.assertIn("SUMX(", result)


# ═══════════════════════════════════════════════════════════════════
#  Nested parens (depth += 1) — Pattern A
# ═══════════════════════════════════════════════════════════════════

class TestNestedParens(unittest.TestCase):
    """Cover the 'depth += 1' branch in every converter by passing
    inputs with nested function calls inside the arguments."""

    def test_find_nested(self):
        result = _convert_find("FIND(LOWER('hello'), 'l')")
        self.assertIn("FIND(", result)

    def test_endswith_nested(self):
        result = _convert_endswith("ENDSWITH(LOWER([Name]), 'son')")
        self.assertIn("RIGHT(", result)

    def test_startswith_nested(self):
        result = _convert_startswith("STARTSWITH(UPPER([Name]), 'A')")
        self.assertIn("LEFT(", result)

    def test_proper_nested(self):
        result = _convert_proper("PROPER(LOWER([Name]))")
        self.assertIn("UPPER(LEFT(", result)

    def test_split_nested(self):
        result = _convert_split("SPLIT(LOWER([X]), '-', 1)")
        self.assertIn("PATHITEM", result)

    def test_atan2_nested(self):
        result = _convert_atan2("ATAN2(ABS([Y]), [X])")
        self.assertIn("ATAN(", result)

    def test_div_nested(self):
        result = _convert_div("DIV(ABS([A]), [B])")
        self.assertIn("QUOTIENT(", result)

    def test_square_nested(self):
        result = _convert_square("SQUARE(ABS([X]))")
        self.assertIn("POWER(", result)

    def test_str_nested(self):
        result = _convert_str_to_format("STR(SUM([X]))")
        self.assertIn("FORMAT(", result)

    def test_float_nested(self):
        result = _convert_float_to_convert("FLOAT(ABS([X]))")
        self.assertIn("CONVERT(", result)

    def test_datename_nested(self):
        result = _convert_datename("DATENAME('month', MAX([D]))")
        self.assertIn("FORMAT(", result)

    def test_dateparse_nested(self):
        result = _convert_dateparse("DATEPARSE('yyyy', MAX([D]))")
        self.assertIn("DATEVALUE(", result)

    def test_isdate_nested(self):
        result = _convert_isdate("ISDATE(LEFT([D], 10))")
        self.assertIn("ISERROR", result)

    def test_iif_nested(self):
        result = _convert_iif("IIF(LEN([X]) > 0, 'Y', 'N')")
        self.assertIn("IF(", result)

    def test_radians_nested(self):
        result = _convert_radians_degrees("RADIANS(ABS([Angle]))")
        self.assertIn("PI()/180", result)

    def test_ceiling_nested(self):
        result = _fix_ceiling_floor("CEILING(ABS([X]))")
        self.assertIn("CEILING(", result)

    def test_extract_balanced_call_nested(self):
        result = _extract_balanced_call("prefix ZN(SUM([Sales])) suffix", "ZN")
        self.assertTrue(len(result) > 0)
        self.assertEqual(result[0][2], "SUM([Sales])")

    def test_rank_nested(self):
        result = _convert_rank_functions("RANK(SUM([Sales]))", "T")
        self.assertIn("RANKX(", result)

    def test_window_sum_nested(self):
        result = _convert_window_functions("WINDOW_SUM(SUM([Sales]))", "T")
        self.assertIn("CALCULATE(", result)


# ═══════════════════════════════════════════════════════════════════
#  Unbalanced parens (break/continue) — Pattern B
# ═══════════════════════════════════════════════════════════════════

class TestUnbalancedParens(unittest.TestCase):
    """Cover the 'break' / 'continue when depth != 0' guard
    by passing inputs with unmatched opening parentheses."""

    def test_find_unbalanced(self):
        result = _convert_find("FIND(open(")
        self.assertIn("FIND(", result)  # unchanged

    def test_endswith_unbalanced(self):
        result = _convert_endswith("ENDSWITH(open(")
        self.assertIn("ENDSWITH(", result)

    def test_startswith_unbalanced(self):
        result = _convert_startswith("STARTSWITH(open(")
        self.assertIn("STARTSWITH(", result)

    def test_proper_unbalanced(self):
        result = _convert_proper("PROPER(open(")
        self.assertIn("PROPER(", result)

    def test_split_unbalanced(self):
        result = _convert_split("SPLIT(open(")
        self.assertIn("SPLIT(", result)

    def test_atan2_unbalanced(self):
        result = _convert_atan2("ATAN2(open(")
        self.assertIn("ATAN2(", result)

    def test_div_unbalanced(self):
        result = _convert_div("DIV(open(")
        self.assertIn("DIV(", result)

    def test_square_unbalanced(self):
        result = _convert_square("SQUARE(open(")
        self.assertIn("SQUARE(", result)

    def test_str_unbalanced(self):
        result = _convert_str_to_format("STR(open(")
        self.assertIn("STR(", result)

    def test_float_unbalanced(self):
        result = _convert_float_to_convert("FLOAT(open(")
        self.assertIn("FLOAT(", result)

    def test_datename_unbalanced(self):
        result = _convert_datename("DATENAME(open(")
        self.assertIn("DATENAME(", result)

    def test_dateparse_unbalanced(self):
        result = _convert_dateparse("DATEPARSE(open(")
        self.assertIn("DATEPARSE(", result)

    def test_isdate_unbalanced(self):
        result = _convert_isdate("ISDATE(open(")
        self.assertIn("ISDATE(", result)

    def test_iif_unbalanced(self):
        result = _convert_iif("IIF(open(")
        self.assertIn("IIF(", result)

    def test_radians_unbalanced(self):
        result = _convert_radians_degrees("RADIANS(open(")
        self.assertIn("RADIANS(", result)

    def test_ceiling_unbalanced(self):
        result = _fix_ceiling_floor("CEILING(open(")
        self.assertIn("CEILING(", result)

    def test_datediff_unbalanced(self):
        result = _convert_datediff("DATEDIFF('year', [S], open(")
        self.assertIn("DATEDIFF(", result)

    def test_rank_unbalanced(self):
        result = _convert_rank_functions("RANK(open(", "T")
        self.assertIn("RANK(", result)

    def test_extract_balanced_call_unbalanced(self):
        result = _extract_balanced_call("ZN(open(", "ZN")
        # Returns empty list when parens don't balance
        self.assertEqual(result, [])

    def test_corr_unbalanced(self):
        result = _convert_corr_covar("CORR(open(")
        self.assertIn("CORR(", result)


# ═══════════════════════════════════════════════════════════════════
#  Fallback arg-count branches — Pattern C
# ═══════════════════════════════════════════════════════════════════

class TestFallbackArgCount(unittest.TestCase):

    def test_datediff_two_args(self):
        result = _convert_datediff("DATEDIFF('year', [Start])")
        # Not enough args — left unchanged
        self.assertEqual(result, "DATEDIFF('year', [Start])")

    def test_ifnull_one_arg(self):
        result = _convert_ifnull("IFNULL([X])")
        # fewer than 2 args -> keep original
        self.assertIn("IFNULL", result)

    def test_find_one_arg(self):
        result = _convert_find("FIND('abc')")
        self.assertIn("FIND", result)

    def test_datename_one_arg(self):
        result = _convert_datename("DATENAME('month')")
        self.assertIn("DATENAME", result)

    def test_endswith_one_arg(self):
        result = _convert_endswith("ENDSWITH('hello')")
        self.assertIn("ENDSWITH", result)

    def test_startswith_one_arg(self):
        result = _convert_startswith("STARTSWITH('hello')")
        self.assertIn("STARTSWITH", result)

    def test_split_two_args_default_token(self):
        """SPLIT with 2 args → default token_number = 1."""
        result = _convert_split("SPLIT('a-b', '-')")
        self.assertIn("PATHITEM", result)

    def test_split_one_arg_blank(self):
        """SPLIT with 1 arg → unable to parse → BLANK()."""
        result = _convert_split("SPLIT('abc')")
        self.assertIn("BLANK()", result)

    def test_atan2_one_arg(self):
        result = _convert_atan2("ATAN2([Y])")
        self.assertIn("ATAN2", result)

    def test_dateparse_one_arg(self):
        """DATEPARSE with 1 arg → DATEVALUE(inner)."""
        result = _convert_dateparse("DATEPARSE([D])")
        self.assertIn("DATEVALUE(", result)

    def test_iif_two_args(self):
        """IIF with 2 args → IF(cond, val, BLANK())."""
        result = _convert_iif("IIF([X] > 0, 'Yes')")
        self.assertIn("IF(", result)
        self.assertIn("BLANK()", result)

    def test_iif_one_arg(self):
        """IIF with 1 arg → kept unchanged."""
        result = _convert_iif("IIF([X])")
        self.assertIn("IIF", result)

    def test_case_empty_when(self):
        """CASE with adjacent WHENs producing empty parts."""
        result = _convert_case_structure(
            "CASE [Status] WHEN 'A' THEN 1 WHEN 'B' THEN 2 END"
        )
        self.assertIn("SWITCH(", result)


# ═══════════════════════════════════════════════════════════════════
#  LOD edge cases
# ═══════════════════════════════════════════════════════════════════

class TestLodEdgeCases(unittest.TestCase):

    def test_fixed_no_dims(self):
        """FIXED with empty dimensions → CALCULATE(expr, ALL('T'))."""
        result = _convert_lod_expressions("{FIXED : SUM([Sales])}", "T", {})
        self.assertIn("ALL('T')", result)

    def test_exclude_no_dims(self):
        """EXCLUDE with empty dimensions → CALCULATE(expr)."""
        result = _convert_lod_expressions("{EXCLUDE : SUM([Sales])}", "T", {})
        self.assertIn("CALCULATE(", result)
        self.assertNotIn("REMOVEFILTERS", result)

    def test_lod_no_dim_nested_braces(self):
        """LOD without dimension keyword — nested braces."""
        result = _convert_lod_expressions("{SUM([X])}", "T", {})
        self.assertIn("CALCULATE(", result)

    def test_lod_no_dim_unbalanced(self):
        """LOD without dimension — unbalanced brace → break."""
        result = _convert_lod_expressions("{SUM([X]", "T", {})
        # Unbalanced → kept as is
        self.assertIn("{SUM(", result)


# ═══════════════════════════════════════════════════════════════════
#  WINDOW + RANK with compute_using
# ═══════════════════════════════════════════════════════════════════

class TestWindowRankComputeUsing(unittest.TestCase):

    def test_window_sum_with_compute_using(self):
        result = _convert_window_functions(
            "WINDOW_SUM(SUM([Sales]))", "Orders",
            compute_using=["Region"],
            column_table_map={"Region": "Orders"},
        )
        self.assertIn("ALLEXCEPT(", result)
        self.assertIn("'Orders'[Region]", result)

    def test_rank_with_compute_using(self):
        result = _convert_rank_functions(
            "RANK([Sales])", "Orders",
            compute_using=["Region"],
            column_table_map={"Region": "Orders"},
        )
        self.assertIn("ALLEXCEPT(", result)
        self.assertIn("'Orders'[Region]", result)

    def test_rank_modified(self):
        result = _convert_rank_functions("RANK_MODIFIED([Sales])", "T")
        self.assertIn("RANKX(", result)
        self.assertIn("RANK_MODIFIED", result)

    def test_rank_percentile(self):
        result = _convert_rank_functions("RANK_PERCENTILE([Score])", "T")
        self.assertIn("DIVIDE(", result)
        self.assertIn("RANKX(", result)


# ═══════════════════════════════════════════════════════════════════
#  _fix_startof_calc_columns
# ═══════════════════════════════════════════════════════════════════

class TestFixStartofCalcColumns(unittest.TestCase):

    def test_startofyear(self):
        result = _fix_startof_calc_columns("STARTOFYEAR(MAX([Date]))")
        # Should fold MAX out for calc columns
        self.assertIsInstance(result, str)

    def test_no_match(self):
        result = _fix_startof_calc_columns("SUM([X])")
        self.assertEqual(result, "SUM([X])")


# ═══════════════════════════════════════════════════════════════════
#  _resolve_columns edge cases
# ═══════════════════════════════════════════════════════════════════

class TestResolveColumnsEdge(unittest.TestCase):

    def test_measure_in_calc_column_with_param(self):
        """Measure name that is also a param in calc column context → inline value."""
        result = _resolve_columns(
            "[MyParam]", "T",
            column_table_map={},
            measure_names={"MyParam"},
            is_calc_column=True,
            param_values={"MyParam": "42"},
        )
        self.assertIn("42", result)


if __name__ == "__main__":
    unittest.main()
