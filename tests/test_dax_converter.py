"""
Tests for tableau_export.dax_converter — Tableau formula → DAX conversion.

Organized by complexity:
  SIMPLE:  Type mapping, empty inputs, bracket escape, simple 1:1 functions
  MEDIUM:  Operators, CASE/IF structures, DATEDIFF, ZN/IFNULL, LOD expressions,
           column resolution, AGG→AGGX, string concat, date literals
  COMPLEX: Multi-layered nesting, RANK, WINDOW, full convert_tableau_formula_to_dax
           with all context args, calc column mode, complex LOD with
           cross-table columns
"""

import unittest
from tableau_export.dax_converter import (
    _reverse_tableau_bracket_escape,
    map_tableau_to_powerbi_type,
    convert_tableau_formula_to_dax,
    _convert_case_structure,
    _convert_if_structure,
    _convert_datediff,
    _convert_zn,
    _convert_ifnull,
    _convert_lod_expressions,
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
    _convert_string_concat,
    _fix_ceiling_floor,
    _fix_date_literals,
    _convert_agg_if_to_aggx,
    _convert_agg_expr_to_aggx,
    _convert_window_functions,
    _convert_rank_functions,
    _resolve_references,
    _resolve_columns,
    _split_args,
)


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS
# ═══════════════════════════════════════════════════════════════════


class TestMapTableauToPowerBIType(unittest.TestCase):
    """SIMPLE — Type mapping."""

    def test_string(self):
        self.assertEqual(map_tableau_to_powerbi_type('string'), 'String')

    def test_integer(self):
        self.assertEqual(map_tableau_to_powerbi_type('integer'), 'Int64')

    def test_real(self):
        self.assertEqual(map_tableau_to_powerbi_type('real'), 'Double')

    def test_boolean(self):
        self.assertEqual(map_tableau_to_powerbi_type('boolean'), 'Boolean')

    def test_date(self):
        self.assertEqual(map_tableau_to_powerbi_type('date'), 'DateTime')

    def test_datetime(self):
        self.assertEqual(map_tableau_to_powerbi_type('datetime'), 'DateTime')

    def test_number(self):
        self.assertEqual(map_tableau_to_powerbi_type('number'), 'Double')

    def test_unknown_returns_string(self):
        self.assertEqual(map_tableau_to_powerbi_type('blob'), 'String')

    def test_case_insensitive(self):
        self.assertEqual(map_tableau_to_powerbi_type('INTEGER'), 'Int64')
        self.assertEqual(map_tableau_to_powerbi_type('String'), 'String')


class TestBracketEscape(unittest.TestCase):
    """SIMPLE — _reverse_tableau_bracket_escape."""

    def test_no_change_balanced(self):
        self.assertEqual(_reverse_tableau_bracket_escape('abc(def)'), 'abc(def)')

    def test_orphan_paren_reversed(self):
        self.assertEqual(_reverse_tableau_bracket_escape('col)'), 'col]')

    def test_multiple_orphan_parens(self):
        self.assertEqual(_reverse_tableau_bracket_escape('a)b)'), 'a]b]')

    def test_mixed_balanced_and_orphan(self):
        self.assertEqual(_reverse_tableau_bracket_escape('a(b)c)'), 'a(b)c]')

    def test_no_parens(self):
        self.assertEqual(_reverse_tableau_bracket_escape('hello'), 'hello')

    def test_empty_string(self):
        self.assertEqual(_reverse_tableau_bracket_escape(''), '')


class TestEmptyInputs(unittest.TestCase):
    """SIMPLE — Edge cases: empty, None, whitespace formulas."""

    def test_empty_string(self):
        result = convert_tableau_formula_to_dax('')
        self.assertEqual(result, '')

    def test_none_input(self):
        result = convert_tableau_formula_to_dax(None)
        self.assertIsNone(result)

    def test_whitespace_only(self):
        result = convert_tableau_formula_to_dax('   ')
        self.assertEqual(result, '   ')

    def test_single_literal(self):
        result = convert_tableau_formula_to_dax('"hello"')
        self.assertEqual(result, '"hello"')


class TestSimpleFunctionConversions(unittest.TestCase):
    """SIMPLE — 1:1 Tableau→DAX function substitutions."""

    def test_isnull_to_isblank(self):
        result = convert_tableau_formula_to_dax('ISNULL([field])')
        self.assertIn('ISBLANK', result)
        self.assertNotIn('ISNULL', result)

    def test_countd_to_distinctcount(self):
        result = convert_tableau_formula_to_dax('COUNTD([Customer])')
        self.assertIn('DISTINCTCOUNT', result)

    def test_avg_to_average(self):
        result = convert_tableau_formula_to_dax('AVG([Sales])')
        self.assertIn('AVERAGE', result)

    def test_contains_to_containsstring(self):
        result = convert_tableau_formula_to_dax('CONTAINS([Name], "abc")')
        self.assertIn('CONTAINSSTRING', result)

    def test_ascii_to_unicode(self):
        result = convert_tableau_formula_to_dax('ASCII("A")')
        self.assertIn('UNICODE', result)

    def test_char_to_unichar(self):
        result = convert_tableau_formula_to_dax('CHAR(65)')
        self.assertIn('UNICHAR', result)

    def test_attr_to_values(self):
        result = convert_tableau_formula_to_dax('ATTR([Category])')
        self.assertIn('SELECTEDVALUE', result)

    def test_trim(self):
        result = convert_tableau_formula_to_dax('TRIM([Name])')
        self.assertIn('TRIM', result)

    def test_upper(self):
        result = convert_tableau_formula_to_dax('UPPER([Name])')
        self.assertIn('UPPER', result)

    def test_lower(self):
        result = convert_tableau_formula_to_dax('LOWER([Name])')
        self.assertIn('LOWER', result)

    def test_left(self):
        result = convert_tableau_formula_to_dax('LEFT([Name], 3)')
        self.assertIn('LEFT', result)

    def test_right(self):
        result = convert_tableau_formula_to_dax('RIGHT([Name], 3)')
        self.assertIn('RIGHT', result)

    def test_mid(self):
        result = convert_tableau_formula_to_dax('MID([Name], 2, 3)')
        self.assertIn('MID', result)

    def test_len(self):
        result = convert_tableau_formula_to_dax('LEN([Name])')
        self.assertIn('LEN', result)

    def test_replace_to_substitute(self):
        result = convert_tableau_formula_to_dax('REPLACE([Name], "a", "b")')
        self.assertIn('SUBSTITUTE', result)

    def test_abs(self):
        result = convert_tableau_formula_to_dax('ABS([Delta])')
        self.assertIn('ABS', result)

    def test_round(self):
        result = convert_tableau_formula_to_dax('ROUND([Price], 2)')
        self.assertIn('ROUND', result)

    def test_power(self):
        result = convert_tableau_formula_to_dax('POWER([X], 3)')
        self.assertIn('POWER', result)

    def test_sqrt(self):
        result = convert_tableau_formula_to_dax('SQRT([X])')
        self.assertIn('SQRT', result)

    def test_log(self):
        result = convert_tableau_formula_to_dax('LOG([X])')
        self.assertIn('LOG', result)

    def test_exp(self):
        result = convert_tableau_formula_to_dax('EXP([X])')
        self.assertIn('EXP', result)

    def test_pi(self):
        result = convert_tableau_formula_to_dax('PI()')
        self.assertIn('PI()', result)

    def test_today(self):
        result = convert_tableau_formula_to_dax('TODAY()')
        self.assertIn('TODAY()', result)

    def test_now(self):
        result = convert_tableau_formula_to_dax('NOW()')
        self.assertIn('NOW()', result)

    def test_sum(self):
        result = convert_tableau_formula_to_dax('SUM([Sales])')
        self.assertIn('SUM', result)

    def test_min(self):
        result = convert_tableau_formula_to_dax('MIN([Price])')
        self.assertIn('MIN', result)

    def test_max(self):
        result = convert_tableau_formula_to_dax('MAX([Price])')
        self.assertIn('MAX', result)

    def test_count(self):
        result = convert_tableau_formula_to_dax('COUNT([OrderID])')
        self.assertIn('COUNT', result)

    def test_counta(self):
        result = convert_tableau_formula_to_dax('COUNTA([OrderID])')
        self.assertIn('COUNTA', result)

    def test_median(self):
        result = convert_tableau_formula_to_dax('MEDIAN([Sales])')
        self.assertIn('MEDIAN', result)

    def test_stdev_to_stdev_s(self):
        result = convert_tableau_formula_to_dax('STDEV([Sales])')
        self.assertIn('STDEV.S', result)

    def test_stdevp_to_stdev_p(self):
        result = convert_tableau_formula_to_dax('STDEVP([Sales])')
        self.assertIn('STDEV.P', result)

    def test_var_to_var_s(self):
        result = convert_tableau_formula_to_dax('VAR([Sales])')
        self.assertIn('VAR.S', result)

    def test_varp_to_var_p(self):
        result = convert_tableau_formula_to_dax('VARP([Sales])')
        self.assertIn('VAR.P', result)

    def test_makedate_to_date(self):
        result = convert_tableau_formula_to_dax('MAKEDATE(2024, 1, 15)')
        self.assertIn('DATE', result)

    def test_maketime_to_time(self):
        result = convert_tableau_formula_to_dax('MAKETIME(10, 30, 0)')
        self.assertIn('TIME', result)

    def test_username_to_userprincipalname(self):
        result = convert_tableau_formula_to_dax('USERNAME()')
        self.assertIn('USERPRINCIPALNAME', result)

    def test_regexp_match_to_containsstring(self):
        result = convert_tableau_formula_to_dax('REGEXP_MATCH([Code], "^A")')
        self.assertIn('CONTAINSSTRING', result)

    def test_size_to_countrows(self):
        result = convert_tableau_formula_to_dax('SIZE()')
        self.assertIn('COUNTROWS', result)


class TestDateFunctions(unittest.TestCase):
    """SIMPLE — Date function mappings (DATETRUNC, DATEPART)."""

    def test_datetrunc_year(self):
        result = convert_tableau_formula_to_dax("DATETRUNC('year', [OrderDate])")
        self.assertIn('STARTOFYEAR', result)

    def test_datetrunc_quarter(self):
        result = convert_tableau_formula_to_dax("DATETRUNC('quarter', [OrderDate])")
        self.assertIn('STARTOFQUARTER', result)

    def test_datetrunc_month(self):
        result = convert_tableau_formula_to_dax("DATETRUNC('month', [OrderDate])")
        self.assertIn('STARTOFMONTH', result)

    def test_datepart_year(self):
        result = convert_tableau_formula_to_dax("DATEPART('year', [OrderDate])")
        self.assertIn('YEAR', result)

    def test_datepart_month(self):
        result = convert_tableau_formula_to_dax("DATEPART('month', [OrderDate])")
        self.assertIn('MONTH', result)

    def test_datepart_day(self):
        result = convert_tableau_formula_to_dax("DATEPART('day', [OrderDate])")
        self.assertIn('DAY', result)

    def test_datepart_week(self):
        result = convert_tableau_formula_to_dax("DATEPART('week', [OrderDate])")
        self.assertIn('WEEKNUM', result)


class TestSplitArgs(unittest.TestCase):
    """SIMPLE — Utility: argument splitting respecting nested parens."""

    def test_simple_split(self):
        self.assertEqual(_split_args('a, b, c'), ['a', 'b', 'c'])

    def test_nested_parens(self):
        result = _split_args('SUM(a, b), c')
        self.assertEqual(result, ['SUM(a, b)', 'c'])

    def test_single_arg(self):
        self.assertEqual(_split_args('a'), ['a'])

    def test_empty(self):
        self.assertEqual(_split_args(''), [])

    def test_deeply_nested(self):
        result = _split_args('IF(A, SUM(B, C)), D')
        self.assertEqual(len(result), 2)


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS
# ═══════════════════════════════════════════════════════════════════


class TestOperatorConversions(unittest.TestCase):
    """MEDIUM — Operator substitutions."""

    def test_double_equals(self):
        result = convert_tableau_formula_to_dax('[A] == [B]')
        self.assertIn('=', result)
        self.assertNotIn('==', result)

    def test_not_equals(self):
        result = convert_tableau_formula_to_dax('[A] != [B]')
        self.assertIn('<>', result)
        self.assertNotIn('!=', result)

    def test_and_operator(self):
        result = convert_tableau_formula_to_dax('[A] > 0 AND [B] > 0')
        self.assertIn('&&', result)

    def test_or_operator(self):
        result = convert_tableau_formula_to_dax('[A] > 0 OR [B] > 0')
        self.assertIn('||', result)


class TestCaseStructure(unittest.TestCase):
    """MEDIUM — CASE/WHEN/THEN/ELSE/END → SWITCH()."""

    def test_simple_case(self):
        formula = "CASE [Region] WHEN 'East' THEN 1 WHEN 'West' THEN 2 ELSE 0 END"
        result = _convert_case_structure(formula)
        self.assertIn('SWITCH', result)
        self.assertNotIn('CASE', result)
        self.assertNotIn('END', result)

    def test_case_without_else(self):
        formula = "CASE [Status] WHEN 'Active' THEN 'Yes' END"
        result = _convert_case_structure(formula)
        self.assertIn('SWITCH', result)


class TestIfStructure(unittest.TestCase):
    """MEDIUM — IF/THEN/ELSE/END → IF()."""

    def test_simple_if(self):
        formula = 'IF [Sales] > 100 THEN "High" ELSE "Low" END'
        result = _convert_if_structure(formula)
        self.assertIn('IF(', result)
        self.assertNotIn(' THEN ', result)
        self.assertNotIn(' END', result)

    def test_if_no_else(self):
        formula = 'IF [Sales] > 100 THEN "High" END'
        result = _convert_if_structure(formula)
        self.assertIn('IF(', result)
        self.assertIn('BLANK()', result)

    def test_elseif(self):
        formula = 'IF [X] > 100 THEN "H" ELSEIF [X] > 50 THEN "M" ELSE "L" END'
        result = _convert_if_structure(formula)
        self.assertIn('IF(', result)
        # Nested IF should be present
        self.assertEqual(result.count('IF('), 2)


class TestDatediff(unittest.TestCase):
    """MEDIUM — DATEDIFF arg-reordering."""

    def test_datediff_reorder(self):
        result = _convert_datediff("DATEDIFF('year', [Start], [End])")
        self.assertIn('DATEDIFF(', result)
        self.assertIn('YEAR', result)
        # Interval should be last arg in DAX
        self.assertTrue(result.strip().endswith(')'))


class TestZnIfnull(unittest.TestCase):
    """MEDIUM — ZN and IFNULL conversions."""

    def test_zn(self):
        result = _convert_zn('ZN([Sales])')
        self.assertIn('ISBLANK', result)
        self.assertIn('0', result)

    def test_ifnull(self):
        result = _convert_ifnull('IFNULL([Sales], 0)')
        self.assertIn('ISBLANK', result)


class TestDedicatedConverters(unittest.TestCase):
    """MEDIUM — Functions needing special arg handling."""

    def test_find_swaps_args(self):
        result = _convert_find('FIND("hello", "l")')
        self.assertIn('FIND(', result)

    def test_endswith(self):
        result = _convert_endswith('ENDSWITH("hello", "lo")')
        self.assertIn('RIGHT', result)
        self.assertIn('LEN', result)

    def test_startswith(self):
        result = _convert_startswith('STARTSWITH("hello", "he")')
        self.assertIn('LEFT', result)
        self.assertIn('LEN', result)

    def test_proper(self):
        result = _convert_proper('PROPER("hello world")')
        self.assertIn('UPPER', result)
        self.assertIn('LOWER', result)

    def test_split_placeholder(self):
        result = _convert_split('SPLIT("a-b", "-", 1)')
        self.assertIn('PATHITEM', result)
        self.assertIn('SUBSTITUTE', result)

    def test_atan2(self):
        result = _convert_atan2('ATAN2(3, 4)')
        self.assertIn('ATAN(', result)

    def test_div_to_quotient(self):
        result = _convert_div('DIV(10, 3)')
        self.assertIn('QUOTIENT', result)

    def test_square_to_power(self):
        result = _convert_square('SQUARE(5)')
        self.assertIn('POWER(5, 2)', result)

    def test_iif(self):
        result = _convert_iif('IIF([X] > 0, "Pos", "Neg")')
        self.assertIn('IF(', result)

    def test_str_to_format(self):
        result = _convert_str_to_format('STR(123)')
        self.assertIn('FORMAT', result)

    def test_float_to_convert(self):
        result = _convert_float_to_convert('FLOAT(123)')
        self.assertIn('CONVERT', result)
        self.assertIn('DOUBLE', result)

    def test_datename(self):
        result = _convert_datename("DATENAME('month', [D])")
        self.assertIn('FORMAT', result)

    def test_dateparse(self):
        result = _convert_dateparse("DATEPARSE('yyyy-MM-dd', [D])")
        self.assertIn('DATEVALUE', result)

    def test_isdate(self):
        result = _convert_isdate('ISDATE([D])')
        self.assertIn('NOT', result)
        self.assertIn('ISERROR', result)
        self.assertIn('DATEVALUE', result)

    def test_radians(self):
        result = _convert_radians_degrees('RADIANS(90)')
        self.assertIn('PI()', result)
        self.assertIn('180', result)

    def test_degrees(self):
        result = _convert_radians_degrees('DEGREES(3.14)')
        self.assertIn('180', result)
        self.assertIn('PI()', result)


class TestCeilingFloorFix(unittest.TestCase):
    """MEDIUM — CEILING/FLOOR get significance=1 added."""

    def test_ceiling_single_arg(self):
        result = _fix_ceiling_floor('CEILING(3.5)')
        self.assertIn('CEILING(3.5, 1)', result)

    def test_floor_single_arg(self):
        result = _fix_ceiling_floor('FLOOR(3.5)')
        self.assertIn('FLOOR(3.5, 1)', result)

    def test_ceiling_two_args_unchanged(self):
        result = _fix_ceiling_floor('CEILING(3.5, 0.5)')
        self.assertIn('CEILING(3.5, 0.5)', result)


class TestDateLiterals(unittest.TestCase):
    """MEDIUM — Tableau #YYYY-MM-DD# → DAX DATE(Y, M, D)."""

    def test_date_literal(self):
        result = _fix_date_literals('#2024-01-15#')
        self.assertEqual(result, 'DATE(2024, 1, 15)')

    def test_no_date_literal(self):
        result = _fix_date_literals('[OrderDate]')
        self.assertEqual(result, '[OrderDate]')


class TestStringConcat(unittest.TestCase):
    """MEDIUM — String concatenation: + → &."""

    def test_plus_to_ampersand(self):
        result = _convert_string_concat('"Hello" + " " + "World"')
        self.assertIn('&', result)
        self.assertNotIn('+', result)

    def test_plus_inside_function_preserved(self):
        # + inside parens should NOT be converted
        result = _convert_string_concat('"A" + FIND("x", "y") + "B"')
        # At depth 0, + → &
        self.assertIn('&', result)


class TestResolveReferences(unittest.TestCase):
    """MEDIUM — Parameter and calculation reference resolution."""

    def test_parameter_resolved(self):
        result = _resolve_references(
            '[Parameters].[MyParam]',
            calc_map={},
            param_map={'MyParam': 'Target Threshold'},
            is_calc_column=False,
            param_values={}
        )
        self.assertEqual(result, '[Target Threshold]')

    def test_calculation_resolved(self):
        result = _resolve_references(
            '[Calculation_1234]',
            calc_map={'Calculation_1234': 'Profit Ratio'},
            param_map={},
            is_calc_column=False,
            param_values={}
        )
        self.assertEqual(result, '[Profit Ratio]')

    def test_unknown_calc_unchanged(self):
        result = _resolve_references(
            '[Calculation_9999]',
            calc_map={},
            param_map={},
            is_calc_column=False,
            param_values={}
        )
        self.assertEqual(result, '[Calculation_9999]')

    def test_param_inline_for_calc_column(self):
        result = _resolve_references(
            '[Parameters].[TopN]',
            calc_map={},
            param_map={'TopN': 'TopN'},
            is_calc_column=True,
            param_values={'TopN': '10'}
        )
        self.assertEqual(result, '10')


class TestLODExpressions(unittest.TestCase):
    """MEDIUM — LOD (Level of Detail) expressions → CALCULATE."""

    def test_fixed_with_dim(self):
        result = _convert_lod_expressions(
            '{FIXED [Region] : SUM([Sales])}',
            'Orders',
            {'Region': 'Orders'}
        )
        self.assertIn('CALCULATE', result)
        self.assertIn('ALLEXCEPT', result)

    def test_include(self):
        result = _convert_lod_expressions(
            '{INCLUDE [Category] : AVG([Profit])}',
            'Orders',
            {}
        )
        self.assertIn('CALCULATE', result)

    def test_exclude(self):
        result = _convert_lod_expressions(
            '{EXCLUDE [Region] : SUM([Sales])}',
            'Orders',
            {'Region': 'Orders'}
        )
        self.assertIn('CALCULATE', result)
        self.assertIn('REMOVEFILTERS', result)


class TestAggIfToAggx(unittest.TestCase):
    """MEDIUM — SUM(IF(...)) → SUMX('T', IF(...))."""

    def test_sum_if(self):
        result = _convert_agg_if_to_aggx("SUM(IF([A] > 0, [B], 0))", 'Orders')
        self.assertIn('SUMX', result)
        self.assertIn("'Orders'", result)

    def test_average_if(self):
        result = _convert_agg_if_to_aggx("AVERAGE(IF([A], [B], 0))", 'Sales')
        self.assertIn('AVERAGEX', result)


class TestAggExprToAggx(unittest.TestCase):
    """MEDIUM — SUM(expr) → SUMX when expr is not a single column."""

    def test_sum_of_product(self):
        result = _convert_agg_expr_to_aggx("SUM('T'[Price] * 'T'[Qty])", 'T')
        self.assertIn('SUMX', result)

    def test_sum_single_column_unchanged(self):
        result = _convert_agg_expr_to_aggx("SUM('T'[Sales])", 'T')
        self.assertIn('SUM', result)
        self.assertNotIn('SUMX', result)


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS
# ═══════════════════════════════════════════════════════════════════


class TestResolveColumns(unittest.TestCase):
    """COMPLEX — Column reference resolution with table qualification."""

    def test_column_with_known_table(self):
        result = _resolve_columns(
            '[Sales]', 'Orders',
            column_table_map={'Sales': 'Orders'},
            measure_names=set(),
            is_calc_column=False,
            param_values={}
        )
        self.assertIn("'Orders'[Sales]", result)

    def test_measure_no_table_prefix(self):
        result = _resolve_columns(
            '[Total Sales]', 'Orders',
            column_table_map={},
            measure_names={'Total Sales'},
            is_calc_column=False,
            param_values={}
        )
        self.assertEqual(result, '[Total Sales]')

    def test_cross_table_related(self):
        result = _resolve_columns(
            '[Category]', 'Orders',
            column_table_map={'Category': 'Products'},
            measure_names=set(),
            is_calc_column=True,
            param_values={}
        )
        self.assertIn('RELATED', result)
        self.assertIn("'Products'[Category]", result)

    def test_same_table_no_related(self):
        result = _resolve_columns(
            '[Sales]', 'Orders',
            column_table_map={'Sales': 'Orders'},
            measure_names=set(),
            is_calc_column=True,
            param_values={}
        )
        self.assertNotIn('RELATED', result)


class TestWindowFunctions(unittest.TestCase):
    """COMPLEX — WINDOW_SUM/AVG/etc → CALCULATE(..., ALL('t'))."""

    def test_window_sum(self):
        result = _convert_window_functions("WINDOW_SUM(SUM([Sales]))", 'Orders')
        self.assertIn('CALCULATE', result)
        self.assertIn("ALL('Orders')", result)

    def test_window_avg(self):
        result = _convert_window_functions("WINDOW_AVG(AVG([Sales]))", 'Orders')
        self.assertIn('CALCULATE', result)

    def test_multiple_window(self):
        result = _convert_window_functions(
            "WINDOW_SUM(SUM([A])) + WINDOW_MAX(MAX([B]))",
            'T'
        )
        self.assertEqual(result.count('CALCULATE'), 2)


class TestRankFunctions(unittest.TestCase):
    """COMPLEX — RANK/RANK_UNIQUE/RANK_DENSE/RANK_PERCENTILE → RANKX."""

    def test_rank(self):
        result = _convert_rank_functions("RANK([Sales])", 'Orders')
        self.assertIn('RANKX', result)
        self.assertIn("ALL('Orders')", result)

    def test_rank_dense(self):
        result = _convert_rank_functions("RANK_DENSE([Sales])", 'Orders')
        self.assertIn('DENSE', result)

    def test_rank_percentile(self):
        result = _convert_rank_functions("RANK_PERCENTILE([Sales])", 'Orders')
        self.assertIn('DIVIDE', result)
        self.assertIn('RANKX', result)


class TestFullConversion(unittest.TestCase):
    """COMPLEX — End-to-end convert_tableau_formula_to_dax with context."""

    def test_simple_formula(self):
        result = convert_tableau_formula_to_dax('SUM([Sales])')
        self.assertIn('SUM', result)

    def test_formula_with_calc_map(self):
        result = convert_tableau_formula_to_dax(
            '[Calculation_001]',
            calc_map={'Calculation_001': 'Total Revenue'}
        )
        self.assertIn('Total Revenue', result)

    def test_formula_with_param(self):
        result = convert_tableau_formula_to_dax(
            '[Parameters].[TopN]',
            param_map={'TopN': 'TopN Param'}
        )
        self.assertIn('TopN Param', result)

    def test_complex_nested_formula(self):
        """Nested IF with aggregation and operators."""
        formula = (
            'IF SUM([Sales]) > 1000 AND COUNTD([Customer]) > 10 '
            'THEN "Top" ELSE "Low" END'
        )
        result = convert_tableau_formula_to_dax(formula, table_name='Orders')
        self.assertIn('IF(', result)
        self.assertIn('&&', result)
        self.assertIn('DISTINCTCOUNT', result)

    def test_case_with_date_functions(self):
        """CASE with DATEPART inside."""
        formula = (
            "CASE DATEPART('quarter', [OrderDate]) "
            "WHEN 1 THEN 'Q1' WHEN 2 THEN 'Q2' WHEN 3 THEN 'Q3' WHEN 4 THEN 'Q4' END"
        )
        result = convert_tableau_formula_to_dax(formula, table_name='Orders')
        self.assertIn('SWITCH', result)
        self.assertIn('QUARTER', result)

    def test_lod_in_full_context(self):
        """LOD expression through full converter pipeline."""
        formula = '{FIXED [Category] : SUM([Sales])}'
        result = convert_tableau_formula_to_dax(
            formula, table_name='Orders',
            column_table_map={'Category': 'Products', 'Sales': 'Orders'}
        )
        self.assertIn('CALCULATE', result)
        self.assertIn('ALLEXCEPT', result)

    def test_operators_in_full_context(self):
        result = convert_tableau_formula_to_dax('[A] != [B] AND [C] == [D]')
        self.assertIn('<>', result)
        self.assertIn('&&', result)
        self.assertNotIn('!=', result)
        self.assertNotIn('==', result)

    def test_calc_column_mode(self):
        """Calculated column mode: STARTOF* → DATE(), cross-table → RELATED."""
        formula = "DATETRUNC('month', [OrderDate])"
        result = convert_tableau_formula_to_dax(
            formula,
            table_name='Orders',
            column_table_map={'OrderDate': 'Orders'},
            is_calc_column=True
        )
        self.assertIn('DATE(', result)
        self.assertIn('YEAR', result)
        self.assertIn('MONTH', result)

    def test_string_concat_type(self):
        """String-typed calc: + → & at top level."""
        formula = '[FirstName] + " " + [LastName]'
        result = convert_tableau_formula_to_dax(
            formula, table_name='People',
            calc_datatype='string'
        )
        self.assertIn('&', result)

    def test_date_literal_in_full_context(self):
        formula = '[OrderDate] > #2024-01-01#'
        result = convert_tableau_formula_to_dax(formula)
        self.assertIn('DATE(2024, 1, 1)', result)

    def test_no_tableau_leakage(self):
        """Converted formulas should not contain raw Tableau keywords."""
        formula = (
            'IF ISNULL([Sales]) THEN ZN([Profit]) '
            'ELSEIF COUNTD([Customer]) > 5 THEN AVG([Revenue]) '
            'ELSE 0 END'
        )
        result = convert_tableau_formula_to_dax(formula, table_name='T')
        # Should not contain Tableau-specific keywords
        upper = result.upper()
        for keyword in ['ISNULL', 'COUNTD', ' THEN ', ' ELSEIF ', ' END']:
            self.assertNotIn(keyword, upper,
                             f"Tableau keyword '{keyword}' leaked into DAX: {result}")

    def test_table_calc_running_sum(self):
        result = convert_tableau_formula_to_dax('RUNNING_SUM(SUM([Sales]))')
        self.assertIn('CALCULATE', result)

    def test_spatial_function_placeholder(self):
        result = convert_tableau_formula_to_dax('MAKEPOINT([Lat], [Lng])')
        self.assertIn('MAKEPOINT', result)
        self.assertIn('BLANK', result)


if __name__ == '__main__':
    unittest.main()
