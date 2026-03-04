"""
Tests for conversion/ modules.

SIMPLE:  Scalar mapping helpers (type, format, operator, color, etc.)
MEDIUM:  Object converters (worksheet, calculation, filter, parameter, dashboard)
COMPLEX: Full pipelines (convert_all orchestration, LOD expressions,
         CASE WHEN nesting, story navigation pages)
"""

import unittest

# ── Worksheet converter ──────────────────────────────────
from conversion.worksheet_converter import (
    convert_worksheet_to_visual,
    convert_data_fields,
    convert_aggregation,
    convert_data_type,
    convert_formatting,
    convert_color,
    convert_number_format,
    convert_tooltips,
    convert_interactions,
    convert_filters as ws_convert_filters,
)

# ── Calculation converter ────────────────────────────────
from conversion.calculation_converter import (
    convert_calculation_to_measure,
    convert_tableau_formula_to_dax as calc_convert_formula,
    convert_aggregations,
    convert_logic,
    convert_case_when,
    convert_text_functions,
    convert_date_functions,
    convert_math_functions,
    convert_lod_expressions,
    clean_formula,
    convert_format,
)

# ── Dashboard converter ──────────────────────────────────
from conversion.dashboard_converter import (
    convert_dashboard_to_report,
    convert_dashboard_pages,
    convert_dashboard_objects,
    convert_image_scaling,
    convert_dashboard_theme,
    convert_dashboard_filters,
    convert_filter_control_type,
    convert_dashboard_bookmarks,
)

# ── Filter converter ─────────────────────────────────────
from conversion.filter_converter import (
    convert_filter_to_powerbi,
    convert_filter_type as flt_convert_type,
    convert_filter_operator as flt_convert_operator,
    convert_filter_level,
    convert_date_filter,
    convert_date_period,
    convert_date_part,
    convert_topn_filter,
    convert_range_filter,
    generate_filter_dax,
)

# ── Parameter converter ──────────────────────────────────
from conversion.parameter_converter import (
    convert_parameter_to_powerbi,
    convert_param_type,
    determine_parameter_type,
    convert_parameter_config,
    generate_whatif_parameter,
    generate_dax_parameter_usage,
)

# ── Datasource converter ─────────────────────────────────
from conversion.datasource_converter import (
    convert_datasource_to_dataset,
    convert_columns,
    convert_column_datatype,
    convert_format_string,
    convert_data_category,
    convert_relationships,
    convert_cardinality,
    convert_filter_direction,
    convert_connection,
    convert_connection_type,
)

# ── Story converter ──────────────────────────────────────
from conversion.story_converter import (
    convert_story_to_bookmarks,
    convert_story_points,
    generate_navigation_buttons,
    generate_story_navigation_page,
)


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS — Scalar mappings
# ═══════════════════════════════════════════════════════════════════


class TestConvertAggregation(unittest.TestCase):
    def test_sum(self):
        self.assertEqual(convert_aggregation('sum'), 'Sum')

    def test_countd(self):
        self.assertEqual(convert_aggregation('countd'), 'DistinctCount')

    def test_avg(self):
        self.assertEqual(convert_aggregation('avg'), 'Average')

    def test_unknown(self):
        result = convert_aggregation('strange')
        self.assertIsInstance(result, str)


class TestConvertDataType(unittest.TestCase):
    def test_integer(self):
        self.assertEqual(convert_data_type('integer'), 'Whole Number')

    def test_real(self):
        self.assertEqual(convert_data_type('real'), 'Decimal Number')

    def test_string(self):
        self.assertEqual(convert_data_type('string'), 'Text')

    def test_boolean(self):
        self.assertEqual(convert_data_type('boolean'), 'True/False')


class TestConvertColor(unittest.TestCase):
    def test_hex_color(self):
        self.assertEqual(convert_color('#FF0000'), '#FF0000')

    def test_non_hex(self):
        self.assertEqual(convert_color('red'), '#000000')

    def test_empty(self):
        self.assertEqual(convert_color(''), '#000000')


class TestConvertNumberFormat(unittest.TestCase):
    def test_known_format(self):
        result = convert_number_format('d')
        self.assertIsInstance(result, str)

    def test_none_format(self):
        result = convert_number_format(None)
        self.assertIsNotNone(result)


class TestConvertColumnDatatype(unittest.TestCase):
    def test_real(self):
        self.assertEqual(convert_column_datatype('real'), 'double')

    def test_integer(self):
        self.assertEqual(convert_column_datatype('integer'), 'int64')

    def test_string(self):
        self.assertEqual(convert_column_datatype('string'), 'string')

    def test_spatial(self):
        self.assertEqual(convert_column_datatype('spatial'), 'geography')


class TestConvertDataCategory(unittest.TestCase):
    def test_latitude(self):
        self.assertEqual(convert_data_category('latitude'), 'Latitude')

    def test_city(self):
        self.assertEqual(convert_data_category('city'), 'City')

    def test_dimension(self):
        self.assertIsNone(convert_data_category('dimension'))

    def test_measure(self):
        self.assertIsNone(convert_data_category('measure'))


class TestConvertCardinality(unittest.TestCase):
    def test_many_to_one(self):
        self.assertEqual(convert_cardinality('many-to-one'), 'manyToOne')

    def test_one_to_many(self):
        self.assertEqual(convert_cardinality('one-to-many'), 'oneToMany')


class TestConvertFilterDirection(unittest.TestCase):
    def test_single(self):
        self.assertEqual(convert_filter_direction('single'), 'oneDirection')

    def test_both(self):
        self.assertEqual(convert_filter_direction('both'), 'bothDirections')


class TestConvertConnectionType(unittest.TestCase):
    def test_sqlserver(self):
        self.assertEqual(convert_connection_type('sqlserver'), 'SQL Server')

    def test_postgres(self):
        self.assertEqual(convert_connection_type('postgres'), 'PostgreSQL')

    def test_snowflake(self):
        self.assertEqual(convert_connection_type('snowflake'), 'Snowflake')

    def test_unknown(self):
        self.assertEqual(convert_connection_type('xyzdb'), 'Unknown')


class TestConvertImageScaling(unittest.TestCase):
    def test_fit(self):
        self.assertEqual(convert_image_scaling('fit'), 'Fit')

    def test_fill(self):
        self.assertEqual(convert_image_scaling('fill'), 'Fill')

    def test_stretch(self):
        self.assertEqual(convert_image_scaling('stretch'), 'Fill')


class TestConvertFilterControlType(unittest.TestCase):
    def test_list(self):
        self.assertEqual(convert_filter_control_type('list'), 'dropdown')

    def test_date(self):
        self.assertEqual(convert_filter_control_type('date'), 'relativeDateFilter')


class TestFilterConverterOperators(unittest.TestCase):
    def test_equals(self):
        self.assertEqual(flt_convert_operator('equals'), 'Is')

    def test_not_equals(self):
        self.assertEqual(flt_convert_operator('not equals'), 'IsNot')

    def test_symbol_equals(self):
        self.assertEqual(flt_convert_operator('='), 'Is')

    def test_contains(self):
        self.assertEqual(flt_convert_operator('contains'), 'Contains')


class TestFilterConverterLevel(unittest.TestCase):
    def test_worksheet(self):
        self.assertEqual(convert_filter_level('worksheet'), 'visual')

    def test_workbook(self):
        self.assertEqual(convert_filter_level('workbook'), 'report')

    def test_datasource(self):
        self.assertEqual(convert_filter_level('datasource'), 'dataset')


class TestConvertDatePeriod(unittest.TestCase):
    def test_year(self):
        self.assertEqual(convert_date_period('year'), 'Year')

    def test_month(self):
        self.assertEqual(convert_date_period('month'), 'Month')


class TestConvertDatePart(unittest.TestCase):
    def test_weekday(self):
        self.assertEqual(convert_date_part('weekday'), 'DayOfWeek')


class TestConvertParamType(unittest.TestCase):
    def test_real(self):
        self.assertEqual(convert_param_type('real'), 'decimal')

    def test_integer(self):
        self.assertEqual(convert_param_type('integer'), 'number')


class TestConvertFormatCalc(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(convert_format(None))

    def test_known(self):
        result = convert_format('n0')
        self.assertIsInstance(result, str)


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS — Object converters
# ═══════════════════════════════════════════════════════════════════


class TestWorksheetConverter(unittest.TestCase):
    """MEDIUM — worksheet to visual conversion."""

    def test_basic_bar_chart(self):
        ws = {
            'name': 'Sales', 'chart_type': 'bar',
            'fields': [{'name': 'Region', 'shelf': 'rows', 'datatype': 'string'}],
            'filters': [], 'formatting': {}, 'tooltips': [], 'actions': [],
        }
        result = convert_worksheet_to_visual(ws)
        self.assertIn('visualType', result)
        self.assertEqual(result['name'], 'Sales')

    def test_unknown_chart_defaults_table(self):
        ws = {
            'name': 'X', 'chart_type': 'unknownType',
            'fields': [], 'filters': [], 'formatting': {},
            'tooltips': [], 'actions': [],
        }
        result = convert_worksheet_to_visual(ws)
        self.assertEqual(result['visualType'], 'table')


class TestConvertDataFields(unittest.TestCase):
    """MEDIUM — field shelf mapping."""

    def test_rows_to_axis(self):
        fields = [{'name': 'Region', 'shelf': 'rows', 'datatype': 'string',
                    'aggregation': 'none'}]
        result = convert_data_fields(fields)
        self.assertTrue(len(result) >= 1)
        self.assertEqual(result[0]['role'], 'axis')

    def test_columns_to_legend(self):
        fields = [{'name': 'Category', 'shelf': 'columns', 'datatype': 'string',
                    'aggregation': 'none'}]
        result = convert_data_fields(fields)
        self.assertEqual(result[0]['role'], 'legend')


class TestConvertFormatting(unittest.TestCase):
    """MEDIUM — formatting conversion."""

    def test_basic(self):
        fmt = {'font_size': '12', 'color': '#333'}
        result = convert_formatting(fmt)
        self.assertIsInstance(result, dict)

    def test_empty(self):
        result = convert_formatting({})
        self.assertIsInstance(result, dict)


class TestConvertTooltips(unittest.TestCase):
    """MEDIUM — tooltip conversion."""

    def test_basic(self):
        tips = [{'field': 'Sales', 'label': 'Total Sales'}]
        result = convert_tooltips(tips)
        self.assertEqual(len(result), 1)


class TestConvertInteractions(unittest.TestCase):
    """MEDIUM — interaction/action conversion."""

    def test_filter_action(self):
        actions = [{'type': 'filter', 'source': 'V1', 'target': 'V2'}]
        result = convert_interactions(actions)
        self.assertTrue(len(result) >= 1)

    def test_unknown_action_skipped(self):
        actions = [{'type': 'unknownAction'}]
        result = convert_interactions(actions)
        self.assertEqual(len(result), 0)


class TestCalculationConverter(unittest.TestCase):
    """MEDIUM — calculation conversion."""

    def test_basic_measure(self):
        calc = {'name': '[Sales]', 'caption': 'Total Sales',
                'formula': 'SUM([Amount])', 'role': 'measure', 'datatype': 'real'}
        result = convert_calculation_to_measure(calc)
        self.assertIn('name', result)
        self.assertIn('expression', result)

    def test_aggregation_conversion(self):
        result = convert_aggregations('COUNTD([Customer])')
        self.assertIn('DISTINCTCOUNT', result)

    def test_logic_if_then(self):
        result = convert_logic('IF [X] > 0 THEN "Yes" ELSE "No" END')
        self.assertIn('IF(', result)


class TestCaseWhenConversion(unittest.TestCase):
    """MEDIUM — CASE WHEN to nested IF."""

    def test_single_when(self):
        formula = 'CASE WHEN [Status] = "Active" THEN "Y" ELSE "N" END'
        result = convert_case_when(formula)
        self.assertIn('IF(', result)

    def test_multiple_when(self):
        formula = 'CASE WHEN [X] > 10 THEN "A" WHEN [X] > 5 THEN "B" ELSE "C" END'
        result = convert_case_when(formula)
        self.assertEqual(result.count('IF('), 2)

    def test_no_else(self):
        formula = 'CASE WHEN [X] = 1 THEN "A" END'
        result = convert_case_when(formula)
        self.assertIn('BLANK()', result)


class TestTextFunctions(unittest.TestCase):
    """MEDIUM — text function conversion."""

    def test_contains(self):
        result = convert_text_functions('CONTAINS([Name], "abc")')
        self.assertIn('SEARCH', result)

    def test_replace(self):
        result = convert_text_functions("REPLACE([X], 'old', 'new')")
        self.assertIn('SUBSTITUTE', result)


class TestDateFunctions(unittest.TestCase):
    """MEDIUM — date function conversion."""

    def test_dateadd(self):
        result = convert_date_functions("DATEADD('month', 1, [Date])")
        self.assertIn('DATEADD', result)


class TestMathFunctions(unittest.TestCase):
    """MEDIUM — math function conversion."""

    def test_ceiling(self):
        result = convert_math_functions('CEILING([X])')
        self.assertIn('ROUNDUP', result)

    def test_floor(self):
        result = convert_math_functions('FLOOR([X])')
        self.assertIn('ROUNDDOWN', result)


class TestCleanFormula(unittest.TestCase):
    """MEDIUM — formula cleanup."""

    def test_collapses_whitespace(self):
        result = clean_formula('SUM(   [X]   )')
        self.assertNotIn('   ', result)


class TestDashboardConverter(unittest.TestCase):
    """MEDIUM — dashboard conversion."""

    def test_basic_dashboard(self):
        db = {
            'name': 'Overview',
            'size': {'width': 1280, 'height': 720},
            'objects': [
                {'type': 'worksheet', 'name': 'Sales',
                 'position': {'x': 0, 'y': 0, 'w': 640, 'h': 360}},
            ],
            'filters': [], 'theme': {},
        }
        result = convert_dashboard_to_report(db)
        self.assertIn('name', result)
        self.assertIn('pages', result)

    def test_dashboard_objects_dispatch(self):
        objects = [
            {'type': 'worksheet', 'name': 'V1', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 100}},
            {'type': 'text', 'content': 'Hello', 'position': {'x': 100, 'y': 0, 'w': 100, 'h': 50}},
            {'type': 'image', 'source': 'img.png', 'position': {'x': 200, 'y': 0, 'w': 100, 'h': 100}},
        ]
        result = convert_dashboard_objects(objects)
        self.assertEqual(len(result), 3)


class TestDashboardTheme(unittest.TestCase):
    """MEDIUM — theme conversion."""

    def test_default_theme(self):
        result = convert_dashboard_theme({})
        self.assertIn('dataColors', result)

    def test_custom_colors(self):
        result = convert_dashboard_theme({'colors': ['#AA0000', '#00AA00']})
        self.assertEqual(result['dataColors'][0], '#AA0000')


class TestDashboardBookmarks(unittest.TestCase):
    """MEDIUM — bookmarks from stories."""

    def test_basic_bookmarks(self):
        stories = [{'name': 'S1', 'caption': 'First', 'objects': []}]
        result = convert_dashboard_bookmarks(stories)
        self.assertTrue(len(result) >= 1)


class TestFilterConverter(unittest.TestCase):
    """MEDIUM — filter conversion."""

    def test_basic_categorical(self):
        f = {'field': 'Region', 'type': 'categorical',
             'values': ['East'], 'scope': 'worksheet'}
        result = convert_filter_to_powerbi(f)
        self.assertIn('filterType', result)
        self.assertIn('field', result)

    def test_filter_type_mapping(self):
        self.assertEqual(flt_convert_type('categorical'), 'basic')
        self.assertEqual(flt_convert_type('top'), 'topN')
        self.assertEqual(flt_convert_type('context'), 'advanced')


class TestGenerateFilterDAX(unittest.TestCase):
    """MEDIUM — filter DAX generation."""

    def test_in_operator(self):
        f = {'field': 'Region', 'operator': 'in', 'values': ['East', 'West']}
        result = generate_filter_dax(f)
        self.assertIn('IN', result)

    def test_equals_operator(self):
        f = {'field': 'Status', 'operator': 'equals', 'values': ['Active']}
        result = generate_filter_dax(f)
        self.assertIn('=', result)

    def test_contains_operator(self):
        f = {'field': 'Name', 'operator': 'contains', 'values': ['abc']}
        result = generate_filter_dax(f)
        self.assertIn('SEARCH', result)

    def test_between_operator(self):
        f = {'field': 'Price', 'operator': 'between', 'values': [10, 100]}
        result = generate_filter_dax(f)
        self.assertIn('>=', result)
        self.assertIn('<=', result)


class TestParameterConverter(unittest.TestCase):
    """MEDIUM — parameter conversion."""

    def test_basic_param(self):
        p = {'name': 'Target', 'caption': 'Target Value', 'datatype': 'integer',
             'value': '100', 'allowable_values_type': 'any', 'used_in_query': False}
        result = convert_parameter_to_powerbi(p)
        self.assertIn('name', result)

    def test_determine_type_whatif(self):
        p = {'datatype': 'integer', 'allowable_values_type': 'range',
             'used_in_query': False}
        self.assertEqual(determine_parameter_type(p), 'what-if')

    def test_determine_type_query(self):
        p = {'datatype': 'string', 'allowable_values_type': 'any',
             'used_in_query': True}
        self.assertEqual(determine_parameter_type(p), 'query')


class TestWhatIfParameter(unittest.TestCase):
    """MEDIUM — What-If parameter generation."""

    def test_generates_table(self):
        p = {'name': 'Discount', 'caption': 'Discount', 'datatype': 'real',
             'value': '0.1', 'min_value': '0', 'max_value': '1', 'step_size': '0.05'}
        result = generate_whatif_parameter(p)
        self.assertIn('tableName', result)
        self.assertIn('measureName', result)

    def test_dax_usage(self):
        p = {'caption': 'Target', 'type': 'what-if'}
        calc = {'formula': 'SUM([Amount]) > [Target]'}
        result = generate_dax_parameter_usage(p, calc)
        self.assertIn('SELECTEDVALUE', result)


class TestDatasourceConverter(unittest.TestCase):
    """MEDIUM — datasource to dataset."""

    def test_basic_conversion(self):
        ds = {
            'name': 'DS1',
            'connection': {'class': 'sqlserver', 'server': 'srv', 'dbname': 'db', 'live': False},
            'tables': [{'name': 'T1', 'columns': [{'name': 'X', 'datatype': 'string'}]}],
            'relationships': [],
            'calculations': [],
        }
        result = convert_datasource_to_dataset(ds)
        self.assertIn('tables', result)
        self.assertIn('dataSource', result)

    def test_connection_live(self):
        conn = {'class': 'sqlserver', 'server': 'srv', 'dbname': 'db', 'live': True}
        result = convert_connection(conn)
        self.assertEqual(result['connectionMode'], 'DirectQuery')

    def test_connection_import(self):
        conn = {'class': 'sqlserver', 'server': 'srv', 'dbname': 'db', 'live': False}
        result = convert_connection(conn)
        self.assertEqual(result['connectionMode'], 'Import')


class TestConvertFormatString(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(convert_format_string(None))

    def test_empty(self):
        self.assertIsNone(convert_format_string(''))

    def test_known(self):
        result = convert_format_string('n0')
        self.assertIsInstance(result, str)


class TestConvertRelationships(unittest.TestCase):
    def test_basic(self):
        rels = [{'from_table': 'A', 'from_column': 'X',
                 'to_table': 'B', 'to_column': 'X',
                 'cardinality': 'many-to-one', 'filter_direction': 'single'}]
        result = convert_relationships(rels)
        self.assertEqual(len(result), 1)

    def test_empty(self):
        self.assertEqual(convert_relationships([]), [])


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS — Full pipelines
# ═══════════════════════════════════════════════════════════════════


class TestLODExpressions(unittest.TestCase):
    """COMPLEX — LOD expression conversion."""

    def test_fixed(self):
        formula = '{FIXED [Region] : SUM([Sales])}'
        result = convert_lod_expressions(formula)
        self.assertIn('CALCULATE', result)
        self.assertIn('ALL', result)

    def test_include(self):
        formula = '{INCLUDE [Category] : AVG([Price])}'
        result = convert_lod_expressions(formula)
        self.assertIn('CALCULATE', result)

    def test_exclude(self):
        formula = '{EXCLUDE [Region] : SUM([Sales])}'
        result = convert_lod_expressions(formula)
        self.assertIn('ALLEXCEPT', result)


class TestFullFormulaConversion(unittest.TestCase):
    """COMPLEX — Full formula conversion pipeline."""

    def test_sum(self):
        result = calc_convert_formula('SUM([Sales])')
        self.assertIn('SUM', result)

    def test_countd(self):
        result = calc_convert_formula('COUNTD([Customer])')
        self.assertIn('DISTINCTCOUNT', result)

    def test_if_then_else(self):
        result = calc_convert_formula('IF [X] > 0 THEN "Yes" ELSE "No" END')
        self.assertIn('IF(', result)

    def test_empty_formula(self):
        result = calc_convert_formula('')
        self.assertEqual(result, '')

    def test_none_formula(self):
        result = calc_convert_formula(None)
        self.assertEqual(result, '')


class TestStoryConverter(unittest.TestCase):
    """COMPLEX — Story to bookmarks + navigation."""

    def test_convert_story(self):
        story = {
            'name': 'Story1', 'caption': 'My Story',
            'story_points': [
                {'name': 'P1', 'caption': 'Point One', 'objects': [],
                 'filters': [], 'annotations': []},
                {'name': 'P2', 'caption': 'Point Two', 'objects': [],
                 'filters': [], 'annotations': []},
            ],
        }
        result = convert_story_to_bookmarks(story)
        self.assertIn('bookmarks', result)
        self.assertIn('navigationButtons', result)

    def test_navigation_buttons(self):
        points = [
            {'name': 'P1', 'caption': 'Point 1'},
            {'name': 'P2', 'caption': 'Point 2'},
            {'name': 'P3', 'caption': 'Point 3'},
        ]
        buttons = generate_navigation_buttons(points)
        # Should have prev + next + one per point
        self.assertTrue(len(buttons) >= 5)

    def test_navigation_page(self):
        story = {
            'name': 'S', 'caption': 'S',
            'story_points': [
                {'name': 'P1', 'caption': 'A', 'description': ''},
                {'name': 'P2', 'caption': 'B', 'description': ''},
                {'name': 'P3', 'caption': 'C', 'description': ''},
                {'name': 'P4', 'caption': 'D', 'description': ''},
            ],
        }
        page = generate_story_navigation_page(story)
        self.assertIn('elements', page)
        self.assertTrue(len(page['elements']) >= 4)


class TestConvertStoryPoints(unittest.TestCase):
    """COMPLEX — story point details."""

    def test_basic_points(self):
        points = [
            {'name': 'P1', 'caption': 'First', 'objects': [],
             'filters': [], 'annotations': []},
        ]
        result = convert_story_points(points)
        self.assertEqual(len(result), 1)
        self.assertIn('name', result[0])
        self.assertIn('displayName', result[0])


class TestDashboardObjectsFull(unittest.TestCase):
    """COMPLEX — all dashboard object types."""

    def test_all_types(self):
        objects = [
            {'type': 'worksheet', 'name': 'V1', 'position': {'x': 0, 'y': 0, 'w': 300, 'h': 200}},
            {'type': 'text', 'content': 'Title', 'position': {'x': 0, 'y': 200, 'w': 300, 'h': 50}},
            {'type': 'image', 'source': 'logo.png', 'position': {'x': 0, 'y': 250, 'w': 100, 'h': 100}},
            {'type': 'web', 'url': 'https://example.com', 'position': {'x': 300, 'y': 0, 'w': 300, 'h': 200}},
            {'type': 'blank', 'position': {'x': 300, 'y': 200, 'w': 300, 'h': 50}},
        ]
        result = convert_dashboard_objects(objects)
        self.assertEqual(len(result), 5)

    def test_unknown_type_handled(self):
        objects = [{'type': 'unknown_widget', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 100}}]
        result = convert_dashboard_objects(objects)
        self.assertEqual(len(result), 1)


class TestTopNFilter(unittest.TestCase):
    """COMPLEX — TopN filter conversion."""

    def test_top_filter(self):
        f = {'count': 10, 'direction': 'top', 'by_field': 'Sales'}
        result = convert_topn_filter(f)
        self.assertIn('count', result)
        self.assertEqual(result['direction'], 'Top')

    def test_bottom_filter(self):
        f = {'count': 5, 'direction': 'bottom', 'by_field': 'Sales'}
        result = convert_topn_filter(f)
        self.assertEqual(result['direction'], 'Bottom')


class TestRangeFilter(unittest.TestCase):
    """COMPLEX — Range filter conversion."""

    def test_range_filter(self):
        f = {'min_value': 0, 'max_value': 100, 'include_min': True, 'include_max': False}
        result = convert_range_filter(f)
        self.assertEqual(result['minValue'], 0)
        self.assertEqual(result['maxValue'], 100)


class TestDateFilter(unittest.TestCase):
    """COMPLEX — Date filter conversion."""

    def test_relative_date(self):
        f = {'date_filter_type': 'relative', 'period': 'year', 'anchor': 'now',
             'range_n': 1, 'date_period': 'year'}
        result = convert_date_filter(f)
        self.assertIn('filterType', result)


if __name__ == '__main__':
    unittest.main()
