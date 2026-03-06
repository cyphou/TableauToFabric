"""
Extra coverage tests for conversion/filter_converter.py.

Targets uncovered branches: quantitative/top filter types, date filters
(relative/datepart/unknown), topN bottom, operator variants, range filter,
generate_filter_dax branches, filter_action, convert_filter_level context/datasource.
"""

import unittest

from conversion.filter_converter import (
    convert_filter_to_powerbi,
    convert_filter_type,
    convert_filter_operator,
    convert_filter_level,
    convert_date_filter,
    convert_date_period,
    convert_date_part,
    convert_topn_filter,
    convert_topn_direction,
    convert_range_filter,
    generate_filter_dax,
    convert_filter_action,
)


# ── convert_filter_to_powerbi specific type branches ─────────

class TestConvertFilterToPowerbi(unittest.TestCase):

    def test_date_type(self):
        f = convert_filter_to_powerbi({
            'field': 'OrderDate', 'type': 'date',
            'date_filter_type': 'range', 'start_date': '2024-01-01', 'end_date': '2024-12-31'
        })
        self.assertIn('dateFilterConfig', f)
        self.assertEqual(f['filterType'], 'relative')

    def test_top_type(self):
        f = convert_filter_to_powerbi({
            'field': 'Product', 'type': 'top', 'top_n': 5, 'direction': 'bottom'
        })
        self.assertIn('topNConfig', f)
        self.assertEqual(f['topNConfig']['direction'], 'Bottom')

    def test_quantitative_type(self):
        f = convert_filter_to_powerbi({
            'field': 'Sales', 'type': 'quantitative',
            'min_value': 10, 'max_value': 500
        })
        self.assertIn('rangeConfig', f)
        self.assertEqual(f['rangeConfig']['minValue'], 10)

    def test_default_categorical(self):
        f = convert_filter_to_powerbi({'field': 'City', 'values': ['NYC']})
        self.assertNotIn('dateFilterConfig', f)
        self.assertNotIn('topNConfig', f)
        self.assertNotIn('rangeConfig', f)


# ── convert_filter_type ──────────────────────────────────────

class TestConvertFilterType(unittest.TestCase):
    def test_wildcard(self):
        self.assertEqual(convert_filter_type('wildcard'), 'advanced')

    def test_context(self):
        self.assertEqual(convert_filter_type('context'), 'advanced')

    def test_unknown(self):
        self.assertEqual(convert_filter_type('custom'), 'basic')


# ── convert_filter_operator ──────────────────────────────────

class TestConvertFilterOperator(unittest.TestCase):
    def test_not_in(self):
        self.assertEqual(convert_filter_operator('not in'), 'NotIn')

    def test_not_equals_angle(self):
        self.assertEqual(convert_filter_operator('<>'), 'IsNot')

    def test_gte(self):
        self.assertEqual(convert_filter_operator('>='), 'GreaterThanOrEqual')

    def test_gte_text(self):
        self.assertEqual(convert_filter_operator('greater than or equal'), 'GreaterThanOrEqual')

    def test_lt(self):
        self.assertEqual(convert_filter_operator('<'), 'LessThan')

    def test_lte(self):
        self.assertEqual(convert_filter_operator('<='), 'LessThanOrEqual')

    def test_between(self):
        self.assertEqual(convert_filter_operator('between'), 'Between')

    def test_starts_with(self):
        self.assertEqual(convert_filter_operator('starts with'), 'StartsWith')

    def test_ends_with(self):
        self.assertEqual(convert_filter_operator('ends with'), 'EndsWith')

    def test_is_null(self):
        self.assertEqual(convert_filter_operator('is null'), 'IsBlank')

    def test_is_not_null(self):
        self.assertEqual(convert_filter_operator('is not null'), 'IsNotBlank')

    def test_unknown(self):
        self.assertEqual(convert_filter_operator('like'), 'In')


# ── convert_filter_level ─────────────────────────────────────

class TestConvertFilterLevel(unittest.TestCase):
    def test_context(self):
        self.assertEqual(convert_filter_level('context'), 'report')

    def test_datasource(self):
        self.assertEqual(convert_filter_level('datasource'), 'dataset')

    def test_unknown(self):
        self.assertEqual(convert_filter_level('global'), 'visual')


# ── convert_date_filter ──────────────────────────────────────

class TestConvertDateFilter(unittest.TestCase):
    def test_relative(self):
        result = convert_date_filter({
            'date_filter_type': 'relative', 'period': 'week', 'offset': -3
        })
        self.assertIn('relativeConfig', result)
        self.assertEqual(result['relativeConfig']['period'], 'Week')

    def test_datepart(self):
        result = convert_date_filter({
            'date_filter_type': 'datepart', 'date_part': 'weekday', 'values': [1, 2]
        })
        self.assertIn('datePartConfig', result)
        self.assertEqual(result['datePartConfig']['datePart'], 'DayOfWeek')

    def test_range(self):
        result = convert_date_filter({
            'date_filter_type': 'range', 'start_date': '2024-01-01'
        })
        self.assertIn('rangeConfig', result)

    def test_unknown_type(self):
        result = convert_date_filter({'date_filter_type': 'custom'})
        self.assertNotIn('relativeConfig', result)
        self.assertNotIn('datePartConfig', result)
        self.assertNotIn('rangeConfig', result)


# ── convert_date_period ──────────────────────────────────────

class TestConvertDatePeriod(unittest.TestCase):
    def test_quarter(self):
        self.assertEqual(convert_date_period('quarter'), 'Quarter')

    def test_week(self):
        self.assertEqual(convert_date_period('week'), 'Week')

    def test_unknown(self):
        self.assertEqual(convert_date_period('century'), 'Day')


# ── convert_date_part ────────────────────────────────────────

class TestConvertDatePart(unittest.TestCase):
    def test_weekday(self):
        self.assertEqual(convert_date_part('weekday'), 'DayOfWeek')

    def test_hour(self):
        self.assertEqual(convert_date_part('hour'), 'Hour')

    def test_unknown(self):
        self.assertEqual(convert_date_part('second'), 'Month')


# ── convert_topn_direction ───────────────────────────────────

class TestTopNDirection(unittest.TestCase):
    def test_bottom(self):
        self.assertEqual(convert_topn_direction('bottom'), 'Bottom')

    def test_unknown(self):
        self.assertEqual(convert_topn_direction('random'), 'Top')


# ── generate_filter_dax ─────────────────────────────────────

class TestGenerateFilterDax(unittest.TestCase):
    def test_in_operator(self):
        dax = generate_filter_dax({'field': '[City]', 'operator': 'in', 'values': ['NYC', 'LA']})
        self.assertIn('IN', dax)
        self.assertIn('"NYC"', dax)

    def test_in_numeric(self):
        dax = generate_filter_dax({'field': '[ID]', 'operator': 'in', 'values': [1, 2]})
        self.assertIn('1', dax)

    def test_equals(self):
        dax = generate_filter_dax({'field': '[City]', 'operator': 'equals', 'values': ['NYC']})
        self.assertEqual(dax, '[City] = "NYC"')

    def test_equals_empty(self):
        dax = generate_filter_dax({'field': '[City]', 'operator': 'equals', 'values': []})
        self.assertIn('=', dax)

    def test_greater_than(self):
        dax = generate_filter_dax({'field': '[Sales]', 'operator': 'greater than', 'values': [100]})
        self.assertEqual(dax, '[Sales] > 100')

    def test_less_than(self):
        dax = generate_filter_dax({'field': '[Sales]', 'operator': 'less than', 'values': [50]})
        self.assertEqual(dax, '[Sales] < 50')

    def test_between(self):
        dax = generate_filter_dax({'field': '[Sales]', 'operator': 'between', 'values': [10, 90]})
        self.assertIn('>=', dax)
        self.assertIn('<=', dax)

    def test_between_empty_values(self):
        dax = generate_filter_dax({'field': '[X]', 'operator': 'between', 'values': []})
        self.assertIn('0', dax)

    def test_between_one_value(self):
        dax = generate_filter_dax({'field': '[X]', 'operator': 'between', 'values': [5]})
        self.assertIn('5', dax)

    def test_contains(self):
        dax = generate_filter_dax({'field': '[Name]', 'operator': 'contains', 'values': ['abc']})
        self.assertIn('SEARCH', dax)

    def test_unknown_operator(self):
        dax = generate_filter_dax({'field': '[X]', 'operator': 'like', 'values': []})
        self.assertIn('BLANK()', dax)


# ── convert_filter_action ────────────────────────────────────

class TestConvertFilterAction(unittest.TestCase):
    def test_basic(self):
        action = {'source_sheet': 'S1', 'target_sheets': ['S2'], 'fields': ['City']}
        result = convert_filter_action(action)
        self.assertEqual(result['type'], 'filter')
        self.assertEqual(result['sourceVisual'], 'S1')


# ── convert_range_filter ─────────────────────────────────────

class TestConvertRangeFilter(unittest.TestCase):
    def test_basic(self):
        result = convert_range_filter({
            'min_value': 0, 'max_value': 100, 'include_min': False
        })
        self.assertEqual(result['minValue'], 0)
        self.assertFalse(result['includeMin'])


if __name__ == '__main__':
    unittest.main()
