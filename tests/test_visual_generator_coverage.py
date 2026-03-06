"""
Extra coverage tests for fabric_import/visual_generator.py.

Targets uncovered branches: custom visual GUID injection, dataFields fallback,
colorBy mode, conditional formatting, shape encoding, pages shelf, filters,
sort state, reference lines, action button page+url, slicer syncGroup,
cross-filter disable, drilldown flag, build_query_state (tableEx,
measure_lookup, small multiples, color legend, tooltips, drilldown),
_build_visual_filters topN, create_projections, create_prototype_query,
generate_visual_containers grid wrap, resolve_visual_type empty.
"""

import unittest

from fabric_import.visual_generator import (
    resolve_visual_type,
    generate_visual_containers,
    create_visual_container,
    build_query_state,
    _build_visual_filters,
    create_projections,
    create_prototype_query,
    create_filters_config,
    create_page_layout,
)


# ── resolve_visual_type ──────────────────────────────────────

class TestResolveVisualType(unittest.TestCase):

    def test_none(self):
        self.assertEqual(resolve_visual_type(None), 'tableEx')

    def test_empty(self):
        self.assertEqual(resolve_visual_type(''), 'tableEx')

    def test_known(self):
        self.assertEqual(resolve_visual_type('piechart'), 'pieChart')

    def test_unknown(self):
        self.assertEqual(resolve_visual_type('xyzChart'), 'tableEx')


# ── generate_visual_containers ───────────────────────────────

class TestGenerateVisualContainers(unittest.TestCase):

    def test_empty(self):
        result = generate_visual_containers([])
        self.assertEqual(result, [])

    def test_basic(self):
        ws = [{'name': 'V1', 'visualType': 'bar',
               'dimensions': [{'field': 'City'}], 'measures': []}]
        result = generate_visual_containers(ws, col_table_map={'City': 'T'})
        self.assertEqual(len(result), 1)

    def test_max_twenty(self):
        ws = [{'name': f'V{i}', 'visualType': 'bar',
               'dimensions': [{'field': 'City'}], 'measures': []}
              for i in range(25)]
        result = generate_visual_containers(ws, col_table_map={'City': 'T'})
        self.assertEqual(len(result), 20)

    def test_grid_wrap(self):
        ws = [{'name': f'V{i}', 'visualType': 'bar',
               'dimensions': [{'field': 'City'}], 'measures': []}
              for i in range(5)]
        result = generate_visual_containers(ws, col_table_map={'City': 'T'})
        # After 3 visuals x_pos wraps (300+20=320 * 3 > 1000)
        positions = [r['position'] for r in result]
        # Check Y changes after wrap
        y_values = set(p['y'] for p in positions)
        self.assertTrue(len(y_values) >= 2)


# ── create_visual_container branches ─────────────────────────

class TestCreateVisualContainer(unittest.TestCase):

    def test_custom_visual_guid(self):
        ws = {'name': 'WC', 'visualType': 'wordcloud',
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('customVisualGuid', vc['visual'])

    def test_datafields_fallback(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'dataFields': [{'name': 'Sales', 'role': 'values'}]}
        vc = create_visual_container(ws)
        self.assertIn('projections', vc['visual'])
        self.assertIn('prototypeQuery', vc['visual'])

    def test_subtitle(self):
        ws = {'name': 'V', 'visualType': 'bar', 'subtitle': 'Sub text',
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('subTitle', vc['visual']['vcObjects'])

    def test_color_by_measure(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'colorBy': {'mode': 'byMeasure'},
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('dataPoint', vc['visual'].get('objects', {}))

    def test_conditional_formatting(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'conditionalFormatting': [{'rule': 'x'}],
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('dataPoint', vc['visual'].get('objects', {}))

    def test_shape_encoding(self):
        ws = {'name': 'V', 'visualType': 'scatterchart',
              'mark_encoding': {'shape': {'type': 'diamond'}},
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        dp = vc['visual']['objects']['dataPoint'][0]['properties']
        self.assertIn('markerShape', dp)

    def test_pages_shelf_play(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'pages_shelf': {'field': 'Year'},
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T', 'Year': 'T'})
        self.assertIn('play', vc['visual'].get('objects', {}))

    def test_pages_shelf_slicer_no_play(self):
        ws = {'name': 'V', 'visualType': 'slicer',
              'pages_shelf': {'field': 'Year'},
              'dimensions': [{'field': 'Year'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'Year': 'T'})
        self.assertNotIn('play', vc['visual'].get('objects', {}))

    def test_visual_filters(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'filters': [{'field': 'City', 'type': 'basic', 'values': ['NYC']}],
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('filters', vc['visual'])

    def test_sort_by_list(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'sortBy': [{'field': 'City', 'direction': 'descending'}],
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        sort = vc['visual']['query']['sortDefinition']['sort']
        self.assertEqual(sort[0]['direction'], 2)

    def test_sort_by_single_dict(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'sorting': {'column': 'City', 'direction': 'ascending'},
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        sort = vc['visual']['query']['sortDefinition']['sort']
        self.assertEqual(sort[0]['direction'], 1)

    def test_reference_lines(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'referenceLines': [{'value': 50, 'label': 'Avg', 'color': '#F00'}],
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('constantLine', vc['visual']['objects'])

    def test_action_button_page(self):
        ws = {'name': 'Nav', 'visualType': 'actionbutton',
              'navigation': {'sheet': 'Page2'},
              'dimensions': [], 'measures': []}
        vc = create_visual_container(ws)
        action = vc['visual']['objects']['action'][0]['properties']
        self.assertIn('PageNavigation', str(action['type']))

    def test_action_button_url(self):
        ws = {'name': 'Link', 'visualType': 'actionbutton',
              'action': {'url': 'https://example.com'},
              'dimensions': [], 'measures': []}
        vc = create_visual_container(ws)
        action = vc['visual']['objects']['action'][0]['properties']
        self.assertIn('WebUrl', str(action['type']))

    def test_slicer_sync_group(self):
        ws = {'name': 'Slicer', 'visualType': 'slicer',
              'syncGroup': 'group1',
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertIn('syncGroup', vc)
        self.assertEqual(vc['syncGroup']['groupName'], 'group1')

    def test_cross_filter_disabled(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'interactions': {'disabled': True},
              'dimensions': [{'field': 'City'}], 'measures': []}
        vc = create_visual_container(ws, col_table_map={'City': 'T'})
        self.assertTrue(vc['filterConfig']['disabled'])


# ── build_query_state ────────────────────────────────────────

class TestBuildQueryState(unittest.TestCase):

    def test_tableex_special(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Sales', 'expression': 'Sum(Sales)'}]
        qs = build_query_state('tableEx', dims, meas,
                               {'City': 'T', 'Sales': 'T'}, {})
        self.assertIn('Values', qs)

    def test_measure_lookup_hit(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Total', 'expression': 'SUM(Sales)'}]
        ml = {'Total': ('Orders', 'SUM([Sales])')}
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'Orders'}, ml)
        self.assertIn('Y', qs)
        # Should use Measure ref not Aggregation
        y_proj = qs['Y']['projections'][0]
        self.assertIn('Measure', y_proj['field'])

    def test_no_match_regex(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'X', 'expression': 'custom_calc'}]
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'T', 'custom_calc': 'T'}, {})
        self.assertIn('Y', qs)

    def test_small_multiples(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Sales', 'expression': 'Sum(Sales)'}]
        ws = {'small_multiples': 'Region'}
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'T', 'Sales': 'T', 'Region': 'T'}, {},
                               worksheet=ws)
        self.assertIn('SmallMultiple', qs)

    def test_color_legend(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Sales', 'expression': 'Sum(Sales)'}]
        ws = {'mark_encoding': {'color': {'field': 'Region'}}}
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'T', 'Sales': 'T', 'Region': 'T'}, {},
                               worksheet=ws)
        # Should have Legend or Series
        self.assertTrue('Legend' in qs or 'Series' in qs)

    def test_tooltips(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Sales', 'expression': 'Sum(Sales)'}]
        ws = {'tooltips': [{'field': 'Profit'}]}
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'T', 'Sales': 'T', 'Profit': 'T'}, {},
                               worksheet=ws)
        self.assertIn('Tooltips', qs)

    def test_drilldown_flag(self):
        dims = [{'field': 'City'}]
        meas = [{'label': 'Sales', 'expression': 'Sum(Sales)'}]
        ws = {'hierarchies': [{'name': 'Geo'}]}
        qs = build_query_state('clusteredBarChart', dims, meas,
                               {'City': 'T', 'Sales': 'T'}, {},
                               worksheet=ws)
        self.assertTrue(qs.get('_drilldown'))

    def test_no_roles_returns_none(self):
        qs = build_query_state('unknownVisual', [], [], {}, {})
        self.assertIsNone(qs)

    def test_no_projs_returns_none(self):
        qs = build_query_state('clusteredBarChart', [], [], {}, {})
        self.assertIsNone(qs)

    def test_empty_col_table_map(self):
        dims = [{'field': 'City'}]
        qs = build_query_state('clusteredBarChart', dims, [], {}, {})
        # No table found → no projections → None
        self.assertIsNone(qs)


# ── _build_visual_filters ────────────────────────────────────

class TestBuildVisualFilters(unittest.TestCase):

    def test_topn(self):
        filters = [{'field': 'Product', 'type': 'topN', 'count': 5}]
        result = _build_visual_filters(filters, {'Product': 'T'})
        self.assertEqual(result[0]['type'], 'TopN')

    def test_categorical(self):
        filters = [{'field': 'City', 'type': 'basic', 'values': ['NYC']}]
        result = _build_visual_filters(filters, {'City': 'T'})
        self.assertEqual(result[0]['type'], 'Categorical')

    def test_empty_values_skipped(self):
        filters = [{'field': 'City', 'type': 'basic', 'values': []}]
        result = _build_visual_filters(filters, {'City': 'T'})
        self.assertEqual(len(result), 0)

    # ── Federated prefix handling ──

    def test_federated_prefix_cleaned(self):
        """federated.HASH.none:City:qk must resolve to clean column name."""
        filters = [{'field': 'federated.abc.none:City:qk', 'type': 'basic',
                     'values': ['NYC']}]
        result = _build_visual_filters(filters, {'City': 'T'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['expression']['Column']['Property'], 'City')

    def test_federated_prefix_topn(self):
        """TopN filter with federated prefix must use clean field name."""
        filters = [{'field': 'federated.hash.sum:Sales:nk', 'type': 'topN',
                     'count': 10}]
        result = _build_visual_filters(filters, {'Sales': 'T'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['expression']['Column']['Property'], 'Sales')

    # ── Measure Names / Measure Values skipping ──

    def test_measure_names_skipped(self):
        """:Measure Names filter must be dropped."""
        filters = [{'field': ':Measure Names', 'type': 'basic',
                     'values': ['sum:Sales']}]
        result = _build_visual_filters(filters, {})
        self.assertEqual(len(result), 0)

    def test_measure_values_skipped(self):
        filters = [{'field': ':Measure Values', 'type': 'basic'}]
        result = _build_visual_filters(filters, {})
        self.assertEqual(len(result), 0)

    def test_federated_measure_names_skipped(self):
        """federated.HASH.:Measure Names must also be skipped."""
        filters = [{'field': 'federated.abc.:Measure Names', 'type': 'basic',
                     'values': ['x']}]
        result = _build_visual_filters(filters, {})
        self.assertEqual(len(result), 0)

    def test_valid_plus_measure_names_mixed(self):
        """Valid + Measure Names → only valid kept."""
        filters = [
            {'field': 'City', 'type': 'basic', 'values': ['NYC']},
            {'field': ':Measure Names', 'type': 'basic', 'values': ['x']},
        ]
        result = _build_visual_filters(filters, {'City': 'T'})
        self.assertEqual(len(result), 1)


# ── create_projections ───────────────────────────────────────

class TestCreateProjections(unittest.TestCase):

    def test_basic(self):
        ws = {'dataFields': [
            {'name': 'City', 'role': 'category'},
            {'name': 'Sales', 'role': 'values'},
        ]}
        p = create_projections(ws)
        self.assertIn('category', p)
        self.assertIn('values', p)

    def test_default_count(self):
        ws = {'dataFields': [{'name': 'City', 'role': 'category'}]}
        p = create_projections(ws)
        self.assertIn('values', p)
        self.assertEqual(p['values'][0]['queryRef'], 'Count')


# ── create_prototype_query ───────────────────────────────────

class TestCreatePrototypeQuery(unittest.TestCase):

    def test_basic(self):
        ws = {'dataFields': [{'name': 'Sales'}]}
        q = create_prototype_query(ws)
        self.assertEqual(len(q['Select']), 1)

    def test_dedup(self):
        ws = {'dataFields': [{'name': 'Sales'}, {'name': 'Sales'}]}
        q = create_prototype_query(ws)
        self.assertEqual(len(q['Select']), 1)


# ── create_filters_config ────────────────────────────────────

class TestCreateFiltersConfig(unittest.TestCase):

    def test_basic(self):
        f = create_filters_config([{'field': 'City', 'values': ['NYC']}])
        self.assertEqual(len(f), 1)
        self.assertIn('filter', f[0])


# ── create_page_layout ───────────────────────────────────────

class TestCreatePageLayout(unittest.TestCase):

    def test_returns_defaults(self):
        layout = create_page_layout([])
        self.assertEqual(layout['width'], 1280)


# ── Drilldown flag consumed in create_visual_container ───────

class TestDrilldownFlag(unittest.TestCase):

    def test_drilldown_keepLayerOrder(self):
        ws = {'name': 'V', 'visualType': 'bar',
              'dimensions': [{'field': 'City'}],
              'measures': [{'label': 'Sales', 'expression': 'Sum(Sales)'}],
              'hierarchies': [{'name': 'Geo'}]}
        vc = create_visual_container(ws,
                                     col_table_map={'City': 'T', 'Sales': 'T'})
        props = vc['visual']['objects']['general'][0]['properties']
        self.assertIn('keepLayerOrder', props)


if __name__ == '__main__':
    unittest.main()
