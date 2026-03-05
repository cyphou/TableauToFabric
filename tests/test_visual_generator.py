"""
Tests for fabric_import.visual_generator — Power BI visual generation.

SIMPLE:  VISUAL_TYPE_MAP coverage, resolve_visual_type, create_page_layout
MEDIUM:  create_visual_container, build_query_state, create_projections,
         create_prototype_query, create_filters_config, _get_config_template,
         _build_visual_filters
COMPLEX: generate_visual_containers, slicer sync, action buttons,
         cross-filter disable, sort state, reference lines, conditional formatting
"""

import unittest
import json
from fabric_import.visual_generator import (
    VISUAL_TYPE_MAP,
    VISUAL_DATA_ROLES,
    resolve_visual_type,
    generate_visual_containers,
    create_visual_container,
    build_query_state,
    create_projections,
    create_prototype_query,
    create_filters_config,
    create_page_layout,
    _get_config_template,
    _build_visual_filters,
)


# ═══════════════════════════════════════════════════════════════════
# SIMPLE TESTS
# ═══════════════════════════════════════════════════════════════════


class TestVisualTypeMap(unittest.TestCase):
    """SIMPLE — Visual type mapping completeness."""

    def test_bar_chart(self):
        self.assertEqual(VISUAL_TYPE_MAP['bar'], 'clusteredBarChart')

    def test_line_chart(self):
        self.assertEqual(VISUAL_TYPE_MAP['line'], 'lineChart')

    def test_pie_chart(self):
        self.assertEqual(VISUAL_TYPE_MAP['pie'], 'pieChart')

    def test_scatter(self):
        self.assertEqual(VISUAL_TYPE_MAP['scatter'], 'scatterChart')

    def test_table(self):
        self.assertEqual(VISUAL_TYPE_MAP['table'], 'tableEx')

    def test_map(self):
        self.assertEqual(VISUAL_TYPE_MAP['map'], 'map')

    def test_treemap(self):
        self.assertEqual(VISUAL_TYPE_MAP['treemap'], 'treemap')

    def test_waterfall(self):
        self.assertEqual(VISUAL_TYPE_MAP['waterfall'], 'waterfallChart')

    def test_slicer(self):
        self.assertEqual(VISUAL_TYPE_MAP['slicer'], 'slicer')

    def test_combo(self):
        self.assertEqual(VISUAL_TYPE_MAP['combo'], 'lineStackedColumnComboChart')

    def test_heatmap(self):
        self.assertEqual(VISUAL_TYPE_MAP['heatmap'], 'matrix')

    def test_wordcloud(self):
        self.assertEqual(VISUAL_TYPE_MAP['wordcloud'], 'wordCloud')

    def test_at_least_60_mappings(self):
        self.assertTrue(len(VISUAL_TYPE_MAP) >= 60)


class TestResolveVisualType(unittest.TestCase):
    """SIMPLE — resolve_visual_type helper."""

    def test_known_type(self):
        self.assertEqual(resolve_visual_type('bar'), 'clusteredBarChart')

    def test_unknown_type(self):
        self.assertEqual(resolve_visual_type('unknownViz'), 'tableEx')

    def test_none_type(self):
        self.assertEqual(resolve_visual_type(None), 'tableEx')

    def test_case_insensitive(self):
        self.assertEqual(resolve_visual_type('BAR'), 'clusteredBarChart')

    def test_empty_string(self):
        self.assertEqual(resolve_visual_type(''), 'tableEx')


class TestDataRoles(unittest.TestCase):
    """SIMPLE — VISUAL_DATA_ROLES coverage."""

    def test_card_roles(self):
        dim, meas = VISUAL_DATA_ROLES['card']
        self.assertEqual(dim, [])
        self.assertIn('Fields', meas)

    def test_bar_chart_roles(self):
        dim, meas = VISUAL_DATA_ROLES['clusteredBarChart']
        self.assertIn('Category', dim)
        self.assertIn('Y', meas)

    def test_scatter_roles(self):
        dim, meas = VISUAL_DATA_ROLES['scatterChart']
        self.assertIn('X', meas)
        self.assertIn('Y', meas)

    def test_slicer_roles(self):
        dim, meas = VISUAL_DATA_ROLES['slicer']
        self.assertIn('Values', dim)
        self.assertEqual(meas, [])


class TestCreatePageLayout(unittest.TestCase):
    """SIMPLE — page layout."""

    def test_default_layout(self):
        layout = create_page_layout([])
        self.assertEqual(layout['width'], 1280)
        self.assertEqual(layout['height'], 720)


class TestConfigTemplate(unittest.TestCase):
    """SIMPLE — config template retrieval."""

    def test_known_type(self):
        cfg = _get_config_template('tableEx')
        self.assertIn('autoSelectVisualType', cfg)

    def test_unknown_type(self):
        cfg = _get_config_template('nonExistentType')
        self.assertEqual(cfg, {})

    def test_bar_chart_has_objects(self):
        cfg = _get_config_template('clusteredBarChart')
        self.assertIn('objects', cfg)

    def test_slicer_config(self):
        cfg = _get_config_template('slicer')
        self.assertIn('objects', cfg)


# ═══════════════════════════════════════════════════════════════════
# MEDIUM TESTS
# ═══════════════════════════════════════════════════════════════════


class TestCreateVisualContainer(unittest.TestCase):
    """MEDIUM — single visual container creation."""

    def test_basic_table_visual(self):
        ws = {'name': 'MyTable', 'visualType': 'table', 'dimensions': [], 'measures': []}
        container = create_visual_container(ws, visual_id='v1', x=10, y=20,
                                             width=300, height=200, z_index=0)
        self.assertEqual(container['name'], 'v1')
        self.assertEqual(container['position']['x'], 10)
        self.assertEqual(container['position']['y'], 20)
        self.assertEqual(container['visual']['visualType'], 'tableEx')

    def test_bar_chart(self):
        ws = {
            'name': 'SalesChart', 'visualType': 'bar',
            'dimensions': [{'field': 'Region'}],
            'measures': [{'name': 'Sales', 'expression': 'sum(Amount)'}],
        }
        container = create_visual_container(ws, col_table_map={'Region': 'Orders', 'Amount': 'Orders'})
        self.assertEqual(container['visual']['visualType'], 'clusteredBarChart')

    def test_title_in_vc_objects(self):
        ws = {'name': 'TestTitle', 'visualType': 'table'}
        container = create_visual_container(ws)
        vc_objects = container['visual'].get('vcObjects', {})
        self.assertIn('title', vc_objects)

    def test_subtitle_when_present(self):
        ws = {'name': 'V', 'visualType': 'table', 'subtitle': 'Details'}
        container = create_visual_container(ws)
        vc_objects = container['visual'].get('vcObjects', {})
        self.assertIn('subTitle', vc_objects)

    def test_no_subtitle_when_absent(self):
        ws = {'name': 'V', 'visualType': 'table'}
        container = create_visual_container(ws)
        vc_objects = container['visual'].get('vcObjects', {})
        self.assertNotIn('subTitle', vc_objects)


class TestBuildQueryState(unittest.TestCase):
    """MEDIUM — query state building."""

    def test_card_measure_only(self):
        qs = build_query_state(
            'card', [], [{'name': 'Revenue', 'expression': 'sum(Amount)'}],
            {'Amount': 'Orders'}, {}
        )
        self.assertIn('Fields', qs)

    def test_bar_chart_dim_and_meas(self):
        qs = build_query_state(
            'clusteredBarChart',
            [{'field': 'Region'}],
            [{'name': 'Sales', 'expression': 'sum(Amount)'}],
            {'Region': 'Orders', 'Amount': 'Orders'}, {}
        )
        self.assertIn('Category', qs)
        self.assertIn('Y', qs)

    def test_table_all_in_values(self):
        qs = build_query_state(
            'tableEx',
            [{'field': 'Region'}],
            [{'name': 'Sum', 'expression': 'sum(Amount)'}],
            {'Region': 'T', 'Amount': 'T'}, {}
        )
        self.assertIn('Values', qs)

    def test_empty_returns_none(self):
        qs = build_query_state('clusteredBarChart', [], [], {}, {})
        self.assertIsNone(qs)

    def test_scatter_chart(self):
        qs = build_query_state(
            'scatterChart',
            [{'field': 'Name'}],
            [{'name': 'X', 'expression': 'sum(A)'},
             {'name': 'Y', 'expression': 'sum(B)'}],
            {'Name': 'T', 'A': 'T', 'B': 'T'}, {}
        )
        self.assertIn('X', qs)
        self.assertIn('Y', qs)

    def test_measure_lookup(self):
        qs = build_query_state(
            'card', [],
            [{'name': 'Total Revenue', 'label': 'Total Revenue'}],
            {}, {'Total Revenue': ('Sales', 'SUM(Sales[Amount])')}
        )
        self.assertIn('Fields', qs)


class TestCreateProjections(unittest.TestCase):
    """MEDIUM — field projections."""

    def test_basic_projections(self):
        ws = {'dataFields': [
            {'name': 'Region', 'role': 'category'},
            {'name': 'Sales', 'role': 'values'},
        ]}
        proj = create_projections(ws)
        self.assertIn('category', proj)
        self.assertIn('values', proj)

    def test_empty_fields(self):
        proj = create_projections({'dataFields': []})
        self.assertIn('values', proj)  # fallback

    def test_no_data_fields_key(self):
        proj = create_projections({})
        self.assertIn('values', proj)


class TestCreatePrototypeQuery(unittest.TestCase):
    """MEDIUM — prototype query."""

    def test_basic_query(self):
        ws = {'dataFields': [{'name': 'Sales'}, {'name': 'Region'}]}
        query = create_prototype_query(ws)
        self.assertEqual(query['Version'], 2)
        self.assertTrue(len(query['Select']) >= 2)

    def test_empty_fields(self):
        query = create_prototype_query({'dataFields': []})
        self.assertEqual(query['Select'], [])


class TestCreateFiltersConfig(unittest.TestCase):
    """MEDIUM — filter config creation."""

    def test_single_filter(self):
        filters = [{'field': 'Region', 'values': ['East', 'West']}]
        config = create_filters_config(filters)
        self.assertEqual(len(config), 1)
        self.assertEqual(config[0]['expression']['Column']['Property'], 'Region')

    def test_empty_filters(self):
        config = create_filters_config([])
        self.assertEqual(config, [])


class TestBuildVisualFilters(unittest.TestCase):
    """MEDIUM — visual-level filter entries."""

    def test_topn_filter(self):
        filters = [{'field': 'Product', 'type': 'topN', 'count': 5}]
        result = _build_visual_filters(filters, {'Product': 'T'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['type'], 'TopN')
        self.assertEqual(result[0]['itemCount'], 5)

    def test_categorical_filter(self):
        filters = [{'field': 'Country', 'type': 'basic', 'values': ['US', 'UK']}]
        result = _build_visual_filters(filters, {'Country': 'T'})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['type'], 'Categorical')


# ═══════════════════════════════════════════════════════════════════
# COMPLEX TESTS
# ═══════════════════════════════════════════════════════════════════


class TestGenerateVisualContainers(unittest.TestCase):
    """COMPLEX — generating multiple visual containers."""

    def test_multiple_worksheets(self):
        worksheets = [
            {'name': 'V1', 'visualType': 'bar',
             'dimensions': [{'field': 'Region'}],
             'measures': [{'name': 'Sales', 'expression': 'sum(Amount)'}]},
            {'name': 'V2', 'visualType': 'line',
             'dimensions': [{'field': 'Date'}],
             'measures': [{'name': 'Revenue', 'expression': 'sum(Rev)'}]},
        ]
        containers = generate_visual_containers(
            worksheets, 'Report',
            col_table_map={'Region': 'T', 'Amount': 'T', 'Date': 'T', 'Rev': 'T'}
        )
        self.assertEqual(len(containers), 2)

    def test_empty_worksheets(self):
        containers = generate_visual_containers([], 'Report')
        self.assertEqual(len(containers), 0)

    def test_max_20_visuals(self):
        worksheets = [{'name': f'V{i}', 'visualType': 'bar'} for i in range(25)]
        containers = generate_visual_containers(worksheets, 'R')
        self.assertEqual(len(containers), 20)


class TestActionButton(unittest.TestCase):
    """COMPLEX — Action button navigation."""

    def test_page_navigation(self):
        ws = {
            'name': 'NavBtn', 'visualType': 'button',
            'navigation': {'sheet': 'Page2'},
        }
        container = create_visual_container(ws)
        self.assertEqual(container['visual']['visualType'], 'actionButton')
        objects = container['visual'].get('objects', {})
        self.assertIn('action', objects)

    def test_url_navigation(self):
        ws = {
            'name': 'URLBtn', 'visualType': 'button',
            'navigation': {'url': 'https://example.com'},
        }
        container = create_visual_container(ws)
        objects = container['visual'].get('objects', {})
        self.assertIn('action', objects)


class TestSlicerSync(unittest.TestCase):
    """COMPLEX — Slicer sync groups."""

    def test_sync_group(self):
        ws = {
            'name': 'DateSlicer', 'visualType': 'slicer',
            'syncGroup': 'DateSync',
        }
        container = create_visual_container(ws)
        self.assertIn('syncGroup', container)
        self.assertEqual(container['syncGroup']['groupName'], 'DateSync')


class TestCrossFilterDisable(unittest.TestCase):
    """COMPLEX — Cross-filter disable."""

    def test_disabled_interactions(self):
        ws = {
            'name': 'V', 'visualType': 'bar',
            'interactions': {'disabled': True},
        }
        container = create_visual_container(ws)
        self.assertIn('filterConfig', container)
        self.assertTrue(container['filterConfig']['disabled'])


class TestSortState(unittest.TestCase):
    """COMPLEX — Sort state migration."""

    def test_sort_ascending(self):
        ws = {
            'name': 'V', 'visualType': 'bar',
            'sortBy': [{'field': 'Sales', 'direction': 'ascending'}],
        }
        container = create_visual_container(ws, col_table_map={'Sales': 'T'})
        sort_def = container['visual'].get('query', {}).get('sortDefinition', {})
        self.assertIn('sort', sort_def)
        self.assertEqual(sort_def['sort'][0]['direction'], 1)

    def test_sort_descending(self):
        ws = {
            'name': 'V', 'visualType': 'bar',
            'sortBy': [{'field': 'Sales', 'direction': 'descending'}],
        }
        container = create_visual_container(ws, col_table_map={'Sales': 'T'})
        sort_def = container['visual'].get('query', {}).get('sortDefinition', {})
        self.assertEqual(sort_def['sort'][0]['direction'], 2)


class TestReferenceLines(unittest.TestCase):
    """COMPLEX — Reference lines."""

    def test_reference_line(self):
        ws = {
            'name': 'V', 'visualType': 'bar',
            'referenceLines': [
                {'value': 100, 'label': 'Target', 'color': '#FF0000'},
            ],
        }
        container = create_visual_container(ws)
        objects = container['visual'].get('objects', {})
        self.assertIn('constantLine', objects)


class TestColorBy(unittest.TestCase):
    """COMPLEX — Color encoding."""

    def test_color_by_measure(self):
        ws = {
            'name': 'V', 'visualType': 'bar',
            'colorBy': {'mode': 'byMeasure'},
        }
        container = create_visual_container(ws)
        objects = container['visual'].get('objects', {})
        self.assertIn('dataPoint', objects)


# ═══════════════════════════════════════════════════════════════════
# EXHAUSTIVE VISUAL TYPE MAP TESTS
# ═══════════════════════════════════════════════════════════════════


class TestExhaustiveVisualTypeMap(unittest.TestCase):
    """Verify every entry in VISUAL_TYPE_MAP maps to a non-empty string."""

    def test_all_entries_produce_nonempty_string(self):
        """Every VISUAL_TYPE_MAP value must be a non-empty PBI visual type string."""
        for key, value in VISUAL_TYPE_MAP.items():
            with self.subTest(key=key):
                self.assertIsInstance(value, str, f"Value for '{key}' is not a string")
                self.assertTrue(len(value) > 0, f"Value for '{key}' is empty")

    def test_all_entries_resolvable(self):
        """resolve_visual_type should return each map value for its key."""
        for key, expected in VISUAL_TYPE_MAP.items():
            with self.subTest(key=key):
                self.assertEqual(resolve_visual_type(key), expected)

    def test_minimum_entry_count(self):
        """Must have at least 100 entries (we claimed ~120)."""
        self.assertGreaterEqual(len(VISUAL_TYPE_MAP), 100)

    def test_all_categories_have_entries(self):
        """Verify coverage of all major visual categories."""
        values = set(VISUAL_TYPE_MAP.values())
        expected_types = {
            'clusteredBarChart', 'stackedBarChart', 'clusteredColumnChart',
            'lineChart', 'areaChart', 'pieChart', 'donutChart', 'scatterChart',
            'map', 'filledMap', 'tableEx', 'matrix', 'card', 'gauge',
            'treemap', 'waterfallChart', 'funnel', 'slicer',
        }
        for vtype in expected_types:
            with self.subTest(vtype=vtype):
                self.assertIn(vtype, values, f"PBI type '{vtype}' has no mapping")

    def test_specialty_fallback_mappings_exist(self):
        """Approximate mappings for specialty Tableau types should exist."""
        specialty = {
            'histogram': 'clusteredColumnChart',
            'ganttbar': 'clusteredBarChart',
            'bumpchart': 'lineChart',
            'slopechart': 'lineChart',
            'butterfly': 'hundredPercentStackedBarChart',
            'waffle': 'hundredPercentStackedBarChart',
            'pareto': 'lineClusteredColumnComboChart',
            'network': 'decompositionTree',
            'mekko': 'stackedBarChart',
            'lollipop': 'clusteredBarChart',
        }
        for key, expected in specialty.items():
            with self.subTest(key=key):
                self.assertEqual(VISUAL_TYPE_MAP.get(key), expected)

    def test_custom_visuals_mapped(self):
        """Custom visuals (sankey, chord) should map to AppSource types."""
        self.assertEqual(VISUAL_TYPE_MAP['sankey'], 'sankeyChart')
        self.assertEqual(VISUAL_TYPE_MAP['chord'], 'chordChart')

    def test_pbi_passthrough_entries_consistent(self):
        """Keys that are PBI names should map back to themselves."""
        passthrough = [
            ('clusteredbarchart', 'clusteredBarChart'),
            ('stackedcolumnchart', 'stackedColumnChart'),
            ('piechart', 'pieChart'),
            ('waterfallchart', 'waterfallChart'),
        ]
        for key, expected in passthrough:
            with self.subTest(key=key):
                self.assertEqual(VISUAL_TYPE_MAP[key], expected)



if __name__ == '__main__':
    unittest.main()
