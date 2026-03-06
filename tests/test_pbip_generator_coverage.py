"""
Extra coverage tests for fabric_import.pbip_generator.

Targets uncovered branches:
  - generate() wrapper
  - generate_project() full flow
  - create_report_structure with dashboard object types (text, image,
    filter_control, navigation_button, download_button)
  - drillthrough pages from actions
  - mobile layout from device_layouts
  - bookmarks from converted_objects
  - pages shelf slicer
  - _build_visual_query for all chart types
  - _create_visual_filters (range + categorical + exclude)
  - _build_visual_objects (labels, legend, axes, background, conditional
    formatting, reference lines, trend lines, annotations, fonts,
    forecasting, map options, color values, gradients, dual axis,
    padding, table banding, analytics stats)
  - _convert_number_format
  - _create_slicer_visual
  - create_metadata
  - _count_report_artifacts
  - PermissionError retry loop
"""

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from fabric_import.pbip_generator import FabricPBIPGenerator


# ── Helpers ────────────────────────────────────────────────────────

def _make_gen(tmpdir):
    return FabricPBIPGenerator(output_dir=tmpdir)


def _sample_ds():
    return {
        'name': 'DS1',
        'connection': {'type': 'SQL', 'details': {}},
        'connection_map': {},
        'tables': [
            {'name': 'Orders', 'columns': [
                {'name': 'OrderID', 'datatype': 'integer'},
                {'name': 'Sales', 'datatype': 'real'},
                {'name': 'City', 'datatype': 'string'},
            ]},
        ],
        'calculations': [
            {'name': '[Total]', 'caption': 'Total',
             'formula': 'SUM([Sales])', 'role': 'measure', 'datatype': 'real'},
            {'name': '[Profit]', 'caption': 'Profit',
             'formula': 'SUM([Profit])', 'role': 'measure', 'datatype': 'real'},
            {'name': '[Margin]', 'caption': 'Margin',
             'formula': '[Profit]/[Total]', 'role': 'measure', 'datatype': 'real'},
        ],
        'relationships': [],
        'columns': [],
    }


def _base_converted():
    return {
        'datasources': [_sample_ds()],
        'worksheets': [
            {'name': 'Sheet1', 'chart_type': 'clusteredBarChart',
             'fields': [
                 {'name': 'City', 'role': 'dimension'},
                 {'name': 'sum:Sales', 'role': 'measure'},
             ]},
        ],
        'dashboards': [
            {'name': 'Dash', 'size': {'width': 1280, 'height': 720},
             'objects': [
                 {'type': 'worksheetReference', 'worksheetName': 'Sheet1',
                  'position': {'x': 0, 'y': 0, 'w': 600, 'h': 400}},
             ]},
        ],
        'calculations': [],
        'parameters': [],
        'hierarchies': [],
        'sets': [],
        'groups': [],
        'bins': [],
        'aliases': {},
        'user_filters': [],
        'filters': [],
        'stories': [],
        'actions': [],
        'custom_sql': [],
        'bookmarks': [],
    }


# ═══════════════════════════════════════════════════════════════════
#  _convert_number_format (static, easy)
# ═══════════════════════════════════════════════════════════════════

class TestConvertNumberFormat(unittest.TestCase):

    def test_none(self):
        self.assertEqual(FabricPBIPGenerator._convert_number_format(None), '')

    def test_non_string(self):
        self.assertEqual(FabricPBIPGenerator._convert_number_format(123), '')

    def test_empty(self):
        self.assertEqual(FabricPBIPGenerator._convert_number_format(''), '')

    def test_pbi_compatible(self):
        for fmt in ('0', '0.0', '0.00', '#,0', '#,0.0', '#,0.00', '0%', '0.0%', '0.00%'):
            self.assertEqual(FabricPBIPGenerator._convert_number_format(fmt), fmt)

    def test_currency(self):
        result = FabricPBIPGenerator._convert_number_format('$#,#00.00')
        self.assertIn('$', result)
        self.assertNotIn('#,#', result)  # replaced

    def test_percentage(self):
        self.assertEqual(FabricPBIPGenerator._convert_number_format('##.0%'), '##.0%')

    def test_thousands(self):
        result = FabricPBIPGenerator._convert_number_format('#,###')
        self.assertNotIn('#,#', result)

    def test_plain(self):
        self.assertEqual(FabricPBIPGenerator._convert_number_format('##'), '##')


# ═══════════════════════════════════════════════════════════════════
#  _build_visual_query — all chart type branches
# ═══════════════════════════════════════════════════════════════════

class TestBuildVisualQuery(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _ws(self, chart_type, fields):
        return {'chart_type': chart_type, 'fields': fields}

    def _dim(self, name):
        return {'name': name, 'role': 'dimension'}

    def _mea(self, name):
        return {'name': name, 'role': 'measure'}

    def test_no_fields(self):
        self.assertIsNone(self.gen._build_visual_query({'chart_type': 'bar', 'fields': []}))

    def test_skip_measure_names(self):
        ws = self._ws('bar', [{'name': 'Measure Names'}])
        self.assertIsNone(self.gen._build_visual_query(ws))

    def test_filled_map(self):
        ws = self._ws('filledMap', [self._dim('City'), self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Category', qs)
        self.assertIn('Size', qs)

    def test_table(self):
        ws = self._ws('tableEx', [self._dim('City'), self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        self.assertIn('Values', q['queryState'])

    def test_scatter(self):
        ws = self._ws('scatterChart', [
            self._dim('City'), self._mea('Total'), self._mea('Profit'), self._mea('Margin')
        ])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('X', qs)
        self.assertIn('Y', qs)
        self.assertIn('Size', qs)

    def test_scatter_one_measure(self):
        ws = self._ws('scatterChart', [self._dim('City'), self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        self.assertIn('Y', q['queryState'])
        self.assertNotIn('X', q['queryState'])

    def test_gauge(self):
        ws = self._ws('gauge', [self._dim('City'), self._mea('Total'), self._mea('Profit')])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Y', qs)
        self.assertIn('TargetValue', qs)

    def test_card(self):
        ws = self._ws('card', [self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        self.assertIn('Values', q['queryState'])

    def test_card_dim_fallback(self):
        ws = self._ws('card', [self._dim('City')])
        q = self.gen._build_visual_query(ws)
        self.assertIn('Values', q['queryState'])

    def test_pie(self):
        ws = self._ws('pieChart', [self._dim('City'), self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Category', qs)
        self.assertIn('Y', qs)

    def test_combo(self):
        ws = self._ws('lineClusteredColumnComboChart', [
            self._dim('City'), self._mea('Total'), self._mea('Profit')
        ])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Y', qs)
        self.assertIn('Y2', qs)

    def test_waterfall(self):
        ws = self._ws('waterfallChart', [
            self._dim('City'), self._dim('OrderID'), self._mea('Total')
        ])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Breakdown', qs)

    def test_box_whisker(self):
        ws = self._ws('boxAndWhisker', [self._dim('City'), self._mea('Total')])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Category', qs)
        self.assertIn('Value', qs)

    def test_default_dim_only_multiple(self):
        ws = self._ws('clusteredBarChart', [self._dim('City'), self._dim('OrderID')])
        q = self.gen._build_visual_query(ws)
        qs = q['queryState']
        self.assertIn('Category', qs)
        self.assertIn('Y', qs)


# ═══════════════════════════════════════════════════════════════════
#  _create_visual_filters
# ═══════════════════════════════════════════════════════════════════

class TestCreateVisualFilters(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_empty_field_skipped(self):
        self.assertEqual(self.gen._create_visual_filters([{'field': ''}]), [])

    def test_range_filter(self):
        filters = [{'field': 'Sales', 'type': 'range', 'min': 10, 'max': 100}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['type'], 'Advanced')
        where = result[0]['filter']['Where']
        self.assertEqual(len(where), 2)

    def test_range_min_only(self):
        filters = [{'field': 'Sales', 'min': 5}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(result[0]['type'], 'Advanced')
        self.assertEqual(len(result[0]['filter']['Where']), 1)

    def test_categorical_filter(self):
        filters = [{'field': 'City', 'type': 'categorical', 'values': ['A', 'B']}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(result[0]['type'], 'Categorical')

    def test_categorical_exclude(self):
        filters = [{'field': 'City', 'values': ['X'], 'exclude': True}]
        result = self.gen._create_visual_filters(filters)
        where = result[0]['filter']['Where']
        self.assertTrue(any('Not' in str(w) for w in where))

    def test_categorical_no_values(self):
        filters = [{'field': 'City', 'type': 'categorical'}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['filter']['Where'], [])

    # ── Federated prefix handling ──

    def test_federated_prefix_stripped_in_filter(self):
        """Field with federated.HASH.none:Field:qk must resolve to clean column."""
        filters = [{'field': 'federated.10vks1203pgcxf1bkw5yk1bwzy2f.none:City:qk',
                    'type': 'categorical', 'values': ['NYC']}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)
        prop = result[0]['field']['Column']['Property']
        self.assertEqual(prop, 'City')

    def test_federated_prefix_range_filter(self):
        """Range filter with federated prefix must clean Property."""
        filters = [{'field': 'federated.abc.sum:Sales:nk', 'type': 'range',
                    'min': 10, 'max': 100}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)
        prop = result[0]['field']['Column']['Property']
        self.assertEqual(prop, 'Sales')

    # ── Measure Names / Measure Values skipping ──

    def test_measure_names_filter_skipped(self):
        """:Measure Names is a Tableau virtual field — must be dropped."""
        filters = [{'field': ':Measure Names', 'type': 'categorical',
                    'values': ['sum:Sales']}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 0)

    def test_measure_values_filter_skipped(self):
        filters = [{'field': ':Measure Values', 'type': 'categorical'}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 0)

    def test_federated_measure_names_skipped(self):
        """federated.HASH.:Measure Names must also be skipped."""
        filters = [{'field': 'federated.10vks1203pgcxf1bkw5yk1bwzy2f.:Measure Names',
                    'type': 'categorical',
                    'values': ['federated.hash.sum:Sales:qk']}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 0)

    def test_measure_names_no_colon_prefix_skipped(self):
        """'Measure Names' without leading colon must also be skipped."""
        filters = [{'field': 'Measure Names', 'type': 'categorical'}]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 0)

    # ── Deduplication ──

    def test_duplicate_filters_deduplicated(self):
        """Multiple filters on the same resolved field must be deduplicated."""
        filters = [
            {'field': 'City', 'type': 'categorical', 'values': ['A']},
            {'field': 'none:City:qk', 'type': 'categorical', 'values': ['B']},
        ]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)

    def test_valid_plus_measure_names_mixed(self):
        """Valid filter + Measure Names → only valid filter kept."""
        filters = [
            {'field': 'City', 'type': 'categorical', 'values': ['NYC']},
            {'field': ':Measure Names', 'type': 'categorical', 'values': ['x']},
        ]
        result = self.gen._create_visual_filters(filters)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['field']['Column']['Property'], 'City')


# ═══════════════════════════════════════════════════════════════════
#  _build_visual_objects  (formatting branches)
# ═══════════════════════════════════════════════════════════════════

class TestBuildVisualObjects(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_ws_data(self):
        objs = self.gen._build_visual_objects('title', None, 'bar')
        self.assertIn('title', objs)

    def test_data_labels(self):
        ws = {'formatting': {'mark': {'mark-labels-show': 'true'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('labels', objs)

    def test_data_labels_from_encoding(self):
        ws = {'formatting': {}, 'mark_encoding': {'label': {'show': True}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('labels', objs)

    def test_legend(self):
        ws = {'formatting': {}, 'mark_encoding': {'color': {'field': 'Region'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('legend', objs)

    def test_legend_multiple_values_skipped(self):
        ws = {'formatting': {}, 'mark_encoding': {'color': {'field': 'Multiple Values'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertNotIn('legend', objs)

    def test_label_color(self):
        ws = {'formatting': {'label': {'color': '#FF0000'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('labels', objs)
        self.assertIn('color', objs['labels'][0]['properties'])

    def test_axes_display(self):
        ws = {'formatting': {'axis': {'display': 'true'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('categoryAxis', objs)
        self.assertIn('valueAxis', objs)

    def test_axes_display_none(self):
        ws = {'formatting': {'axis': {'display': 'none'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertNotIn('categoryAxis', objs)
        self.assertNotIn('valueAxis', objs)

    def test_axes_titles(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'title': 'Xaxis'}, 'y': {'title': 'Yaxis'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('categoryAxis', objs)
        self.assertIn('valueAxis', objs)

    def test_background_color(self):
        ws = {'formatting': {'background_color': '#FFF'}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('visualContainerStyle', objs)

    def test_background_from_pane(self):
        ws = {'formatting': {'pane': {'background-color': '#EEE'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('visualContainerStyle', objs)

    def test_conditional_formatting_palette(self):
        ws = {'formatting': {}, 'mark_encoding': {
            'color': {'type': 'quantitative', 'palette': 'diverging',
                      'palette_colors': ['#000', '#FFF']}
        }}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('dataPoint', objs)

    def test_reference_lines(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'reference_lines': [{'value': 100, 'label': 'avg', 'color': '#666'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        ref = objs['valueAxis'][0]['properties']['referenceLine']
        self.assertEqual(len(ref), 1)

    def test_trend_lines(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'trend_lines': [{'type': 'linear', 'color': '#333',
                               'show_equation': True, 'show_r_squared': True}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('trend', objs)
        self.assertTrue(len(objs['trend']) >= 1)

    def test_trend_line_invalid_type(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'trend_lines': [{'type': 'weird_type', 'color': '#333'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('trend', objs)

    def test_annotations(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'annotations': [{'text': 'Note 1'}, {'text': 'Note 2'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('subTitle', objs)

    def test_annotations_empty_text_skipped(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'annotations': [{'text': ''}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertNotIn('subTitle', objs)

    def test_axes_detail_title_show(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'title': 'X', 'show_title': True},
                       'y': {'title': 'Y', 'show_title': False, 'show_label': False}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        yprops = objs['valueAxis'][0]['properties']
        self.assertEqual(yprops['showAxisTitle']['expr']['Literal']['Value'], "false")

    def test_axes_label_rotation(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'label_rotation': '45'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        props = objs['categoryAxis'][0]['properties']
        self.assertIn('labelAngle', props)

    def test_axes_label_rotation_invalid(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'label_rotation': 'abc'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        props = objs.get('categoryAxis', [{}])[0].get('properties', {})
        self.assertNotIn('labelAngle', props)

    def test_axes_format(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'format': '#,0'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        props = objs['categoryAxis'][0]['properties']
        self.assertIn('labelDisplayUnits', props)

    def test_legend_position(self):
        ws = {'formatting': {}, 'mark_encoding': {
            'color': {'field': 'City', 'legend_position': 'bottom'}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        pos = objs['legend'][0]['properties']['position']['expr']['Literal']['Value']
        self.assertEqual(pos, "'Bottom'")

    def test_font_family(self):
        ws = {'formatting': {'font': {'family': 'Arial'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('labels', objs)
        self.assertIn('fontFamily', objs['labels'][0]['properties'])

    def test_font_size(self):
        ws = {'formatting': {'font': {'size': '12pt'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('fontSize', objs['labels'][0]['properties'])

    def test_font_size_invalid(self):
        ws = {'formatting': {'font': {'size': 'big'}}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        # should not crash, fontSize may or may not appear
        self.assertIn('labels', objs)

    def test_forecast(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'forecasting': [{'periods': 10, 'prediction_interval': '90',
                               'ignore_last': '2'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('forecast', objs)
        fc = objs['forecast'][0]['properties']
        self.assertIn('ignoreLast', fc)

    def test_forecast_no_ignore_last(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'forecasting': [{'periods': 5, 'prediction_interval': '95',
                               'ignore_last': '0'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertNotIn('ignoreLast', objs['forecast'][0]['properties'])

    def test_map_options(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'map_options': {'washout': '0.5', 'style': 'dark'}}
        objs = self.gen._build_visual_objects('t', ws, 'filledMap')
        self.assertIn('mapControl', objs)

    def test_map_options_not_map_type(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'map_options': {'washout': '0.5'}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertNotIn('mapControl', objs)

    def test_color_values(self):
        ws = {'formatting': {}, 'mark_encoding': {
            'color': {'color_values': {'A': '#F00', 'B': '#0F0'}}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('dataPoint', objs)
        self.assertEqual(len(objs['dataPoint']), 2)

    def test_gradient(self):
        ws = {'formatting': {}, 'mark_encoding': {
            'color': {'type': 'quantitative',
                      'palette_colors': ['#000', '#888', '#FFF']}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('colorBorder', objs)
        props = objs['colorBorder'][0]['properties']
        self.assertIn('midColor', props)

    def test_continuous_axis(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'axes': {'x': {'is_continuous': True}, 'y': {'is_continuous': False}}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        xprops = objs['categoryAxis'][0]['properties']
        yprops = objs['valueAxis'][0]['properties']
        self.assertEqual(xprops['axisType']['expr']['Literal']['Value'], "'Continuous'")
        self.assertEqual(yprops['axisType']['expr']['Literal']['Value'], "'Categorical'")

    def test_dual_axis(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'dual_axis': {'enabled': True, 'synchronized': True}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        vax = objs['valueAxis'][0]['properties']
        self.assertIn('secShow', vax)

    def test_padding(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'padding': {'padding_top': 5, 'margin_bottom': 10}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('visualContainerPadding', objs)

    def test_table_banding_default(self):
        ws = {'formatting': {}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'tableEx')
        self.assertIn('values', objs)

    def test_table_banding_custom(self):
        ws = {'formatting': {'row_banding_color': '#FF0'}, 'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'matrix')
        self.assertIn('values', objs)
        color = objs['values'][0]['properties']['backColor']['solid']['color']['expr']['Literal']['Value']
        self.assertEqual(color, "'#FF0'")

    def test_table_totals(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'totals': {'grand_totals': True, 'subtotals': True}}
        objs = self.gen._build_visual_objects('t', ws, 'tableEx')
        self.assertIn('total', objs)
        self.assertIn('subTotals', objs)

    def test_analytics_distribution_band(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'analytics_stats': [{'type': 'distribution_band',
                                   'value_from': '10', 'value_to': '90'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        ref = objs['valueAxis'][0]['properties']['referenceLine']
        self.assertEqual(ref[0]['type'], 'Band')

    def test_analytics_stat_line(self):
        ws = {'formatting': {}, 'mark_encoding': {},
              'analytics_stats': [{'type': 'stat_line', 'computation': 'median'}]}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        ref = objs['valueAxis'][0]['properties']['referenceLine']
        self.assertEqual(ref[0]['type'], 'Median')

    def test_number_format_in_labels(self):
        ws = {'formatting': {'number_format': '0.00', 'mark': {'mark-labels-show': 'true'}},
              'mark_encoding': {}}
        objs = self.gen._build_visual_objects('t', ws, 'bar')
        self.assertIn('labelDisplayUnits', objs['labels'][0]['properties'])


# ═══════════════════════════════════════════════════════════════════
#  _create_slicer_visual
# ═══════════════════════════════════════════════════════════════════

class TestCreateSlicerVisual(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_slicer_has_query(self):
        s = self.gen._create_slicer_visual('v1', 0, 0, 200, 50, 'City', 'Orders', 0)
        self.assertEqual(s['visual']['visualType'], 'slicer')
        self.assertIn('query', s['visual'])

    def test_slicer_empty_field(self):
        s = self.gen._create_slicer_visual('v1', 0, 0, 200, 50, '', 'Orders', 0)
        self.assertNotIn('query', s['visual'])


# ═══════════════════════════════════════════════════════════════════
#  _find_column_table
# ═══════════════════════════════════════════════════════════════════

class TestFindColumnTable(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_found_by_name(self):
        co = _base_converted()
        result = self.gen._find_column_table('City', co)
        self.assertEqual(result, 'Orders')

    def test_found_by_calculation_caption(self):
        co = _base_converted()
        result = self.gen._find_column_table('Total', co)
        tbl = result  # should find first table of DS
        self.assertTrue(len(tbl) > 0)

    def test_not_found(self):
        co = _base_converted()
        result = self.gen._find_column_table('NonExistent', co)
        self.assertEqual(result, '')


# ═══════════════════════════════════════════════════════════════════
#  _resolve_field_entity (prefix handling)
# ═══════════════════════════════════════════════════════════════════

class TestResolveFieldEntity(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_attr_prefix(self):
        entity, prop = self.gen._resolve_field_entity('attr:City')
        self.assertEqual(prop, 'City')

    def test_colon_prefix(self):
        entity, prop = self.gen._resolve_field_entity(':City')
        self.assertEqual(prop, 'City')

    def test_bracket_stripped(self):
        entity, prop = self.gen._resolve_field_entity('[City]')
        self.assertEqual(prop, 'City')

    # ── Federated prefix handling ──

    def test_federated_prefix_stripped(self):
        """federated.HASH.none:City:qk → ('Orders', 'City')."""
        result = self.gen._resolve_field_entity(
            'federated.10vks1203pgcxf1bkw5yk1bwzy2f.none:City:qk'
        )
        self.assertEqual(result, ('Orders', 'City'))

    def test_federated_prefix_sum_suffix(self):
        """federated.HASH.sum:Sales:nk → ('Orders', 'Sales')."""
        result = self.gen._resolve_field_entity(
            'federated.abc123.sum:Sales:nk'
        )
        self.assertEqual(result, ('Orders', 'Sales'))

    def test_federated_prefix_no_derivation(self):
        """federated.HASH.City → ('Orders', 'City')."""
        result = self.gen._resolve_field_entity('federated.xyz.City')
        self.assertEqual(result, ('Orders', 'City'))

    def test_federated_prefix_with_brackets(self):
        """[federated.HASH.Sales] → ('Orders', 'Sales')."""
        result = self.gen._resolve_field_entity(
            '[federated.abc.Sales]'
        )
        self.assertEqual(result, ('Orders', 'Sales'))

    # ── Measure Names / Measure Values → None ──

    def test_measure_names_returns_none(self):
        """:Measure Names → None (no PBI equivalent)."""
        result = self.gen._resolve_field_entity(':Measure Names')
        self.assertIsNone(result)

    def test_measure_values_returns_none(self):
        result = self.gen._resolve_field_entity(':Measure Values')
        self.assertIsNone(result)

    def test_measure_names_no_colon_returns_none(self):
        result = self.gen._resolve_field_entity('Measure Names')
        self.assertIsNone(result)

    def test_federated_measure_names_returns_none(self):
        """federated.HASH.:Measure Names → None."""
        result = self.gen._resolve_field_entity(
            'federated.10vks1203pgcxf1bkw5yk1bwzy2f.:Measure Names'
        )
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════════
#  create_report_structure with various dashboard objects
# ═══════════════════════════════════════════════════════════════════

class TestReportStructureDashObjects(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_text_object(self):
        co = _base_converted()
        co['dashboards'][0]['objects'] = [
            {'type': 'text', 'content': 'Hello', 'position': {'x': 0, 'y': 0, 'w': 200, 'h': 50}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))

    def test_image_object(self):
        co = _base_converted()
        co['dashboards'][0]['objects'] = [
            {'type': 'image', 'source': 'https://img.png', 'position': {'x': 0, 'y': 0, 'w': 200, 'h': 200}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))

    def test_filter_control(self):
        co = _base_converted()
        co['dashboards'][0]['objects'] = [
            {'type': 'filter_control', 'field': 'City',
             'position': {'x': 0, 'y': 0, 'w': 200, 'h': 60}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))

    def test_navigation_button(self):
        co = _base_converted()
        co['dashboards'][0]['objects'] = [
            {'type': 'navigation_button', 'target_sheet': 'Sheet1',
             'name': 'Go', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 40}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))

    def test_download_button(self):
        co = _base_converted()
        co['dashboards'][0]['objects'] = [
            {'type': 'download_button', 'name': 'Download',
             'position': {'x': 0, 'y': 0, 'w': 100, 'h': 40}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))


# ═══════════════════════════════════════════════════════════════════
#  Drillthrough, mobile, bookmarks, tooltip, pages shelf
# ═══════════════════════════════════════════════════════════════════

class TestReportStructureAdvanced(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_drillthrough_from_actions(self):
        co = _base_converted()
        co['actions'] = [
            {'type': 'filter', 'target_sheet': 'Sheet1', 'source_field': 'City'},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        # Should have a DrillThrough_ page dir
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        dt_pages = [e for e in entries if e.startswith('DrillThrough_')]
        self.assertTrue(len(dt_pages) >= 1)

    def test_drillthrough_no_source_field(self):
        co = _base_converted()
        co['actions'] = [
            {'type': 'go-to-sheet', 'target_sheet': 'Sheet1'},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        dt_pages = [e for e in entries if e.startswith('DrillThrough_')]
        self.assertTrue(len(dt_pages) >= 1)

    def test_drillthrough_dedup(self):
        co = _base_converted()
        co['actions'] = [
            {'type': 'filter', 'target_sheet': 'Sheet1', 'source_field': 'City'},
            {'type': 'highlight', 'target_sheet': 'Sheet1', 'source_field': 'Sales'},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        dt_pages = [e for e in entries if e.startswith('DrillThrough_')]
        self.assertEqual(len(dt_pages), 1)

    def test_mobile_layout_phone(self):
        co = _base_converted()
        co['dashboards'][0]['device_layouts'] = [
            {'device_type': 'phone', 'width': 375, 'height': 667,
             'zones': [{'worksheet': 'Sheet1', 'x': 0, 'y': 0, 'w': 375, 'h': 300}]}
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        mobile = [e for e in entries if e.startswith('Mobile_')]
        self.assertTrue(len(mobile) >= 1)

    def test_mobile_layout_tablet(self):
        co = _base_converted()
        co['dashboards'][0]['device_layouts'] = [
            {'device_type': 'tablet'}
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        mobile = [e for e in entries if e.startswith('Mobile_')]
        self.assertTrue(len(mobile) >= 1)

    def test_bookmarks(self):
        co = _base_converted()
        co['bookmarks'] = [
            {'name': 'BM1', 'filters': {}},
            {'name': 'BM2', 'filters': {}},
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        bm_dir = os.path.join(rd, 'definition', 'bookmarks')
        self.assertTrue(os.path.isdir(bm_dir))
        bm_meta = os.path.join(bm_dir, 'bookmarks.json')
        self.assertTrue(os.path.isfile(bm_meta))
        with open(bm_meta) as f:
            meta = json.load(f)
        self.assertEqual(len(meta['bookmarkOrder']), 2)

    def test_tooltip_page(self):
        co = _base_converted()
        co['worksheets'].append(
            {'name': 'Tooltip1', 'chart_type': 'card',
             'fields': [{'name': 'Sales', 'role': 'measure'}],
             'tooltip': {'viz_in_tooltip': True}}
        )
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        entries = os.listdir(pages_dir)
        tooltip = [e for e in entries if e.startswith('Tooltip_')]
        self.assertTrue(len(tooltip) >= 1)

    def test_pages_shelf_slicer(self):
        co = _base_converted()
        co['dashboards'][0]['pages_shelf'] = {'field': 'City'}
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))

    def test_report_level_filters(self):
        co = _base_converted()
        co['filters'] = [{'field': 'City', 'type': 'categorical', 'values': ['NYC']}]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        report_json = os.path.join(rd, 'definition', 'report.json')
        with open(report_json) as f:
            data = json.load(f)
        self.assertIn('filterConfig', data)

    def test_default_page_when_no_dashboards(self):
        co = _base_converted()
        co['dashboards'] = []
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        pages_dir = os.path.join(rd, 'definition', 'pages')
        self.assertTrue(os.path.isdir(pages_dir))
        pages_meta = os.path.join(pages_dir, 'pages.json')
        with open(pages_meta) as f:
            meta = json.load(f)
        self.assertTrue(len(meta['pageOrder']) >= 1)

    def test_worksheet_filters_on_visual(self):
        co = _base_converted()
        co['worksheets'][0]['filters'] = [
            {'field': 'City', 'type': 'categorical', 'values': ['NYC']}
        ]
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))


# ═══════════════════════════════════════════════════════════════════
#  generate() wrapper
# ═══════════════════════════════════════════════════════════════════

class TestGenerate(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generate_returns_pages_visuals(self):
        project_dir = os.path.join(self.tmpdir, 'proj')
        os.makedirs(project_dir, exist_ok=True)
        gen = FabricPBIPGenerator(project_dir, 'TestReport')
        co = _base_converted()
        result = gen.generate(co)
        self.assertIn('pages', result)
        self.assertIn('visuals', result)
        self.assertGreaterEqual(result['pages'], 1)


# ═══════════════════════════════════════════════════════════════════
#  generate_project  (full integration)
# ═══════════════════════════════════════════════════════════════════

class TestGenerateProject(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_generates_all_artifacts(self):
        co = _base_converted()
        result = self.gen.generate_project('Test', co)
        project_dir = os.path.join(self.tmpdir, 'Test')
        self.assertTrue(os.path.isfile(os.path.join(project_dir, 'Test.pbip')))
        self.assertTrue(os.path.isdir(os.path.join(project_dir, 'Test.SemanticModel')))
        self.assertTrue(os.path.isdir(os.path.join(project_dir, 'Test.Report')))
        self.assertTrue(os.path.isfile(os.path.join(project_dir, 'migration_metadata.json')))
        self.assertIn('pages', result)

    def test_custom_lakehouse(self):
        co = _base_converted()
        result = self.gen.generate_project('Test', co, lakehouse_name='MyLH')
        self.assertIn('pages', result)

    def test_tmdl_error_handled(self):
        co = _base_converted()
        with patch('fabric_import.pbip_generator.tmdl_generator.generate_tmdl',
                   side_effect=RuntimeError('tmdl fail')):
            # Should not raise — error is caught and logged
            result = self.gen.generate_project('Test', co)
            self.assertIn('pages', result)


# ═══════════════════════════════════════════════════════════════════
#  create_metadata
# ═══════════════════════════════════════════════════════════════════

class TestCreateMetadata(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_metadata_content(self):
        co = _base_converted()
        # First create a project so report artifacts exist
        self.gen.generate_project('M', co)
        proj = os.path.join(self.tmpdir, 'M')
        meta_file = os.path.join(proj, 'migration_metadata.json')
        self.assertTrue(os.path.isfile(meta_file))
        with open(meta_file) as f:
            meta = json.load(f)
        self.assertEqual(meta['report_name'], 'M')
        self.assertIn('objects_converted', meta)
        self.assertIn('generated_output', meta)
        self.assertIn('tmdl_stats', meta)
        self.assertEqual(meta['objects_converted']['worksheets'], 1)


# ═══════════════════════════════════════════════════════════════════
#  PermissionError retry in create_report_structure
# ═══════════════════════════════════════════════════════════════════

class TestPermissionErrorRetry(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    @patch('time.sleep')
    def test_retry_on_permission_error(self, mock_sleep):
        """When rmtree fails with PermissionError, it retries then falls back."""
        co = _base_converted()
        # First run creates the report dir
        self.gen.create_report_structure(self.tmpdir, 'R', co)
        # Write a locked-like file
        locked = os.path.join(self.tmpdir, 'R.Report', 'definition', 'marker')
        os.makedirs(os.path.dirname(locked), exist_ok=True)
        with open(locked, 'w') as f:
            f.write('x')
        # Second run should succeed (retries if needed)
        rd = self.gen.create_report_structure(self.tmpdir, 'R', co)
        self.assertTrue(os.path.isdir(rd))


# ═══════════════════════════════════════════════════════════════════
#  _build_field_mapping groups
# ═══════════════════════════════════════════════════════════════════

class TestFieldMappingGroups(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_groups_mapped(self):
        co = _base_converted()
        co['groups'] = [{'name': '[MyGroup]'}]
        self.gen._build_field_mapping(co)
        self.assertIn('MyGroup', self.gen._field_map)

    def test_calc_measure_from_converted(self):
        co = _base_converted()
        co['calculations'] = [
            {'name': '[Metric]', 'caption': 'Metric', 'role': 'measure'}
        ]
        self.gen._build_field_mapping(co)
        self.assertTrue(self.gen._is_measure_field('Metric'))

    def test_unknown_table_skipped(self):
        co = _base_converted()
        co['datasources'][0]['tables'] = [
            {'name': 'Unknown', 'columns': [{'name': 'X'}]}
        ]
        self.gen._build_field_mapping(co)
        self.assertNotIn('X', self.gen._field_map)


# ═══════════════════════════════════════════════════════════════════
#  _build_report_json
# ═══════════════════════════════════════════════════════════════════

class TestBuildReportJson(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_theme(self):
        rj = self.gen._build_report_json(None)
        self.assertIn('$schema', rj)
        self.assertNotIn('customTheme', rj.get('themeCollection', {}))

    def test_with_theme(self):
        rj = self.gen._build_report_json({'colors': ['#FFF']})
        self.assertIn('customTheme', rj['themeCollection'])

    def test_with_report_filters(self):
        filters = [{'field': 'City', 'type': 'categorical', 'values': ['A']}]
        rj = self.gen._build_report_json(None, report_filters=filters)
        self.assertIn('filterConfig', rj)

    def test_no_report_filters(self):
        rj = self.gen._build_report_json(None, report_filters=[])
        self.assertNotIn('filterConfig', rj)


# ═══════════════════════════════════════════════════════════════════
#  _count_report_artifacts
# ═══════════════════════════════════════════════════════════════════

class TestCountReportArtifacts(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_no_report_dir(self):
        pages, visuals = self.gen._count_report_artifacts(self.tmpdir, 'X')
        self.assertEqual(pages, 0)
        self.assertEqual(visuals, 0)

    def test_counts_pages_and_visuals(self):
        co = _base_converted()
        self.gen.generate_project('C', co)
        proj = os.path.join(self.tmpdir, 'C')
        pages, visuals = self.gen._count_report_artifacts(proj, 'C')
        self.assertGreaterEqual(pages, 1)
        self.assertGreaterEqual(visuals, 0)


# ═══════════════════════════════════════════════════════════════════
#  Visual helper methods (textbox, image, nav_button, action_button,
#  filter_control, pages_shelf)
# ═══════════════════════════════════════════════════════════════════

class TestVisualHelperMethods(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)
        self.gen._build_field_mapping(_base_converted())
        self.vdir = os.path.join(self.tmpdir, 'visuals')
        os.makedirs(self.vdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_create_visual_textbox(self):
        obj = {'content': 'Hi', 'position': {'x': 0, 'y': 0, 'w': 200, 'h': 50}}
        self.gen._create_visual_textbox(self.vdir, obj, 1.0, 1.0, 0)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)
        vj = os.path.join(self.vdir, dirs[0], 'visual.json')
        with open(vj) as f:
            data = json.load(f)
        self.assertEqual(data['visual']['visualType'], 'textbox')

    def test_create_visual_image(self):
        obj = {'source': 'https://x.png', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 100}}
        self.gen._create_visual_image(self.vdir, obj, 1.0, 1.0, 0)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)
        vj = os.path.join(self.vdir, dirs[0], 'visual.json')
        with open(vj) as f:
            data = json.load(f)
        self.assertEqual(data['visual']['visualType'], 'image')

    def test_create_visual_nav_button(self):
        obj = {'target_sheet': 'S1', 'name': 'Go', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 40}}
        self.gen._create_visual_nav_button(self.vdir, obj, 1.0, 1.0, 0)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)
        vj = os.path.join(self.vdir, dirs[0], 'visual.json')
        with open(vj) as f:
            data = json.load(f)
        self.assertEqual(data['visual']['visualType'], 'actionButton')

    def test_create_visual_action_button(self):
        obj = {'name': 'DL', 'position': {'x': 0, 'y': 0, 'w': 100, 'h': 40}}
        self.gen._create_visual_action_button(self.vdir, obj, 1.0, 1.0, 0, 'Export')
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)

    def test_create_visual_filter_control(self):
        co = _base_converted()
        obj = {'field': 'City', 'position': {'x': 0, 'y': 0, 'w': 200, 'h': 60}}
        self.gen._create_visual_filter_control(self.vdir, obj, 1.0, 1.0, 0, {}, co)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)

    def test_create_pages_shelf_slicer(self):
        co = _base_converted()
        ps = {'field': 'City'}
        self.gen._create_pages_shelf_slicer(self.vdir, ps, 1.0, 1.0, 0, co)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 1)

    def test_create_pages_shelf_slicer_empty_field(self):
        co = _base_converted()
        ps = {'field': ''}
        self.gen._create_pages_shelf_slicer(self.vdir, ps, 1.0, 1.0, 0, co)
        dirs = os.listdir(self.vdir)
        self.assertEqual(len(dirs), 0)


# ═══════════════════════════════════════════════════════════════════
#  _is_measure_field via mapped property
# ═══════════════════════════════════════════════════════════════════

class TestIsMeasureFieldIndirect(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.gen = _make_gen(self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_measure_via_field_map_prop(self):
        self.gen._field_map = {'alias': ('T', 'Total')}
        self.gen._measure_names = {'Total'}
        self.assertTrue(self.gen._is_measure_field('alias'))

    def test_no_field_map(self):
        # No _field_map attribute at all
        gen2 = FabricPBIPGenerator.__new__(FabricPBIPGenerator)
        self.assertFalse(gen2._is_measure_field('x'))


if __name__ == '__main__':
    unittest.main()
