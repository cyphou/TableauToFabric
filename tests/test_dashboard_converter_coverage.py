"""
Extra coverage tests for conversion/dashboard_converter.py.

Targets uncovered branches: web, blank, navigation_button, download_button,
extension, data-story, ask-data object types; show_hide_button; is_floating;
image scaling variants; filter_control_type variants; param datataype/control
variants; device layout tablet; containers None guard.
"""

import unittest

from conversion.dashboard_converter import (
    convert_dashboard_to_report,
    convert_dashboard_pages,
    convert_dashboard_objects,
    convert_image_scaling,
    convert_dashboard_theme,
    convert_dashboard_filters,
    convert_filter_control_type,
    convert_dashboard_parameters,
    convert_param_datatype,
    convert_param_control,
    convert_dashboard_bookmarks,
    convert_dashboard_containers,
    convert_device_layouts,
)


# ── Object types ──────────────────────────────────────────────

class TestConvertDashboardObjects(unittest.TestCase):

    def test_worksheet(self):
        objs = convert_dashboard_objects([
            {'type': 'worksheet', 'worksheet': 'S1',
             'position': {'x': 10, 'y': 20}, 'size': {'width': 300, 'height': 200}}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'worksheetReference')

    def test_text(self):
        objs = convert_dashboard_objects([
            {'type': 'text', 'text': 'Hello', 'font_size': 14}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'textbox')
        self.assertEqual(objs[0]['visual']['content'], 'Hello')

    def test_image(self):
        objs = convert_dashboard_objects([
            {'type': 'image', 'image_url': 'https://img.png', 'scaling': 'fill'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'image')
        self.assertEqual(objs[0]['visual']['scaling'], 'Fill')

    def test_web(self):
        objs = convert_dashboard_objects([
            {'type': 'web', 'url': 'https://x.com'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'webContent')

    def test_blank(self):
        objs = convert_dashboard_objects([
            {'type': 'blank', 'background_color': '#EEE'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'shape')

    def test_navigation_button(self):
        objs = convert_dashboard_objects([
            {'type': 'navigation_button', 'name': 'Go', 'target_sheet': 'S1'}
        ])
        self.assertEqual(objs[0]['visual']['buttonStyle'], 'navigation')

    def test_download_button(self):
        objs = convert_dashboard_objects([
            {'type': 'download_button', 'name': 'DL', 'export_type': 'CSV'}
        ])
        self.assertEqual(objs[0]['visual']['buttonStyle'], 'export')

    def test_extension(self):
        objs = convert_dashboard_objects([
            {'type': 'extension', 'extension_id': 'ext1', 'name': 'My Ext'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'extension')

    def test_data_story(self):
        objs = convert_dashboard_objects([
            {'type': 'data-story', 'name': 'Story'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'smartNarrative')

    def test_ask_data(self):
        objs = convert_dashboard_objects([
            {'type': 'ask-data', 'name': 'Q&A'}
        ])
        self.assertEqual(objs[0]['visual']['type'], 'qnaVisual')

    def test_show_hide_button(self):
        objs = convert_dashboard_objects([
            {'type': 'text', 'text': 'X',
             'show_hide_button': {'target': 'ws1'}}
        ])
        self.assertTrue(objs[0]['showHideToggle']['enabled'])

    def test_is_floating(self):
        objs = convert_dashboard_objects([
            {'type': 'text', 'text': 'X', 'is_floating': True}
        ])
        self.assertTrue(objs[0]['position']['isFixed'])

    def test_unknown_type_no_visual(self):
        objs = convert_dashboard_objects([
            {'type': 'unknown_widget'}
        ])
        self.assertNotIn('visual', objs[0])


# ── Image scaling ─────────────────────────────────────────────

class TestImageScaling(unittest.TestCase):
    def test_fit(self):
        self.assertEqual(convert_image_scaling('fit'), 'Fit')

    def test_fill(self):
        self.assertEqual(convert_image_scaling('fill'), 'Fill')

    def test_stretch(self):
        self.assertEqual(convert_image_scaling('stretch'), 'Fill')

    def test_normal(self):
        self.assertEqual(convert_image_scaling('normal'), 'Normal')

    def test_unknown(self):
        self.assertEqual(convert_image_scaling('zoom'), 'Fit')


# ── Filter control types ─────────────────────────────────────

class TestFilterControlType(unittest.TestCase):
    def test_slider(self):
        self.assertEqual(convert_filter_control_type('slider'), 'slider')

    def test_date(self):
        self.assertEqual(convert_filter_control_type('date'), 'relativeDateFilter')

    def test_wildcard(self):
        self.assertEqual(convert_filter_control_type('wildcard'), 'search')

    def test_unknown(self):
        self.assertEqual(convert_filter_control_type('custom'), 'dropdown')


# ── Param datatypes ──────────────────────────────────────────

class TestParamDatatype(unittest.TestCase):
    def test_boolean(self):
        self.assertEqual(convert_param_datatype('boolean'), 'Boolean')

    def test_date(self):
        self.assertEqual(convert_param_datatype('date'), 'Date')

    def test_datetime(self):
        self.assertEqual(convert_param_datatype('datetime'), 'DateTime')

    def test_unknown(self):
        self.assertEqual(convert_param_datatype('binary'), 'Text')


# ── Param control types ─────────────────────────────────────

class TestParamControl(unittest.TestCase):
    def test_range(self):
        self.assertEqual(convert_param_control('range'), 'slider')

    def test_text(self):
        self.assertEqual(convert_param_control('text'), 'textbox')

    def test_unknown(self):
        self.assertEqual(convert_param_control('spinner'), 'dropdown')


# ── Device layouts ───────────────────────────────────────────

class TestDeviceLayouts(unittest.TestCase):
    def test_phone(self):
        layouts = convert_device_layouts([
            {'device_type': 'phone', 'zones': [
                {'worksheet': 'Sheet1', 'x': 0, 'y': 0, 'w': 375, 'h': 300}
            ]}
        ])
        self.assertEqual(layouts[0]['device_type'], 'phone')
        self.assertEqual(layouts[0]['width'], 375)

    def test_tablet_defaults(self):
        layouts = convert_device_layouts([
            {'device_type': 'tablet', 'zones': []}
        ])
        self.assertEqual(layouts[0]['width'], 768)
        self.assertEqual(layouts[0]['height'], 1024)

    def test_zone_no_worksheet_skipped(self):
        layouts = convert_device_layouts([
            {'device_type': 'phone', 'zones': [{'x': 0, 'y': 0}]}
        ])
        self.assertEqual(len(layouts[0]['visuals']), 0)

    def test_none_input(self):
        self.assertEqual(convert_device_layouts(None), [])


# ── Containers ───────────────────────────────────────────────

class TestContainers(unittest.TestCase):
    def test_none_input(self):
        self.assertEqual(convert_dashboard_containers(None), [])

    def test_vertical(self):
        result = convert_dashboard_containers([
            {'orientation': 'vertical', 'children': [{'name': 'c1'}]}
        ])
        self.assertEqual(result[0]['orientation'], 'vertical')
        self.assertEqual(result[0]['children'], ['c1'])

    def test_children_by_id(self):
        result = convert_dashboard_containers([
            {'orientation': 'horizontal', 'children': [{'id': 'x1'}]}
        ])
        self.assertEqual(result[0]['children'], ['x1'])


# ── Full orchestrator ────────────────────────────────────────

class TestConvertDashboardToReport(unittest.TestCase):
    def test_full(self):
        dashboard = {
            'name': 'Overview',
            'title': 'My Overview',
            'size': {'width': 1280, 'height': 720},
            'objects': [{'type': 'text', 'text': 'Hello'}],
            'theme': {'colors': ['#000']},
            'filters': [{'field': 'City', 'control': 'slider'}],
            'parameters': [{'name': 'P1', 'datatype': 'integer', 'control': 'range'}],
            'stories': [{'caption': 'BM1'}],
            'containers': [{'orientation': 'horizontal', 'children': []}],
            'device_layouts': [{'device_type': 'phone', 'zones': []}],
        }
        result = convert_dashboard_to_report(dashboard)
        self.assertEqual(result['name'], 'Overview')
        self.assertEqual(len(result['pages']), 1)
        self.assertEqual(len(result['filters']), 1)
        self.assertEqual(len(result['bookmarks']), 1)


if __name__ == '__main__':
    unittest.main()
