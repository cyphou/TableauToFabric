"""Coverage tests for tableau_export/extract_tableau_data.py.

Uses inline XML via ET.fromstring() to test ~45 extraction methods
without requiring real .twb/.twbx files.
"""

import unittest
import xml.etree.ElementTree as ET

from tableau_export.extract_tableau_data import TableauExtractor


def _ext():
    """Create a minimal extractor without touching the filesystem."""
    e = TableauExtractor.__new__(TableauExtractor)
    e.workbook_data = {}
    e.tableau_file = "test.twb"
    e.output_dir = "test_output/"
    return e


# ═══════════════════════════════════════════════════════════════
#  _map_tableau_mark_to_type  (Category A — no XML)
# ═══════════════════════════════════════════════════════════════

class TestMapTableauMarkToType(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_bar(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Bar"), "clusteredBarChart")

    def test_line(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Line"), "lineChart")

    def test_pie(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Pie"), "pieChart")

    def test_map(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Map"), "map")

    def test_text(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Text"), "tableEx")

    def test_automatic(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Automatic"), "table")

    def test_donut(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Donut"), "donutChart")

    def test_waterfall(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Waterfall"), "waterfallChart")

    def test_area(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Area"), "areaChart")

    def test_scatter(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Circle"), "scatterChart")

    def test_gantt(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("GanttBar"), "clusteredBarChart")

    def test_treemap(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("Square"), "treemap")

    def test_unknown_fallback(self):
        self.assertEqual(self.ext._map_tableau_mark_to_type("UnknownType"), "clusteredBarChart")


# ═══════════════════════════════════════════════════════════════
#  determine_chart_type
# ═══════════════════════════════════════════════════════════════

class TestDetermineChartType(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_pane_mark(self):
        ws = ET.fromstring('<worksheet><pane><mark class="Line"/></pane></worksheet>')
        result = self.ext.determine_chart_type(ws)
        self.assertEqual(result, "lineChart")

    def test_style_mark(self):
        ws = ET.fromstring('<worksheet><style><mark class="Pie"/></style></worksheet>')
        result = self.ext.determine_chart_type(ws)
        self.assertEqual(result, "pieChart")

    def test_map_fallback(self):
        ws = ET.fromstring('<worksheet><encoding><map/></encoding></worksheet>')
        result = self.ext.determine_chart_type(ws)
        self.assertIn("map", result.lower())

    def test_default_is_bar(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        result = self.ext.determine_chart_type(ws)
        self.assertIn("bar", result.lower())


# ═══════════════════════════════════════════════════════════════
#  extract_worksheet_fields
# ═══════════════════════════════════════════════════════════════

class TestExtractWorksheetFields(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_cols_and_rows(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <table>'
            '    <cols>[DS1].[sum:Sales:qk] [DS1].[Category]</cols>'
            '    <rows>[DS1].[none:Region:nk]</rows>'
            '  </table>'
            '</worksheet>'
        )
        fields = self.ext.extract_worksheet_fields(ws)
        names = {f.get("name", f.get("field", "")) for f in fields}
        self.assertTrue(len(fields) >= 2)

    def test_encoding_fields(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <encodings>'
            '    <color column="[DS1].[sum:Profit:qk]"/>'
            '    <size column="[DS1].[Quantity]"/>'
            '  </encodings>'
            '</worksheet>'
        )
        fields = self.ext.extract_worksheet_fields(ws)
        self.assertTrue(len(fields) >= 1)

    def test_empty_worksheet(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        fields = self.ext.extract_worksheet_fields(ws)
        self.assertEqual(fields, [])


# ═══════════════════════════════════════════════════════════════
#  extract_worksheet_filters
# ═══════════════════════════════════════════════════════════════

class TestExtractWorksheetFilters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_categorical_member(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <filter column="[DS].[Region]">'
            '    <groupfilter function="member" member="East"/>'
            '  </filter>'
            '</worksheet>'
        )
        filters = self.ext.extract_worksheet_filters(ws)
        self.assertTrue(len(filters) >= 1)

    def test_categorical_union(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <filter column="[DS].[Region]">'
            '    <groupfilter function="union">'
            '      <groupfilter function="member" member="East"/>'
            '      <groupfilter function="member" member="West"/>'
            '    </groupfilter>'
            '  </filter>'
            '</worksheet>'
        )
        filters = self.ext.extract_worksheet_filters(ws)
        self.assertTrue(len(filters) >= 1)

    def test_range_filter(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <filter column="[DS].[Sales]">'
            '    <groupfilter function="range" from="100" to="500"/>'
            '  </filter>'
            '</worksheet>'
        )
        filters = self.ext.extract_worksheet_filters(ws)
        self.assertTrue(len(filters) >= 1)

    def test_level_members_all(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <filter column="[DS].[Category]">'
            '    <groupfilter function="level-members"/>'
            '  </filter>'
            '</worksheet>'
        )
        filters = self.ext.extract_worksheet_filters(ws)
        self.assertTrue(len(filters) >= 1)

    def test_empty_worksheet(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        filters = self.ext.extract_worksheet_filters(ws)
        self.assertEqual(filters, [])


# ═══════════════════════════════════════════════════════════════
#  extract_formatting
# ═══════════════════════════════════════════════════════════════

class TestExtractFormatting(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_style_rules(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <style-rule element="cell">'
            '    <format attr="font-size" value="12"/>'
            '  </style-rule>'
            '</worksheet>'
        )
        fmt = self.ext.extract_formatting(ws)
        self.assertIsInstance(fmt, dict)

    def test_empty_element(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        fmt = self.ext.extract_formatting(ws)
        self.assertIsInstance(fmt, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_tooltips
# ═══════════════════════════════════════════════════════════════

class TestExtractTooltips(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_text_tooltip(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <tooltip>'
            '    <formatted-text><run>Sales: </run></formatted-text>'
            '  </tooltip>'
            '</worksheet>'
        )
        tips = self.ext.extract_tooltips(ws)
        self.assertTrue(len(tips) >= 1)

    def test_viz_in_tooltip(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <tooltip viz="OtherSheet"/>'
            '</worksheet>'
        )
        tips = self.ext.extract_tooltips(ws)
        self.assertTrue(len(tips) >= 1)

    def test_empty_worksheet(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        tips = self.ext.extract_tooltips(ws)
        self.assertEqual(tips, [])


# ═══════════════════════════════════════════════════════════════
#  extract_mark_encoding
# ═══════════════════════════════════════════════════════════════

class TestExtractMarkEncoding(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_color_encoding(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <encodings>'
            '    <color column="[DS].[sum:Profit:qk]" palette="green_gold" type="continuous">'
            '      <color>#FF0000</color>'
            '      <color>#00FF00</color>'
            '    </color>'
            '  </encodings>'
            '</worksheet>'
        )
        enc = self.ext.extract_mark_encoding(ws)
        self.assertIn("color", enc)

    def test_size_shape_label(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <encodings>'
            '    <size column="[DS].[Quantity]"/>'
            '    <shape column="[DS].[Category]"/>'
            '    <label column="[DS].[Name]" show-label="true"/>'
            '  </encodings>'
            '</worksheet>'
        )
        enc = self.ext.extract_mark_encoding(ws)
        self.assertIn("size", enc)
        self.assertIn("shape", enc)
        self.assertIn("label", enc)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        enc = self.ext.extract_mark_encoding(ws)
        self.assertIsInstance(enc, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_axes
# ═══════════════════════════════════════════════════════════════

class TestExtractAxes(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_dual_axis(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <axis type="x" auto-range="false" range-min="0" range-max="100">'
            '    <title>X Axis</title>'
            '  </axis>'
            '  <axis type="y" quantitative="true"/>'
            '</worksheet>'
        )
        axes = self.ext.extract_axes(ws)
        self.assertIn("x", axes)
        self.assertIn("y", axes)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        axes = self.ext.extract_axes(ws)
        self.assertIsInstance(axes, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_annotations
# ═══════════════════════════════════════════════════════════════

class TestExtractAnnotations(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_point_annotation(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <point-annotation column="Sales" row="2023">'
            '    <formatted-text><run>Max Sales</run></formatted-text>'
            '  </point-annotation>'
            '</worksheet>'
        )
        anns = self.ext.extract_annotations(ws)
        self.assertTrue(len(anns) >= 1)

    def test_area_annotation(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <area-annotation>'
            '    <formatted-text><run>Shaded</run></formatted-text>'
            '  </area-annotation>'
            '</worksheet>'
        )
        anns = self.ext.extract_annotations(ws)
        self.assertTrue(len(anns) >= 1)

    def test_text_annotation(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <text-annotation>'
            '    <formatted-text><run>Note</run></formatted-text>'
            '  </text-annotation>'
            '</worksheet>'
        )
        anns = self.ext.extract_annotations(ws)
        self.assertTrue(len(anns) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        anns = self.ext.extract_annotations(ws)
        self.assertEqual(anns, [])


# ═══════════════════════════════════════════════════════════════
#  extract_trend_lines
# ═══════════════════════════════════════════════════════════════

class TestExtractTrendLines(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_trend_line(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <trend-line type="polynomial" column="Sales" color="#FF0000"'
            '              show-confidence="true" show-equation="true"'
            '              show-r-squared="true" per-color="true"/>'
            '</worksheet>'
        )
        lines = self.ext.extract_trend_lines(ws)
        self.assertTrue(len(lines) >= 1)

    def test_nested_trend_lines(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <trend-lines>'
            '    <trend-line type="exponential" column="Revenue"/>'
            '  </trend-lines>'
            '</worksheet>'
        )
        lines = self.ext.extract_trend_lines(ws)
        self.assertTrue(len(lines) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        lines = self.ext.extract_trend_lines(ws)
        self.assertEqual(lines, [])


# ═══════════════════════════════════════════════════════════════
#  extract_reference_lines
# ═══════════════════════════════════════════════════════════════

class TestExtractReferenceLines(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_reference_line(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <reference-line value="50" label="Target" label-type="custom"'
            '                  scope="per-cell" axis="x" color="#333" style="dashed"'
            '                  computation="constant" column="Sales"/>'
            '</worksheet>'
        )
        refs = self.ext.extract_reference_lines(ws)
        self.assertTrue(len(refs) >= 1)

    def test_reference_band(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <reference-band value-from="20" value-to="80" label="Band"'
            '                  scope="per-pane" axis="y" color="#EEE"'
            '                  fill-above="gray" fill-below="white"/>'
            '</worksheet>'
        )
        refs = self.ext.extract_reference_lines(ws)
        self.assertTrue(len(refs) >= 1)

    def test_reference_distribution(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <reference-distribution computation="normal" scope="per-pane"'
            '                          axis="y" color="#666" label="Dist" percentile="95"/>'
            '</worksheet>'
        )
        refs = self.ext.extract_reference_lines(ws)
        self.assertTrue(len(refs) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        refs = self.ext.extract_reference_lines(ws)
        self.assertEqual(refs, [])


# ═══════════════════════════════════════════════════════════════
#  extract_pages_shelf
# ═══════════════════════════════════════════════════════════════

class TestExtractPagesShelf(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_with_ds_prefix(self):
        ws = ET.fromstring('<worksheet><pages>[DS1].[Date]</pages></worksheet>')
        result = self.ext.extract_pages_shelf(ws)
        self.assertIn("field", result)

    def test_without_prefix(self):
        ws = ET.fromstring('<worksheet><pages>[Year]</pages></worksheet>')
        result = self.ext.extract_pages_shelf(ws)
        self.assertIn("field", result)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        result = self.ext.extract_pages_shelf(ws)
        self.assertEqual(result, {})


# ═══════════════════════════════════════════════════════════════
#  extract_table_calcs
# ═══════════════════════════════════════════════════════════════

class TestExtractTableCalcs(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_table_calc(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <table-calc column="[Calc1]" type="Percent Difference"'
            '              ordering-type="Columns" direction="Across" at-level="2">'
            '    <compute-using>Category</compute-using>'
            '    <compute-using>Region</compute-using>'
            '    <order-by column="[Sales]" direction="DESC"/>'
            '  </table-calc>'
            '</worksheet>'
        )
        calcs = self.ext.extract_table_calcs(ws)
        self.assertTrue(len(calcs) >= 1)
        calc = calcs[0]
        self.assertIn("type", calc)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        calcs = self.ext.extract_table_calcs(ws)
        self.assertEqual(calcs, [])


# ═══════════════════════════════════════════════════════════════
#  extract_forecasting
# ═══════════════════════════════════════════════════════════════

class TestExtractForecasting(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_forecast_element(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <forecast forecast-forward="10" forecast-backward="2"'
            '            prediction-interval="90" ignore-last="1"'
            '            model="multiplicative" show-prediction-bands="false"'
            '            fill-between="false"/>'
            '</worksheet>'
        )
        fc = self.ext.extract_forecasting(ws)
        self.assertIsNotNone(fc)

    def test_forecast_model_fallback(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <forecast-model periods="7" prediction-interval="99" model="additive"/>'
            '</worksheet>'
        )
        fc = self.ext.extract_forecasting(ws)
        self.assertIsNotNone(fc)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        fc = self.ext.extract_forecasting(ws)
        self.assertEqual(fc, [])


# ═══════════════════════════════════════════════════════════════
#  extract_map_options
# ═══════════════════════════════════════════════════════════════

class TestExtractMapOptions(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_map_options(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <map-options washout="0.5" map-style="dark" show-map-search="true"'
            '               pan-zoom="false" unit="kilometers">'
            '    <map-layer name="Streets" enabled="true"/>'
            '  </map-options>'
            '</worksheet>'
        )
        opts = self.ext.extract_map_options(ws)
        self.assertIsInstance(opts, dict)
        self.assertTrue(len(opts) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        opts = self.ext.extract_map_options(ws)
        self.assertEqual(opts, {})


# ═══════════════════════════════════════════════════════════════
#  extract_clustering
# ═══════════════════════════════════════════════════════════════

class TestExtractClustering(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_cluster(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <cluster num-clusters="5" seed="42">'
            '    <variable column="[Sales]"/>'
            '    <variable column="[Profit]"/>'
            '  </cluster>'
            '</worksheet>'
        )
        cl = self.ext.extract_clustering(ws)
        self.assertIsNotNone(cl)

    def test_cluster_analysis_fallback(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <cluster-analysis num-clusters="auto" seed="123"/>'
            '</worksheet>'
        )
        cl = self.ext.extract_clustering(ws)
        self.assertIsNotNone(cl)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        cl = self.ext.extract_clustering(ws)
        self.assertEqual(cl, [])


# ═══════════════════════════════════════════════════════════════
#  extract_dual_axis_sync
# ═══════════════════════════════════════════════════════════════

class TestExtractDualAxisSync(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_dual_axis_synced(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <axis type="y1"/><axis type="y2" synchronized="true"/>'
            '</worksheet>'
        )
        result = self.ext.extract_dual_axis_sync(ws)
        self.assertTrue(result.get("enabled", False))

    def test_single_axis(self):
        ws = ET.fromstring('<worksheet><axis type="y"/></worksheet>')
        result = self.ext.extract_dual_axis_sync(ws)
        self.assertEqual(result, {})

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        result = self.ext.extract_dual_axis_sync(ws)
        self.assertEqual(result, {})


# ═══════════════════════════════════════════════════════════════
#  extract_totals_subtotals
# ═══════════════════════════════════════════════════════════════

class TestExtractTotalsSubtotals(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_grandtotals(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <grandtotals>'
            '    <grand-total type="sum" position="bottom" enabled="true"/>'
            '  </grandtotals>'
            '</worksheet>'
        )
        result = self.ext.extract_totals_subtotals(ws)
        self.assertIsInstance(result, dict)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        result = self.ext.extract_totals_subtotals(ws)
        self.assertIsInstance(result, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_worksheet_description
# ═══════════════════════════════════════════════════════════════

class TestExtractWorksheetDescription(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_from_attribute(self):
        ws = ET.fromstring('<worksheet description="Sales overview"/>')
        result = self.ext.extract_worksheet_description(ws)
        self.assertEqual(result, "Sales overview")

    def test_from_child(self):
        ws = ET.fromstring('<worksheet><description>Revenue</description></worksheet>')
        result = self.ext.extract_worksheet_description(ws)
        self.assertEqual(result, "Revenue")

    def test_empty(self):
        ws = ET.fromstring('<worksheet/>')
        result = self.ext.extract_worksheet_description(ws)
        self.assertEqual(result, "")


# ═══════════════════════════════════════════════════════════════
#  extract_show_hide_headers
# ═══════════════════════════════════════════════════════════════

class TestExtractShowHideHeaders(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_style_based(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <style show-row-headers="false" show-col-headers="true"/>'
            '</worksheet>'
        )
        result = self.ext.extract_show_hide_headers(ws)
        self.assertIsInstance(result, dict)

    def test_default(self):
        ws = ET.fromstring('<worksheet/>')
        result = self.ext.extract_show_hide_headers(ws)
        self.assertIsInstance(result, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_dynamic_title
# ═══════════════════════════════════════════════════════════════

class TestExtractDynamicTitle(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_dynamic_title(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <title>'
            '    <run>Sales for </run>'
            '    <run><field name="Year"/></run>'
            '  </title>'
            '</worksheet>'
        )
        result = self.ext.extract_dynamic_title(ws)
        self.assertIsNotNone(result)

    def test_static_title(self):
        ws = ET.fromstring(
            '<worksheet><title><run>Static Title</run></title></worksheet>'
        )
        result = self.ext.extract_dynamic_title(ws)
        self.assertIsNotNone(result)

    def test_no_title(self):
        ws = ET.fromstring('<worksheet/>')
        result = self.ext.extract_dynamic_title(ws)
        self.assertIsNone(result)


# ═══════════════════════════════════════════════════════════════
#  extract_analytics_pane_stats
# ═══════════════════════════════════════════════════════════════

class TestExtractAnalyticsPaneStats(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_stat_line(self):
        ws = ET.fromstring(
            '<worksheet>'
            '  <stat-line stat="mean" scope="per-pane" value="42"/>'
            '</worksheet>'
        )
        stats = self.ext.extract_analytics_pane_stats(ws)
        self.assertTrue(len(stats) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        stats = self.ext.extract_analytics_pane_stats(ws)
        self.assertEqual(stats, [])


# ═══════════════════════════════════════════════════════════════
#  extract_dashboard_objects
# ═══════════════════════════════════════════════════════════════

class TestExtractDashboardObjects(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_text_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="text" name="title" id="1" x="0" y="0" w="300" h="50" is-floating="true">'
            '    <formatted-text><run>Dashboard Title</run></formatted-text>'
            '  </zone>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        text_objs = [o for o in objs if o.get("type") == "text"]
        self.assertTrue(len(text_objs) >= 1)

    def test_image_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="bitmap" id="2" x="0" y="50" w="200" h="100">'
            '    <zone-style><format attr="image" value="logo.png"/></zone-style>'
            '  </zone>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        img_objs = [o for o in objs if o.get("type") == "image"]
        self.assertTrue(len(img_objs) >= 1)

    def test_web_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="web" id="3" x="0" y="0" w="400" h="300" url="https://example.com"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        web_objs = [o for o in objs if o.get("type") == "web"]
        self.assertTrue(len(web_objs) >= 1)

    def test_empty_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="empty" id="4" x="0" y="0" w="100" h="20"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        blank_objs = [o for o in objs if o.get("type") == "blank"]
        self.assertTrue(len(blank_objs) >= 1)

    def test_nav_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="nav" id="5" name="GoTo" target-sheet="Sheet2" x="0" y="0" w="100" h="30"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        self.assertTrue(len(objs) >= 1)

    def test_export_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="export" id="6" x="0" y="0" w="80" h="30"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        self.assertTrue(len(objs) >= 1)

    def test_extension_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="extension" id="7" extension-id="com.vendor.ext" x="0" y="0" w="200" h="200"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        self.assertTrue(len(objs) >= 1)

    def test_filter_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone type="filter" id="8" name="Region Filter" param="none:Region:nk"'
            '        x="0" y="0" w="150" h="30"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        filter_objs = [o for o in objs if o.get("type") == "filter_control"]
        self.assertTrue(len(filter_objs) >= 1)

    def test_worksheet_zone(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone name="Sales Chart" id="9" x="100" y="100" w="600" h="400"/>'
            '</dashboard>'
        )
        objs = self.ext.extract_dashboard_objects(db)
        ws_objs = [o for o in objs if o.get("type") == "worksheetReference"]
        self.assertTrue(len(ws_objs) >= 1)

    def test_empty_dashboard(self):
        db = ET.fromstring('<dashboard></dashboard>')
        objs = self.ext.extract_dashboard_objects(db)
        self.assertEqual(objs, [])


# ═══════════════════════════════════════════════════════════════
#  extract_dashboard_filters
# ═══════════════════════════════════════════════════════════════

class TestExtractDashboardFilters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_filters(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <filter column="[DS].[Region]"><value>East</value><value>West</value></filter>'
            '</dashboard>'
        )
        filters = self.ext.extract_dashboard_filters(db)
        self.assertTrue(len(filters) >= 1)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        filters = self.ext.extract_dashboard_filters(db)
        self.assertEqual(filters, [])


# ═══════════════════════════════════════════════════════════════
#  extract_dashboard_parameters
# ═══════════════════════════════════════════════════════════════

class TestExtractDashboardParameters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_parameters(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone param="[Parameter 1]" name="Param Control" x="10" y="20" w="200" h="30"/>'
            '  <zone name="NoParam"/>'
            '</dashboard>'
        )
        params = self.ext.extract_dashboard_parameters(db)
        self.assertTrue(len(params) >= 1)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        params = self.ext.extract_dashboard_parameters(db)
        self.assertEqual(params, [])


# ═══════════════════════════════════════════════════════════════
#  extract_theme
# ═══════════════════════════════════════════════════════════════

class TestExtractTheme(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_theme_elements(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <preferences>'
            '    <color-palette name="Custom" type="ordered-diverging">'
            '      <color>#FF0000</color><color>#00FF00</color>'
            '    </color-palette>'
            '  </preferences>'
            '  <style>'
            '    <style-rule element="worksheet">'
            '      <format attr="font-size" value="12"/>'
            '    </style-rule>'
            '  </style>'
            '  <format attr="font-family" value="Segoe UI"/>'
            '</dashboard>'
        )
        theme = self.ext.extract_theme(db)
        self.assertIsInstance(theme, dict)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        theme = self.ext.extract_theme(db)
        self.assertIsInstance(theme, dict)


# ═══════════════════════════════════════════════════════════════
#  extract_device_layouts
# ═══════════════════════════════════════════════════════════════

class TestExtractDeviceLayouts(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_device_layout(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <devicelayout type="phone" width="375" height="667">'
            '    <zone name="Chart1" type="worksheet" x="0" y="0" w="375" h="300" is-visible="true"/>'
            '  </devicelayout>'
            '</dashboard>'
        )
        layouts = self.ext.extract_device_layouts(db)
        self.assertTrue(len(layouts) >= 1)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        layouts = self.ext.extract_device_layouts(db)
        self.assertEqual(layouts, [])


# ═══════════════════════════════════════════════════════════════
#  extract_dashboard_containers
# ═══════════════════════════════════════════════════════════════

class TestExtractDashboardContainers(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_container(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <layout-container orientation="horizontal" name="Row1"'
            '                    x="0" y="0" w="800" h="200">'
            '    <zone name="Chart1" x="0" y="0" w="400" h="200"/>'
            '    <zone name="Chart2" x="400" y="0" w="400" h="200"/>'
            '  </layout-container>'
            '</dashboard>'
        )
        containers = self.ext.extract_dashboard_containers(db)
        self.assertTrue(len(containers) >= 1)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        containers = self.ext.extract_dashboard_containers(db)
        self.assertEqual(containers, [])


# ═══════════════════════════════════════════════════════════════
#  extract_show_hide_containers
# ═══════════════════════════════════════════════════════════════

class TestExtractShowHideContainers(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_show_hide(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone name="FilterPanel" id="1">'
            '    <show-hide-button default-state="hide" style="icon"/>'
            '  </zone>'
            '</dashboard>'
        )
        result = self.ext.extract_show_hide_containers(db)
        self.assertTrue(len(result) >= 1)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        result = self.ext.extract_show_hide_containers(db)
        self.assertEqual(result, [])


# ═══════════════════════════════════════════════════════════════
#  extract_floating_tiled
# ═══════════════════════════════════════════════════════════════

class TestExtractFloatingTiled(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_floating_and_tiled(self):
        db = ET.fromstring(
            '<dashboard>'
            '  <zone name="Z1" id="1" is-floating="true" x="10" y="20" w="300" h="200"/>'
            '  <zone name="Z2" id="2" x="0" y="0" w="400" h="300"/>'
            '</dashboard>'
        )
        result = self.ext.extract_floating_tiled(db)
        self.assertIsInstance(result, list)

    def test_empty(self):
        db = ET.fromstring('<dashboard></dashboard>')
        result = self.ext.extract_floating_tiled(db)
        self.assertIsInstance(result, list)


# ═══════════════════════════════════════════════════════════════
#  extract_parameters (root-level)
# ═══════════════════════════════════════════════════════════════

class TestExtractParameters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_new_format_parameters(self):
        root = ET.fromstring(
            '<workbook>'
            '  <parameters>'
            '    <parameter name="[TopN]" caption="Top N" datatype="integer" value="10">'
            '      <range min="1" max="100" granularity="1"/>'
            '    </parameter>'
            '  </parameters>'
            '</workbook>'
        )
        self.ext.extract_parameters(root)
        params = self.ext.workbook_data.get('parameters', [])
        self.assertTrue(len(params) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_parameters(root)
        params = self.ext.workbook_data.get('parameters', [])
        self.assertEqual(params, [])


# ═══════════════════════════════════════════════════════════════
#  extract_filters (root-level)
# ═══════════════════════════════════════════════════════════════

class TestExtractFilters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_filters(self):
        root = ET.fromstring(
            '<workbook>'
            '  <filter column="Region" type="categorical">'
            '    <value>East</value><value>West</value>'
            '  </filter>'
            '</workbook>'
        )
        self.ext.extract_filters(root)
        filters = self.ext.workbook_data.get('filters', [])
        self.assertTrue(len(filters) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_filters(root)
        filters = self.ext.workbook_data.get('filters', [])
        self.assertEqual(filters, [])


# ═══════════════════════════════════════════════════════════════
#  extract_stories
# ═══════════════════════════════════════════════════════════════

class TestExtractStories(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_story(self):
        root = ET.fromstring(
            '<workbook>'
            '  <story name="Revenue Story">'
            '    <story-point captured-sheet="Sheet1">'
            '      <caption>Q1 Overview</caption>'
            '    </story-point>'
            '  </story>'
            '</workbook>'
        )
        self.ext.extract_stories(root)
        stories = self.ext.workbook_data.get('stories', [])
        self.assertTrue(len(stories) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_stories(root)
        stories = self.ext.workbook_data.get('stories', [])
        self.assertEqual(stories, [])


# ═══════════════════════════════════════════════════════════════
#  extract_workbook_actions
# ═══════════════════════════════════════════════════════════════

class TestExtractWorkbookActions(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_filter_action(self):
        root = ET.fromstring(
            '<workbook>'
            '  <action name="FilterSales" type="filter" command="run"'
            '          clearing="keep" run-on="select">'
            '    <source worksheet="Sheet1"/>'
            '    <target worksheet="Sheet2"/>'
            '    <field-mapping source-field="[Region]" target-field="[Region]"/>'
            '  </action>'
            '</workbook>'
        )
        self.ext.extract_workbook_actions(root)
        actions = self.ext.workbook_data.get('actions', [])
        self.assertTrue(len(actions) >= 1)
        self.assertEqual(actions[0].get("type"), "filter")

    def test_url_action(self):
        root = ET.fromstring(
            '<workbook>'
            '  <action name="OpenDocs" type="url" url="https://docs.example.com"/>'
            '</workbook>'
        )
        self.ext.extract_workbook_actions(root)
        actions = self.ext.workbook_data.get('actions', [])
        self.assertTrue(len(actions) >= 1)

    def test_highlight_action(self):
        root = ET.fromstring(
            '<workbook>'
            '  <action name="HL" type="highlight">'
            '    <source worksheet="Sheet1"/>'
            '  </action>'
            '</workbook>'
        )
        self.ext.extract_workbook_actions(root)
        actions = self.ext.workbook_data.get('actions', [])
        self.assertTrue(len(actions) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_workbook_actions(root)
        actions = self.ext.workbook_data.get('actions', [])
        self.assertEqual(actions, [])


# ═══════════════════════════════════════════════════════════════
#  extract_sets
# ═══════════════════════════════════════════════════════════════

class TestExtractSets(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_set_with_members(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <column name="[TopCust-set-]" caption="Top Customers" datatype="boolean">'
            '      <set formula="SUM([Sales]) &gt; 10000">'
            '        <member value="Customer A"/>'
            '        <member value="Customer B"/>'
            '      </set>'
            '    </column>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_sets(root)
        sets = self.ext.workbook_data.get('sets', [])
        self.assertTrue(len(sets) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_sets(root)
        sets = self.ext.workbook_data.get('sets', [])
        self.assertEqual(sets, [])


# ═══════════════════════════════════════════════════════════════
#  extract_groups
# ═══════════════════════════════════════════════════════════════

class TestExtractGroups(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_combined_group(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <group name="CityState" caption="City-State">'
            '      <groupfilter function="crossjoin">'
            '        <groupfilter function="level-members" level="none:City:nk"/>'
            '        <groupfilter function="level-members" level="none:State:nk"/>'
            '      </groupfilter>'
            '    </group>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_groups(root)
        groups = self.ext.workbook_data.get('groups', [])
        self.assertTrue(len(groups) >= 1)

    def test_value_group(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <group name="RegionGroup" caption="Region Group">'
            '      <groupfilter function="union">'
            '        <groupfilter function="member" member="East"/>'
            '        <groupfilter function="member" member="West"/>'
            '      </groupfilter>'
            '    </group>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_groups(root)
        groups = self.ext.workbook_data.get('groups', [])
        self.assertTrue(len(groups) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_groups(root)
        groups = self.ext.workbook_data.get('groups', [])
        self.assertEqual(groups, [])


# ═══════════════════════════════════════════════════════════════
#  extract_bins
# ═══════════════════════════════════════════════════════════════

class TestExtractBins(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_bin(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <column name="[SalesBin]" caption="Sales (bin)" datatype="integer">'
            '      <bin source="[Sales]" size="50"/>'
            '    </column>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_bins(root)
        bins = self.ext.workbook_data.get('bins', [])
        self.assertTrue(len(bins) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_bins(root)
        bins = self.ext.workbook_data.get('bins', [])
        self.assertEqual(bins, [])


# ═══════════════════════════════════════════════════════════════
#  extract_hierarchies
# ═══════════════════════════════════════════════════════════════

class TestExtractHierarchies(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_hierarchy(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <drill-path name="Location">'
            '      <field name="[Country]"/><field name="[State]"/><field name="[City]"/>'
            '    </drill-path>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_hierarchies(root)
        hiers = self.ext.workbook_data.get('hierarchies', [])
        self.assertTrue(len(hiers) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_hierarchies(root)
        hiers = self.ext.workbook_data.get('hierarchies', [])
        self.assertEqual(hiers, [])


# ═══════════════════════════════════════════════════════════════
#  extract_sort_orders
# ═══════════════════════════════════════════════════════════════

class TestExtractSortOrders(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_sort_orders(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <sort column="[Region]" direction="ASC" type="data" key="name"/>'
            '    <sort column="[Sales]" direction="DESC" type="manual">'
            '      <value>East</value><value>West</value>'
            '    </sort>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_sort_orders(root)
        sorts = self.ext.workbook_data.get('sort_orders', [])
        self.assertTrue(len(sorts) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_sort_orders(root)
        sorts = self.ext.workbook_data.get('sort_orders', [])
        self.assertEqual(sorts, [])


# ═══════════════════════════════════════════════════════════════
#  extract_aliases
# ═══════════════════════════════════════════════════════════════

class TestExtractAliases(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_aliases(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource>'
            '    <column name="[Status]">'
            '      <aliases>'
            '        <alias key="A" value="Active"/>'
            '        <alias key="I" value="Inactive"/>'
            '      </aliases>'
            '    </column>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_aliases(root)
        aliases = self.ext.workbook_data.get('aliases', {})
        self.assertTrue(len(aliases) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_aliases(root)
        aliases = self.ext.workbook_data.get('aliases', {})
        self.assertEqual(aliases, {})


# ═══════════════════════════════════════════════════════════════
#  extract_custom_sql
# ═══════════════════════════════════════════════════════════════

class TestExtractCustomSql(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_custom_sql(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource name="CustomDS">'
            '    <relation type="text" name="My Query">SELECT * FROM orders</relation>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_custom_sql(root)
        sql = self.ext.workbook_data.get('custom_sql', [])
        self.assertTrue(len(sql) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_custom_sql(root)
        sql = self.ext.workbook_data.get('custom_sql', [])
        self.assertEqual(sql, [])


# ═══════════════════════════════════════════════════════════════
#  extract_user_filters
# ═══════════════════════════════════════════════════════════════

class TestExtractUserFilters(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_user_filter(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource name="Sales" caption="Sales DS">'
            '    <user-filter name="[UserRegion]" column="[Region]">'
            '      <member user="alice@corp.com" value="East"/>'
            '    </user-filter>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_user_filters(root)
        uf = self.ext.workbook_data.get('user_filters', [])
        self.assertTrue(len(uf) >= 1)

    def test_calculated_security(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource name="Sales" caption="Sales DS">'
            '    <column name="[SecurityCalc]" caption="Row Security">'
            '      <calculation formula="IF ISMEMBEROF(\'Admins\') THEN TRUE ELSE FALSE END"/>'
            '    </column>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_user_filters(root)
        uf = self.ext.workbook_data.get('user_filters', [])
        # Should detect ISMEMBEROF as security calc
        self.assertTrue(len(uf) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_user_filters(root)
        uf = self.ext.workbook_data.get('user_filters', [])
        self.assertEqual(uf, [])


# ═══════════════════════════════════════════════════════════════
#  extract_published_datasources
# ═══════════════════════════════════════════════════════════════

class TestExtractPublishedDatasources(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_published_ds(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource name="PubDS" caption="Published Sales">'
            '    <repository-location site="MySite" path="/ds/sales" id="abc-123"/>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_published_datasources(root)
        pds = self.ext.workbook_data.get('published_datasources', [])
        self.assertTrue(len(pds) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_published_datasources(root)
        pds = self.ext.workbook_data.get('published_datasources', [])
        self.assertEqual(pds, [])


# ═══════════════════════════════════════════════════════════════
#  extract_data_blending
# ═══════════════════════════════════════════════════════════════

class TestExtractDataBlending(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_blending(self):
        root = ET.fromstring(
            '<workbook>'
            '  <datasource name="Primary" caption="Primary DS">'
            '    <column name="[Revenue]">'
            '      <link expression="[Sec].[Revenue]" key="Revenue"/>'
            '    </column>'
            '    <datasource-dependencies datasource="Secondary">'
            '      <column name="[Region]" key="blend_key"/>'
            '    </datasource-dependencies>'
            '  </datasource>'
            '</workbook>'
        )
        self.ext.extract_data_blending(root)
        blending = self.ext.workbook_data.get('data_blending', [])
        self.assertTrue(len(blending) >= 1)

    def test_empty(self):
        root = ET.fromstring('<workbook></workbook>')
        self.ext.extract_data_blending(root)
        blending = self.ext.workbook_data.get('data_blending', [])
        self.assertEqual(blending, [])


# ═══════════════════════════════════════════════════════════════
#  extract_worksheet_sort_orders
# ═══════════════════════════════════════════════════════════════

class TestExtractWorksheetSortOrders(unittest.TestCase):

    def setUp(self):
        self.ext = _ext()

    def test_sorts(self):
        ws = ET.fromstring(
            '<worksheet><sort column="[Region]" direction="DESC"/></worksheet>'
        )
        sorts = self.ext.extract_worksheet_sort_orders(ws)
        self.assertTrue(len(sorts) >= 1)

    def test_empty(self):
        ws = ET.fromstring('<worksheet></worksheet>')
        sorts = self.ext.extract_worksheet_sort_orders(ws)
        self.assertEqual(sorts, [])


# ═══════════════════════════════════════════════════════════════
#  extract_actions (backward-compat stub)
# ═══════════════════════════════════════════════════════════════

class TestExtractActions(unittest.TestCase):

    def test_returns_empty_list(self):
        ext = _ext()
        ws = ET.fromstring('<worksheet></worksheet>')
        result = ext.extract_actions(ws)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
