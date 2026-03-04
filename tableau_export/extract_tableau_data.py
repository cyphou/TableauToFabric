"""
Script for extracting Tableau objects from .twb, .twbx, .tds, .tdsx files

This script extracts metadata and structures from Tableau workbooks
and exports them in JSON format for conversion to Power BI.
"""

import os
import json
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
import re
from .datasource_extractor import extract_datasource


class TableauExtractor:
    """Tableau objects extractor"""
    
    def __init__(self, tableau_file, output_dir='tableau_export/'):
        self.tableau_file = tableau_file
        self.output_dir = output_dir
        self.workbook_data = {}
        
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract_all(self):
        """Extracts all objects from the Tableau workbook"""
        
        print(f"Extracting {self.tableau_file}...")
        
        # Read the Tableau file
        xml_content = self.read_tableau_file()
        
        if not xml_content:
            print("❌ Unable to read the Tableau file")
            return False
        
        # Parse the XML
        root = ET.fromstring(xml_content)
        
        # Extract the different objects
        self.extract_worksheets(root)
        self.extract_dashboards(root)
        self.extract_datasources(root)
        self.extract_calculations(root)
        self.extract_parameters(root)
        self.extract_filters(root)
        self.extract_stories(root)
        self.extract_workbook_actions(root)
        self.extract_sets(root)
        self.extract_groups(root)
        self.extract_bins(root)
        self.extract_hierarchies(root)
        self.extract_sort_orders(root)
        self.extract_aliases(root)
        self.extract_custom_sql(root)
        self.extract_user_filters(root)
        
        # Save the exports
        self.save_extractions()
        
        print("✓ Extraction complete")
        return True
    
    def read_tableau_file(self):
        """Reads the XML content of the Tableau file"""
        
        file_ext = os.path.splitext(self.tableau_file)[1].lower()
        
        if file_ext in ['.twb', '.tds']:
            # Direct XML file
            with open(self.tableau_file, 'r', encoding='utf-8') as f:
                return f.read()
        
        elif file_ext in ['.twbx', '.tdsx']:
            # Packaged file (ZIP)
            with zipfile.ZipFile(self.tableau_file, 'r') as z:
                # Find the .twb or .tds file
                for name in z.namelist():
                    if name.endswith('.twb') or name.endswith('.tds'):
                        with z.open(name) as f:
                            return f.read().decode('utf-8')
        
        return None
    
    def extract_worksheets(self, root):
        """Extracts worksheets"""
        
        worksheets = []
        
        for worksheet in root.findall('.//worksheet'):
            ws_data = {
                'name': worksheet.get('name', ''),
                'title': worksheet.findtext('.//title', ''),
                'chart_type': self.determine_chart_type(worksheet),
                'fields': self.extract_worksheet_fields(worksheet),
                'filters': self.extract_worksheet_filters(worksheet),
                'formatting': self.extract_formatting(worksheet),
                'tooltips': self.extract_tooltips(worksheet),
                'actions': self.extract_actions(worksheet),
                'sort_orders': self.extract_worksheet_sort_orders(worksheet),
                'mark_encoding': self.extract_mark_encoding(worksheet),
                'axes': self.extract_axes(worksheet),
                'annotations': self.extract_annotations(worksheet),
                'trend_lines': self.extract_trend_lines(worksheet),
                'reference_lines': self.extract_reference_lines(worksheet),
                'pages_shelf': self.extract_pages_shelf(worksheet),
                'table_calcs': self.extract_table_calcs(worksheet),
            }
            # Detect viz-in-tooltip worksheet reference
            tooltip_data = ws_data.get('tooltips', [])
            viz_refs = [t for t in tooltip_data if t.get('type') == 'viz_in_tooltip']
            if viz_refs:
                ws_data['tooltip'] = {'viz_in_tooltip': True,
                                      'worksheet': viz_refs[0].get('worksheet', '')}
            worksheets.append(ws_data)
        
        self.workbook_data['worksheets'] = worksheets
        print(f"  ✓ {len(worksheets)} worksheets extracted")
    
    def extract_dashboards(self, root):
        """Extracts dashboards"""
        
        dashboards = []
        
        for dashboard in root.findall('.//dashboard'):
            db_data = {
                'name': dashboard.get('name', ''),
                'title': dashboard.findtext('.//title', ''),
                'size': {
                    'width': int(dashboard.get('width', 1280)),
                    'height': int(dashboard.get('height', 720)),
                },
                'objects': self.extract_dashboard_objects(dashboard),
                'filters': self.extract_dashboard_filters(dashboard),
                'parameters': self.extract_dashboard_parameters(dashboard),
                'theme': self.extract_theme(dashboard),
                'device_layouts': self.extract_device_layouts(dashboard),
                'containers': self.extract_dashboard_containers(dashboard),
            }
            dashboards.append(db_data)
        
        self.workbook_data['dashboards'] = dashboards
        print(f"  ✓ {len(dashboards)} dashboards extracted")
    
    def extract_datasources(self, root):
        """Extracts datasources with enhanced extraction.
        
        Filters out empty datasources and deduplicates by name to keep
        only the most complete version (with the most tables/calculations).
        """
        
        raw_datasources = []
        
        for datasource in root.findall('.//datasource'):
            ds_data = extract_datasource(datasource, twbx_path=self.tableau_file)
            raw_datasources.append(ds_data)
        
        # Deduplicate: keep the richest DS by name
        best_ds = {}  # ds_name -> ds_data
        for ds in raw_datasources:
            ds_name = ds.get('name', '')
            tables = ds.get('tables', [])
            calcs = ds.get('calculations', [])
            richness = len(tables) + len(calcs)
            
            if ds_name not in best_ds or richness > (len(best_ds[ds_name].get('tables', [])) + len(best_ds[ds_name].get('calculations', []))):
                best_ds[ds_name] = ds
        
        # Filter: keep only DSs with real content
        datasources = []
        for ds in best_ds.values():
            has_tables = len(ds.get('tables', [])) > 0
            has_calcs = len(ds.get('calculations', [])) > 0
            has_rels = len(ds.get('relationships', [])) > 0
            if has_tables or has_calcs or has_rels:
                datasources.append(ds)
        
        self.workbook_data['datasources'] = datasources
        print(f"  ✓ {len(datasources)} datasources extracted (filtered from {len(raw_datasources)} raw)")
    
    def extract_calculations(self, root):
        """Extracts calculated fields - now integrated in enhanced datasource extraction"""
        
        # Calculations are now extracted directly in extract_datasource
        # This method maintains backward compatibility
        calculations = []
        
        for datasource in root.findall('.//datasource'):
            ds_data = extract_datasource(datasource, twbx_path=self.tableau_file)
            calculations.extend(ds_data.get('calculations', []))
        
        self.workbook_data['calculations'] = calculations
        print(f"  ✓ {len(calculations)} calculations extracted")
    
    def extract_parameters(self, root):
        """Extracts parameters (deduplicated by name).
        Handles both XML formats:
        - Old: <column param-domain-type="..."> (Tableau Desktop classic)
        - New: <parameters><parameter> (Tableau Desktop modern)
        """
        
        parameters = []
        seen_names = set()
        
        # Format 1: Old-style column-based parameters
        for param in root.findall('.//column[@param-domain-type]'):
            param_name = param.get('name', '')
            if param_name in seen_names:
                continue
            seen_names.add(param_name)
            
            param_data = {
                'name': param_name,
                'caption': param.get('caption', ''),
                'datatype': param.get('datatype', ''),
                'value': param.get('value', ''),
                'domain_type': param.get('param-domain-type', ''),
                'allowable_values': self.extract_allowable_values(param),
            }
            parameters.append(param_data)
        
        # Format 2: New-style <parameters><parameter> elements
        for param in root.findall('.//parameters/parameter'):
            param_name = param.get('name', '')
            if param_name in seen_names:
                continue
            seen_names.add(param_name)
            
            # Determine domain type from children
            domain_type = 'any'
            if param.find('range') is not None:
                domain_type = 'range'
            elif param.find('domain') is not None:
                domain_type = 'list'
            
            param_data = {
                'name': param_name,
                'caption': param.get('caption', ''),
                'datatype': param.get('datatype', ''),
                'value': param.get('value', ''),
                'domain_type': domain_type,
                'allowable_values': self.extract_allowable_values(param),
            }
            parameters.append(param_data)
        
        self.workbook_data['parameters'] = parameters
        print(f"  ✓ {len(parameters)} parameters extracted")
    
    def extract_filters(self, root):
        """Extracts filters"""
        
        filters = []
        
        for filt in root.findall('.//filter'):
            filter_data = {
                'field': filt.get('column', ''),
                'type': filt.get('type', ''),
                'values': [v.text for v in filt.findall('.//value')],
            }
            filters.append(filter_data)
        
        self.workbook_data['filters'] = filters
        print(f"  ✓ {len(filters)} filters extracted")
    
    def extract_stories(self, root):
        """Extracts stories"""
        
        stories = []
        
        for story in root.findall('.//story'):
            story_data = {
                'name': story.get('name', ''),
                'title': story.findtext('.//title', ''),
                'story_points': self.extract_story_points(story),
            }
            stories.append(story_data)
        
        self.workbook_data['stories'] = stories
        print(f"  ✓ {len(stories)} stories extracted")
    
    # Helper methods
    
    def determine_chart_type(self, worksheet):
        """Determines the chart type from the Tableau mark type"""
        # Search for the mark class in panes
        for pane in worksheet.findall('.//pane'):
            mark = pane.find('.//mark')
            if mark is not None and mark.get('class'):
                return self._map_tableau_mark_to_type(mark.get('class'))
        
        # Search in style/mark
        for mark in worksheet.findall('.//style/mark'):
            if mark.get('class'):
                return self._map_tableau_mark_to_type(mark.get('class'))
        
        # Fallback
        if worksheet.find('.//encoding/map') is not None:
            return 'map'
        return 'bar'
    
    def _map_tableau_mark_to_type(self, mark_class):
        """Maps Tableau mark types to Power BI visual types.

        Covers all Tableau mark classes and maps them to the closest
        Power BI visual type string expected by PBIR v4.0.
        """
        mark_map = {
            # ── Standard mark classes ──────────────────────────────
            'Automatic': 'table',
            'Bar': 'clusteredBarChart',
            'Stacked Bar': 'stackedBarChart',
            'Line': 'lineChart',
            'Area': 'areaChart',
            'Square': 'treemap',
            'Circle': 'scatterChart',
            'Shape': 'scatterChart',
            'Text': 'tableEx',
            'Map': 'map',
            'Pie': 'pieChart',
            'Gantt Bar': 'clusteredBarChart',
            'Polygon': 'filledMap',
            'Multipolygon': 'filledMap',
            'Density': 'map',
            # ── Extended mark/chart types (Tableau 2020+) ───────────
            'SemiCircle': 'donutChart',
            'Hex': 'treemap',
            'Histogram': 'clusteredColumnChart',
            'Box Plot': 'boxAndWhisker',
            'Box-and-Whisker': 'boxAndWhisker',
            'Bullet': 'gauge',
            'Waterfall': 'waterfallChart',
            'Funnel': 'funnel',
            'Treemap': 'treemap',
            'Heat Map': 'matrix',
            'Highlight Table': 'matrix',
            'Packed Bubble': 'scatterChart',
            'Packed Bubbles': 'scatterChart',
            'Word Cloud': 'wordCloud',
            'Radial': 'gauge',
            'Dual Axis': 'lineClusteredColumnComboChart',
            'Combo': 'lineClusteredColumnComboChart',
            'Combined Axis': 'lineClusteredColumnComboChart',
            'Line and Bar': 'lineClusteredColumnComboChart',
            'Reference Line': 'lineChart',
            'Reference Band': 'lineChart',
            'Trend Line': 'lineChart',
            'Dot Plot': 'scatterChart',
            'Strip Plot': 'scatterChart',
            'Lollipop': 'clusteredBarChart',
            'Bump Chart': 'lineChart',
            'Slope Chart': 'lineChart',
            'Butterfly Chart': 'hundredPercentStackedBarChart',
            'Pareto Chart': 'lineClusteredColumnComboChart',
            'Sankey': 'decompositionTree',
            'Chord': 'decompositionTree',
            'Network': 'decompositionTree',
            'Calendar': 'matrix',
            'Timeline': 'lineChart',
            'KPI': 'card',
            'Sparkline': 'lineChart',
            'Donut': 'donutChart',
            'Ring': 'donutChart',
            'Rose Chart': 'donutChart',
            'Waffle': 'hundredPercentStackedBarChart',
            'Gauge': 'gauge',
            'Speedometer': 'gauge',
            'Image': 'image',
        }
        return mark_map.get(mark_class, 'clusteredBarChart')
    
    def extract_worksheet_fields(self, worksheet):
        """Extracts fields used in the worksheet"""
        fields = []
        
        # Regex for Tableau derivation prefixes (none, sum, avg, count, usr, yr, etc.)
        derivation_re = r'^(none|sum|avg|count|min|max|usr|yr|mn|dy|qr|wk|attr|md|mdy|hms|hr|mt|sc|thr|trunc):'
        suffix_re = r':(nk|qk|ok|fn|tn)$'
        
        # Extract from <table><rows> and <table><cols> (text content with field refs)
        for shelf_name, shelf_tag in [('columns', 'cols'), ('rows', 'rows')]:
            shelf = worksheet.find(f'./table/{shelf_tag}')
            if shelf is not None and shelf.text:
                # Text contains refs like [datasource].[field:type]
                refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', shelf.text)
                for ds_ref, field_ref in refs:
                    # Clean the field name (remove derivation prefix and type suffix)
                    clean_name = re.sub(derivation_re, '', field_ref)
                    clean_name = re.sub(suffix_re, '', clean_name)
                    fields.append({
                        'name': clean_name,
                        'shelf': shelf_name,
                        'datasource': ds_ref
                    })
        
        # Extract from encodings (color, size, detail, tooltip, label, text)
        for encoding in worksheet.findall('.//encodings'):
            for enc_type in ['color', 'size', 'detail', 'tooltip', 'label', 'text']:
                for enc_elem in encoding.findall(f'./{enc_type}'):
                    column = enc_elem.get('column', '')
                    if column:
                        # Extract [datasource].[field]
                        col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column)
                        if col_refs:
                            clean = re.sub(derivation_re, '', col_refs[0][1])
                            clean = re.sub(suffix_re, '', clean)
                            fields.append({
                                'name': clean,
                                'shelf': enc_type,
                                'datasource': col_refs[0][0]
                            })
        
        return fields
    
    def extract_worksheet_filters(self, worksheet):
        """Extracts worksheet filters from <filter> elements"""
        filters = []
        for filt in worksheet.findall('.//filter'):
            column_ref = filt.get('column', '')
            # Extract field name from [datasource].[field]
            col_match = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column_ref)
            if col_match:
                ds_ref, field_ref = col_match[0]
                clean_name = re.sub(r'^(none|sum|avg|count|min|max):', '', field_ref)
                clean_name = re.sub(r':(nk|qk|ok|fn|tn)$', '', clean_name)
            else:
                ds_ref = ''
                clean_name = column_ref.replace('[', '').replace(']', '')
            
            filter_type = ''
            filter_values = []
            filter_min = None
            filter_max = None
            include_null = False
            exclude_mode = False
            
            # Determine the filter type
            groupfilter = filt.find('.//groupfilter')
            if groupfilter is not None:
                func = groupfilter.get('function', '')
                if func == 'member':
                    # Filter by exact value
                    filter_type = 'categorical'
                    val = groupfilter.get('member', '')
                    if val:
                        filter_values.append(val.replace('&quot;', '"'))
                elif func == 'union':
                    filter_type = 'categorical'
                    for gf in groupfilter.findall('.//groupfilter[@function="member"]'):
                        val = gf.get('member', '')
                        if val:
                            filter_values.append(val.replace('&quot;', '"'))
                elif func == 'range':
                    filter_type = 'range'
                    from_val = groupfilter.get('from', '')
                    to_val = groupfilter.get('to', '')
                    filter_min = from_val if from_val else None
                    filter_max = to_val if to_val else None
                elif func == 'level-members':
                    filter_type = 'all'  # filter "all selected"
                elif func == 'except' or func == 'not':
                    exclude_mode = True
                    filter_type = 'categorical'
                    for gf in groupfilter.findall('.//groupfilter[@function="member"]'):
                        val = gf.get('member', '')
                        if val:
                            filter_values.append(val.replace('&quot;', '"'))
            
            # Values from <value>
            for v in filt.findall('.//value'):
                if v.text:
                    filter_values.append(v.text)
            
            filters.append({
                'field': clean_name,
                'datasource': ds_ref,
                'type': filter_type,
                'values': filter_values,
                'min': filter_min,
                'max': filter_max,
                'exclude': exclude_mode,
                'include_null': include_null
            })
        return filters
    
    def extract_formatting(self, element):
        """Extracts formatting information (colors, fonts, borders, backgrounds, shading)"""
        formatting = {}
        
        # Extract styles from <style-rule>  
        for style_rule in element.findall('.//style-rule'):
            rule_element = style_rule.get('element', '')
            for format_elem in style_rule.findall('.//format'):
                attrs = dict(format_elem.attrib)
                if attrs:
                    formatting.setdefault(rule_element, {}).update(attrs)
        
        # Extract format encodings from <format>
        for fmt in element.findall('.//format'):
            field = fmt.get('field', '')
            fmt_str = fmt.get('value', '')
            if field and fmt_str:
                formatting.setdefault('field_formats', {})[field] = fmt_str
        
        # Background color
        for pane_fmt in element.findall('.//pane/format'):
            if pane_fmt.get('attr') == 'fill-color':
                formatting['background_color'] = pane_fmt.get('value', '')
        
        # Font properties from style
        for style in element.findall('.//style'):
            for rule in style.findall('.//style-rule'):
                elem_name = rule.get('element', '')
                for fmt in rule.findall('.//format'):
                    attr = fmt.get('attr', '')
                    value = fmt.get('value', '')
                    if attr and value:
                        formatting.setdefault(elem_name, {})[attr] = value
        
        # Header formatting
        for header in element.findall('.//header'):
            for fmt in header.findall('.//format'):
                attr = fmt.get('attr', '')
                value = fmt.get('value', '')
                if attr and value:
                    formatting.setdefault('header', {})[attr] = value
        
        # Cell formatting
        for cell in element.findall('.//cell'):
            for fmt in cell.findall('.//format'):
                attr = fmt.get('attr', '')
                value = fmt.get('value', '')
                if attr and value:
                    formatting.setdefault('cell', {})[attr] = value
        
        return formatting
    
    def extract_tooltips(self, worksheet):
        """Extracts tooltips (fields and viz-in-tooltip)"""
        tooltips = []
        
        # Text tooltip from <formatted-text>
        for tooltip_elem in worksheet.findall('.//tooltip'):
            formatted = tooltip_elem.find('.//formatted-text')
            if formatted is not None:
                # Reconstruct the text
                parts = []
                for run in formatted.findall('.//run'):
                    if run.text:
                        parts.append(run.text)
                if parts:
                    tooltips.append({'type': 'text', 'content': ''.join(parts)})
            
            # Viz in tooltip (reference to another worksheet)
            viz_ref = tooltip_elem.get('viz', '')
            if viz_ref:
                tooltips.append({'type': 'viz_in_tooltip', 'worksheet': viz_ref})
        
        return tooltips
    
    def extract_actions(self, worksheet):
        """Extracts actions referenced in this worksheet"""
        # Actions are at the workbook level, not worksheet
        # This method remains for backward compatibility
        return []
    
    def extract_dashboard_objects(self, dashboard):
        """Extracts all dashboard objects: worksheets, text, images, web, filters, blank.
        
        Also detects floating vs tiled mode.
        """
        objects = []
        seen_names = set()
        
        for zone in dashboard.findall('.//zone'):
            zone_name = zone.get('name', '')
            zone_type = zone.get('type', '')
            zone_id = zone.get('id', '')
            is_fixed = zone.get('is-fixed') == 'true' or zone.get('type-v2') == 'fix'
            is_floating = zone.get('is-floating') == 'true'
            
            pos = {
                'x': int(zone.get('x', 0)),
                'y': int(zone.get('y', 0)),
                'w': int(zone.get('w', 300)),
                'h': int(zone.get('h', 200)),
            }
            
            layout_mode = 'floating' if is_floating else ('fixed' if is_fixed else 'tiled')
            
            # Texte
            if zone_type == 'text' or zone.get('type-v2') == 'text':
                text_content = ''
                formatted = zone.find('.//formatted-text')
                if formatted is not None:
                    parts = []
                    for run in formatted.findall('.//run'):
                        if run.text:
                            parts.append(run.text)
                    text_content = ''.join(parts)
                objects.append({
                    'type': 'text',
                    'name': zone_name or f'text_{zone_id}',
                    'content': text_content,
                    'position': pos,
                    'layout': layout_mode
                })
                continue
            
            # Image
            if zone_type == 'bitmap' or zone.get('type-v2') == 'bitmap':
                img_src = ''
                img_elem = zone.find('.//zone-style/format[@attr="image"]')
                if img_elem is not None:
                    img_src = img_elem.get('value', '')
                objects.append({
                    'type': 'image',
                    'name': zone_name or f'image_{zone_id}',
                    'source': img_src,
                    'position': pos,
                    'layout': layout_mode
                })
                continue
            
            # Page web
            if zone_type == 'web' or zone.get('type-v2') == 'web':
                url = zone.get('url', '') or zone.findtext('.//url', '')
                objects.append({
                    'type': 'web',
                    'name': zone_name or f'web_{zone_id}',
                    'url': url,
                    'position': pos,
                    'layout': layout_mode
                })
                continue
            
            # Blank / spacer
            if zone_type == 'empty' or zone.get('type-v2') == 'empty':
                objects.append({
                    'type': 'blank',
                    'name': f'blank_{zone_id}',
                    'position': pos,
                    'layout': layout_mode
                })
                continue
            
            # Filtre (quick filter / parameter control)
            if zone_type == 'filter' or zone.get('type-v2') == 'filter':
                param_ref = zone.get('param', '')
                # Deduplicate by param (nested zones create duplicates)
                dedup_key = f"fc_{param_ref}" if param_ref else f"fc_{zone_name}_{zone_id}"
                if dedup_key not in seen_names:
                    seen_names.add(dedup_key)
                    # Extract the column/calculation name from the param
                    calc_column_name = ''
                    if 'none:' in param_ref:
                        calc_id = param_ref.split('none:')[1].split(':')[0]
                        calc_column_name = calc_id
                    objects.append({
                        'type': 'filter_control',
                        'name': zone_name or f'filter_{zone_id}',
                        'field': zone_name,
                        'param': param_ref,
                        'calc_column_id': calc_column_name,
                        'position': pos,
                        'layout': layout_mode
                    })
                continue
            
            # Worksheet reference (the default case)
            if zone_name and zone_name not in seen_names:
                seen_names.add(zone_name)
                objects.append({
                    'type': 'worksheetReference',
                    'name': zone_name,
                    'worksheetName': zone_name,
                    'position': pos,
                    'layout': layout_mode
                })
        
        return objects
    
    def extract_dashboard_filters(self, dashboard):
        """Extracts dashboard filters from <filter> elements"""
        filters = []
        for filt in dashboard.findall('.//filter'):
            column_ref = filt.get('column', '')
            col_match = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column_ref)
            if col_match:
                ds_ref, field_ref = col_match[0]
                clean_name = re.sub(r'^(none|sum|avg|count|min|max):', '', field_ref)
                clean_name = re.sub(r':(nk|qk|ok|fn|tn)$', '', clean_name)
            else:
                ds_ref = ''
                clean_name = column_ref.replace('[', '').replace(']', '')
            
            filter_values = [v.text for v in filt.findall('.//value') if v.text]
            filters.append({
                'field': clean_name,
                'datasource': ds_ref,
                'values': filter_values
            })
        return filters
    
    def extract_dashboard_parameters(self, dashboard):
        """Extracts parameter controls from the dashboard"""
        params = []
        for zone in dashboard.findall('.//zone'):
            param_ref = zone.get('param', '')
            if param_ref:
                params.append({
                    'name': param_ref,
                    'zone_name': zone.get('name', ''),
                    'position': {
                        'x': int(zone.get('x', 0)),
                        'y': int(zone.get('y', 0)),
                        'w': int(zone.get('w', 200)),
                        'h': int(zone.get('h', 30)),
                    }
                })
        return params
    
    def extract_theme(self, dashboard):
        """Extracts the theme (colors, fonts, custom color palettes) from the dashboard or workbook"""
        theme = {}
        
        # Palette colors from dashboard preferences
        for prefs in dashboard.findall('.//preferences'):
            colors = []
            for color in prefs.findall('.//color-palette/color'):
                if color.text:
                    colors.append(color.text)
            if colors:
                theme['color_palette'] = colors
        
        # Custom color palettes (named palettes)
        custom_palettes = {}
        for palette in dashboard.findall('.//color-palette'):
            palette_name = palette.get('name', '')
            palette_type = palette.get('type', '')
            palette_colors = []
            for color in palette.findall('.//color'):
                if color.text:
                    palette_colors.append(color.text)
            if palette_name and palette_colors:
                custom_palettes[palette_name] = {
                    'type': palette_type,
                    'colors': palette_colors,
                }
        if custom_palettes:
            theme['custom_palettes'] = custom_palettes
        
        # Global formatting style
        for style in dashboard.findall('.//style'):
            for rule in style.findall('.//style-rule'):
                elem = rule.get('element', '')
                for fmt in rule.findall('.//format'):
                    attrs = dict(fmt.attrib)
                    if attrs:
                        theme.setdefault('styles', {})[elem] = attrs
        
        # Font family from formatting
        for fmt in dashboard.findall('.//format[@attr="font-family"]'):
            theme['font_family'] = fmt.get('value', '')
        
        # Extract global workbook-level color palette from parent
        parent = dashboard
        for color_pal in parent.findall('.//preferences/color-palette'):
            pal_name = color_pal.get('name', 'default')
            pal_colors = [c.text for c in color_pal.findall('.//color') if c.text]
            if pal_colors:
                theme.setdefault('custom_palettes', {})[pal_name] = {
                    'type': color_pal.get('type', 'regular'),
                    'colors': pal_colors,
                }
        
        return theme
    
    def extract_allowable_values(self, param):
        """Extracts the allowed values for a parameter (list, range).
        Handles both old (<members><member>) and new (<domain><member>) formats.
        """
        result = []
        
        # List values — old format: <members><member>
        for member in param.findall('.//members/member'):
            val = member.get('value', '')
            alias = member.get('alias', val)
            if val:
                result.append({'value': val, 'alias': alias})
        
        # List values — new format: <domain><member>
        for member in param.findall('.//domain/member'):
            val = member.get('value', '')
            alias = member.get('alias', val)
            if val:
                # Strip surrounding quotes from string values (e.g., '"All"' → 'All')
                clean_val = val.strip('"')
                clean_alias = alias.strip('"') if alias else clean_val
                result.append({'value': clean_val, 'alias': clean_alias})
        
        # Range (min/max/step)
        range_elem = param.find('.//range')
        if range_elem is not None:
            min_val = range_elem.get('min', '')
            max_val = range_elem.get('max', '')
            step = range_elem.get('granularity', '')
            if min_val or max_val:
                result.append({
                    'type': 'range',
                    'min': min_val,
                    'max': max_val,
                    'step': step
                })
        
        return result
    
    def extract_story_points(self, story):
        """Extracts story points (= slides of a story)"""
        story_points = []
        for sp in story.findall('.//story-point'):
            caption = sp.get('captured-sheet', '')
            sp_data = {
                'caption': sp.findtext('.//caption', '') or caption,
                'captured_sheet': caption,
                'description': sp.findtext('.//description', ''),
                'filters_state': []
            }
            # Capture active filters at the time of the story point
            for filt in sp.findall('.//filter'):
                col = filt.get('column', '').replace('[', '').replace(']', '')
                vals = [v.text for v in filt.findall('.//value') if v.text]
                if col:
                    sp_data['filters_state'].append({'field': col, 'values': vals})
            story_points.append(sp_data)
        return story_points
    
    def extract_worksheet_sort_orders(self, worksheet):
        """Extracts sort orders of a worksheet"""
        sorts = []
        for sort in worksheet.findall('.//sort'):
            col = sort.get('column', '').replace('[', '').replace(']', '')
            direction = sort.get('direction', 'ASC')
            sorts.append({'field': col, 'direction': direction.upper()})
        return sorts
    
    def extract_mark_encoding(self, worksheet):
        """Extracts visual mark encodings (color, size, shape, label)"""
        encoding = {}
        
        for enc_elem in worksheet.findall('.//encodings'):
            # Helper to clean Tableau derivation prefixes from field refs
            def _clean_field_ref(raw):
                clean = re.sub(r'^(none|sum|avg|count|min|max|usr|yr|mn|dy|qr|wk|attr|md|mdy|hms|hr|mt|sc|thr|trunc):', '', raw)
                return re.sub(r':(nk|qk|ok|fn|tn)$', '', clean)
            
            # Color
            color = enc_elem.find('.//color')
            if color is not None:
                column = color.get('column', '')
                palette = color.get('palette', '')
                col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column)
                color_data = {
                    'field': _clean_field_ref(col_refs[0][1]) if col_refs else column.replace('[', '').replace(']', ''),
                    'palette': palette,
                }
                # Continuous vs discrete type
                col_type = color.get('type', '')
                if col_type:
                    color_data['type'] = col_type
                # Palette colors
                palette_colors = []
                for pc in color.findall('.//color'):
                    if pc.text:
                        palette_colors.append(pc.text)
                if palette_colors:
                    color_data['palette_colors'] = palette_colors
                encoding['color'] = color_data
            
            # Size
            size = enc_elem.find('.//size')
            if size is not None:
                column = size.get('column', '')
                col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column)
                encoding['size'] = {
                    'field': _clean_field_ref(col_refs[0][1]) if col_refs else column.replace('[', '').replace(']', '')
                }
            
            # Shape
            shape = enc_elem.find('.//shape')
            if shape is not None:
                column = shape.get('column', '')
                col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column)
                encoding['shape'] = {
                    'field': _clean_field_ref(col_refs[0][1]) if col_refs else column.replace('[', '').replace(']', '')
                }
            
            # Label
            label = enc_elem.find('.//label')
            if label is not None:
                column = label.get('column', '')
                col_refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', column)
                show_labels = label.get('show-label', 'false') == 'true'
                encoding['label'] = {
                    'field': _clean_field_ref(col_refs[0][1]) if col_refs else column.replace('[', '').replace(']', ''),
                    'show': show_labels
                }
        
        return encoding
    
    def extract_axes(self, worksheet):
        """Extracts axis configuration (range, scale, title, format, rotation, continuous/discrete)"""
        axes = {}
        for axis in worksheet.findall('.//axis'):
            axis_type = axis.get('type', '')  # x, y
            axis_data = {
                'auto_range': axis.get('auto-range', 'true') == 'true',
                'range_min': axis.get('range-min', None),
                'range_max': axis.get('range-max', None),
                'scale': axis.get('scale', 'linear'),
                'title': axis.findtext('.//title', ''),
                'reversed': axis.get('reversed', 'false') == 'true',
                'format': axis.get('format', ''),
                'label_rotation': axis.get('label-rotation', ''),
                'show_label': axis.get('show-label', 'true') == 'true',
                'show_title': axis.get('show-title', 'true') == 'true',
            }
            # Determine if continuous or discrete
            if axis.get('dimension', '') or axis.get('ordinal', ''):
                axis_data['is_continuous'] = False
            elif axis.get('quantitative', ''):
                axis_data['is_continuous'] = True
            axes[axis_type] = axis_data
        return axes
    
    def extract_annotations(self, worksheet):
        """Extracts annotations (point, area, text annotations)"""
        annotations = []
        for anno in worksheet.findall('.//point-annotation'):
            formatted = anno.find('.//formatted-text')
            text = ''
            if formatted is not None:
                parts = [r.text for r in formatted.findall('.//run') if r.text]
                text = ''.join(parts)
            annotations.append({
                'type': 'point',
                'text': text,
                'column': anno.get('column', ''),
                'row': anno.get('row', ''),
            })
        for anno in worksheet.findall('.//area-annotation'):
            formatted = anno.find('.//formatted-text')
            text = ''
            if formatted is not None:
                parts = [r.text for r in formatted.findall('.//run') if r.text]
                text = ''.join(parts)
            annotations.append({
                'type': 'area',
                'text': text,
            })
        for anno in worksheet.findall('.//text-annotation'):
            formatted = anno.find('.//formatted-text')
            text = ''
            if formatted is not None:
                parts = [r.text for r in formatted.findall('.//run') if r.text]
                text = ''.join(parts)
            annotations.append({
                'type': 'text',
                'text': text,
            })
        # Also check <annotation> generic elements
        for anno in worksheet.findall('.//annotation'):
            formatted = anno.find('.//formatted-text')
            text = ''
            if formatted is not None:
                parts = [r.text for r in formatted.findall('.//run') if r.text]
                text = ''.join(parts)
            if text:
                annotations.append({
                    'type': anno.get('type', 'text'),
                    'text': text,
                })
        return annotations

    def extract_trend_lines(self, worksheet):
        """Extracts trend lines from a worksheet"""
        trend_lines = []
        for tl in worksheet.findall('.//trend-line'):
            trend_lines.append({
                'type': tl.get('type', 'linear'),
                'field': tl.get('column', ''),
                'color': tl.get('color', ''),
                'show_confidence': tl.get('show-confidence', 'false') == 'true',
                'show_equation': tl.get('show-equation', 'false') == 'true',
                'show_r_squared': tl.get('show-r-squared', 'false') == 'true',
                'per_color': tl.get('per-color', 'false') == 'true',
            })
        # Also check <trend-lines><trend-line> nested format
        for tl_container in worksheet.findall('.//trend-lines'):
            for tl in tl_container.findall('.//trend-line'):
                if not any(t.get('type') == tl.get('type', 'linear') for t in trend_lines):
                    trend_lines.append({
                        'type': tl.get('type', 'linear'),
                        'field': tl.get('column', ''),
                        'color': tl.get('color', ''),
                        'show_confidence': tl.get('show-confidence', 'false') == 'true',
                        'show_equation': tl.get('show-equation', 'false') == 'true',
                        'show_r_squared': tl.get('show-r-squared', 'false') == 'true',
                        'per_color': tl.get('per-color', 'false') == 'true',
                    })
        return trend_lines

    def extract_reference_lines(self, worksheet):
        """Extracts reference lines, bands, and distributions"""
        ref_lines = []
        for rl in worksheet.findall('.//reference-line'):
            ref_lines.append({
                'type': 'line',
                'value': rl.get('value', ''),
                'label': rl.get('label', ''),
                'label_type': rl.get('label-type', 'value'),
                'scope': rl.get('scope', 'per-pane'),
                'axis': rl.get('axis', 'y'),
                'color': rl.get('color', '#666666'),
                'style': rl.get('style', 'solid'),
                'computation': rl.get('computation', 'constant'),
                'field': rl.get('column', ''),
            })
        for rb in worksheet.findall('.//reference-band'):
            ref_lines.append({
                'type': 'band',
                'value_from': rb.get('value-from', ''),
                'value_to': rb.get('value-to', ''),
                'label': rb.get('label', ''),
                'scope': rb.get('scope', 'per-pane'),
                'axis': rb.get('axis', 'y'),
                'color': rb.get('color', '#E0E0E0'),
                'fill_above': rb.get('fill-above', ''),
                'fill_below': rb.get('fill-below', ''),
            })
        for rd in worksheet.findall('.//reference-distribution'):
            ref_lines.append({
                'type': 'distribution',
                'computation': rd.get('computation', ''),
                'scope': rd.get('scope', 'per-pane'),
                'axis': rd.get('axis', 'y'),
                'color': rd.get('color', '#666666'),
                'label': rd.get('label', ''),
                'percentile': rd.get('percentile', ''),
            })
        return ref_lines

    def extract_pages_shelf(self, worksheet):
        """Extracts the Pages shelf field (animation shelf in Tableau)"""
        pages = {}
        pages_elem = worksheet.find('.//pages')
        if pages_elem is not None and pages_elem.text:
            refs = re.findall(r'\[([^\]]+)\]\.\[([^\]]+)\]', pages_elem.text)
            if refs:
                pages['field'] = refs[0][1]
                pages['datasource'] = refs[0][0]
            else:
                pages['field'] = pages_elem.text.strip().replace('[', '').replace(']', '')
        return pages

    def extract_table_calcs(self, worksheet):
        """Extracts table calculation addressing/partitioning from <table-calc> elements"""
        table_calcs = []
        for tc in worksheet.findall('.//table-calc'):
            calc_data = {
                'field': tc.get('column', '').replace('[', '').replace(']', ''),
                'type': tc.get('type', ''),
                'ordering_type': tc.get('ordering-type', 'Rows'),
                'compute_using': [],
                'direction': tc.get('direction', ''),
                'at_level': tc.get('at-level', ''),
            }
            # Compute-using dimensions
            for dim in tc.findall('.//compute-using'):
                val = dim.text or dim.get('column', '')
                if val:
                    clean = val.strip().replace('[', '').replace(']', '')
                    calc_data['compute_using'].append(clean)
            # Also check <order-by> for secondary sort
            for ob in tc.findall('.//order-by'):
                field = ob.get('column', '').replace('[', '').replace(']', '')
                direction = ob.get('direction', 'ASC')
                calc_data.setdefault('order_by', []).append({
                    'field': field, 'direction': direction
                })
            table_calcs.append(calc_data)
        return table_calcs

    def extract_device_layouts(self, dashboard):
        """Extracts device-specific layouts (phone, tablet) from dashboard"""
        layouts = []
        for layout in dashboard.findall('.//devicelayout'):
            device_type = layout.get('type', '')
            zones = []
            for zone in layout.findall('.//zone'):
                zone_data = {
                    'name': zone.get('name', ''),
                    'type': zone.get('type', ''),
                    'position': {
                        'x': int(zone.get('x', 0)),
                        'y': int(zone.get('y', 0)),
                        'w': int(zone.get('w', 0)),
                        'h': int(zone.get('h', 0)),
                    },
                    'visible': zone.get('is-visible', 'true') == 'true',
                }
                zones.append(zone_data)
            layouts.append({
                'device_type': device_type,
                'width': int(layout.get('width', 0)),
                'height': int(layout.get('height', 0)),
                'zones': zones,
            })
        # Also check for <layout device-type="phone"> style elements
        for layout in dashboard.findall('.//layout[@device-type]'):
            device_type = layout.get('device-type', '')
            if device_type and not any(l['device_type'] == device_type for l in layouts):
                zones = []
                for zone in layout.findall('.//zone'):
                    zones.append({
                        'name': zone.get('name', ''),
                        'type': zone.get('type', ''),
                        'position': {
                            'x': int(zone.get('x', 0)),
                            'y': int(zone.get('y', 0)),
                            'w': int(zone.get('w', 0)),
                            'h': int(zone.get('h', 0)),
                        },
                        'visible': zone.get('is-visible', 'true') == 'true',
                    })
                layouts.append({
                    'device_type': device_type,
                    'width': int(layout.get('width', 0)),
                    'height': int(layout.get('height', 0)),
                    'zones': zones,
                })
        return layouts

    def extract_dashboard_containers(self, dashboard):
        """Extracts horizontal/vertical layout containers with nesting and padding"""
        containers = []
        for lc in dashboard.findall('.//layout-container'):
            orientation = lc.get('orientation', '')
            container = {
                'orientation': orientation,
                'name': lc.get('name', ''),
                'position': {
                    'x': int(lc.get('x', 0)),
                    'y': int(lc.get('y', 0)),
                    'w': int(lc.get('w', 0)),
                    'h': int(lc.get('h', 0)),
                },
                'padding': {
                    'top': int(lc.get('padding-top', lc.get('margin-top', 0))),
                    'bottom': int(lc.get('padding-bottom', lc.get('margin-bottom', 0))),
                    'left': int(lc.get('padding-left', lc.get('margin-left', 0))),
                    'right': int(lc.get('padding-right', lc.get('margin-right', 0))),
                },
                'children': [],
            }
            for child in lc:
                if child.tag == 'zone':
                    container['children'].append({
                        'type': 'zone',
                        'name': child.get('name', ''),
                        'position': {
                            'x': int(child.get('x', 0)),
                            'y': int(child.get('y', 0)),
                            'w': int(child.get('w', 0)),
                            'h': int(child.get('h', 0)),
                        }
                    })
            containers.append(container)
        return containers

    def extract_workbook_actions(self, root):
        """Extracts actions at the workbook level (filter, highlight, url, navigate, param, set)"""
        actions = []
        
        for action in root.findall('.//action'):
            action_type = action.get('type', '')  # filter, highlight, url, sheet-navigate, param, set-value
            action_name = action.get('name', '')
            
            action_data = {
                'name': action_name,
                'type': action_type,
                'source_worksheets': [],
                'target_worksheets': [],
                'command': action.get('command', ''),
            }
            
            # Source sheets
            for source in action.findall('.//source'):
                ws = source.get('worksheet', '')
                if ws:
                    action_data['source_worksheets'].append(ws)
            
            # Target sheets
            for target in action.findall('.//target'):
                ws = target.get('worksheet', '')
                if ws:
                    action_data['target_worksheets'].append(ws)
            
            # URL action
            if action_type == 'url':
                action_data['url'] = action.get('url', '')
            
            # Filter action: filtered fields
            if action_type == 'filter':
                field_mappings = []
                for fm in action.findall('.//field-mapping'):
                    src = fm.get('source-field', '').replace('[', '').replace(']', '')
                    tgt = fm.get('target-field', '').replace('[', '').replace(']', '')
                    field_mappings.append({'source': src, 'target': tgt})
                action_data['field_mappings'] = field_mappings
            
            # Parameter action
            if action_type == 'param':
                action_data['parameter'] = action.get('param', '')
                action_data['source_field'] = action.get('source-field', '').replace('[', '').replace(']', '')
            
            actions.append(action_data)
        
        self.workbook_data['actions'] = actions
        print(f"  ✓ {len(actions)} actions extracted")
    
    def extract_sets(self, root):
        """Extracts sets (IN/OUT sets)"""
        sets = []
        
        for ds in root.findall('.//datasource'):
            for col in ds.findall('.//column'):
                # Sets have a set attribute or a <set> element
                set_elem = col.find('.//set')
                if set_elem is not None or '-set-' in col.get('name', ''):
                    set_data = {
                        'name': col.get('caption', col.get('name', '')).replace('[', '').replace(']', ''),
                        'raw_name': col.get('name', '').replace('[', '').replace(']', ''),
                        'datatype': col.get('datatype', 'boolean'),
                    }
                    
                    if set_elem is not None:
                        # Conditional set (formula)
                        formula = set_elem.get('formula', '')
                        if formula:
                            set_data['formula'] = formula
                        
                        # Set by list of members
                        members = []
                        for member in set_elem.findall('.//member'):
                            val = member.get('value', '')
                            if val:
                                members.append(val)
                        if members:
                            set_data['members'] = members
                    
                    sets.append(set_data)
        
        self.workbook_data['sets'] = sets
        print(f"  ✓ {len(sets)} sets extracted")
    
    def extract_groups(self, root):
        """Extracts manual groups (value grouping)
        
        Two types of Tableau groups:
        1. crossjoin/level-members: combined field
           → calculated columns concatenating the sources
        2. union/member: value grouping into categories
           → calculated columns with SWITCH
        """
        groups = []
        
        for ds in root.findall('.//datasource'):
            for group_elem in ds.findall('.//group'):
                group_name = group_elem.get('caption', group_elem.get('name', '')).replace('[', '').replace(']', '')
                if not group_name:
                    continue
                
                top_gf = group_elem.find('./groupfilter')
                if top_gf is None:
                    continue
                
                func = top_gf.get('function', '')
                
                if func == 'crossjoin':
                    # Combined Field — extract source fields
                    levels = []
                    for lm in group_elem.findall('.//groupfilter[@function="level-members"]'):
                        level = lm.get('level', '').replace('[', '').replace(']', '')
                        # Clean the prefixes none:xxx:nk/qk
                        if level.startswith('none:') and ':' in level[5:]:
                            level = level[5:level.rfind(':')]
                        levels.append(level)
                    
                    groups.append({
                        'name': group_name,
                        'group_type': 'combined',
                        'source_fields': levels,
                        'source_field': '',
                        'members': {}
                    })
                
                elif func == 'union':
                    # Value grouping — extract members
                    source_field = ''
                    first_member = group_elem.find('.//groupfilter[@function="member"]')
                    if first_member is not None:
                        level = first_member.get('level', '')
                        source_field = level.replace('[', '').replace(']', '')
                    
                    members = {}
                    for child_gf in top_gf.findall('./groupfilter'):
                        if child_gf.get('function') == 'union':
                            group_label = ''
                            group_values = []
                            for member_gf in child_gf.findall('./groupfilter'):
                                if member_gf.get('function') == 'member':
                                    member_val = member_gf.get('member', '')
                                    if member_gf.get('user:ui-marker') == 'true':
                                        group_label = member_gf.get('user:ui-marker-value', member_val)
                                    if member_val:
                                        group_values.append(member_val)
                            if not group_label and group_values:
                                group_label = group_values[0]
                            if group_label:
                                members[group_label] = group_values
                        elif child_gf.get('function') == 'member':
                            member_val = child_gf.get('member', '')
                            marker = child_gf.get('user:ui-marker-value', member_val)
                            if member_val:
                                if marker not in members:
                                    members[marker] = []
                                members[marker].append(member_val)
                    
                    groups.append({
                        'name': group_name,
                        'group_type': 'values',
                        'source_field': source_field,
                        'source_fields': [],
                        'members': members
                    })
                
                else:
                    # Other types — record as-is
                    groups.append({
                        'name': group_name,
                        'group_type': func or 'unknown',
                        'source_field': '',
                        'source_fields': [],
                        'members': {}
                    })
        
        self.workbook_data['groups'] = groups
        print(f"  ✓ {len(groups)} groups extracted")
    
    def extract_bins(self, root):
        """Extracts bins (intervals)"""
        bins = []
        
        for ds in root.findall('.//datasource'):
            for col in ds.findall('.//column'):
                bin_elem = col.find('.//bin')
                if bin_elem is not None:
                    bins.append({
                        'name': col.get('caption', col.get('name', '')).replace('[', '').replace(']', ''),
                        'source_field': bin_elem.get('source', '').replace('[', '').replace(']', ''),
                        'size': bin_elem.get('size', '10'),
                        'datatype': col.get('datatype', 'integer')
                    })
        
        self.workbook_data['bins'] = bins
        print(f"  ✓ {len(bins)} bins extracted")
    
    def extract_hierarchies(self, root):
        """Extracts hierarchies (drill-paths) from datasources"""
        hierarchies = []
        
        for ds in root.findall('.//datasource'):
            for drill_path in ds.findall('.//drill-path'):
                h_name = drill_path.get('name', '')
                levels = []
                for field in drill_path.findall('.//field'):
                    level_name = field.get('name', '').replace('[', '').replace(']', '')
                    if level_name:
                        levels.append(level_name)
                
                if h_name and levels:
                    hierarchies.append({
                        'name': h_name,
                        'levels': levels
                    })
        
        self.workbook_data['hierarchies'] = hierarchies
        print(f"  ✓ {len(hierarchies)} hierarchies extracted")
    
    def extract_sort_orders(self, root):
        """Extracts global sort orders"""
        sorts = []
        
        for ds in root.findall('.//datasource'):
            for sort in ds.findall('.//sort'):
                col = sort.get('column', '').replace('[', '').replace(']', '')
                direction = sort.get('direction', 'ASC')
                if col:
                    sorts.append({
                        'field': col,
                        'direction': direction.upper(),
                        'key': sort.get('key', '')
                    })
        
        self.workbook_data['sort_orders'] = sorts
        print(f"  ✓ {len(sorts)} sort orders extracted")
    
    def extract_aliases(self, root):
        """Extracts aliases (display name overrides for values)"""
        aliases = {}
        
        for ds in root.findall('.//datasource'):
            for col in ds.findall('.//column'):
                col_name = col.get('name', '').replace('[', '').replace(']', '')
                aliases_elem = col.find('.//aliases')
                if aliases_elem is not None:
                    col_aliases = {}
                    for alias in aliases_elem.findall('.//alias'):
                        key = alias.get('key', '')
                        value = alias.get('value', '')
                        if key and value:
                            col_aliases[key] = value
                    if col_aliases:
                        aliases[col_name] = col_aliases
        
        self.workbook_data['aliases'] = aliases
        print(f"  ✓ {len(aliases)} columns with aliases extracted")
    
    def extract_custom_sql(self, root):
        """Extracts custom SQL queries from datasources"""
        custom_sql = []
        
        for ds in root.findall('.//datasource'):
            ds_name = ds.get('name', '')
            for relation in ds.findall('.//relation[@type=\"text\"]'):
                query = relation.text or ''
                if query.strip():
                    custom_sql.append({
                        'datasource': ds_name,
                        'name': relation.get('name', 'Custom SQL Query'),
                        'query': query.strip()
                    })
        
        self.workbook_data['custom_sql'] = custom_sql
        print(f"  ✓ {len(custom_sql)} custom SQL queries extracted")
    
    def extract_user_filters(self, root):
        """Extracts user filters and security-related calculations for RLS migration.
        
        Parses:
        1. <user-filter> elements (explicit user-to-row mappings)
        2. <group-filter> elements within user filters
        3. Calculations using USERNAME(), FULLNAME(), USERDOMAIN(), ISMEMBEROF()
        
        These are converted to Power BI Row-Level Security (RLS) roles.
        """
        user_filters = []
        
        # ---- 1. Explicit user filters (<user-filter> elements) ----
        for ds in root.findall('.//datasource'):
            ds_name = ds.get('caption', ds.get('name', ''))
            
            for uf in ds.findall('.//user-filter'):
                filter_name = uf.get('name', '').replace('[', '').replace(']', '')
                filter_column = uf.get('column', '').replace('[', '').replace(']', '')
                
                # Extract user-to-value mappings
                user_mappings = []
                for member in uf.findall('.//member'):
                    user = member.get('user', '')
                    value = member.get('value', '')
                    if user or value:
                        user_mappings.append({
                            'user': user,
                            'value': value
                        })
                
                # Extract group-filter if present
                group_filter = uf.find('.//groupfilter')
                gf_data = None
                if group_filter is not None:
                    gf_func = group_filter.get('function', '')
                    gf_member = group_filter.get('member', '')
                    gf_level = group_filter.get('level', '').replace('[', '').replace(']', '')
                    gf_data = {
                        'function': gf_func,
                        'member': gf_member,
                        'level': gf_level
                    }
                
                if filter_name or filter_column:
                    user_filters.append({
                        'type': 'user_filter',
                        'name': filter_name,
                        'column': filter_column,
                        'datasource': ds_name,
                        'user_mappings': user_mappings,
                        'group_filter': gf_data
                    })
            
            # ---- 2. Calculation-based user filters ----
            # Look for calculations that reference USERNAME(), FULLNAME(), USERDOMAIN(), ISMEMBEROF()
            user_func_pattern = re.compile(
                r'\b(USERNAME|FULLNAME|USERDOMAIN|ISMEMBEROF)\s*\(', re.IGNORECASE
            )
            
            for col in ds.findall('.//column'):
                calc = col.find('.//calculation')
                if calc is not None:
                    formula = calc.get('formula', '')
                    if formula and user_func_pattern.search(formula):
                        col_name = col.get('caption', col.get('name', '')).replace('[', '').replace(']', '')
                        raw_name = col.get('name', '').replace('[', '').replace(']', '')
                        
                        # Detect which user functions are used
                        functions_used = list(set(
                            m.upper() for m in user_func_pattern.findall(formula)
                        ))
                        
                        # Extract ISMEMBEROF group names if present
                        ismemberof_groups = re.findall(
                            r'ISMEMBEROF\s*\(\s*["\']([^"\']+)["\']\s*\)', formula, re.IGNORECASE
                        )
                        
                        user_filters.append({
                            'type': 'calculated_security',
                            'name': col_name,
                            'raw_name': raw_name,
                            'datasource': ds_name,
                            'formula': formula,
                            'functions_used': functions_used,
                            'ismemberof_groups': ismemberof_groups
                        })
        
        self.workbook_data['user_filters'] = user_filters
        print(f"  ✓ {len(user_filters)} user filters/security rules extracted")

    def save_extractions(self):
        """Saves extractions to JSON"""
        
        for obj_type, data in self.workbook_data.items():
            output_path = os.path.join(self.output_dir, f'{obj_type}.json')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"  → {output_path}")


def main():
    """Main entry point"""
    
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python extract_tableau_data.py <tableau_file.twbx>")
        sys.exit(1)
    
    tableau_file = sys.argv[1]
    
    if not os.path.exists(tableau_file):
        print(f"❌ File not found: {tableau_file}")
        sys.exit(1)
    
    extractor = TableauExtractor(tableau_file)
    extractor.extract_all()


if __name__ == '__main__':
    main()
