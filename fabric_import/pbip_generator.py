"""
Power BI Project (.pbip) generator — Fabric Edition

Creates a complete Power BI project that connects to a Fabric Lakehouse
via DirectLake mode.  The SemanticModel uses TMDL with DirectLake entity
partitions referencing Delta tables in the Lakehouse.  The Report layer
(PBIR v4.0) is identical to regular Power BI.

Generated structure:
  {Name}/
    {Name}.pbip
    .gitignore
    {Name}.SemanticModel/
        .platform
        definition.pbism
        definition/
            database.tmdl
            model.tmdl
            relationships.tmdl
            expressions.tmdl
            roles.tmdl
            tables/*.tmdl
    {Name}.Report/
        .platform
        definition.pbir
        definition/
            version.json
            report.json
            pages/
                pages.json
                {page}/ page.json + visuals/{id}/visual.json
"""

import os
import json
import logging
from datetime import datetime
import uuid
import re
import sys

from .naming import clean_field_name as _clean_field_name_impl
from .constants import new_visual_id, Z_INDEX_MULTIPLIER
import shutil
import time

# Generator imports (Fabric editions)
from fabric_import import tmdl_generator


class FabricPBIPGenerator:
    """Generates a Power BI Project (.pbip) connected to Fabric Lakehouse."""

    def __init__(self, converted_dir='artifacts/fabric_objects/',
                 output_dir='artifacts/fabric_projects/'):
        self.converted_dir = os.path.abspath(converted_dir)
        self.output_dir = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

    # ════════════════════════════════════════════════════════════════
    #  PUBLIC API
    # ════════════════════════════════════════════════════════════════

    def generate(self, extracted):
        """Generate Report artifact within the project directory.

        Compatible with the (project_dir, name) constructor convention
        used by other generators (LakehouseGenerator, NotebookGenerator, etc.).

        When constructed as FabricPBIPGenerator(project_dir, safe_name):
          - self.converted_dir = os.path.abspath(project_dir)
          - self.output_dir = os.path.abspath(safe_name)  (resolved vs CWD)

        We always use converted_dir as the project directory and extract
        just the basename from output_dir as the report name.

        Args:
            extracted: dict of extracted Tableau objects.

        Returns:
            dict: {'pages': int, 'visuals': int}
        """
        # converted_dir is the actual project directory (first constructor arg)
        project_dir = self.converted_dir
        # output_dir was the safe_name but got abspath'd; extract just the name
        report_name = os.path.basename(self.output_dir)

        # Create Report structure (PBIR v4.0)
        self._build_field_mapping(extracted)
        self.create_report_structure(project_dir, report_name, extracted)

        pages_count, visuals_count = self._count_report_artifacts(
            project_dir, report_name
        )
        return {'pages': pages_count, 'visuals': visuals_count}

    def generate_project(self, report_name, converted_objects,
                         lakehouse_name=None):
        """
        Generate a complete Power BI Project for Fabric.

        Args:
            report_name: Report name
            converted_objects: Dict containing all converted Tableau objects
            lakehouse_name: Target Lakehouse name (defaults to report_name)

        Returns:
            dict: {'pages': int, 'visuals': int}
        """
        lh_name = lakehouse_name or report_name

        print(f"\n\U0001f528 Generating Fabric Power BI Project: {report_name}")
        print(f"   Lakehouse target: {lh_name}")

        project_dir = os.path.join(self.output_dir, report_name)
        os.makedirs(project_dir, exist_ok=True)

        # 1. .pbip file
        pbip_file = self.create_pbip_file(project_dir, report_name)
        print(f"  \u2713 .pbip file: {pbip_file}")

        # 2. SemanticModel (DirectLake)
        sm_dir = self.create_semantic_model_structure(
            project_dir, report_name, converted_objects,
            lakehouse_name=lh_name
        )
        print(f"  \u2713 SemanticModel (DirectLake): {sm_dir}")

        # 3. Report (PBIR v4.0)
        report_dir = self.create_report_structure(
            project_dir, report_name, converted_objects
        )
        print(f"  \u2713 Report: {report_dir}")

        # 4. Metadata
        self.create_metadata(project_dir, report_name, converted_objects)
        print(f"  \u2713 Metadata created")

        # Compute stats
        pages_count, visuals_count = self._count_report_artifacts(
            project_dir, report_name
        )

        print(f"\n\u2705 Fabric PBI Project generated: {project_dir}")
        print(f"   \U0001f4c2 Open in Power BI Desktop: {pbip_file}")

        return {'pages': pages_count, 'visuals': visuals_count}

    # ════════════════════════════════════════════════════════════════
    #  PBIP FILE
    # ════════════════════════════════════════════════════════════════

    def create_pbip_file(self, project_dir, report_name):
        """Create the main .pbip file."""
        pbip_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
            "version": "1.0",
            "artifacts": [
                {"report": {"path": f"{report_name}.Report"}}
            ],
            "settings": {"enableAutoRecovery": True}
        }
        pbip_file = os.path.join(project_dir, f"{report_name}.pbip")
        with open(pbip_file, 'w', encoding='utf-8') as f:
            json.dump(pbip_content, f, indent=2)

        gitignore = os.path.join(project_dir, '.gitignore')
        with open(gitignore, 'w', encoding='utf-8') as f:
            f.write(".pbi/\n")

        return pbip_file

    # ════════════════════════════════════════════════════════════════
    #  SEMANTIC MODEL (DirectLake)
    # ════════════════════════════════════════════════════════════════

    def create_semantic_model_structure(self, project_dir, report_name,
                                        converted_objects, lakehouse_name=None):
        """Create the SemanticModel structure with DirectLake TMDL."""
        sm_dir = os.path.join(project_dir, f"{report_name}.SemanticModel")
        os.makedirs(sm_dir, exist_ok=True)

        # .platform
        platform = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {
                "type": "SemanticModel",
                "displayName": report_name
            },
            "config": {
                "version": "2.0",
                "logicalId": str(uuid.uuid4())
            }
        }
        with open(os.path.join(sm_dir, '.platform'), 'w', encoding='utf-8') as f:
            json.dump(platform, f, indent=2)

        # definition.pbism
        pbism = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
            "version": "4.2",
            "settings": {"qnaEnabled": True}
        }
        with open(os.path.join(sm_dir, 'definition.pbism'), 'w', encoding='utf-8') as f:
            json.dump(pbism, f, indent=2)

        # Generate TMDL (DirectLake)
        self._create_tmdl_model(sm_dir, report_name, converted_objects,
                                lakehouse_name=lakehouse_name)

        return sm_dir

    def _create_tmdl_model(self, sm_dir, report_name, converted_objects,
                            lakehouse_name=None):
        """Generate the semantic model in TMDL format (DirectLake)."""
        datasources = converted_objects.get('datasources', [])
        extra_objects = {
            'hierarchies': converted_objects.get('hierarchies', []),
            'sets': converted_objects.get('sets', []),
            'groups': converted_objects.get('groups', []),
            'bins': converted_objects.get('bins', []),
            'aliases': converted_objects.get('aliases', {}),
            'parameters': converted_objects.get('parameters', []),
            'user_filters': converted_objects.get('user_filters', []),
            '_datasources': converted_objects.get('datasources', []),
        }
        try:
            stats = tmdl_generator.generate_tmdl(
                datasources=datasources,
                report_name=report_name,
                extra_objects=extra_objects,
                output_dir=sm_dir,
                lakehouse_name=lakehouse_name
            )
            print(f"    \u2713 TMDL DirectLake model:")
            print(f"      - {stats['tables']} tables")
            print(f"      - {stats['columns']} columns")
            print(f"      - {stats['measures']} DAX measures")
            print(f"      - {stats['relationships']} relationships")
            if stats['hierarchies']:
                print(f"      - {stats['hierarchies']} hierarchies")
            if stats['roles']:
                print(f"      - {stats['roles']} RLS roles")
        except Exception as e:
            print(f"  \u26a0 Error during TMDL generation: {e}")
            logging.exception("TMDL generation failed")

    # ════════════════════════════════════════════════════════════════
    #  REPORT STRUCTURE (PBIR v4.0)
    # ════════════════════════════════════════════════════════════════

    def create_report_structure(self, project_dir, report_name, converted_objects):
        """Create the Report structure in PBIR v4.0 format."""

        # Build field mapping
        self._build_field_mapping(converted_objects)

        report_dir = os.path.join(project_dir, f"{report_name}.Report")

        # Clean previous content
        if os.path.exists(report_dir):
            for attempt in range(5):
                try:
                    shutil.rmtree(report_dir)
                    break
                except PermissionError:
                    if attempt < 4:
                        time.sleep(0.5 * (attempt + 1))
                    else:
                        for root, dirs, files in os.walk(report_dir, topdown=False):
                            for name in files:
                                try:
                                    os.remove(os.path.join(root, name))
                                except PermissionError:
                                    pass
                            for name in dirs:
                                try:
                                    os.rmdir(os.path.join(root, name))
                                except (PermissionError, OSError):
                                    pass
        os.makedirs(report_dir, exist_ok=True)

        # .platform
        platform = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Report", "displayName": report_name},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid4())}
        }
        with open(os.path.join(report_dir, '.platform'), 'w', encoding='utf-8') as f:
            json.dump(platform, f, indent=2)

        # definition.pbir — points to SemanticModel
        report_definition = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {
                "byPath": {"path": f"../{report_name}.SemanticModel"}
            }
        }
        with open(os.path.join(report_dir, 'definition.pbir'), 'w', encoding='utf-8') as f:
            json.dump(report_definition, f, indent=2)

        # definition/ folder
        def_dir = os.path.join(report_dir, 'definition')
        os.makedirs(def_dir, exist_ok=True)

        # version.json
        with open(os.path.join(def_dir, 'version.json'), 'w', encoding='utf-8') as f:
            json.dump({
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
                "version": "2.0.0"
            }, f, indent=2)

        # report.json with optional custom theme
        theme_data = None
        dashboards = converted_objects.get('dashboards', [])
        for db in dashboards:
            t = db.get('theme')
            if t and t.get('colors'):
                theme_data = t
                break

        custom_theme = tmdl_generator.generate_theme_json(theme_data)
        # Collect workbook-scope filters for report-level filter config
        wb_filters = converted_objects.get('filters', [])
        report_json = self._build_report_json(theme_data, report_filters=wb_filters)
        with open(os.path.join(def_dir, 'report.json'), 'w', encoding='utf-8') as f:
            json.dump(report_json, f, indent=2)

        if theme_data:
            res_dir = os.path.join(def_dir, 'RegisteredResources')
            os.makedirs(res_dir, exist_ok=True)
            with open(os.path.join(res_dir, 'TableauMigrationTheme.json'), 'w', encoding='utf-8') as f:
                json.dump(custom_theme, f, indent=2)

        # Pages + visuals
        pages_dir = os.path.join(def_dir, 'pages')
        os.makedirs(pages_dir, exist_ok=True)

        worksheets = converted_objects.get('worksheets', [])
        page_names = []

        if dashboards:
            for db_idx, db in enumerate(dashboards):
                page_name = f"ReportSection{new_visual_id()}" if db_idx > 0 else "ReportSection"
                page_display_name = db.get('name', f'Page {db_idx + 1}')
                page_names.append(page_name)
                page_dir = os.path.join(pages_dir, page_name)
                os.makedirs(page_dir, exist_ok=True)

                size = db.get('size', {})
                page_width = size.get('width', 1280)
                page_height = size.get('height', 720)

                page_json = {
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                    "name": page_name,
                    "displayName": page_display_name,
                    "displayOption": "FitToPage",
                    "height": page_height,
                    "width": page_width
                }
                db_filters = db.get('filters', [])
                if db_filters:
                    pf = self._create_visual_filters(db_filters)
                    if pf:
                        page_json["filterConfig"] = {"filters": pf}

                with open(os.path.join(page_dir, 'page.json'), 'w', encoding='utf-8') as f:
                    json.dump(page_json, f, indent=2)

                visuals_dir = os.path.join(page_dir, 'visuals')
                os.makedirs(visuals_dir, exist_ok=True)

                db_objects = db.get('objects', [])
                visual_count = 0

                # calc ID → caption lookup
                calc_id_to_caption = {}
                for c in converted_objects.get('calculations', []):
                    cname = c.get('name', '').strip('[]')
                    ccap = c.get('caption', '')
                    if cname and ccap:
                        calc_id_to_caption[cname] = ccap

                # Scale factor
                max_x = max((o.get('position', {}).get('x', 0) + o.get('position', {}).get('w', 0) for o in db_objects), default=page_width)
                max_y = max((o.get('position', {}).get('y', 0) + o.get('position', {}).get('h', 0) for o in db_objects), default=page_height)
                scale_x = page_width / max(max_x, 1)
                scale_y = page_height / max(max_y, 1)

                for obj in db_objects:
                    if obj.get('type') == 'worksheetReference':
                        ws_name = obj.get('worksheetName', '')
                        ws_data = self._find_worksheet(worksheets, ws_name)
                        self._create_visual_worksheet(visuals_dir, ws_data, obj,
                                                       scale_x, scale_y, visual_count,
                                                       worksheets, converted_objects)
                        visual_count += 1
                    elif obj.get('type') == 'text':
                        self._create_visual_textbox(visuals_dir, obj, scale_x, scale_y, visual_count)
                        visual_count += 1
                    elif obj.get('type') == 'image':
                        self._create_visual_image(visuals_dir, obj, scale_x, scale_y, visual_count)
                        visual_count += 1
                    elif obj.get('type') == 'filter_control':
                        self._create_visual_filter_control(visuals_dir, obj, scale_x, scale_y,
                                                            visual_count, calc_id_to_caption,
                                                            converted_objects)
                        visual_count += 1
                    elif obj.get('type') == 'navigation_button':
                        self._create_visual_nav_button(visuals_dir, obj, scale_x, scale_y, visual_count)
                        visual_count += 1
                    elif obj.get('type') == 'download_button':
                        self._create_visual_action_button(visuals_dir, obj, scale_x, scale_y, visual_count, 'Export')
                        visual_count += 1

                # Pages shelf ? play axis slicer
                for ws in worksheets:
                    ps = ws.get('pages_shelf', {})
                    if ps and ps.get('field'):
                        self._create_pages_shelf_slicer(visuals_dir, ps, scale_x, scale_y,
                                                         visual_count, converted_objects)
                        visual_count += 1
                        break  # one pages_shelf slicer per page is sufficient

                print(f"  \U0001f4ca Page '{page_display_name}': {visual_count} visuals")

        # Fallback: default page
        if not page_names or (dashboards and all(len(d.get('objects', [])) == 0 for d in dashboards)):
            page_name = "ReportSection"
            page_names = [page_name]
            page_dir = os.path.join(pages_dir, page_name)
            os.makedirs(page_dir, exist_ok=True)

            page_json = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                "name": page_name,
                "displayName": "Tableau Migration",
                "displayOption": "FitToPage",
                "height": 720,
                "width": 1280
            }
            with open(os.path.join(page_dir, 'page.json'), 'w', encoding='utf-8') as f:
                json.dump(page_json, f, indent=2)

            visuals_dir = os.path.join(page_dir, 'visuals')
            os.makedirs(visuals_dir, exist_ok=True)
            x, y = 10, 10
            for idx, ws in enumerate(worksheets):
                vid = new_visual_id()
                vdir = os.path.join(visuals_dir, vid)
                os.makedirs(vdir, exist_ok=True)
                vtype = ws.get('chart_type', 'clusteredBarChart')
                visual_json = {
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
                    "name": vid,
                    "position": {"x": x, "y": y, "z": idx * 1000, "height": 200, "width": 300, "tabOrder": idx * 1000},
                    "visual": {"visualType": vtype, "drillFilterOtherVisuals": True}
                }
                if ws.get('fields'):
                    query = self._build_visual_query(ws)
                    if query:
                        visual_json["visual"]["query"] = query
                with open(os.path.join(vdir, 'visual.json'), 'w', encoding='utf-8') as f:
                    json.dump(visual_json, f, indent=2, ensure_ascii=False)
                x += 320
                if x > 1000:
                    x = 10
                    y += 220
            print(f"  \U0001f4ca Default page: {len(worksheets)} visuals")

        # Tooltip pages
        tooltip_ws = [ws for ws in worksheets if ws.get('tooltip', {}).get('viz_in_tooltip')]
        for tip_ws in tooltip_ws:
            tip_name = f"Tooltip_{uuid.uuid4().hex[:12]}"
            tip_display = f"Tooltip - {tip_ws.get('name', 'Tooltip')}"
            page_names.append(tip_name)
            tip_dir = os.path.join(pages_dir, tip_name)
            os.makedirs(tip_dir, exist_ok=True)
            tip_page = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                "name": tip_name, "displayName": tip_display,
                "displayOption": "FitToPage", "height": 320, "width": 480,
                "pageType": "Tooltip"
            }
            with open(os.path.join(tip_dir, 'page.json'), 'w', encoding='utf-8') as f:
                json.dump(tip_page, f, indent=2)
            tv_dir = os.path.join(tip_dir, 'visuals')
            os.makedirs(tv_dir, exist_ok=True)
            self._create_visual_worksheet(
                tv_dir, tip_ws,
                {'type': 'worksheetReference', 'worksheetName': tip_ws.get('name', ''),
                 'position': {'x': 0, 'y': 0, 'w': 480, 'h': 320}},
                1.0, 1.0, 0, worksheets, converted_objects
            )
            print(f"  \U0001f4a1 Tooltip page '{tip_display}'")

        # Drill-through pages from "Go to Sheet" actions
        actions = converted_objects.get('actions', [])
        drillthrough_targets = set()
        for action in actions:
            if action.get('type') in ('filter', 'go-to-sheet', 'highlight'):
                target_sheet = action.get('target_sheet', action.get('target', ''))
                source_field = action.get('source_field', action.get('field', ''))
                if target_sheet and target_sheet not in drillthrough_targets:
                    drillthrough_targets.add(target_sheet)
                    dt_name = f"DrillThrough_{uuid.uuid4().hex[:12]}"
                    dt_display = f"Drillthrough - {target_sheet}"
                    page_names.append(dt_name)
                    dt_dir = os.path.join(pages_dir, dt_name)
                    os.makedirs(dt_dir, exist_ok=True)
                    dt_page = {
                        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                        "name": dt_name,
                        "displayName": dt_display,
                        "displayOption": "FitToPage",
                        "height": 720,
                        "width": 1280,
                        "pageType": "Drillthrough"
                    }
                    # Add drillthrough filter if we have a source field
                    if source_field:
                        clean_field = source_field.replace('[', '').replace(']', '')
                        entity, prop = self._resolve_field_entity(clean_field)
                        dt_page["drillthroughFilters"] = [{
                            "name": f"DT_{uuid.uuid4().hex[:8]}",
                            "type": "Categorical",
                            "field": {
                                "Column": {
                                    "Expression": {"SourceRef": {"Entity": entity}},
                                    "Property": prop
                                }
                            }
                        }]
                    with open(os.path.join(dt_dir, 'page.json'), 'w', encoding='utf-8') as f:
                        json.dump(dt_page, f, indent=2)
                    # Place target worksheet visuals on drillthrough page
                    dt_vis_dir = os.path.join(dt_dir, 'visuals')
                    os.makedirs(dt_vis_dir, exist_ok=True)
                    dt_ws = self._find_worksheet(worksheets, target_sheet)
                    if dt_ws:
                        self._create_visual_worksheet(
                            dt_vis_dir, dt_ws,
                            {'type': 'worksheetReference', 'worksheetName': target_sheet,
                             'position': {'x': 0, 'y': 0, 'w': 1280, 'h': 720}},
                            1.0, 1.0, 0, worksheets, converted_objects
                        )
                    print(f"  \U0001f504 Drillthrough page '{dt_display}'")

        # Mobile layout from device layouts
        for db in dashboards:
            device_layouts = db.get('device_layouts', [])
            for dl in device_layouts:
                device_type = dl.get('device_type', 'phone').lower()
                if device_type in ('phone', 'tablet'):
                    mobile_name = f"Mobile_{device_type}_{uuid.uuid4().hex[:8]}"
                    mobile_display = f"{db.get('name', 'Dashboard')} ({device_type.capitalize()})"
                    page_names.append(mobile_name)
                    mobile_dir = os.path.join(pages_dir, mobile_name)
                    os.makedirs(mobile_dir, exist_ok=True)
                    m_width = dl.get('width', 375 if device_type == 'phone' else 768)
                    m_height = dl.get('height', 667 if device_type == 'phone' else 1024)
                    mobile_page = {
                        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                        "name": mobile_name,
                        "displayName": mobile_display,
                        "displayOption": "FitToPage",
                        "height": m_height,
                        "width": m_width,
                        "mobileState": {
                            "page": {"visualContainers": []}
                        }
                    }
                    with open(os.path.join(mobile_dir, 'page.json'), 'w', encoding='utf-8') as f:
                        json.dump(mobile_page, f, indent=2)
                    # Create visuals for mobile zones
                    m_vis_dir = os.path.join(mobile_dir, 'visuals')
                    os.makedirs(m_vis_dir, exist_ok=True)
                    zones = dl.get('zones', [])
                    for z_idx, zone in enumerate(zones):
                        zone_ws_name = zone.get('worksheet', '')
                        if zone_ws_name:
                            z_ws = self._find_worksheet(worksheets, zone_ws_name)
                            self._create_visual_worksheet(
                                m_vis_dir, z_ws,
                                {'type': 'worksheetReference', 'worksheetName': zone_ws_name,
                                 'position': zone},
                                m_width / max(zone.get('w', m_width), 1),
                                m_height / max(zone.get('h', m_height), 1),
                                z_idx, worksheets, converted_objects
                            )
                    print(f"  \U0001f4f1 Mobile page '{mobile_display}': {len(zones)} zones")

        # -- Bookmarks from stories -
        bookmarks = converted_objects.get('bookmarks', [])
        if bookmarks:
            bm_dir = os.path.join(def_dir, 'bookmarks')
            os.makedirs(bm_dir, exist_ok=True)
            bm_list = []
            for bm_idx, bm in enumerate(bookmarks):
                bm_id = new_visual_id()
                bm_name = bm.get('name', f'Bookmark {bm_idx + 1}')
                bm_list.append({"name": bm_id, "displayName": bm_name})
                bm_json = {
                    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/bookmark/1.0.0/schema.json",
                    "name": bm_id,
                    "displayName": bm_name,
                    "explorationState": {
                        "version": "1.0",
                        "activeSection": page_names[0] if page_names else "",
                        "filters": bm.get('filters', {}),
                    },
                    "options": {
                        "targetVisualType": 0,
                        "applyFilters": True,
                    }
                }
                bm_file = os.path.join(bm_dir, f'{bm_id}.json')
                with open(bm_file, 'w', encoding='utf-8') as f:
                    json.dump(bm_json, f, indent=2)
            # bookmarks.json index
            bm_meta = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/bookmarksMetadata/1.0.0/schema.json",
                "bookmarkOrder": [b["name"] for b in bm_list],
            }
            with open(os.path.join(bm_dir, 'bookmarks.json'), 'w', encoding='utf-8') as f:
                json.dump(bm_meta, f, indent=2)
            print(f"  \U0001f516 {len(bm_list)} bookmarks generated from stories")

        # pages.json
        pages_meta = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": page_names,
            "activePageName": page_names[0] if page_names else ""
        }
        with open(os.path.join(pages_dir, 'pages.json'), 'w', encoding='utf-8') as f:
            json.dump(pages_meta, f, indent=2)

        # Cleanup stale visual dirs
        for pn in page_names:
            vd = os.path.join(pages_dir, pn, 'visuals')
            if os.path.isdir(vd):
                for d in os.listdir(vd):
                    dp = os.path.join(vd, d)
                    if os.path.isdir(dp) and not os.path.exists(os.path.join(dp, 'visual.json')):
                        try:
                            shutil.rmtree(dp)
                        except (PermissionError, OSError):
                            pass

        return report_dir

    # ════════════════════════════════════════════════════════════════
    #  REPORT JSON
    # ════════════════════════════════════════════════════════════════

    def _build_report_json(self, theme_data, report_filters=None):
        """Build report.json content."""
        report_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.1.0/schema.json",
            "themeCollection": {
                "baseTheme": {
                    "name": "CY24SU06",
                    "reportVersionAtImport": {
                        "visual": "1.8.50", "report": "2.0.50", "page": "1.3.50"
                    },
                    "type": "SharedResources"
                }
            },
            "resourcePackages": [{
                "name": "SharedResources", "type": "SharedResources",
                "items": [{"name": "CY24SU06", "path": "BaseThemes/CY24SU06.json", "type": "BaseTheme"}]
            }],
            "settings": {
                "hideVisualContainerHeader": True,
                "useStylableVisualContainerHeader": True,
                "exportDataMode": "None",
                "defaultDrillFilterOtherVisuals": True,
                "allowChangeFilterTypes": True,
                "useEnhancedTooltips": True
            }
        }
        # Report-level filters from workbook-scope filters
        if report_filters:
            filter_defs = self._create_visual_filters(report_filters)
            if filter_defs:
                report_json["filterConfig"] = {"filters": filter_defs}
        if theme_data:
            report_json["resourcePackages"].append({
                "name": "MigrationTheme", "type": "CustomTheme",
                "items": [{"name": "TableauMigrationTheme",
                           "path": "RegisteredResources/TableauMigrationTheme.json",
                           "type": "CustomTheme"}]
            })
            report_json["themeCollection"]["customTheme"] = {
                "name": "TableauMigrationTheme",
                "reportVersionAtImport": {"visual": "1.8.50", "report": "2.0.50", "page": "1.3.50"},
                "type": "CustomTheme"
            }
        return report_json

    # ════════════════════════════════════════════════════════════════
    #  FIELD MAPPING  (Tableau → PBI model)
    # ════════════════════════════════════════════════════════════════

    def _build_field_mapping(self, converted_objects):
        """Build mapping from Tableau field IDs to PBI table/column names."""
        self._field_map = {}
        datasources = converted_objects.get('datasources', [])

        # Collect deduplicated physical tables
        best_tables = {}
        for ds in datasources:
            for table in ds.get('tables', []):
                tname = table.get('name', '?')
                if not tname or tname == 'Unknown':
                    continue
                if tname not in best_tables or len(table.get('columns', [])) > len(best_tables[tname].get('columns', [])):
                    best_tables[tname] = table

        # Main table (most columns)
        main_table = None
        max_cols = 0
        for tname, t in best_tables.items():
            ncols = len(t.get('columns', []))
            if ncols > max_cols:
                max_cols = ncols
                main_table = tname

        # Map columns
        for tname, t in best_tables.items():
            for col in t.get('columns', []):
                cname = col.get('name', '?')
                self._field_map[cname] = (tname, cname)

        # Map calculations
        measures_table = main_table or 'Table'
        self._measure_names = set()
        for ds in datasources:
            for calc in ds.get('calculations', []):
                raw_name = calc.get('name', '').replace('[', '').replace(']', '')
                caption = calc.get('caption', raw_name)
                if raw_name not in self._field_map:
                    self._field_map[raw_name] = (measures_table, caption)
                if caption and caption not in self._field_map:
                    self._field_map[caption] = (measures_table, caption)
                if calc.get('role', '') == 'measure':
                    self._measure_names.add(raw_name)
                    if caption:
                        self._measure_names.add(caption)

        for calc in converted_objects.get('calculations', []):
            if calc.get('role', '') == 'measure':
                raw_name = calc.get('name', '').replace('[', '').replace(']', '')
                caption = calc.get('caption', raw_name)
                self._measure_names.add(raw_name)
                if caption:
                    self._measure_names.add(caption)

        # Map groups
        for g in converted_objects.get('groups', []):
            gname = g.get('name', '').replace('[', '').replace(']', '')
            if gname and gname not in self._field_map:
                self._field_map[gname] = (measures_table, gname)

        self._main_table = measures_table

    def _is_measure_field(self, field_name):
        """Check if a field is a measure vs dimension."""
        clean = field_name.replace('[', '').replace(']', '')
        if hasattr(self, '_measure_names') and clean in self._measure_names:
            return True
        if hasattr(self, '_field_map') and clean in self._field_map:
            _, prop = self._field_map[clean]
            if hasattr(self, '_measure_names') and prop in self._measure_names:
                return True
        return False

    def _clean_field_name(self, name):
        """Strip Tableau derivation prefixes."""
        return _clean_field_name_impl(name)

    def _resolve_field_entity(self, field_name):
        """Resolve a field name to (table, column)."""
        clean = field_name.replace('[', '').replace(']', '')
        if hasattr(self, '_field_map'):
            if clean in self._field_map:
                return self._field_map[clean]
            for prefix in ('attr:', ':'):
                if clean.startswith(prefix) and clean[len(prefix):] in self._field_map:
                    return self._field_map[clean[len(prefix):]]
            for key, val in self._field_map.items():
                if key == clean or val[1] == clean:
                    return val
        main = getattr(self, '_main_table', clean)
        return (main, clean)

    # ════════════════════════════════════════════════════════════════
    #  VISUAL HELPERS
    # ════════════════════════════════════════════════════════════════

    def _make_visual_position(self, pos, scale_x, scale_y, z_index):
        return {
            "x": round(pos.get('x', 0) * scale_x),
            "y": round(pos.get('y', 0) * scale_y),
            "z": z_index * Z_INDEX_MULTIPLIER,
            "height": round(pos.get('h', 200) * scale_y),
            "width": round(pos.get('w', 300) * scale_x),
            "tabOrder": z_index * Z_INDEX_MULTIPLIER
        }

    def _create_visual_worksheet(self, visuals_dir, ws_data, obj, scale_x, scale_y,
                                  visual_count, worksheets, converted_objects):
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)

        pos = obj.get('position', {})
        visual_type = ws_data.get('chart_type', 'clusteredBarChart') if ws_data else 'clusteredBarChart'
        ws_name = obj.get('worksheetName', '')

        visual_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": self._make_visual_position(pos, scale_x, scale_y, visual_count),
            "visual": {"visualType": visual_type, "drillFilterOtherVisuals": True}
        }
        if ws_data and ws_data.get('fields'):
            query = self._build_visual_query(ws_data)
            if query:
                visual_json["visual"]["query"] = query

        visual_json["visual"]["objects"] = self._build_visual_objects(ws_name, ws_data, visual_type)

        if ws_data and ws_data.get('filters'):
            vf = self._create_visual_filters(ws_data['filters'])
            if vf:
                visual_json["filterConfig"] = {"filters": vf}

        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(visual_json, f, indent=2, ensure_ascii=False)

    def _create_visual_textbox(self, visuals_dir, obj, scale_x, scale_y, visual_count):
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        pos = obj.get('position', {})
        content = obj.get('content', '')
        visual_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": self._make_visual_position(pos, scale_x, scale_y, visual_count),
            "visual": {
                "visualType": "textbox",
                "objects": {"general": [{"properties": {"paragraphs": {"expr": {"Literal": {"Value": json.dumps([{"textRuns": [{"value": content}]}])}}}}}]}
            }
        }
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(visual_json, f, indent=2, ensure_ascii=False)

    def _create_visual_image(self, visuals_dir, obj, scale_x, scale_y, visual_count):
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        pos = obj.get('position', {})
        visual_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": self._make_visual_position(pos, scale_x, scale_y, visual_count),
            "visual": {
                "visualType": "image",
                "objects": {"general": [{"properties": {"imageUrl": {"expr": {"Literal": {"Value": f"'{obj.get('source', '')}'"}}}}}]}
            }
        }
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(visual_json, f, indent=2, ensure_ascii=False)

    def _create_visual_filter_control(self, visuals_dir, obj, scale_x, scale_y,
                                       visual_count, calc_id_to_caption, converted_objects):
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        pos = obj.get('position', {})
        calc_col_id = obj.get('calc_column_id', '')
        column_name = calc_id_to_caption.get(calc_col_id, '')
        if not column_name:
            column_name = obj.get('field', obj.get('name', ''))
        table_name = self._find_column_table(column_name, converted_objects)
        vx = round(pos.get('x', 0) * scale_x)
        vy = round(pos.get('y', 0) * scale_y)
        vw = round(pos.get('w', 200) * scale_x)
        vh = round(pos.get('h', 60) * scale_y)
        slicer_json = self._create_slicer_visual(visual_id, vx, vy, vw, vh,
                                                  column_name, table_name, visual_count)
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(slicer_json, f, indent=2, ensure_ascii=False)

    # ════════════════════════════════════════════════════════════════
    #  VISUAL QUERY BUILDER
    # ════════════════════════════════════════════════════════════════

    def _build_visual_query(self, ws_data):
        """Build queryState for a visual (PBIR v4.0)."""
        fields = ws_data.get('fields', [])
        if not fields:
            return None

        skip_names = {'Measure Names', 'Measure Values', 'Multiple Values',
                      ':Measure Names', ':Measure Values'}
        cleaned_fields = []
        seen_names = set()
        for f in fields:
            raw_name = f.get('name', '')
            clean = self._clean_field_name(raw_name)
            if clean in skip_names or raw_name in skip_names:
                continue
            if clean in seen_names:
                continue
            seen_names.add(clean)
            cleaned_fields.append({**f, 'name': clean})

        if not cleaned_fields:
            return None

        query_state = {}
        dim_fields = [f for f in cleaned_fields if not self._is_measure_field(f['name'])]
        mea_fields = [f for f in cleaned_fields if self._is_measure_field(f['name'])]

        visual_type = ws_data.get('chart_type', 'clusteredBarChart')

        if visual_type in ('filledMap', 'map'):
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Size"] = self._make_projection(mea_fields[0])
        elif visual_type in ('tableEx', 'table', 'matrix'):
            all_f = dim_fields + mea_fields
            if all_f:
                query_state["Values"] = {"projections": [self._make_projection_entry(f) for f in all_f[:10]]}
        elif visual_type == 'scatterChart':
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if len(mea_fields) >= 2:
                query_state["X"] = self._make_projection(mea_fields[0])
                query_state["Y"] = self._make_projection(mea_fields[1])
            elif len(mea_fields) == 1:
                query_state["Y"] = self._make_projection(mea_fields[0])
            if len(mea_fields) >= 3:
                query_state["Size"] = self._make_projection(mea_fields[2])
        elif visual_type in ('gauge', 'kpi'):
            if mea_fields:
                query_state["Y"] = self._make_projection(mea_fields[0])
            if len(mea_fields) >= 2:
                query_state["TargetValue"] = self._make_projection(mea_fields[1])
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
        elif visual_type in ('card', 'multiRowCard'):
            all_f = mea_fields if mea_fields else dim_fields
            if all_f:
                query_state["Values"] = {"projections": [self._make_projection_entry(f) for f in all_f[:6]]}
        elif visual_type in ('pieChart', 'donutChart', 'funnel', 'treemap'):
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Y"] = self._make_projection(mea_fields[0])
        elif visual_type in ('lineClusteredColumnComboChart', 'lineStackedColumnComboChart'):
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Y"] = self._make_projection(mea_fields[0])
            if len(mea_fields) >= 2:
                query_state["Y2"] = self._make_projection(mea_fields[1])
        elif visual_type == 'waterfallChart':
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Y"] = self._make_projection(mea_fields[0])
            if len(dim_fields) >= 2:
                query_state["Breakdown"] = self._make_projection(dim_fields[1])
        elif visual_type == 'boxAndWhisker':
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Value"] = self._make_projection(mea_fields[0])
        else:
            if dim_fields:
                query_state["Category"] = self._make_projection(dim_fields[0])
            if mea_fields:
                query_state["Y"] = self._make_projection(mea_fields[0])
            elif len(dim_fields) > 1:
                query_state["Y"] = self._make_projection(dim_fields[-1])

        return {"queryState": query_state} if query_state else None

    def _make_projection(self, field):
        return {"projections": [self._make_projection_entry(field)]}

    def _make_projection_entry(self, field):
        raw_name = field.get('name', 'Field')
        clean_name = self._clean_field_name(raw_name)
        if hasattr(self, '_field_map') and clean_name in self._field_map:
            entity, prop = self._field_map[clean_name]
        else:
            entity = field.get('datasource', 'Table')
            prop = clean_name
        is_measure = self._is_measure_field(clean_name)
        field_type = "Measure" if is_measure else "Column"
        return {
            "field": {field_type: {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
            "queryRef": f"{entity}.{prop}",
            "active": True
        }

    # ════════════════════════════════════════════════════════════════
    #  FILTERS
    # ════════════════════════════════════════════════════════════════

    def _create_visual_filters(self, filters):
        visual_filters = []
        for f in filters:
            field = f.get('field', '')
            if not field:
                continue
            clean_field = field.replace('[', '').replace(']', '')
            entity, prop = self._resolve_field_entity(clean_field)
            filter_type = f.get('type', 'categorical')

            if filter_type == 'range' or f.get('min') is not None:
                pbi_filter = {
                    "name": f"Filter_{uuid.uuid4().hex[:12]}",
                    "type": "Advanced",
                    "field": {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
                    "filter": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Where": []
                    }
                }
                conditions = []
                if f.get('min') is not None:
                    conditions.append({"Comparison": {
                        "ComparisonKind": 2,
                        "Left": {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}},
                        "Right": {"Literal": {"Value": f"'{f['min']}'"}}
                    }})
                if f.get('max') is not None:
                    conditions.append({"Comparison": {
                        "ComparisonKind": 3,
                        "Left": {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}},
                        "Right": {"Literal": {"Value": f"'{f['max']}'"}}
                    }})
                if conditions:
                    pbi_filter["filter"]["Where"] = [{"Condition": c} for c in conditions]
                visual_filters.append(pbi_filter)
            else:
                values = f.get('values', [])
                is_exclude = f.get('exclude', False)
                pbi_filter = {
                    "name": f"Filter_{uuid.uuid4().hex[:12]}",
                    "type": "Categorical",
                    "field": {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}},
                    "filter": {
                        "Version": 2,
                        "From": [{"Name": "t", "Entity": entity, "Type": 0}],
                        "Where": []
                    }
                }
                if values:
                    condition = {
                        "In": {
                            "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": prop}}],
                            "Values": [[{"Literal": {"Value": f"'{v}'"}}] for v in values[:100]]
                        }
                    }
                    if is_exclude:
                        condition = {"Not": {"Expression": condition}}
                    pbi_filter["filter"]["Where"].append({"Condition": condition})
                visual_filters.append(pbi_filter)
        return visual_filters

    # ════════════════════════════════════════════════════════════════
    #  VISUAL OBJECTS (formatting)
    # ════════════════════════════════════════════════════════════════

    def _build_visual_objects(self, ws_name, ws_data, visual_type):
        objects = {}
        objects["title"] = [{"properties": {"text": {"expr": {"Literal": {"Value": f"'{ws_name}'"}}}}}]
        if not ws_data:
            return objects

        formatting = ws_data.get('formatting', {})
        mark_encoding = ws_data.get('mark_encoding', {})

        # Data labels
        show_labels = False
        mark_fmt = formatting.get('mark', {})
        if isinstance(mark_fmt, dict):
            show_labels = mark_fmt.get('mark-labels-show', '').lower() == 'true'
        if mark_encoding.get('label', {}).get('show'):
            show_labels = True
        if show_labels:
            objects["labels"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]

        # Legend
        color_field = mark_encoding.get('color', {}).get('field', '')
        if color_field and color_field != 'Multiple Values':
            objects["legend"] = [{"properties": {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "position": {"expr": {"Literal": {"Value": "'Right'"}}}
            }}]

        # Label color
        label_fmt = formatting.get('label', {})
        if isinstance(label_fmt, dict) and label_fmt.get('color'):
            if "labels" not in objects:
                objects["labels"] = [{"properties": {}}]
            objects["labels"][0]["properties"]["color"] = {
                "solid": {"color": {"expr": {"Literal": {"Value": f"'{label_fmt['color']}'"}}}}
            }

        # Axes
        axis_fmt = formatting.get('axis', {})
        if isinstance(axis_fmt, dict):
            axis_display = axis_fmt.get('display', 'true')
            show_axis = axis_display.lower() != 'none' if axis_display else True
            if show_axis:
                objects["categoryAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
                objects["valueAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]

        axes_data = ws_data.get('axes', {})
        if axes_data:
            x_axis = axes_data.get('x', {})
            if x_axis and x_axis.get('title'):
                objects["categoryAxis"] = [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "title": {"expr": {"Literal": {"Value": f"'{x_axis['title']}'"}}}
                }}]
            y_axis = axes_data.get('y', {})
            if y_axis and y_axis.get('title'):
                objects["valueAxis"] = [{"properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "title": {"expr": {"Literal": {"Value": f"'{y_axis['title']}'"}}}
                }}]

        # Background
        bg_color = formatting.get('background_color', '')
        if not bg_color and isinstance(formatting.get('pane', {}), dict):
            bg_color = formatting.get('pane', {}).get('background-color', '')
        if bg_color:
            objects["visualContainerStyle"] = [{"properties": {
                "background": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{bg_color}'"}}}}}
            }}]

        # Conditional formatting
        color_enc = mark_encoding.get('color', {})
        color_mode = color_enc.get('type', '')
        if color_mode == 'quantitative' or color_enc.get('palette', ''):
            palette_colors = color_enc.get('palette_colors', [])
            if len(palette_colors) >= 2:
                objects["dataPoint"] = [{"properties": {
                    "fill": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{palette_colors[0]}'"}}}}}
                }}]

        # Reference lines
        ref_lines = ws_data.get('reference_lines', [])
        if ref_lines:
            y_ref_lines = []
            for ref in ref_lines:
                y_ref_lines.append({
                    "type": "Constant",
                    "value": str(ref.get('value', 0)),
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "displayName": {"expr": {"Literal": {"Value": f"'{ref.get('label', '')}'"}}},
                    "color": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{ref.get('color', '#666666')}'"}}}}},
                    "style": {"expr": {"Literal": {"Value": "'dashed'"}}}
                })
            if y_ref_lines:
                if "valueAxis" not in objects:
                    objects["valueAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
                objects["valueAxis"][0]["properties"]["referenceLine"] = y_ref_lines

        # -- Trend lines (analytics pane) ----------------------
        trend_lines = ws_data.get('trend_lines', [])
        if trend_lines:
            trend_objs = []
            for tl in trend_lines:
                trend_type = tl.get('type', 'linear').capitalize()
                if trend_type not in ('Linear', 'Exponential', 'Logarithmic', 'Polynomial', 'Power', 'MovingAverage'):
                    trend_type = 'Linear'
                trend_obj = {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "lineColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{tl.get('color', '#666666')}'"}}}}}
                }
                if tl.get('show_equation'):
                    trend_obj["displayEquation"] = {"expr": {"Literal": {"Value": "true"}}}
                if tl.get('show_r_squared'):
                    trend_obj["displayRSquared"] = {"expr": {"Literal": {"Value": "true"}}}
                trend_objs.append({"properties": trend_obj})
            objects["trend"] = trend_objs

        # -- Annotations ? visual annotations (text boxes near visual) --
        annotations = ws_data.get('annotations', [])
        if annotations:
            anno_texts = [a.get('text', '') for a in annotations if a.get('text')]
            if anno_texts:
                subtitle_text = "; ".join(anno_texts[:3])
                objects.setdefault("subTitle", [{"properties": {}}])
                objects["subTitle"][0]["properties"]["show"] = {"expr": {"Literal": {"Value": "true"}}}
                objects["subTitle"][0]["properties"]["text"] = {"expr": {"Literal": {"Value": json.dumps(subtitle_text)}}}

        # -- Enhanced axis config from extracted axes ----------
        axes_detail = ws_data.get('axes', {})
        if axes_detail:
            for axis_key, axis_obj_key in [('x', 'categoryAxis'), ('y', 'valueAxis')]:
                ax = axes_detail.get(axis_key, {})
                if not ax:
                    continue
                props = objects.get(axis_obj_key, [{"properties": {}}])[0].get("properties", {})
                props["show"] = {"expr": {"Literal": {"Value": "true"}}}
                if ax.get('title'):
                    props["showAxisTitle"] = {"expr": {"Literal": {"Value": "true"}}}
                    props["title"] = {"expr": {"Literal": {"Value": f"'{ax['title']}'"}}}
                if ax.get('show_title') is False:
                    props["showAxisTitle"] = {"expr": {"Literal": {"Value": "false"}}}
                if ax.get('show_label') is False:
                    props["show"] = {"expr": {"Literal": {"Value": "false"}}}
                if ax.get('label_rotation'):
                    try:
                        rot = int(float(ax['label_rotation']))
                        if rot != 0:
                            props["labelAngle"] = {"expr": {"Literal": {"Value": f"{rot}L"}}}
                    except (ValueError, TypeError):
                        pass
                if ax.get('format'):
                    props["labelDisplayUnits"] = {"expr": {"Literal": {"Value": "'0L'"}}}
                objects[axis_obj_key] = [{"properties": props}]

        # -- Legend position from mark encoding ----------------
        legend_pos = mark_encoding.get('color', {}).get('legend_position', '')
        if legend_pos and "legend" in objects:
            pos_map = {'top': 'Top', 'bottom': 'Bottom', 'left': 'Left',
                       'right': 'Right', 'top-left': 'TopLeft', 'top-right': 'TopRight',
                       'bottom-left': 'BottomLeft', 'bottom-right': 'BottomRight'}
            pbi_pos = pos_map.get(legend_pos.lower(), 'Right')
            objects["legend"][0]["properties"]["position"] = {"expr": {"Literal": {"Value": f"'{pbi_pos}'"}}}

        # -- Font formatting from extracted formatting ---------
        font_props = formatting.get('font', {})
        if isinstance(font_props, dict):
            font_family = font_props.get('family', '')
            font_size = font_props.get('size', '')
            if font_family or font_size:
                if "labels" not in objects:
                    objects["labels"] = [{"properties": {}}]
                if font_family:
                    objects["labels"][0]["properties"]["fontFamily"] = {"expr": {"Literal": {"Value": f"'{font_family}'"}}}
                if font_size:
                    try:
                        fs_val = int(float(font_size.replace('pt', '').replace('px', '').strip()))
                        objects["labels"][0]["properties"]["fontSize"] = {"expr": {"Literal": {"Value": f"{fs_val}D"}}}
                    except (ValueError, TypeError):
                        pass

        # -- Forecast config (analytics pane) ------------------
        forecasts = ws_data.get('forecasting', [])
        if forecasts:
            fc = forecasts[0]
            forecast_obj = {
                "show": {"expr": {"Literal": {"Value": "true"}}},
                "forecastLength": {"expr": {"Literal": {"Value": f"{fc.get('periods', 5)}L"}}},
                "confidenceBandStyle": {"expr": {"Literal": {"Value": "'fill'"}}},
            }
            ci = fc.get('prediction_interval', '95')
            forecast_obj["confidenceLevel"] = {"expr": {"Literal": {"Value": f"'{ci}'"}}}
            if fc.get('ignore_last', '0') != '0':
                forecast_obj["ignoreLast"] = {"expr": {"Literal": {"Value": f"{fc['ignore_last']}L"}}}
            objects["forecast"] = [{"properties": forecast_obj}]

        # -- Map options (washout/style) -----------------------
        map_opts = ws_data.get('map_options', {})
        if map_opts and visual_type in ('map', 'filledMap'):
            map_props = {}
            washout = map_opts.get('washout', '0.0')
            try:
                wo_val = float(washout)
                if wo_val > 0:
                    map_props["transparency"] = {"expr": {"Literal": {"Value": f"{int(wo_val * 100)}L"}}}
            except (ValueError, TypeError):
                pass
            style = map_opts.get('style', 'road')
            style_map = {'normal': "'road'", 'light': "'grayscale'", 'dark': "'darkGrayscale'",
                         'satellite': "'aerial'", 'streets': "'road'"}
            pbi_style = style_map.get(style.lower(), "'road'")
            map_props["mapStyle"] = {"expr": {"Literal": {"Value": pbi_style}}}
            if map_props:
                objects["mapControl"] = [{"properties": map_props}]

        # -- Per-value color assignments -----------------------
        color_enc = mark_encoding.get('color', {})
        color_values = color_enc.get('color_values', {})
        if color_values:
            dp_rules = []
            for val, clr in list(color_values.items())[:20]:
                dp_rules.append({
                    "properties": {
                        "fill": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{clr}'"}}}}}
                    }
                })
            if dp_rules:
                objects["dataPoint"] = dp_rules

        # -- Conditional formatting (gradient scales) ----------
        if color_enc.get('type') == 'quantitative' and color_enc.get('palette_colors'):
            palette = color_enc['palette_colors']
            if len(palette) >= 2:
                gradient_props = {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "minColor": {"solid": {"color": {"expr": {"Literal": {"Value": f"'{palette[0]}'"}}}}}
                }
                if len(palette) >= 3:
                    gradient_props["midColor"] = {"solid": {"color": {"expr": {"Literal": {"Value": f"'{palette[len(palette)//2]}'"}}}}}
                gradient_props["maxColor"] = {"solid": {"color": {"expr": {"Literal": {"Value": f"'{palette[-1]}'"}}}}}
                objects["colorBorder"] = [{"properties": gradient_props}]

        # -- Continuous vs discrete axis scale -----------------
        axes_detail = ws_data.get('axes', {})
        for axis_key, axis_obj_key in [('x', 'categoryAxis'), ('y', 'valueAxis')]:
            ax = axes_detail.get(axis_key, {})
            if ax.get('is_continuous') is True:
                if axis_obj_key in objects:
                    objects[axis_obj_key][0]["properties"]["axisType"] = {"expr": {"Literal": {"Value": "'Continuous'"}}}
            elif ax.get('is_continuous') is False and axis_obj_key in objects:
                objects[axis_obj_key][0]["properties"]["axisType"] = {"expr": {"Literal": {"Value": "'Categorical'"}}}

        # -- Dual-axis synchronization -------------------------
        dual_axis = ws_data.get('dual_axis', {})
        if dual_axis.get('enabled'):
            if "valueAxis" not in objects:
                objects["valueAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
            if dual_axis.get('synchronized'):
                objects["valueAxis"][0]["properties"]["secShow"] = {"expr": {"Literal": {"Value": "true"}}}
                objects["valueAxis"][0]["properties"]["secAxisLabel"] = {"expr": {"Literal": {"Value": "true"}}}

        # -- Per-object padding --------------------------------
        padding = ws_data.get('padding', {})
        if padding:
            pad_props = {}
            for side in ('top', 'bottom', 'left', 'right'):
                key = f'padding_{side}'
                mkey = f'margin_{side}'
                val = padding.get(key, padding.get(mkey, 0))
                if val:
                    pad_props[side] = {"expr": {"Literal": {"Value": f"{val}L"}}}
            if pad_props:
                objects["visualContainerPadding"] = [{"properties": pad_props}]

        # -- Row banding / alternating colors for table & matrix -
        if visual_type in ('tableEx', 'matrix', 'pivotTable'):
            banding_color = formatting.get('row_banding_color', '')
            if not banding_color:
                banding_color = formatting.get('banded_row_color', '')
            if banding_color:
                objects.setdefault("values", [{"properties": {}}])
                objects["values"][0]["properties"]["backColor"] = {
                    "solid": {"color": {"expr": {"Literal": {"Value": f"'{banding_color}'"}}}}
                }
            else:
                # Default light-grey banding for tables
                objects.setdefault("values", [{"properties": {}}])
                objects["values"][0]["properties"]["backColor"] = {
                    "solid": {"color": {"expr": {"Literal": {"Value": "'#F2F2F2'"}}}}
                }
            # Totals from extraction
            totals = ws_data.get('totals', {})
            if totals and (totals.get('grand_totals') or totals.get('subtotals')):
                objects.setdefault("total", [{"properties": {}}])
                objects["total"][0]["properties"]["totals"] = {"expr": {"Literal": {"Value": "true"}}}
                if totals.get('subtotals'):
                    objects.setdefault("subTotals", [{"properties": {}}])
                    objects["subTotals"][0]["properties"]["rowSubtotals"] = {"expr": {"Literal": {"Value": "true"}}}

        # -- Reference bands from analytics_stats --------------
        analytics_stats = ws_data.get('analytics_stats', [])
        for stat in analytics_stats:
            if stat.get('type') == 'distribution_band':
                band_from = stat.get('value_from', '')
                band_to = stat.get('value_to', '')
                if band_from and band_to:
                    if "valueAxis" not in objects:
                        objects["valueAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
                    objects["valueAxis"][0]["properties"].setdefault("referenceLine", [])
                    objects["valueAxis"][0]["properties"]["referenceLine"].append({
                        "type": "Band",
                        "lowerBound": str(band_from),
                        "upperBound": str(band_to),
                        "transparency": {"expr": {"Literal": {"Value": "50L"}}},
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                    })
            elif stat.get('type') in ('stat_line', 'stat_reference'):
                comp = stat.get('computation', stat.get('stat', ''))
                _STAT_MAP = {'mean': 'Average', 'median': 'Median', 'constant': 'Constant',
                             'percentile': 'Percentile', 'mode': 'Average'}
                stat_type = _STAT_MAP.get(comp.lower(), 'Average')
                if "valueAxis" not in objects:
                    objects["valueAxis"] = [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
                objects["valueAxis"][0]["properties"].setdefault("referenceLine", [])
                objects["valueAxis"][0]["properties"]["referenceLine"].append({
                    "type": stat_type,
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "style": {"expr": {"Literal": {"Value": "'dashed'"}}},
                })

        # -- Number format mapping -----------------------------
        fmt_info = formatting.get('number_format', formatting.get('format_string', ''))
        if fmt_info:
            pbi_fmt = self._convert_number_format(fmt_info)
            if pbi_fmt and "labels" in objects:
                objects["labels"][0]["properties"]["labelDisplayUnits"] = {"expr": {"Literal": {"Value": f"'{pbi_fmt}'"}}}

        return objects

    # ════════════════════════════════════════════════════════════════
    #  SLICER
    # ════════════════════════════════════════════════════════════════

    def _create_slicer_visual(self, visual_id, x, y, w, h, field_name, table_name, z_order):
        clean_field = field_name.replace('[', '').replace(']', '')
        clean_table = table_name.replace("'", "''") if table_name else 'Table'
        slicer = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": {"x": x, "y": y, "z": z_order * 1000, "height": h, "width": w, "tabOrder": z_order * 1000},
            "visual": {
                "visualType": "slicer",
                "objects": {
                    "data": [{"properties": {"mode": {"expr": {"Literal": {"Value": "'Dropdown'"}}}}}],
                    "header": [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}}}]
                },
                "drillFilterOtherVisuals": True
            }
        }
        if clean_field and clean_table:
            slicer["visual"]["query"] = {
                "queryState": {
                    "Values": {
                        "projections": [{
                            "field": {"Column": {"Expression": {"SourceRef": {"Entity": clean_table}}, "Property": clean_field}},
                            "queryRef": f"{clean_table}.{clean_field}"
                        }]
                    }
                }
            }
        return slicer

    def _create_visual_nav_button(self, visuals_dir, obj, scale_x, scale_y, visual_count):
        """Create a PBI action button that navigates to another page (sheet swapping)."""
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        pos = obj.get('position', {})
        target_sheet = obj.get('target_sheet', '')
        btn_text = obj.get('name', 'Navigate')
        visual_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": self._make_visual_position(pos, scale_x, scale_y, visual_count),
            "visual": {
                "visualType": "actionButton",
                "objects": {
                    "icon": [{"properties": {"shapeType": {"expr": {"Literal": {"Value": "'Navigation'"}}}}}],
                    "text": [{"properties": {"text": {"expr": {"Literal": {"Value": json.dumps(btn_text)}}}}}],
                    "action": [{"properties": {
                        "type": {"expr": {"Literal": {"Value": "'PageNavigation'"}}},
                        "destination": {"expr": {"Literal": {"Value": json.dumps(target_sheet)}}}
                    }}]
                }
            }
        }
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(visual_json, f, indent=2, ensure_ascii=False)

    def _create_visual_action_button(self, visuals_dir, obj, scale_x, scale_y, visual_count, action_type='Export'):
        """Create a PBI action button (download/export/bookmark)."""
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        pos = obj.get('position', {})
        btn_text = obj.get('name', action_type)
        visual_json = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.5.0/schema.json",
            "name": visual_id,
            "position": self._make_visual_position(pos, scale_x, scale_y, visual_count),
            "visual": {
                "visualType": "actionButton",
                "objects": {
                    "icon": [{"properties": {"shapeType": {"expr": {"Literal": {"Value": "'" + action_type + "'"}}}}}],
                    "text": [{"properties": {"text": {"expr": {"Literal": {"Value": json.dumps(btn_text)}}}}}]
                }
            }
        }
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(visual_json, f, indent=2, ensure_ascii=False)

    def _create_pages_shelf_slicer(self, visuals_dir, pages_shelf, scale_x, scale_y, visual_count, converted_objects):
        """Create an animation-hint slicer from Tableau Pages shelf."""
        field = pages_shelf.get('field', '')
        if not field:
            return
        table_name = self._find_column_table(field, converted_objects)
        visual_id = new_visual_id()
        visual_dir = os.path.join(visuals_dir, visual_id)
        os.makedirs(visual_dir, exist_ok=True)
        slicer = self._create_slicer_visual(visual_id, 10, 10, 400, 50, field, table_name, visual_count)
        slicer.setdefault('visual', {}).setdefault('objects', {})
        slicer['visual']['objects']['general'] = [{'properties': {
            'comments': {'expr': {'Literal': {'Value': "'Pages Shelf / Play Axis: animate through values'"}}}
        }}]
        with open(os.path.join(visual_dir, 'visual.json'), 'w', encoding='utf-8') as f:
            json.dump(slicer, f, indent=2, ensure_ascii=False)

    # ════════════════════════════════════════════════════════════════
    #  UTILITY
    # ════════════════════════════════════════════════════════════════

    def _find_column_table(self, column_name, converted_objects):
        for ds in converted_objects.get('datasources', []):
            for table in ds.get('tables', []):
                for col in table.get('columns', []):
                    col_caption = col.get('caption', col.get('name', ''))
                    if col_caption == column_name or col.get('name', '') == column_name:
                        return table.get('name', '')
                for calc in ds.get('calculations', []):
                    if calc.get('caption', '') == column_name:
                        tables = ds.get('tables', [])
                        if tables:
                            return tables[0].get('name', '')
        return ''

    def _find_worksheet(self, worksheets, name):
        for ws in worksheets:
            if ws.get('name') == name:
                return ws
        return None

    @staticmethod
    def _convert_number_format(tableau_format):
        """Convert Tableau number format string to PBI display units / format.
        
        Common Tableau patterns:
            ###,###    ? #,0
            $#,#00.00  ? $#,0.00
            0.0%       ? 0.0%
            0.00       ? 0.00
        """
        if not tableau_format or not isinstance(tableau_format, str):
            return ''
        fmt = tableau_format.strip()
        # Already a PBI-compatible format
        if fmt in ('0', '0.0', '0.00', '#,0', '#,0.0', '#,0.00', '0%', '0.0%', '0.00%'):
            return fmt
        # Currency
        if '$' in fmt:
            return fmt.replace('#,#', '#,0').replace('##', '#0')
        # Percentage
        if '%' in fmt:
            return fmt
        # Thousands separator
        if ',' in fmt:
            return fmt.replace('#,#', '#,0')
        return fmt

    def _count_report_artifacts(self, project_dir, report_name):
        """Count pages and visuals from the generated report."""
        pages_count = 0
        visuals_count = 0
        report_def = os.path.join(project_dir, f"{report_name}.Report", "definition", "pages")
        if os.path.isdir(report_def):
            for entry in os.listdir(report_def):
                entry_path = os.path.join(report_def, entry)
                if os.path.isdir(entry_path):
                    pages_count += 1
                    vis_dir = os.path.join(entry_path, 'visuals')
                    if os.path.isdir(vis_dir):
                        visuals_count += len([d for d in os.listdir(vis_dir)
                                              if os.path.isdir(os.path.join(vis_dir, d))])
        return pages_count, visuals_count

    def create_metadata(self, project_dir, report_name, converted_objects):
        """Create migration metadata file."""
        pages_count, visuals_count = self._count_report_artifacts(project_dir, report_name)
        theme_applied = os.path.exists(os.path.join(
            project_dir, f"{report_name}.Report", "definition",
            "RegisteredResources", "TableauMigrationTheme.json"
        ))
        tmdl_stats = {}
        tables_dir = os.path.join(project_dir, f"{report_name}.SemanticModel", "definition", "tables")
        if os.path.isdir(tables_dir):
            tmdl_stats['tables'] = len([f for f in os.listdir(tables_dir) if f.endswith('.tmdl')])

        metadata = {
            "generated_at": datetime.now().isoformat(),
            "source": "Tableau Migration (Fabric Edition)",
            "target": "Microsoft Fabric (DirectLake)",
            "report_name": report_name,
            "objects_converted": {
                "worksheets": len(converted_objects.get('worksheets', [])),
                "dashboards": len(converted_objects.get('dashboards', [])),
                "datasources": len(converted_objects.get('datasources', [])),
                "calculations": len(converted_objects.get('calculations', [])),
                "parameters": len(converted_objects.get('parameters', [])),
                "filters": len(converted_objects.get('filters', [])),
                "stories": len(converted_objects.get('stories', [])),
                "sets": len(converted_objects.get('sets', [])),
                "groups": len(converted_objects.get('groups', [])),
                "bins": len(converted_objects.get('bins', [])),
                "hierarchies": len(converted_objects.get('hierarchies', [])),
                "user_filters": len(converted_objects.get('user_filters', [])),
                "actions": len(converted_objects.get('actions', [])),
                "custom_sql": len(converted_objects.get('custom_sql', []))
            },
            "generated_output": {
                "pages": pages_count,
                "visuals": visuals_count,
                "theme_applied": theme_applied,
                "semantic_model_mode": "DirectLake"
            },
            "tmdl_stats": tmdl_stats
        }
        metadata_file = os.path.join(project_dir, 'migration_metadata.json')
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
