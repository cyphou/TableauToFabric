# Tableau → Fabric Migration Tool: Feature Gap Analysis

> Generated: 2026-03-04 | Updated: 2026-03-04  
> Scope: All Tableau Desktop features vs. current TableauToFabric migration tool coverage

---

## Summary Statistics

| Category | Count |
|----------|-------|
| **Total Tableau features assessed** | 55 |
| **Fully covered (✅)** | 25 |
| **Partially covered (⚠️)** | 16 |
| **Missing entirely (❌)** | 14 |
| **Coverage rate** | 45% full, 29% partial, 26% missing |

---

## 1. MISSING Features (❌) — Not Extracted or Converted

| # | Feature | Description | Priority |
|---|---------|-------------|----------|
| 1 | **Forecasting** | Tableau's built-in forecasting (exponential smoothing). No `<forecast>` XML extraction. Power BI has a forecast feature in analytics pane but requires separate configuration. | Low |
| 2 | **Clustering** | Tableau's k-means clustering (Analytics pane). No equivalent extraction or conversion. Power BI has no built-in clustering visual — requires R/Python visuals or AI visuals. | Low |
| 3 | **Map Options** | Washout percentage, background map style, custom background images for maps. No `<map-options>` or `<background-image>` extraction for map visuals. | Low |
| 4 | **Custom Shapes/Images** | Custom shape marks stored in `.twbx` shapes folder. Not extracted from the packaged file — shape encoding references are lost. | Low |
| 5 | **Sheet Swapping** | Technique of showing/hiding worksheets in a dashboard container via filter or parameter actions. Not detected or converted — requires Power BI bookmarks + buttons or field parameters. | Medium |
| 6 | **Data Blending** | Tableau-specific feature linking primary and secondary datasources on blending keys. Not extracted — secondary datasource link fields and blending relationships are ignored. | Medium |
| 7 | **Extracts (.hyper) Data Import** | Tableau `.hyper` extract files embedded in `.twbx`. The tool extracts metadata only — actual data from `.hyper` is not imported into Lakehouse Delta tables. (Schema is captured; data is not.) | High |
| 8 | **Published Data Source Resolution** | References to Tableau Server/Cloud published data sources. The tool does not connect to Tableau Server to resolve these references — they appear as empty connections. | Medium |
| 9 | **Tableau Server/Cloud Connectivity** | No API integration to pull workbooks, datasources, or metadata from Tableau Server/Online REST API. Migration is file-based only (`.twb`/`.twbx`). | Medium |
| 10 | **Subscriptions & Alerts** | Tableau Server subscriptions (email schedules) and data-driven alerts. Server-side features — no equivalent metadata in `.twb` files and no conversion to Power BI alerts. | Low |
| 11 | **Comments** | Tableau Server comments on views. Server-side feature with no `.twb` representation. | Low |
| 12 | **Embedded Fonts** | Custom fonts embedded in `.twbx` packages. Not extracted from the packaged file — font references in formatting may point to unavailable fonts. | Low |
| 13 | **Custom Geocoding** | Custom geocoding files (`.csv` with lat/long mappings) in Tableau's geocoding folder. Not extracted or converted to Power BI's location column mapping. | Low |
| 14 | **Performance Recording** | Tableau performance recording logs. Diagnostic feature with no migration relevance. | Low |

---

## 2. PARTIALLY COVERED Features (⚠️)

| # | Feature | What's Covered | What's Missing |
|---|---------|---------------|----------------|
| 1 | **Tooltips** | Text tooltip content extracted. Viz-in-tooltip detected and converted to PBI tooltip pages. | Custom tooltip formatting (bold, color, field-level layout) not preserved. Tooltip field order not mapped. |
| 2 | **Map Layers** | Map visual type detected (symbol map → `map`, filled map → `filledMap`). Dual-axis map → `map` (approximate). | Multiple **mark layers** (e.g., circles + filled polygons on the same map) not preserved — only one layer emitted. Map **data layer** blend modes and layer ordering not extracted. |
| 3 | **Dashboard Objects** | `worksheetReference`, `filter_control`, `text`, `image`, `web`, `blank` objects extracted with positions. | **Navigation button**, **download** (PDF/image/data), **extension** (dashboard extensions), and **ask data** objects not extracted or converted. Web objects don't preserve iframe sizing/scrolling config. |
| 4 | **Dashboard Padding/Spacing/Sizing** | Dashboard overall `width` × `height` extracted. Individual object positions (`x`, `y`, `w`, `h`) extracted from zone attributes. Container padding extracted. | Inner padding per object, outer padding of the dashboard, spacing between tiled objects, and margins are not fully mapped to PBI page margins. |
| 5 | **Sort Configuration** | Sort orders extracted as JSON. Visual-level sort state generated in `visual_generator.py` with field + direction (ascending/descending). `sortByColumn` in semantic model. | **Manual/custom sort** (drag-order), **nested sort** (sort within groups), field-level sort by another field, and **computed sort** (sort by aggregation) not fully mapped. |
| 6 | **Dashboard Actions** | Actions extracted with `name`, `type`, `source_worksheets`, `target_worksheets`, `command`. Filter, highlight, URL, sheet navigation, parameter, set actions identified. Go-to-sheet actions generate drillthrough pages. | Action **field mappings** (which source field filters which target field) not fully extracted. **Clearing** behavior (leaving/clearing filter on deselect) not captured. Set actions marked as 🔧 manual. |
| 7 | **Cross-Database Joins** | Multiple connections within a datasource extracted via `connection_map`. Relationships between tables from different connections extracted. | Cross-database join semantics (which connection serves which table) not explicitly modeled. All tables merged into a single Lakehouse — the cross-database relationship context is flattened. |
| 8 | **Continuous vs. Discrete** | Field derivation prefixes parsed during extraction (`qk` = quantitative/continuous, `nk` = nominal/discrete). Color encoding type (continuous/discrete) extracted. | Continuous axis rendering vs. discrete axis behavior not fully translated into Power BI axis scale type. |
| 9 | **Dual-Axis Synchronization** | Dual-axis worksheets mapped to `lineClusteredColumnComboChart`. Visual config includes both valueAxis and legend settings. | Axis **synchronization** (Tableau's "Synchronize Axis" checkbox) not detected. Individual axis range/scale for each axis not mapped. |
| 10 | **Combined Fields** | Groups with `group_type: "combined"` extracted (e.g., `Action (Region)` combining `Region`). Source fields captured. | Combined field is not recreated as a concatenated column or GROUP BY combination in DAX/M. |
| 11 | **Pages Shelf (Animation)** | Pages shelf field and datasource extracted. Converted to filter in PBI. | Not converted to Power BI Play Axis or animation feature. Interactive exploration workflow approximated as a filter only. |
| 12 | **Custom Color Palettes** | Mark color encoding field detected. Custom named palettes and workbook-level palettes extracted with individual colors. Theme generates custom palette. | Specific color-per-value assignments not mapped. Diverging/sequential palette gradient types not fully detected. |
| 13 | **Spatial functions** | MAKEPOINT/MAKELINE → BLANK() with comment | No spatial equivalent in DAX |
| 14 | **PREVIOUS_VALUE / LOOKUP** | Placeholder comments generated | No time-intelligence DAX equivalent automatically generated |
| 15 | **Custom visuals** | Mapped to closest PBI type or custom visual placeholder | Some Tableau-specific visuals (sankey, chord) have no direct PBI equivalent |
| 16 | **Conditional formatting** | Detected; basic data point coloring with palette colors applied | Per-value conditional rules, gradient scales, and icon sets not fully generated |

### Recently Promoted to Full Coverage (from previous Missing/Partial)

The following features were **previously missing or partial** and are **now fully covered** after the latest implementation round:

| Feature | Previous Status | What Was Added |
|---------|----------------|---------------|
| **Annotations** | ❌ Missing | `extract_annotations()` parses point/area/text annotations; rendered as visual subtitles in PBI |
| **Trend Lines** | ❌ Missing | `extract_trend_lines()` parses `<trend-line>` XML; trend line objects generated in PBIR analytics pane with type, color, equation, R² |
| **Reference Lines/Bands/Distributions** | ⚠️ Constant only | `extract_reference_lines()` now parses lines, bands, and distributions with all parameters |
| **Dashboard Device Layouts** | ❌ Missing | `extract_device_layouts()` parses phone/tablet layouts; PBI mobile pages generated with zone mapping |
| **Dashboard Layout Containers** | ⚠️ Flattened | `extract_dashboard_containers()` parses H/V containers with orientation, padding, children |
| **Drill-Through Pages** | ❌ Missing | "Go to Sheet" actions detected; PBI drillthrough pages generated with drillthrough filters |
| **Table Calc Addressing** | ❌ Missing | `extract_table_calcs()` parses `<table-calc>` with COMPUTE USING dims; ALLEXCEPT-based partitioning in DAX |
| **Percent of Total** | ⚠️ Prefix only | `_convert_total_function()` generates `CALCULATE(expr, ALL('table'))` DAX pattern |
| **Running Calcs** | ⚠️ Blanket ALL | `_convert_running_functions()` generates cumulative DAX with FILTER(ALLSELECTED) |
| **Date Hierarchies** | ⚠️ Flat columns | `_auto_date_hierarchies()` generates Year > Quarter > Month > Day hierarchies on date columns |
| **Formatting (font/axis/legend/label)** | ⚠️ Basic | Enhanced extraction of fonts, axis rotation/title/format, legend position; all rendered in PBIR visual objects |
| **Analytics Pane (trend/reference)** | ⚠️ Constant only | Full trend line and reference line/band/distribution rendering in visual objects |

---

## 3. Feature Coverage Detail by Category

### 3.1 Data Connectivity & Modeling

| Feature | Status | Notes |
|---------|--------|-------|
| 25 connector types | ✅ Full | Excel, CSV, SQL Server, PostgreSQL, MySQL, Oracle, BigQuery, Snowflake, etc. |
| Tables & columns | ✅ Full | Multi-phase extraction (up to 4 phases, metadata records, fallback) |
| Relationships / joins | ✅ Full | Both `{from_table, from_column}` and `{left: {table, column}}` formats |
| Custom SQL | ✅ Full | Converted to `Sql.Database(..., [Query=...])` native query |
| Cross-database joins | ⚠️ Partial | Tables flattened into single Lakehouse |
| Data blending | ❌ Missing | Primary/secondary link fields not extracted |
| Extracts (.hyper) | ❌ Missing | Schema extracted; data not imported |
| Published data sources | ❌ Missing | Tableau Server references not resolved |
| Tableau Server connectivity | ❌ Missing | File-based only |
| Data type mappings | ✅ Full | 13 Tableau types → Delta, M, TMDL types |

### 3.2 Calculations & Formulas

| Feature | Status | Notes |
|---------|--------|-------|
| 172+ function conversions | ✅ Full | Aggregation, logical, text, date, math, stats, regex, spatial |
| LOD expressions | ✅ Full | FIXED, INCLUDE, EXCLUDE, no-dimension LOD |
| IF/THEN/ELSE/END | ✅ Full | Nested, ELSEIF, balanced-depth parsing |
| CASE/WHEN/END | ✅ Full | → SWITCH() |
| Cross-table references | ✅ Full | RELATED() / LOOKUPVALUE() |
| SUM(IF) → SUMX | ✅ Full | Automatic iterator detection |
| Table calc addressing | ✅ Full | COMPUTE USING parsed; ALLEXCEPT-based DAX partitioning |
| PREVIOUS_VALUE / LOOKUP | ⚠️ Partial | Placeholder comments only |
| Rank/Running/Window | ✅ Full | ALLEXCEPT partitioning; RUNNING_SUM/AVG/COUNT/MAX/MIN → cumulative DAX |
| Percent of Total | ✅ Full | TOTAL(expr) → CALCULATE(expr, ALL('table')) |
| Spatial functions | ⚠️ Partial | MAKEPOINT/MAKELINE → BLANK() with comment |

### 3.3 Visual Types

| Feature | Status | Notes |
|---------|--------|-------|
| 60+ visual type mappings | ✅ Full | All Tableau mark types mapped |
| 30+ PBIR config templates | ✅ Full | Per-type axis, legend, label defaults |
| Custom visuals (box, bullet, gantt, word cloud, sankey) | ⚠️ Partial | Mapped to closest PBI type or custom visual placeholder |
| Chart dual-axis | ⚠️ Partial | → combo chart, no axis sync |
| Sparklines | ⚠️ Partial | → lineChart (small multiple not configured) |

### 3.4 Worksheet Configuration

| Feature | Status | Notes |
|---------|--------|-------|
| Fields on shelves | ✅ Full | rows, columns, color, size, detail, tooltip, text, pages |
| Mark type detection | ✅ Full | 50+ mark class mappings |
| Tooltips | ✅ Full | Text content + viz-in-tooltip → PBI tooltip pages |
| Annotations | ✅ Full | Point/area/text annotations extracted; rendered as visual subtitles |
| Reference lines | ✅ Full | Lines, bands, distributions extracted and rendered |
| Trend lines | ✅ Full | Linear/polynomial/exponential extracted; analytics pane objects generated |
| Forecasting | ❌ Missing | Not extracted |
| Clustering | ❌ Missing | Not extracted |
| Mark labels | ✅ Full | Show/hide, font family/size, color mapped |
| Axis configuration | ✅ Full | Title, rotation, show/hide, format extracted and rendered |
| Legend configuration | ✅ Full | Show/hide, position extracted and rendered |
| Sort orders | ⚠️ Partial | Basic ascending/descending |
| Continuous vs. discrete | ⚠️ Partial | Detected, not fully applied to axis type |
| Pages shelf | ⚠️ Partial | Field extracted; converted as filter |

### 3.5 Dashboard Configuration

| Feature | Status | Notes |
|---------|--------|-------|
| Dashboard size | ✅ Full | Width × height extracted |
| Object positions | ✅ Full | x, y, w, h from zones |
| Tiled/floating detection | ✅ Full | Layout mode per object |
| Layout containers (H/V) | ✅ Full | Container orientation, padding, children extracted |
| Device layouts | ✅ Full | Phone/tablet layouts extracted; PBI mobile pages generated |
| Text objects | ✅ Full | Content extracted |
| Image objects | ✅ Full | Source reference extracted |
| Web page objects | ✅ Full | URL extracted |
| Navigation buttons | ❌ Missing | Not extracted |
| Download objects | ❌ Missing | Not extracted |
| Extensions | ❌ Missing | Not extracted |
| Padding/spacing | ⚠️ Partial | Not extracted from container/object |
| Filter controls | ✅ Full | Converted to slicers |

### 3.6 Actions & Interactions

| Feature | Status | Notes |
|---------|--------|-------|
| Filter actions | ⚠️ Partial | Cross-filtering approximate; field mappings missing |
| Highlight actions | ⚠️ Partial | Cross-highlighting approximate |
| URL actions | ⚠️ Partial | Button/hyperlink generated |
| Go-to-sheet actions | ⚠️ Partial | Page navigation button generated |
| Parameter actions | ⚠️ Partial | Slicer interaction approximate |
| Set actions | 🔧 Manual | Placeholder only |
| Drill-through | ✅ Full | Go-to-sheet actions → PBI drillthrough pages with filters |
| Sheet swapping | ❌ Missing | Not detected or converted |

### 3.7 Security & Administration

| Feature | Status | Notes |
|---------|--------|-------|
| User filters → RLS | ✅ Full | USERPRINCIPALNAME() + value mapping |
| USERNAME() / ISMEMBEROF() | ✅ Full | Mapped to USERPRINCIPALNAME() / RLS role |
| Row-level security | ✅ Full | TMDL RLS roles generated |
| Subscriptions/alerts | ❌ Missing | Server-side feature |
| Comments | ❌ Missing | Server-side feature |
| Performance recording | ❌ Missing | Diagnostic — not relevant for migration |

### 3.8 Styling & Appearance

| Feature | Status | Notes |
|---------|--------|-------|
| Number formats | ✅ Full | Currency, percent, integer, decimal, date |
| Custom color palettes | ✅ Full | Named palettes and workbook-level palettes extracted; custom theme generated |
| Embedded fonts | ❌ Missing | Not extracted from .twbx |
| Custom geocoding | ❌ Missing | Not extracted |
| Custom shapes | ❌ Missing | Not extracted from .twbx |
| Conditional formatting | ⚠️ Partial | Detected; basic data point coloring only |

---

## 4. Priority Ranking: Most Impactful Gaps

### High Priority (significant impact on migration fidelity)

| # | Gap | Impact | Recommended Action |
|---|-----|--------|-------------------|
| 1 | ~~**Table calc addressing/partitioning**~~ | ✅ **RESOLVED** — COMPUTE USING parsed; ALLEXCEPT DAX generated | — |
| 2 | ~~**Dashboard device layouts**~~ | ✅ **RESOLVED** — Phone/tablet layouts extracted; PBI mobile pages generated | — |
| 3 | ~~**Drill-through pages**~~ | ✅ **RESOLVED** — Go-to-sheet → PBI drillthrough pages with filters | — |
| 4 | **Extract (.hyper) data import** | Data not available in Lakehouse without manual re-ingestion | Add optional `pantab`/`tableauhyperapi` integration to read .hyper → write Delta/Parquet |
| 5 | ~~**Percent of Total / Quick Table Calcs**~~ | ✅ **RESOLVED** — TOTAL(expr) → CALCULATE, RUNNING_SUM → cumulative DAX | — |

### Medium Priority (noticeable gaps, workarounds exist)

| # | Gap | Impact |
|---|-----|--------|
| 6 | ~~Annotations~~ | ✅ **RESOLVED** — Extracted and rendered as visual subtitles |
| 7 | ~~Trend lines~~ | ✅ **RESOLVED** — Extracted and rendered as analytics pane objects |
| 8 | Pages shelf / animation | Interactive exploration workflow approximated as filter |
| 9 | Data blending | Secondary datasource links broken; must recreate as relationships |
| 10 | Published data source resolution | Connections appear empty; must manually reconnect |
| 11 | Sheet swapping | Dynamic dashboard interaction patterns lost |
| 12 | ~~Dashboard layout containers (H/V nesting)~~ | ✅ **RESOLVED** — Containers extracted with orientation, padding, children |
| 13 | ~~Viz-in-tooltip conversion~~ | ✅ **RESOLVED** — Tooltip pages generated |
| 14 | Combined fields | Multi-field combinations may not display correctly |
| 15 | Tableau Server connectivity | Can't pull directly from Server; export to file required |

### Low Priority (minor impact or rare usage)

| # | Gap | Impact |
|---|-----|--------|
| 16 | Forecasting | Rarely used in production dashboards; PBI has built-in forecast |
| 17 | Clustering | Rare; no direct PBI equivalent anyway |
| 18 | Map options (washout, background images) | Cosmetic; easily reconfigured |
| 19 | Custom shapes | Rare; PBI has different shape/icon system |
| 20 | Embedded fonts | Can be reinstalled on PBI service |
| 21 | Custom geocoding | Rare; PBI has extensive built-in geocoding |
| 22 | Subscriptions/alerts | Server-side; recreate in PBI service |
| 23 | Comments | Server-side; not in .twb files |
| 24 | Performance recording | Diagnostic; not relevant to migration |

---

## 5. What's Well Covered (✅ Strengths)

The tool has **strong coverage** in these areas:

- **172+ DAX formula conversions** with balanced-paren depth tracking, nested IF/CASE, LOD expressions, iterator detection (SUM(IF) → SUMX), cross-table RELATED()/LOOKUPVALUE()
- **25 connector types** with complete Power Query M generators
- **60+ visual type mappings** with per-type config templates and data role definitions
- **40+ M transformation generators** (rename, filter, aggregate, pivot, join, union, sort, conditional columns)
- **Calculated column materialisation** — automatic classification and 3-way output (DDL + M + PySpark)
- **RLS migration** — user filters, USERNAME(), ISMEMBEROF() → TMDL roles
- **Parameters** — range/list/any → What-If GENERATESERIES/DATATABLE tables
- **6 Fabric artifact types** generated from a single workbook
- **Full deployment pipeline** — PowerShell scripts with idempotent create, 429 retry, LRO polling
- **775 tests** across 21 test files with CI/CD
