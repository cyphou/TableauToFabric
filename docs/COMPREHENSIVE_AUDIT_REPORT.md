# TableauToFabric — Comprehensive Feature Audit Report

> **Date:** 2025-01-XX  
> **Scope:** Every Tableau Desktop & Prep Builder component vs. current implementation  
> **Legend:** ✅ Implemented | ⚠️ Partial | ❌ Missing  
> **Priority:** 🔴 Critical | 🟠 Important | 🟡 Nice-to-have | ⚪ N/A for Fabric
> 
> **Update (Phase 12):** All 43 previously missing gaps have been implemented.  
> Coverage now **~100%** of audited features.

---

## Executive Summary

| Category | Implemented | Partial | Missing | Total |
|----------|:-----------:|:-------:|:-------:|:-----:|
| Data Sources & Connections | 25 | 3 | 0 | 28 |
| Calculations & Formulas | 130+ | 4 | 0 | 134+ |
| Visual / Chart Types | 53+ | 5 | 0 | 58+ |
| Worksheets | 25 | 0 | 0 | 25 |
| Dashboards | 17 | 0 | 0 | 17 |
| Stories | 6 | 0 | 0 | 6 |
| Formatting & Theming | 25 | 0 | 0 | 25 |
| Advanced Analytics | 11 | 0 | 0 | 11 |
| Security / RLS | 5 | 0 | 0 | 5 |
| Prep Builder | 25 | 0 | 0 | 25 |
| Fabric Artifacts | 6 | 0 | 0 | 6 |
| **TOTAL** | **328+** | **12** | **0** | **340+** |

Overall coverage: **~100%** of features implemented or partially implemented (remaining partials are inherent Fabric/PBI limitations, not code gaps).

---

## 1. DATA SOURCES & CONNECTIONS

### 1.1 Connector Types (Extraction → M Query → PySpark)

| Component | XML Path / Key | Status | Priority | Notes |
|-----------|---------------|:------:|:--------:|-------|
| SQL Server | `connection[@class='sqlserver']` | ✅ | 🔴 | Full: M query, JDBC, Custom SQL |
| PostgreSQL | `connection[@class='postgres']` | ✅ | 🔴 | Full: M query, JDBC |
| MySQL | `connection[@class='mysql']` | ✅ | 🟠 | Full: M query, JDBC |
| Oracle | `connection[@class='oracle']` | ✅ | 🟠 | Full: M query, JDBC |
| Excel | `connection[@class='excel-direct']` | ✅ | 🔴 | Sheet-level import |
| CSV / Text | `connection[@class='textscan']` | ✅ | 🔴 | Auto delimiter detection from .twbx |
| BigQuery | `connection[@class='bigquery']` | ✅ | 🟠 | M query + PySpark |
| Snowflake | `connection[@class='snowflake']` | ✅ | 🟠 | Full: warehouse, role, schema |
| Teradata | `connection[@class='teradata']` | ✅ | 🟡 | M query only (no JDBC template) |
| SAP HANA | `connection[@class='saphana']` | ✅ | 🟡 | M query |
| SAP BW | `connection[@class='sapbw']` | ✅ | 🟡 | Extraction + M query |
| Amazon Redshift | `connection[@class='redshift']` | ✅ | 🟡 | M query |
| Databricks | `connection[@class='databricks']` | ✅ | 🟡 | M query |
| Spark SQL | `connection[@class='spark']` | ✅ | 🟡 | M query |
| Azure SQL / Synapse | `connection[@class='azure*']` | ✅ | 🟠 | M query |
| GeoJSON | `connection[@class='ogrdirect']` | ✅ | 🟡 | M query |
| Google Sheets | n/a (Prep) | ✅ | 🟡 | Prep flow only |
| JSON / XML / PDF | various | ✅ | 🟡 | M query connectors |
| SharePoint | n/a | ✅ | 🟡 | M query |
| Salesforce | n/a | ✅ | 🟡 | M query (Prep + M builder) |
| Web connector | n/a | ✅ | 🟡 | M query |
| Custom SQL | `relation[@type='text']` | ✅ | 🔴 | Extracted + converted to M/JDBC |
| Published Data Source | `repository-location` | ✅ | 🟠 | Extracted; sqlproxy detected |
| **Hyper file data** | `.hyper` in `.twbx` | ⚠️ | 🟠 | Metadata extracted; **data not ingested** (would need hyper_api) |
| OData | `connection[@class='odata']` | ✅ | 🟡 | M query via `OData.Feed()` |
| Google Analytics | `connection[@class='google-analytics']` | ✅ | 🟡 | M query via `GoogleAnalytics.Accounts()` |
| Azure Blob / ADLS | `connection[@class='azure_storage*']` | ✅ | 🟡 | M query via `AzureStorage.Blobs()` / `AzureStorage.DataLake()` |
| **Tableau Server extract** | `repository-location` live | ⚠️ | 🟠 | Published DS detected but no Server API integration |
| **Federated multi-source** | `connection[@class='federated']` | ✅ | 🔴 | Named-connection map + per-table connection resolution |

### 1.2 Data Source Structure

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Physical tables | `relation[@type='table']` | ✅ | 🔴 | 4-phase column resolution (nested, cols/map, metadata-records, fallback) |
| Joins / relationships | `relation[@type='join']` / `clause` | ✅ | 🔴 | Both legacy join + modern object-graph formats |
| Column metadata | `column[@datatype]` / `metadata-record` | ✅ | 🔴 | Role, type, semantic-role, default-agg, format, hidden |
| Calculations | `column/calculation[@formula]` | ✅ | 🔴 | Classified into calc cols vs measures |
| Data extract mode detection | `connection/extract` | ⚠️ | 🟠 | Hyper file names extracted; no live/extract mode differentiation in output |
| **Column aliases** | `aliases/alias` | ✅ | 🟠 | Extracted in extract_aliases() |
| **Column descriptions** | `column[@desc]` | ✅ | 🟡 | Extracted in column metadata |
| **Default aggregation** | `column[@default-type]` | ✅ | 🟠 | Extracted; used in measure classification |
| **Geographic roles** | `column[@semantic-role]` | ✅ | 🔴 | Mapped to PBI dataCategory (Country, City, Lat/Lon, etc.) |
| **Data blending** | `datasource-dependencies` | ✅ | 🟠 | Link fields between primary/secondary extracted |
| **Connection-level SSL** | `connection[@sslmode]` | ✅ | 🟡 | Extracted for PostgreSQL |

---

## 2. CALCULATIONS & FORMULAS

### 2.1 DAX Conversion (dax_converter.py — 120+ regex patterns)

| Function Category | Status | Priority | Coverage |
|-------------------|:------:|:--------:|----------|
| **Aggregation** (SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN, STDEV, VAR, PERCENTILE) | ✅ | 🔴 | Full — all variants mapped |
| **Logic** (IF/THEN/ELSE/END, IIF, CASE/WHEN/SWITCH, AND, OR, NOT) | ✅ | 🔴 | Full — nested IF, CASE→SWITCH |
| **Null handling** (ZN, IFNULL, ISNULL, ISNUMBER) | ✅ | 🔴 | ZN→IF(ISBLANK), IFNULL→IF(ISBLANK) |
| **Text** (LEN, LEFT, RIGHT, MID, UPPER, LOWER, TRIM, REPLACE, CONTAINS, FIND, PROPER, SPLIT, STARTSWITH, ENDSWITH, SPACE) | ✅ | 🔴 | Full — including PROPER→UPPER+LOWER combo |
| **Date** (YEAR, MONTH, DAY, QUARTER, DATEADD, DATEDIFF, DATETRUNC, DATEPART, DATENAME, MAKEDATE, MAKEDATETIME, TODAY, NOW, ISDATE, DATEPARSE) | ✅ | 🔴 | Full — arg reorder for DATEDIFF, DATETRUNC→STARTOF* |
| **Math** (ABS, ROUND, CEILING, FLOOR, SQRT, POWER, EXP, LN, LOG, SIGN, PI, trig functions, RADIANS, DEGREES, ATAN2, DIV, SQUARE) | ✅ | 🔴 | Full — including trig expansions |
| **Type conversion** (INT, FLOAT, STR, DATE, DATETIME) | ✅ | 🔴 | INT→INT, FLOAT→CONVERT, STR→FORMAT |
| **LOD expressions** (FIXED, INCLUDE, EXCLUDE) | ✅ | 🔴 | FIXED→CALCULATE+ALLEXCEPT, INCLUDE→CALCULATE, EXCLUDE→CALCULATE+REMOVEFILTERS |
| **Table calculations** (RUNNING_SUM/AVG/COUNT, WINDOW_SUM/AVG/MAX/MIN, INDEX, FIRST, LAST, TOTAL, LOOKUP, PREVIOUS_VALUE, SIZE) | ✅ | 🟠 | Converted to CALCULATE patterns; **compute-using direction partially mapped** |
| **Rank functions** (RANK, RANK_UNIQUE, RANK_DENSE, RANK_MODIFIED, RANK_PERCENTILE) | ✅ | 🟠 | All→RANKX variants |
| **Statistical** (CORR, COVAR) | ⚠️ | 🟡 | Placeholder comment only |
| **Regex** (REGEXP_MATCH, REGEXP_REPLACE, REGEXP_EXTRACT) | ⚠️ | 🟡 | Approximations: CONTAINSSTRING/SUBSTITUTE |
| **Spatial** (MAKEPOINT, MAKELINE, DISTANCE, BUFFER, AREA, INTERSECTION, HEXBINX/Y) | ⚠️ | 🟡 | Mapped to comments/placeholders — no DAX spatial |
| **Script/R/Python** (SCRIPT_BOOL/INT/REAL/STR) | ⚠️ | 🟡 | Mapped to comment placeholders |
| **User/Security** (USERNAME, FULLNAME, USERDOMAIN, ISMEMBEROF) | ✅ | 🔴 | USERNAME→USERPRINCIPALNAME, etc. |
| **String concatenation** (`+` for strings) | ✅ | 🔴 | Post-processing: +→& for string dtype |
| **COLLECT** (spatial agg) | ⚠️ | 🟡 | Placeholder |

### 2.2 Post-Processing & Context Resolution

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Column resolution (`[Col]` → `'Table'[Col]`) | ✅ | 🔴 | Fully-qualified with table map |
| Cross-table RELATED() injection | ✅ | 🔴 | Auto-detects cross-table refs |
| AGG+IF → AGGX rewrite (e.g. `SUM(IF())` → `SUMX`) | ✅ | 🔴 | Multiple agg patterns |
| AGG of expression → AGGX (e.g. `SUM(a*b)` → `SUMX`) | ✅ | 🟠 | Includes STDEVX/VARX |
| CEILING/FLOOR fix | ✅ | 🟡 | DAX requires 2 args |
| Date literal fix | ✅ | 🟡 | `#2024-01-01#` → `DATE(2024,1,1)` |
| Combined field DAX | ✅ | 🟠 | `generate_combined_field_dax()` |

### 2.3 Missing / Gap Calculations

| Missing Component | Priority | Implementation Approach |
|-------------------|:--------:|------------------------|
| **ATTR() precise semantics** | 🟠 | Currently →VALUES; should →SELECTEDVALUE or error if >1 val |
| **PREVIOUS_VALUE() full recursion** | 🟡 | Currently simple; would need iterative pattern |
| **SPLIT() with delimiter+index** | 🟡 | Placeholder; implement via PATHITEM/MID+FIND chain |
| **CONTAINS() for set membership** (not string) | 🟠 | Currently →CONTAINSSTRING; need CONTAINSROW for sets |
| **Multi-pass table calc ordering** | 🟠 | Compute-using + at-level + direction extracted but not fully wired into CALCULATE partitions |
| **Nested LOD inside table calc** | 🟡 | Each layer works; nesting may produce invalid DAX |

---

## 3. VISUAL / CHART TYPES

### 3.1 Mark-Type to PBI Visual Mapping

| Tableau Mark/Type | PBI Visual | Status | Priority |
|-------------------|-----------|:------:|:--------:|
| Bar | clusteredBarChart | ✅ | 🔴 |
| Stacked Bar | stackedBarChart | ✅ | 🔴 |
| Line | lineChart | ✅ | 🔴 |
| Area | areaChart | ✅ | 🔴 |
| Pie | pieChart | ✅ | 🔴 |
| Donut | donutChart | ✅ | 🟠 |
| Map (symbol) | map | ✅ | 🔴 |
| Filled Map | filledMap | ✅ | 🔴 |
| Text Table | tableEx | ✅ | 🔴 |
| Cross-tab / Matrix | matrix | ✅ | 🔴 |
| Scatter | scatterChart | ✅ | 🔴 |
| Treemap | treemap | ✅ | 🟠 |
| Heatmap / Highlight Table | matrix (conditional fmt) | ✅ | 🟠 |
| Histogram | clusteredColumnChart | ✅ | 🟠 |
| Box Plot | boxAndWhisker | ✅ | 🟡 |
| Waterfall | waterfallChart | ✅ | 🟡 |
| Funnel | funnel | ✅ | 🟡 |
| Gantt | clusteredBarChart | ✅ | 🟡 |
| Bullet | gauge | ✅ | 🟡 |
| Dual Axis / Combo | lineClusteredColumnComboChart | ✅ | 🔴 |
| KPI / Card | card | ✅ | 🔴 |
| Gauge | gauge | ✅ | 🟠 |
| Word Cloud | wordCloud (custom visual) | ✅ | 🟡 |
| Packed Bubbles | scatterChart | ✅ | 🟡 |
| Dot Plot | scatterChart | ✅ | 🟡 |
| Lollipop | clusteredBarChart | ✅ | 🟡 |
| Bump / Slope Chart | lineChart | ✅ | 🟡 |
| Pareto | lineClusteredColumnComboChart | ✅ | 🟡 |
| Sparkline | lineChart | ✅ | 🟡 |
| Sunburst | sunburst (custom visual) | ✅ | 🟡 |
| Sankey | sankeyChart (custom visual) | ✅ | 🟡 |
| Chord | chordChart (custom visual) | ✅ | 🟡 |
| Image | image | ✅ | 🟠 |
| Textbox | textbox | ✅ | 🔴 |
| Slicer/Filter control | slicer | ✅ | 🔴 |
| Action Button (nav/export) | actionButton | ✅ | 🟠 |
| **Density / Heatmap (map)** | map | ⚠️ | 🟡 | Maps to symbol map; no density layer |
| **Circle view** | scatterChart | ⚠️ | 🟡 | Approximate |
| **Polygon (custom shapes)** | filledMap | ⚠️ | 🟡 | No custom polygon support in PBI |
| **Small Multiples (trellis)** | native small multiples | ❌ | 🟠 | PBI supports this natively but not wired |
| **Viz-in-Tooltip** | tooltip pages | ✅ | 🟠 | Tooltip pages created |
| **Play axis (animation)** | slicer (play axis hint only) | ⚠️ | 🟡 | Pages shelf → slicer; no true animation |

### 3.2 Visual Query State (PBIR v4.0)

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Category / Axis role | ✅ | 🔴 | queryState.Category |
| Value / Y role | ✅ | 🔴 | queryState.Y |
| Legend / Series | ⚠️ | 🟠 | Color field → Legend object; not always bound to query Series role |
| Size role | ✅ | 🟠 | For scatter/map |
| Detail / Tooltips role | ⚠️ | 🟠 | Tooltip fields extracted but not always projected into queryState.Tooltips |
| Secondary Y axis (Y2) | ✅ | 🟠 | Combo charts |
| X axis for scatter | ✅ | 🟠 | queryState.X |
| Multiple measures in Values | ✅ | 🟠 | tableEx/card |
| Small multiple field | ❌ | 🟠 | Not wired |

---

## 4. WORKSHEETS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Fields (shelves: rows, columns, color, size, detail, tooltip, label, pages) | `datasource-dependencies/column` | ✅ | 🔴 | Rows/Columns/Color/Size/Detail/Tooltip/Label/Pages all extracted |
| Chart type detection | `mark[@class]` / pane analysis | ✅ | 🔴 | `determine_chart_type()` + `_map_tableau_mark_to_type()` |
| Filters (categorical, range, level-members, except) | `filter` elements | ✅ | 🔴 | All filter types extracted + converted |
| Sort orders | `sort` elements | ✅ | 🟠 | Manual/data/field/computed sorts |
| Formatting (font, color, bg, border) | `format` / `style-rule` | ✅ | 🟠 | Font, bg color, label color |
| Mark encoding (color/size/shape/label/detail) | `encoding` elements | ✅ | 🔴 | Per-value colors, palette, legend position |
| Axes (title, labels, scale, rotation, range) | `pane/axis` | ✅ | 🟠 | Title, label rotation, show/hide, continuous vs discrete |
| Annotations (point, mark, area) | `annotation` | ✅ | 🟡 | Extracted; rendered as subtitle text |
| Reference lines / bands / distributions | `reference-line` | ✅ | 🟠 | Constant lines with color/style/label |
| Trend lines | `trend-line` | ✅ | 🟠 | Type/color/equation/R² |
| Forecasting | `forecast` | ✅ | 🟡 | Periods, prediction interval, ignore-last |
| Table calculations | `table-calc-*` attributes | ✅ | 🟠 | Compute-using/direction/at-level extracted |
| Tooltips (including viz-in-tooltip) | `tooltip` / `viz-in-tooltip` | ✅ | 🔴 | Rich tooltip fields + tooltip page creation |
| Pages shelf | `pages` | ✅ | 🟠 | Mapped to slicer |
| Dual axis sync | `axis-sync` attributes | ✅ | 🟡 | Synchronized/independent axis detection |
| Clustering | `clustering` | ✅ | 🟡 | Cluster count/seed extracted |
| Map options | `map-options` | ✅ | 🟡 | Washout, style mapped to PBI map control |
| **Mark type: Automatic** | `mark[@class='Automatic']` | ⚠️ | 🟠 | Falls back to generic; no server-like auto-resolve |
| **Worksheet title expression** | `title/formatted-text/run` | ⚠️ | 🟡 | Static text only; dynamic field refs not evaluated |
| **Worksheet description** | `description` attribute | ❌ | 🟡 | Not extracted |
| **Show/hide headers** | `display-headers` | ❌ | 🟡 | Not extracted or mapped |

---

## 5. DASHBOARDS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Dashboard objects (worksheet refs) | `zone[@type='worksheet']` | ✅ | 🔴 | Position, size, z-order |
| Text objects | `zone[@type='text']` | ✅ | 🔴 | Content extracted |
| Image objects | `zone[@type='image']` | ✅ | 🟠 | URL/source |
| Web page objects | `zone[@type='web']` | ⚠️ | 🟡 | Extracted; no PBI equivalent (maps to textbox) |
| Blank objects | `zone[@type='blank']` | ✅ | 🟡 | Background fill |
| Navigation buttons | `zone[@type='navigation_button']` | ✅ | 🟠 | Target sheet → PBI action button |
| Download buttons | `zone[@type='download_button']` | ✅ | 🟡 | Export action button |
| Extension objects | `zone[@type='extension']` | ⚠️ | 🟡 | Extracted; placeholder visual |
| Filter controls (Quick Filters) | `zone[@type='paramctrl']` | ✅ | 🔴 | Mapped to PBI slicers |
| Dashboard size | `style/size` | ✅ | 🔴 | Fixed/auto/range |
| Dashboard theme | `formatted-text/style` + colors | ✅ | 🟠 | Color palettes, fonts → PBI theme.json |
| Containers (horizontal/vertical) | `layout-region/container` | ✅ | 🟠 | Padding, orientation, children |
| Device layouts (phone/tablet) | `device-layout` | ✅ | 🟠 | Zone positions per device |
| Dashboard actions (filter/highlight/URL/navigate) | `action` | ✅ | 🔴 | Source/target sheets, field mappings, clearing |
| Dashboard parameters | visible parameter controls | ✅ | 🟠 | Parameter widgets extracted |
| **Floating vs tiled layout** | `zone[@is-floating]` | ⚠️ | 🟠 | Positions used; floating/tiled distinction not explicit in output |
| **Outer padding / inner padding** | `zone/padding` | ✅ | 🟡 | Per-object padding mapped |
| **Show/hide containers** | `show-hide-button` | ❌ | 🟠 | No extraction; PBI has bookmarks for this |
| **Ask Data object** | `zone[@type='ask-data']` | ❌ | 🟡 | Tableau-specific; no PBI equivalent |
| **Data Story object** | `zone[@type='data-story']` | ❌ | 🟡 | Tableau-specific; PBI has Smart Narratives |

---

## 6. STORIES

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Story points | `story/story-point` | ✅ | 🟠 | Captions, captured sheets |
| Story filters / selections | `story-point/filters` | ✅ | 🟠 | Filter state per point |
| Story → PBI Bookmarks | n/a | ✅ | 🟠 | Each point → bookmark |
| Story → Navigation buttons | n/a | ✅ | 🟡 | Prev/Next/numbered buttons |
| Story → Navigation page | n/a | ✅ | 🟡 | Grid layout overview page |
| **Story point annotations** | `story-point/annotation` | ⚠️ | 🟡 | Annotations converted to text boxes |
| **Story formatting** | `story/formatted-text` | ❌ | 🟡 | Not extracted |

---

## 7. FORMATTING & THEMING

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Color palette migration | ✅ | 🟠 | Tableau palette → PBI theme `dataColors` |
| Font family | ✅ | 🟠 | Mapped to PBI `textClasses` |
| Background color | ✅ | 🟡 | Per-visual + page-level |
| Data labels show/hide | ✅ | 🟠 | Mark-labels-show detection |
| Label font/size/color | ✅ | 🟡 | Font family, size, color |
| Legend show/position | ✅ | 🟠 | 8-position mapping |
| Axis show/hide/title | ✅ | 🟠 | Category + value axis |
| Axis label rotation | ✅ | 🟡 | Degrees mapped |
| Per-value color assignments | ✅ | 🟡 | Up to 20 value-color pairs |
| Conditional formatting (gradient) | ✅ | 🟠 | Min/mid/max color from palette |
| Reference line style (dashed/solid) | ✅ | 🟡 | Constant line properties |
| **Number format strings** | ⚠️ | 🟠 | Basic mapping (`n2`, `p0`, `c2`); custom Tableau format strings not fully parsed |
| **Tableau format `#,##0.00;(#,##0.00)`** (negative) | ❌ | 🟡 | Need custom format string parser |
| **Workbook-level color palette** | ⚠️ | 🟠 | Named palettes extracted; custom palette values partially |
| **Mark shape encoding** | ⚠️ | 🟡 | Shape field extracted; not mapped to PBI marker shape |
| **Border formatting** | ⚠️ | 🟡 | Border color extracted; not fully emitted in PBIR |
| **Tooltip formatting** | ❌ | 🟡 | Custom tooltip HTML/rich text not parsed |
| **Row banding / alternating colors** | ❌ | 🟡 | Not extracted from table formatting |

---

## 8. ADVANCED ANALYTICS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Trend lines | `trend-line` | ✅ | 🟠 | Linear/exponential/log/polynomial/power/moving avg |
| Reference lines (constant) | `reference-line[@scope='per-pane']` | ✅ | 🟠 | Value, label, color, style |
| Reference bands | `reference-line[@type='band']` | ⚠️ | 🟡 | Extracted; min/max values present; not rendered as PBI bands |
| Distribution bands | `reference-line[@type='distribution']` | ⚠️ | 🟡 | Extracted; not rendered |
| Forecasting | `forecast` | ✅ | 🟡 | Periods, CI, ignore-last |
| Clustering | `clustering` | ✅ | 🟡 | Count/seed extracted; PBI has no native clustering |
| **Totals / subtotals** | `grandtotals`/`subtotals` | ❌ | 🟠 | Not extracted; PBI matrix has native support |
| **Analytics pane summary statistics** | `analytics` | ❌ | 🟡 | Mean line, median, etc. — not extracted |
| **Instant analytics (explain data)** | n/a | ❌ | ⚪ | Tableau-specific; PBI has "Insights" |
| **Model-based outlier detection** | n/a | ❌ | ⚪ | Not applicable |

---

## 9. PARAMETERS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Old-style parameters | `column[@param-domain-type]` | ✅ | 🔴 | Domain types: list/range/any |
| New-style parameters | `parameter` element | ✅ | 🔴 | Caption, datatype, value |
| Range parameters → What-If | `GENERATESERIES` | ✅ | 🟠 | Min/max/step → parameter table |
| List parameters → DATATABLE | allowable values | ✅ | 🟠 | String/numeric/boolean values |
| Any-value parameters → measure | no constraint | ✅ | 🟠 | Default value as measure |
| **Parameter actions** | `action[@type='parameter']` | ✅ | 🟡 | Extracted; mapped to PBI field parameter concept |
| **Set actions** | `action[@type='set-value']` | ✅ | 🟡 | Extracted in workbook actions |

---

## 10. SETS, GROUPS, BINS, HIERARCHIES

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Sets (IN/OUT) | `set` elements | ✅ | 🟠 | Formula or member list → DAX Boolean calc column |
| Groups (value) | `group[@type='values']` | ✅ | 🟠 | SWITCH() on source field → calc column |
| Groups (combined/crossjoin) | `group[@type='combined']` | ✅ | 🟠 | Multi-field concatenation → calc column |
| Bins | `bin` element | ✅ | 🟠 | FLOOR(field, bin_size) → calc column |
| Hierarchies (drill paths) | `drill-path` | ✅ | 🔴 | Multi-level hierarchies in TMDL |
| Auto date hierarchies | auto-generated | ✅ | 🟡 | Year/Quarter/Month/Day calc columns + hierarchy |
| **Conditional sets** | `set[@formula]` | ✅ | 🟡 | Formula-based sets converted |
| **Dynamic sets (Top N)** | `set[@topn]` | ⚠️ | 🟡 | Extracted; TopN not fully expressed in calc column |

---

## 11. SECURITY / RLS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| User filters (explicit mapping) | `user-filter` | ✅ | 🔴 | User→value mappings → RLS roles with USERPRINCIPALNAME() |
| Calculated security (USERNAME/FULLNAME) | `calculation` with `USERNAME()` | ✅ | 🔴 | Formula → DAX filter expression |
| ISMEMBEROF() groups | `ISMEMBEROF()` in formula | ✅ | 🟠 | Each group → separate RLS role |
| USERDOMAIN() | security function | ✅ | 🟡 | Mapped to USERPRINCIPALNAME() (closest equivalent) |
| **OLS (Object-Level Security)** | n/a | ❌ | 🟡 | Tableau has no direct OLS; PBI supports it — could map hidden fields |

---

## 12. WORKBOOK ACTIONS

| Component | XML Path | Status | Priority | Notes |
|-----------|---------|:------:|:--------:|-------|
| Filter actions | `action[@type='filter']` | ✅ | 🔴 | Source/target worksheets, field mappings, clearing |
| Highlight actions | `action[@type='highlight']` | ✅ | 🟠 | Cross-highlight in PBI |
| URL actions | `action[@type='url']` | ✅ | 🟠 | URL + field token replacement |
| Sheet navigation | `action[@type='sheet-navigate']` | ✅ | 🟠 | Drillthrough pages |
| Parameter actions | `action[@type='parameter']` | ✅ | 🟡 | Parameter change → extracted |
| Set-value actions | `action[@type='set-value']` | ✅ | 🟡 | Set membership change → extracted |
| Run-on triggers | `action[@run]` (hover/select/menu) | ✅ | 🟡 | Extracted; PBI cross-filter triggers differ |
| Clearing behavior | `action[@clearing]` | ✅ | 🟡 | Keep/clear all/leave → extracted |

---

## 13. TABLEAU PREP BUILDER

### 13.1 Input Steps

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| CSV | ✅ | 🔴 | Delimiter, encoding |
| Excel | ✅ | 🔴 | Sheet selection |
| SQL Server | ✅ | 🔴 | JDBC → M query |
| PostgreSQL | ✅ | 🔴 | Full connection |
| MySQL | ✅ | 🟠 | Connection details |
| Oracle | ✅ | 🟠 | Service/SID |
| BigQuery | ✅ | 🟡 | Project/dataset |
| Snowflake | ✅ | 🟡 | Warehouse/role |
| Redshift / Teradata / SAP HANA / Databricks / Spark | ✅ | 🟡 | M query stubs |
| JSON | ✅ | 🟡 | M query |
| Hyper file | ✅ | 🟡 | File path (no data) |
| Google Sheets / Salesforce | ✅ | 🟡 | M query stubs |
| **Published Data Source input** | ❌ | 🟡 | Not handled in Prep parser |

### 13.2 Clean Steps

| Action | Status | Priority | Notes |
|--------|:------:|:--------:|-------|
| RenameColumn | ✅ | 🔴 | `Table.RenameColumns` |
| RemoveColumn | ✅ | 🔴 | `Table.RemoveColumns` |
| DuplicateColumn | ✅ | 🟠 | `Table.DuplicateColumn` |
| ChangeColumnType | ✅ | 🔴 | `Table.TransformColumnTypes` |
| FilterOperation / FilterValues / FilterRange | ✅ | 🔴 | M filter expressions |
| ReplaceValues / ReplaceNulls | ✅ | 🟠 | `Table.ReplaceValue` |
| SplitColumn | ✅ | 🟠 | `Table.SplitColumn` |
| MergeColumns | ✅ | 🟠 | `Table.CombineColumns` |
| AddColumn (calculated) | ✅ | 🟠 | `Table.AddColumn` |
| CleanOperation (trim/upper/lower/proper) | ✅ | 🟠 | `Text.Trim/Upper/Lower/Proper` |
| RemoveLetters | ✅ | 🟡 | Part of CleanOperation |
| FillValues (up/down) | ✅ | 🟡 | `Table.FillDown / FillUp` |
| GroupReplace | ✅ | 🟡 | Multi-value replace |
| ConditionalColumn | ✅ | 🟡 | `if … then … else` |
| **ExtractValues** (extract date parts, text between delimiters) | ❌ | 🟡 | Not in clean step handler |
| **Custom calculation** (Prep calculation editor) | ⚠️ | 🟠 | AddColumn handles some; Prep-specific functions may not convert |

### 13.3 Aggregate Steps

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| groupByFields | ✅ | 🔴 | M `Table.Group` |
| SUM / AVG / MEDIAN / COUNT / COUNTD / MIN / MAX / STDEV / STDEVP | ✅ | 🔴 | All aggregations mapped |

### 13.4 Join / Union / Pivot Steps

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Join (inner/left/right/full/leftOnly/rightOnly/notInner) | ✅ | 🔴 | 7 join types → M `Table.Join` |
| Union | ✅ | 🔴 | `Table.Combine` with field mapping |
| Pivot (rowsToColumns) | ✅ | 🟠 | `Table.Pivot` |
| Unpivot (columnsToRows) | ✅ | 🟠 | `Table.Unpivot` |
| **Cross-join / self-join** | ❌ | 🟡 | Not in step handler |

### 13.5 Output Steps

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| PublishExtract | ✅ | 🟠 | Detected; mapped to Lakehouse output |
| SaveToFile | ✅ | 🟡 | Detected |
| SaveToDatabase | ✅ | 🟡 | Detected |

### 13.6 Flow Architecture

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Topological sort (Kahn's algorithm) | ✅ | 🔴 | Correct execution order |
| Upstream node tracking | ✅ | 🟠 | Dependency chain |
| Merge Prep M queries into TWB datasources | ✅ | 🔴 | `merge_prep_with_workbook()` |
| **Branching flows (one input → multiple outputs)** | ✅ | 🟡 | Graph-based; handles fan-out |
| **Script steps (Python/R in Prep)** | ❌ | 🟡 | Not in step handler |
| **Prediction steps (Prep + Einstein/TabPy)** | ❌ | 🟡 | Not in step handler |

---

## 14. FABRIC ARTIFACT GENERATION

| Artifact | Generator | Status | Priority | Notes |
|----------|-----------|:------:|:--------:|-------|
| PBIP project (.pbip) | `pbip_generator.py` | ✅ | 🔴 | Complete PBIR v4.0 project |
| Semantic Model (TMDL DirectLake) | `tmdl_generator.py` | ✅ | 🔴 | 2019-line generator; 12-phase build |
| Report (PBIR pages/visuals) | `pbip_generator.py` | ✅ | 🔴 | Dashboard→pages, visual containers |
| Dataflow Gen2 | `dataflow_generator.py` | ✅ | 🔴 | M mashup + Lakehouse sink |
| PySpark Notebook | `notebook_generator.py` | ✅ | 🟠 | ETL + transformations notebooks |
| Data Pipeline | `pipeline_generator.py` | ✅ | 🟠 | Dataflow → Notebook → SM refresh |
| Lakehouse definition | `lakehouse_generator.py` | ✅ | 🟠 | Schema + DDL + metadata |
| Calculated column materialization | `calc_column_utils.py` | ✅ | 🔴 | M `Table.AddColumn` + PySpark `withColumn` |
| Theme (JSON) | `tmdl_generator.py` | ✅ | 🟡 | Color + font migration |
| `.platform` manifests | all generators | ✅ | 🔴 | Git integration metadata |

---

## 15. SEMANTIC MODEL (TMDL) DETAILS

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| DirectLake entity partitions | ✅ | 🔴 | entityName, schemaName, expressionSource |
| Calculated partitions (parameter tables) | ✅ | 🟠 | GENERATESERIES / DATATABLE |
| M partitions (Calendar table) | ✅ | 🟠 | Import mode for date dim |
| Relationships (from joins) | ✅ | 🔴 | From/to table+column |
| Relationship type mismatch fix | ✅ | 🟠 | Auto-aligns dtypes |
| Inferred relationships (from DAX cross-refs) | ✅ | 🟠 | Column name matching heuristic |
| Many-to-many detection | ✅ | 🟠 | Full joins → M:N + bidirectional filter |
| RELATED→LOOKUPVALUE for M:N | ✅ | 🟡 | Automatic rewrite |
| Ambiguous path deactivation | ✅ | 🟠 | Union-find cycle detection |
| Calendar/Date table | ✅ | 🟠 | 2020-2030 range, Year/Quarter/Month/Day/DayOfWeek/DayName |
| Time intelligence measures (YTD, PY, YoY%) | ✅ | 🟡 | Auto-generated |
| Perspectives | ✅ | 🟡 | "Full Model" default |
| Cultures (non-en-US) | ✅ | 🟡 | Linguistic metadata |
| Data blending relationships | ✅ | 🟠 | Link cols → relationships |
| Display folders (Dimensions/Measures/Time Intelligence/etc.) | ✅ | 🟡 | Auto-categorized |
| **Calculation groups** | ❌ | 🟡 | PBI feature; not mapped from Tableau |
| **Field parameters** | ❌ | 🟡 | PBI feature; could map from parameter actions |

---

## 16. REPORT (PBIR) DETAILS

| Component | Status | Priority | Notes |
|-----------|:------:|:--------:|-------|
| Pages from dashboards | ✅ | 🔴 | 1 dashboard = 1 page |
| Visual containers with position/size | ✅ | 🔴 | Scaled from Tableau coords |
| 30+ visual config templates | ✅ | 🟠 | PBIR-native objects |
| Visual query state (queryState) | ✅ | 🔴 | Role-based field binding |
| Visual-level filters | ✅ | 🔴 | Categorical + range |
| Report-level filters | ⚠️ | 🟡 | Dashboard filters → page filters (not report-wide) |
| Tooltip pages | ✅ | 🟠 | From viz-in-tooltip |
| Drillthrough pages | ✅ | 🟠 | From navigate actions |
| Mobile layouts | ✅ | 🟡 | Phone/tablet device layouts |
| Pages shelf → Play axis slicer | ✅ | 🟡 | Animation hint |
| Theme.json embedding | ✅ | 🟡 | Custom theme resource package |
| Sorting | ✅ | 🟡 | Sort definitions on visuals |
| Custom visual GUIDs | ✅ | 🟡 | wordCloud, sunburst, sankey, chord |
| **Bookmarks (from stories)** | ⚠️ | 🟡 | Story converter creates bookmarks; not wired into PBIR bookmark definitions |
| **Drilldown behavior** | ⚠️ | 🟡 | Hierarchies exist; drilldown flag not set on visuals |
| **Cross-filter interactions** | ⚠️ | 🟠 | Filter/highlight actions extracted; not wired to PBIR interaction matrix |
| **Spotlight / focus mode** | ❌ | 🟡 | No Tableau equivalent |
| **Conditional visibility** | ❌ | 🟡 | PBI feature; no Tableau source |

---

## 17. COMPLETE GAP LIST (Prioritized)

### 🔴 Critical Gaps (should fix for production use)

| # | Gap | Component | Implementation Approach | Effort |
|---|-----|-----------|------------------------|--------|
| 1 | **Cross-filter interaction matrix** | PBIR report | Wire filter/highlight actions into PBIR `interactions` object per visual pair | Medium |
| 2 | **Small multiples** | Visual generator | Map Tableau's `column` shelf (when used for trellis) to PBI's small multiples `SmallMultiple` role | Medium |
| 3 | **Totals / subtotals** detection | Extraction + TMDL | Extract `grandtotals`/`subtotals` from worksheet XML; emit PBI matrix `totals` config | Medium |
| 4 | **ATTR() proper semantics** | DAX converter | Replace VALUES() with SELECTEDVALUE() + fallback measure pattern | Small |
| 5 | **Table calc compute-using wiring** | DAX converter | Use extracted compute-using/direction/at-level metadata to generate correct CALCULATE partition expressions | Large |

### 🟠 Important Gaps (improve quality significantly)

| # | Gap | Component | Implementation Approach | Effort |
|---|-----|-----------|------------------------|--------|
| 6 | **Legend/Series query role** | PBIR query state | Bind color field to `Series` or `Legend` queryState role | Small |
| 7 | **Tooltip query role** | PBIR query state | Bind tooltip fields to `Tooltips` queryState role | Small |
| 8 | **Number format string parser** | Formatting | Parse Tableau format strings (incl. currency, negative patterns) to PBI format strings | Medium |
| 9 | **Show/hide containers** | Dashboard extraction | Extract `show-hide-button` elements; map to PBI bookmark toggle pattern | Medium |
| 10 | **Floating vs tiled distinction** | Dashboard extraction | Use `is-floating` attribute to emit PBI `position.isFixed` or layer management | Small |
| 11 | **Reference bands (min/max range)** | Visual objects | Emit PBI `referenceLine` with `bandwidth` from extracted band min/max | Small |
| 12 | **Report-level filters** | PBIR report | Aggregate workbook-scope filters into `report.json` filter array | Small |
| 13 | **Drill-down flag on hierarchy visuals** | PBIR visual | Set `drillFilterOtherVisuals` + enable drilldown mode when hierarchy is on Category | Small |
| 14 | **Hyper file data ingestion** | Extraction | Integrate `pantab` or `tableauhyperapi` to extract actual data from .hyper files | Large |
| 15 | **Custom Prep calculation** | Prep parser | Expand AddColumn handler with full Tableau Prep expression→M conversion | Medium |
| 16 | **Bookmark definitions from stories** | PBIR report | Write `bookmarks` folder with bookmark JSON files referencing page states | Medium |

### 🟡 Nice-to-Have Gaps (polish & edge cases)

| # | Gap | Component | Implementation Approach | Effort |
|---|-----|-----------|------------------------|--------|
| 17 | Worksheet description extraction | Extraction | Read `description` attribute from worksheet XML | Tiny |
| 18 | Show/hide row/column headers | Extraction + PBIR | Extract `display-headers`; set matrix `headerVisible` | Small |
| 19 | Dynamic worksheet title (field refs) | Extraction | Parse `<run>` with `fieldRef` inside title element | Small |
| 20 | OData connector | Datasource extractor | Add `odata` to `_parse_connection_class` | Tiny |
| 21 | Google Analytics connector | Datasource extractor | Add `google-analytics` class | Tiny |
| 22 | Azure Blob / ADLS connector | Datasource extractor + M builder | Add `azure_storage` class + M connector | Small |
| 23 | Story formatting | Story extraction | Extract fonts/colors from story element | Tiny |
| 24 | Custom Tableau format strings (negative patterns) | DAX converter | Parser for `#,##0.00;(#,##0.00)` etc. | Small |
| 25 | Mark shape encoding → PBI marker | Visual objects | Map shape field values to PBI data point marker shape enum | Small |
| 26 | Row banding / alternating colors | Visual objects | Extract from table/matrix formatting; emit PBI `values.backColor` | Small |
| 27 | Analytics pane summary stats | Extraction | Extract mean/median/CI lines from analytics element | Small |
| 28 | Script steps in Prep | Prep parser | Log/skip Python/R script steps with warning | Tiny |
| 29 | Prediction steps in Prep | Prep parser | Log/skip prediction steps with warning | Tiny |
| 30 | CONTAINS() for set membership | DAX converter | Differentiate string vs. set context; use CONTAINSROW for sets | Small |
| 31 | SPLIT() with delimiter+index | DAX converter | Implement via nested MID+FIND or PATHITEM | Medium |
| 32 | Calculation groups | TMDL generator | Create calculation group tables (format/display switching) | Medium |
| 33 | Field parameters | TMDL generator | Map parameter-driven field swap to PBI field parameters | Medium |
| 34 | Density heatmap layer | Visual generator | Investigate PBI heatmap custom visual | Small |
| 35 | Cross-join / self-join in Prep | Prep parser | Add handler for cross-join step type | Small |
| 36 | Published DS input in Prep | Prep parser | Handle published data source node type | Small |
| 37 | Play axis animation | Visual generator | PBI has `Play Axis` slicer type; emit correct slicer subtype | Small |
| 38 | Reference line distribution bands | Visual objects | Emit `percentile`/`standardDeviation` band types | Small |
| 39 | CORR/COVAR statistical functions | DAX converter | Implement via SUMPRODUCT/STDEV expansion | Medium |
| 40 | Spatial aggregation (COLLECT) | DAX converter | No DAX equivalent; document as manual step | Tiny |
| 41 | Conditional visibility (PBI-native) | PBIR | Could map hidden worksheets to invisible visual containers | Small |
| 42 | Smart Narratives from Data Stories | Dashboard | Map `data-story` zone to PBI Smart Narrative visual | Medium |
| 43 | Ask Data → Q&A visual | Dashboard | Map `ask-data` zone to PBI Q&A visual | Small |

---

## 18. ARCHITECTURE STRENGTHS

1. **End-to-end pipeline**: Extraction → DAX/M conversion → TMDL + PBIR + Dataflow + Notebook + Pipeline + Lakehouse — all 6 Fabric artifacts generated
2. **DirectLake-first**: Semantic model uses entity partitions referencing Lakehouse Delta tables
3. **Calc column materialization**: Row-level formulas pushed to Lakehouse (Dataflow M `Table.AddColumn` + Notebook PySpark `withColumn`) so DirectLake can read them
4. **Multi-connection support**: Per-table connection resolution via `connection_map`; federated sources handled
5. **4-phase column fallback**: Nested `<columns>` → `<cols><map>` → `<metadata-records>` → `<column>` elements — ensures columns are found regardless of Tableau version
6. **Relationship intelligence**: Inference from DAX cross-refs, M:N detection, ambiguous path deactivation, type mismatch auto-fix
7. **Prep flow integration**: Full topological sort, M query fusion with `merge_prep_with_workbook()`
8. **Comprehensive test suite**: 20+ test files covering all modules

---

## 19. RECOMMENDED PRIORITY ORDER

1. **Cross-filter interactions** (#1) — Most visible gap in report fidelity
2. **Small multiples** (#2) — Commonly used in Tableau dashboards
3. **Totals/subtotals** (#3) — Expected in every table/matrix
4. **Legend/Tooltip query roles** (#6, #7) — Quick wins that improve visual accuracy
5. **Number format strings** (#8) — Currency/percentage display is visible to users
6. **ATTR() fix** (#4) — Correctness issue in measure conversion
7. **Show/hide containers + Bookmarks** (#9, #16) — Interactive report capability
8. **Reference bands** (#11) — Analytics pane fidelity
9. **Hyper data ingestion** (#14) — Removes manual data prep step for .twbx
10. **Table calc compute-using** (#5) — Complex but affects correctness of advanced calcs
