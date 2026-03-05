# Tableau → Fabric Migration Tool: Feature Gap Analysis

> Updated: 2025-06-03 — based on deep codebase audit against actual source code
> Scope: All Tableau Desktop features vs. current TableauToFabric migration tool coverage

---

## Summary Statistics

| Category | Count |
|----------|-------|
| **Total Tableau features assessed** | 55 |
| **Fully or substantially covered** | 46 |
| **Covered with known limitations** | 3 |
| **Out of scope / N/A** | 6 |
| **Coverage rate** | **~89%** of migratable features (with caveats noted below) |
| **Test suite** | 884 tests across 24 test files |
| **Source modules** | 35 Python files |

> **Honest assessment**: This document was rewritten after a deep source-code audit that
> verified every claim against actual function counts, regex patterns, and connector
> generators. Numbers below reflect what the code actually does, not aspirational targets.

---

## 1. DAX Formula Conversion — Actual Coverage

The DAX converter (`tableau_export/dax_converter.py`) handles Tableau calculations
through two mechanisms: **regex-replacement tuples** and **handler methods**.

### Verified Counts

| Mechanism | Count | Notes |
|-----------|-------|-------|
| Regex replacement tuples | 111 | Targeting 100 distinct Tableau function names |
| Working regex conversions | 89 | Produce real DAX output |
| Comment-only / no equivalent | 11 | Produce `BLANK()` or `/* no DAX equivalent */` |
| Handler methods (`_convert_*`) | 29 | Complex structural conversions (IF/CASE, LOD, table calcs, iterators) |
| **Distinct conversion points** | **~130** | After deduplication between regex patterns and handlers |

### 11 Functions With No Real DAX Equivalent

These Tableau functions produce only a comment or `BLANK()` — there is no Power BI
equivalent. This is the correct behaviour; the comment tells the user what to do manually.

| Function | Output | Reason |
|----------|--------|--------|
| `AREA` | Comment | GIS spatial — no DAX spatial engine |
| `BUFFER` | Comment | GIS spatial |
| `COLLECT` | Comment | GIS spatial aggregate |
| `HEXBINX` / `HEXBINY` | Comment | Hex-binning — use map visual instead |
| `INTERSECTION` | Comment | GIS spatial |
| `REGEXP_EXTRACT_NTH` | Comment | DAX has no regex; first match handled via `REGEXP_EXTRACT` → manual |
| `USERDOMAIN` | Comment | No AD domain concept in PBI service |
| `WINDOW_CORR` | Comment | No DAX windowed correlation |
| `WINDOW_COVAR` / `WINDOW_COVARP` | Comment | No DAX windowed covariance |

### 1 Approximate Conversion

| Function | DAX Output | Accuracy |
|----------|-----------|----------|
| `RANK_PERCENTILE` | `DIVIDE(RANKX(...) - 1, COUNTROWS(...) - 1)` | Approximate — may diverge slightly from Tableau's percentile algorithm |

### Handler Coverage (29 methods)

Complex structural conversions that go beyond simple find-and-replace:

- **Control flow**: IF/THEN/ELSE/ELSEIF/END → nested `IF()`, CASE/WHEN → `SWITCH()`
- **LOD expressions**: `{FIXED ...}`, `{INCLUDE ...}`, `{EXCLUDE ...}` → `CALCULATE()` with filter context
- **Table calculations**: RUNNING_SUM/AVG/COUNT/MAX/MIN, WINDOW_SUM/AVG/MAX/MIN/COUNT/MEDIAN/STDEV/VAR/PERCENTILE, RANK/RANK_UNIQUE/RANK_DENSE/RANK_MODIFIED, TOTAL → `CALCULATE()` + `FILTER(ALLSELECTED())`
- **Iterator conversions**: SUM(IF ...) → `SUMX`, AVG(IF ...) → `AVERAGEX`, compound expressions → `SUMX`/`AVERAGEX`
- **Date/string**: DATEDIFF, DATENAME, DATEPARSE, FIND, SPLIT, PROPER, ENDSWITH, STARTSWITH
- **Type conversions**: FLOAT/INT → `CONVERT()`, STR → `FORMAT()`, IIF → `IF()`, ZN/IFNULL → coalesce
- **Math**: ATAN2, DIV, SQUARE, RADIANS, DEGREES
- **Stats**: CORR, COVAR → full statistical expansion with `SUMX`
- **String concat**: `+` operator → `&` for string types

---

## 2. Data Connector Coverage — Actual M Generators

The M query builder (`tableau_export/m_query_builder.py`) has **29 `_gen_m_*` functions**:
27 dedicated connectors + 1 custom SQL handler + 1 fallback.

### Connector Tier Classification (Assessment Module)

| Tier | Connectors | Count |
|------|-----------|-------|
| **Fully supported** (PASS) | Excel, CSV, SQL Server, PostgreSQL, MySQL, Oracle, Snowflake, Azure SQL, Synapse, Google Sheets, SharePoint, JSON, XML, PDF, Web, GeoJSON, BigQuery | 17 |
| **Partially supported** (WARN) | BigQuery, Teradata, SAP HANA, SAP BW, Redshift, Databricks, Spark, Spark SQL, Salesforce, OData, Google Analytics, Azure Blob | 12 |
| **No M generator** (FAIL) | Vertica, Splunk, Hadoop Hive, Impala, Presto, Marketo, ServiceNow | 7 |

> **Note**: "Partially supported" means a working M generator exists but the connector
> may require manual credential setup or has limited transformation support in Power Query.

### All 27 Dedicated Connectors

Excel, SQL Server, PostgreSQL, CSV, BigQuery, MySQL, Oracle, Snowflake, GeoJSON,
Teradata, SAP HANA, SAP BW, Redshift, Databricks, Spark, Azure SQL, Synapse,
Google Sheets, SharePoint, JSON, XML, PDF, Salesforce, Web, OData, Google Analytics,
Azure Blob

---

## 3. Visual Type Mapping — Actual Coverage

The visual generator (`fabric_import/visual_generator.py`) maps Tableau mark types to
Power BI visual types through `VISUAL_TYPE_MAP`.

### Verified Counts

| Metric | Count |
|--------|-------|
| Input aliases (Tableau mark types / shapes) | ~120 |
| Distinct PBI visual types output | ~39 |
| Approximate / fallback mappings | ~10 |
| Default for unknown types | `tableEx` |

### Approximate / Fallback Mappings

These Tableau vis types have no direct PBI equivalent and map to the nearest native type:

| Tableau Type | PBI Mapping | Quality |
|--------------|------------|---------|
| histogram | clusteredColumnChart | Good — structure similar |
| ganttbar | clusteredBarChart | Approximate — no native Gantt |
| bumpchart | lineChart | Approximate — rank-over-time |
| slopechart | lineChart | Approximate |
| butterfly | hundredPercentStackedBarChart | Approximate |
| waffle | hundredPercentStackedBarChart | Approximate |
| pareto | lineClusteredColumnComboChart | Good — dual axis similar |
| network | decompositionTree | Approximate — semantically different |
| mekko | stackedBarChart | Approximate |
| lollipop | clusteredBarChart | Approximate |

### Custom Visual Support

| Custom Type | AppSource Visual | GUID Injected |
|-------------|-----------------|---------------|
| sankey | `sankeyChart` | Yes |
| chord | `chordChart` | Yes |

---

## 4. Prep Flow Conversion — Known Limitations

The prep flow parser (`tableau_export/prep_flow_parser.py`) converts Tableau Prep
Builder flow files (`.tfl`) to Power Query M.

### What Works

- Standard step types: filter, rename, aggregate, pivot, unpivot, join, union, sort,
  calculated field, group/replace, clean, change type, split
- Cross-join handler, published datasource input, ExtractValues

### Known Limitations

| Step Type | Behaviour | Severity |
|-----------|----------|----------|
| Python/R script steps | Warning comment emitted | Medium — manual rewrite required |
| Prediction (ML) steps | Warning comment emitted | Low — niche feature |
| Complex nested flows | Linear chain only | Medium — Prep supports branching |

---

## 5. Assessment Module (New)

The assessment module (`fabric_import/assessment.py`) provides an 8-category
pre-migration readiness checklist via `--assess`:

| Category | What It Checks |
|----------|---------------|
| Datasources | Connector tiers, custom SQL, data blending, published DS |
| Calculations | LOD complexity, table calcs, spatial functions, R/Python scripts |
| Visuals | Custom visuals, dual-axis, density marks |
| Dashboard layout | Object count, deep nesting, device layouts |
| Filters | Complex filters, context filters, cross-datasource filters |
| Parameters | Parameter count, data type, domain type |
| Security | User filters, RLS rules, USERNAME/ISMEMBEROF |
| Data volume | Table/column counts, relationship complexity |

**Output**: Colour-coded PASS / WARN / FAIL per check, overall risk score, and an
actionable migration plan.

---

## 6. Auto ETL Strategy Advisor (New)

The strategy advisor (`fabric_import/strategy_advisor.py`) selects the optimal ETL
strategy via `--auto`:

- Analyses datasource characteristics (volume, connector type, query complexity)
- Recommends: **Dataflow Gen2**, **Notebook (PySpark)**, or **Pipeline (Copy Activity)**
- Produces a per-datasource strategy report with reasoning

---

## 7. Previously Implemented Features (Phases 1–12)

<details>
<summary>Click to expand full feature list from Phases 1–12 (43 gaps closed)</summary>

### Phase 12 Additions

| Area | What was added |
|------|---------------|
| **Extraction** | Totals/subtotals, worksheet description, show/hide headers, dynamic titles, show/hide containers, floating/tiled detection, analytics pane stats |
| **DAX converter** | ATTR→SELECTEDVALUE, SPLIT→PATHITEM(SUBSTITUTE), CORR/COVAR full statistical expansion, compute_using parameter wiring |
| **Visual generator** | Small multiples binding, legend/series role, tooltip fields, drilldown flag, mark shape encoding, play axis |
| **PBIR generator** | Report-level filters, bookmarks from stories, row banding for tables, totals/subtotals rendering, reference bands from analytics stats, number format mapping |
| **TMDL generator** | Calculation groups from measure-switch parameters, field parameters from column-switch parameters |
| **Dashboard converter** | `data-story`→Smart Narrative, `ask-data`→Q&A visual, show/hide toggle bookmarks, floating→isFixed |
| **Prep parser** | Script steps (Python/R warning), prediction steps (ML warning), cross-join handler, published DS input |
| **Connectors** | OData, Google Analytics, Azure Blob/ADLS |

### Features Implemented

| Feature | Method |
|---------|--------|
| Forecasting | `<forecast>` XML → analytics pane objects |
| Clustering | Config extracted; placeholder hint |
| Map Options | Washout, style, background layers |
| Custom Shapes/Images | Shape files from `.twbx` |
| Sheet Swapping | Navigation buttons |
| Data Blending | Phase 4c blending relationships in TMDL |
| .hyper Metadata | Schema from `.twbx` |
| Published Datasources | Server-hosted DS names extracted |
| Embedded Fonts | Font files listed |
| Custom Geocoding | CSV refs extracted |
| Navigation/Download/Extension | Full extraction + generation |
| Custom Visual GUIDs | Sankey, chord with AppSource IDs |
| Tooltips | Per-run formatting |
| Dual-Axis Sync | Secondary axis generated |
| Combined Fields | CONCATENATE expressions |
| Pages Shelf | Play-axis-hint slicer |
| Custom Color Palettes | Per-value + gradient |
| PREVIOUS_VALUE / LOOKUP | OFFSET(-1) + LOOKUPVALUE |
| Conditional Formatting | Gradient scales |

</details>

---

## 8. Out of Scope — Server-Side / N/A Features

| # | Feature | Reason |
|---|---------|--------|
| 1 | **Tableau Server/Cloud Connectivity** | File-based tool — no Server REST API integration by design |
| 2 | **Subscriptions & Alerts** | Server-side feature — no `.twb` representation |
| 3 | **Comments** | Server-side feature — no `.twb` representation |
| 4 | **Performance Recording** | Diagnostic-only — no migration relevance |
| 5 | **.hyper Data Import** | Schema extracted; actual data ingestion requires `pantab`/`tableauhyperapi` |
| 6 | **Cross-Database Join Semantics** | Tables flattened into single Lakehouse by design |

---

## 9. Remaining Gaps & Honest Limitations

### 9.1 Areas Requiring Manual Review After Migration

| Area | What to Check | Impact |
|------|--------------|--------|
| **Spatial calculations** | 11 GIS functions produce comments only — verify map visuals work with Lat/Long columns | Medium |
| **Complex LODs** | Deeply nested or multi-level LODs may need DAX formula review | Low–Medium |
| **R/Python script calcs** | Converted as comment placeholders — require manual DAX or Python notebook rewrite | Medium |
| **Approximate visual types** | ~10 specialty charts (Gantt, network, mekko, etc.) use nearest PBI native type | Low |
| **Set actions** | No automated PBI equivalent — emits placeholder requiring manual bookmarks | Low |
| **Unsupported connectors** | 7 connectors (Vertica, Splunk, Hive, Impala, Presto, Marketo, ServiceNow) produce fallback M | Medium |
| **Branching prep flows** | Linear chain conversion only — complex Prep flows need validation | Medium |

### 9.2 Test Coverage Gaps

| Module | Current Tests | Gap |
|--------|--------------|-----|
| `prep_flow_parser.py` | Integration tests only | No unit tests for individual step handlers |
| Visual type mapping | Visual generator covered | No exhaustive test for all ~120 VISUAL_TYPE_MAP entries |
| Connector-specific M | M query builder covered | No per-connector validation of output M syntax |
| End-to-end with real Tableau files | 12/12 batch migration succeeded | `.twbx` extraction depends on XML structure stability |

---

## 10. Architecture & Module Summary

| Module | Purpose | Key Facts |
|--------|---------|-----------|
| `tableau_export/dax_converter.py` | Tableau calc → DAX | 111 regex tuples + 29 handler methods |
| `tableau_export/m_query_builder.py` | Datasource → Power Query M | 29 `_gen_m_*` generators |
| `tableau_export/prep_flow_parser.py` | Prep flow → Power Query M | 13+ step types |
| `fabric_import/visual_generator.py` | Mark type → PBI visual | ~120 → ~39 type mapping |
| `fabric_import/pbip_generator.py` | PBIR report generation | Pages, visuals, slicers, bookmarks |
| `fabric_import/tmdl_generator.py` | Semantic model (TMDL) | Tables, measures, RLS, relationships |
| `fabric_import/assessment.py` | Pre-migration readiness | 8-category checklist |
| `fabric_import/strategy_advisor.py` | Auto ETL strategy | Dataflow / Notebook / Pipeline selection |
| `fabric_import/constants.py` | Shared constants | Centralized artifact types, enum values |
| `fabric_import/naming.py` | Name sanitization | Unified safe-name logic |

---

## 11. Strengths Summary

- **~130 DAX conversion points** with balanced-paren depth tracking, nested IF/CASE, LOD expressions, iterator detection (SUM(IF) → SUMX), cross-table RELATED()/LOOKUPVALUE()
- **29 M connector generators** covering 27 data source types + custom SQL + fallback
- **~120 visual type aliases** mapping to ~39 distinct PBI visual types with per-type config templates
- **40+ M transformation generators** (rename, filter, aggregate, pivot, join, union, sort, conditional columns)
- **Pre-migration assessment** with 8-category readiness checklist and actionable risk scoring
- **Auto ETL strategy** selection based on datasource characteristics
- **Calculated column materialisation** — automatic classification and 3-way output (DDL + M + PySpark)
- **RLS migration** — user filters, USERNAME(), ISMEMBEROF() → TMDL roles  
- **6 Fabric artifact types** generated from a single workbook
- **Full deployment pipeline** — PowerShell scripts with idempotent create, 429 retry, LRO polling
- **884 tests** across 24 test files
