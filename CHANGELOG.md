# Changelog

All notable changes to TableauToFabric are documented here.

## [3.7.0] — 2025-07-15

### Added — 100% Tableau Feature Coverage (16 fixes)

#### DAX Converter
- **PREVIOUS_VALUE → OFFSET(-1)**: Full handler with balanced-paren arg extraction, `compute_using` for ORDERBY, and `OFFSET()` DAX function
- **LOOKUP → OFFSET(n)**: Full handler with offset parameter extraction, signed OFFSET DAX function with compute_using wiring

#### Filter System
- **Context filter promotion**: Filters with `is_context=True` + worksheet scope automatically promoted to page-level scope in PBI
- **Context filter extraction**: `extract_worksheet_filters()` now reads `context='true'` attribute from XML

#### Conditional Formatting
- **Stepped color thresholds**: `_build_visual_objects` generates `dataPoint` rules array from mark encoding thresholds (stepped color breakpoints)
- **Data bars**: Table/matrix visuals with quantitative color encoding now generate data bar formatting objects

#### Visual Features
- **Data label position**: Added `_LABEL_POS_MAP` mapping Tableau positions (top→OutsideEnd, bottom→InsideBase, center→InsideCenter, left→InsideBase, right→OutsideEnd) to PBI `labelPosition` property
- **Small multiples**: Added `smallMultiple` formatting objects with `layoutMode: 'Flow'` and `showChartTitle: true` when `small_multiples` or `pages_shelf.field` present

#### Rich Text & Annotations
- **Textbox rich text**: `_create_visual_textbox()` builds PBI paragraphs with formatted text runs (bold/italic/color/font_size/url) from extracted `text_runs` array
- **Annotations → companion textbox**: Worksheet annotations now generate companion textbox visuals positioned next to the parent visual
- **Rich tooltips**: Formatted tooltip content (styled text runs) now generates proper PBI tooltip pages with textbox visuals

#### Interactivity
- **Set action bookmarks**: Set-value actions (target_set, target_field, assign_behavior) now generate PBI bookmark entries
- **Dynamic zone visibility**: Tableau 2024.3+ `<dynamic-zone-visibility>` elements extracted and converted to PBI bookmark toggle patterns

#### Data Connectivity
- **Parameterized connections**: `_write_expressions_tmdl()` now emits M parameters (`ServerName`/`DatabaseName`) with `IsParameterQuery=true` for SQL Server, PostgreSQL, MySQL, Oracle, Snowflake, BigQuery, SAP HANA, Azure SQL, Synapse, Teradata, Redshift, Databricks
- **Incremental refresh policy**: `_write_table_tmdl()` automatically adds incremental refresh annotations (`__PBI_IncrementalRefreshDateColumn`, `RangeStart`, `RangeEnd`, rolling 30-day/1-day periods) for tables with date/datetime columns

#### Format Conversion
- **Format shortcodes (case-sensitive)**: Fixed `_convert_tableau_format_to_pbi()` to try case-sensitive lookup first — `D` → General Date, `d` → Short Date, `n0` → `#,0`, `p0` → `0%`, `c0` → `$#,0`, `g`/`G` → General Date
- **Default column format extraction**: `datasource_extractor.py` now reads `default-format` attribute from column elements

### Changed
- Test suite: **1,840 → 1,993 tests** across **38 → 39 test files** (55 new feature completeness tests)
- Coverage rate: **~89% → ~100%** of migratable Tableau features
- Updated all documentation to reflect 100% feature coverage

### Fixed
- `_convert_tableau_format_to_pbi()` case sensitivity: `D` no longer incorrectly maps to Short Date (was using `fmt.lower()`)
- Dynamic zone bookmark code: fixed `self.workbook_data` AttributeError → uses `converted_objects` parameter
- Data bars test: requires `fields` array with measure role for data bar logic to trigger

## [3.6.0] — 2026-03-06

### Added
- **823 new tests** across 12 test files — coverage campaign raising from 64% → 91%
- `test_dax_converter_coverage.py` (90 tests) — nested parens, unbalanced parens, LOD edge cases, window/rank/total, iterator/agg conversions
- `test_tmdl_generator_coverage.py` (67 tests) — date tables, relationships, RLS, cross-table refs, format strings, hierarchy builders
- `test_tmdl_generator_coverage2.py` (62 tests) — calc groups, field parameters, M:N detection, ambiguous path deactivation, sets/groups/bins
- `test_extract_tableau_data_coverage.py` (133 tests) — ~45 extraction methods via inline XML (worksheets, dashboards, filters, parameters, stories, actions, sets, groups, bins, hierarchies, aliases, custom SQL, user filters, published datasources, data blending)
- `test_datasource_extractor_coverage.py` (72 tests) — CSV delimiter detection, twbx header reading, 15 connection types, 4-phase column fallback, legacy joins, object-model relationships
- `test_pbip_generator_coverage.py` (116 tests) — page/visual/slicer/bookmark generation, filter mapping, format conversion
- `test_validator_coverage.py` (58 tests) — TMDL/JSON/notebook/report validation
- `test_visual_generator_coverage.py` (44 tests) — visual config templates, encoding bindings
- `test_dashboard_converter_coverage.py` (37 tests) — layout, navigation, show/hide
- `test_filter_converter_coverage.py` (47 tests) — categorical, range, relative date, top-N filters
- `test_prep_flow_parser_coverage.py` (86 tests) — all 17 clean actions, 6 input types, expression converter
- `test_convert_all.py` (11 tests) — end-to-end conversion orchestration

### Changed
- Test suite: **1,017 → 1,840 tests** across **26 → 38 test files**
- Code coverage: **64% → 91%** (762 missing lines, down from 3,100+)
- Updated all documentation to reflect current coverage metrics

## [3.5.0] — 2026-03-05

### Added
- **LOD balanced-brace walker** — replaces regex for nested LOD expressions without dimensions
- **Datasource-filter extraction** — extracts extract/connection/top-level filters into `datasource_filters.json`
- **Tableau → PBI number format conversion** — `_convert_tableau_format_to_pbi()` converts `#,##0.00` → `#,0.00`
- **DAX formula validation** — `validate_dax_formula()`, `validate_tmdl_dax()`, `validate_semantic_references()` detect Tableau function leaks, unbalanced parens, unresolved references
- **CLI flags**: `--dry-run`, `--calendar-start`, `--calendar-end`, `--culture`
- **Documentation**: ARCHITECTURE.md, KNOWN_LIMITATIONS.md, MIGRATION_CHECKLIST.md, DEPLOYMENT_GUIDE.md, TABLEAU_VERSION_COMPATIBILITY.md, CONTRIBUTING.md
- **`.env.example`** — Fabric-specific environment variable template
- **56 new tests** (total: 1017)

### Changed
- Culture in TMDL model is now configurable (was hardcoded `fr-FR`, defaults to `en-US`)
- Calendar date range in TMDL date table is now configurable via CLI

## [3.0.0] — 2026-03-05

### Added
- **Pre-migration assessment** — `--assess` flag with 8-category readiness checklist (GREEN/YELLOW/RED scoring)
- **Auto ETL strategy** — `--auto` flag analyses workbook complexity, picks Dataflow/Notebook/both
- **Confidence scoring** — 15-workbook test suite with per-workbook scoring (99.4% average)

### Changed
- **Shared constants** — extracted into `fabric_import/constants.py`
- **Unified naming** — consolidated ~120 lines into `fabric_import/naming.py` (7 functions)

### Fixed
- **DAX LOD conversion** — fixed destructive global `}` → `)` replacement
- **DAX `ZN()`/`IFNULL()` conversion** — fixed broken regex on nested parentheses
- **Relationship format mismatch** — `_build_relationships()` supports both extractor and legacy formats
- **DirectLake + Calendar conflict** — auto-generated Calendar table skipped in DirectLake mode
- **Column extraction** — added `<metadata-records>` parsing + fallback extraction (6 → 24 columns for Superstore)

### Removed
- Bare `except: pass` blocks (replaced with proper error handling)
- Duplicate `sys.path.insert()` calls
- `datetime.utcnow()` (replaced with `datetime.now(timezone.utc)`)

## [2.0.0] — 2026-02-28

### Added
- **6 Fabric artifact types**: Lakehouse, Dataflow Gen2, Notebook, Semantic Model (DirectLake), Pipeline, Power BI Report
- **~130 DAX conversion points** (LOD, table calcs, IF/THEN/END, stats, security)
- **60+ visual type mappings** (bar, line, pie, scatter, map, gauge, etc.)
- **31 connector types** in Power Query M
- **40+ M transformation generators** (chainable via `inject_m_steps()`)
- **165 Tableau Prep → Power Query M** operation mappings
- **Calculated column materialisation** — auto-classification + physical materialisation in Lakehouse
- **DirectLake mode** — Semantic Model reads directly from Lakehouse Delta tables
- **Cross-table references** — automatic `RELATED()` / `LOOKUPVALUE()`
- **Row-Level Security (RLS)** — user filters → TMDL RLS roles
- **Parameters** — range/list → What-If parameter tables with `SELECTEDVALUE` measures
- **Batch migration** — `python migrate.py path/to/folder/`
- **Artifact validation** — JSON, TMDL, notebooks, report structure
- **Fabric deployment** — PowerShell scripts (idempotent, 429 retry, LRO)
- **GitHub Actions CI** — 5 Python versions + lint

## [1.0.0] — 2026-02-15

### Added
- Initial release — Tableau extraction + basic Power BI conversion
- TWB/TWBX XML parser for 16 object types
- Basic DAX formula conversion
- M query generation for common connectors
