# Changelog

All notable changes to TableauToFabric are documented here.

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
