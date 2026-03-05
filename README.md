# Tableau to Microsoft Fabric Migration

Automated migration tool for Tableau workbooks (`.twb`, `.twbx`) and Tableau Prep flows (`.tfl`, `.tflx`) to **Microsoft Fabric** artifacts  Lakehouse, Dataflow Gen2, Notebook, Semantic Model, Pipeline, and Power BI Report.

## Features

- **Full extraction** of 16 object types (datasources, calculations, parameters, filters, dashboards, stories, etc.)
- **6 Fabric artifact types**: Lakehouse, Dataflow Gen2, Notebook, Semantic Model (DirectLake), Pipeline, Power BI Report
- **~130 DAX conversion points**  LOD, table calcs, IF/THEN/END, ISNULL, CONTAINS, security, stats ([reference](docs/TABLEAU_TO_DAX_REFERENCE.md))
- **60+ visual type mappings**  Tableau marks  Power BI visuals
- **31 connector types** in Power Query M + **40+ transformation generators** ([reference](docs/TABLEAU_TO_POWERQUERY_REFERENCE.md))
- **165 Tableau Prep  Power Query M** operation mappings ([reference](docs/TABLEAU_PREP_TO_POWERQUERY_REFERENCE.md))
- **Calculated column materialisation** in Lakehouse Delta tables via Dataflow Gen2 (M) and Notebook (PySpark) ([guide](docs/CALCULATED_COLUMNS_GUIDE.md))
- **DirectLake mode**  Semantic Model reads directly from Lakehouse Delta tables
- **Cross-table references**, **RLS migration**, **parameter  What-If tables**
- **Pre-migration assessment** (`--assess`) with 8-category readiness scoring
- **Auto ETL selection** (`--auto`)  picks Dataflow, Notebook, or both based on complexity
- **Batch migration**, **artifact selection**, **structured logging**
- **1017 tests**, 0 failures

## Quick Start

```bash
# One-command migration
python migrate.py your_workbook.twbx

# Pre-migration assessment
python migrate.py your_workbook.twbx --assess

# Auto ETL selection
python migrate.py your_workbook.twbx --auto

# Specific artifacts only
python migrate.py your_workbook.twb --artifacts lakehouse semanticmodel pbi

# Batch migration
python migrate.py "path/to/folder/" -o output/

# Dry run (preview without generating)
python migrate.py your_workbook.twbx --dry-run
```

### Prerequisites

- Python 3.8+
- No external dependencies for core migration (Python standard library only)
- Optional for deployment: PowerShell 5.1+ with `Az` module

### CLI Options

| Flag | Description |
|------|-------------|
| `-o` / `--output-dir DIR` | Custom output directory (default: `artifacts/fabric_projects/`) |
| `--artifacts TYPE [...]` | Artifact types to generate (default: all 6) |
| `--assess` | Run pre-migration assessment only |
| `--auto` | Auto-select Dataflow vs Notebook vs both |
| `--dry-run` | Preview extraction without generating artifacts |
| `--calendar-start YEAR` | Calendar table start year (default: 2020) |
| `--calendar-end YEAR` | Calendar table end year (default: 2030) |
| `--culture LOCALE` | TMDL culture setting (default: `en-US`) |
| `--verbose` / `-v` | Enable DEBUG logging |
| `--log-file FILE` | Write logs to a file |

Available artifact types: `lakehouse`, `dataflow`, `notebook`, `semanticmodel`, `pipeline`, `pbi`.

## Output Structure

```
[ReportName]/
 [ReportName].Lakehouse/           # Delta table DDL + metadata
 [ReportName].Dataflow/            # Power Query M mashup + queries
 [ReportName].Notebook/            # PySpark ETL notebooks
 [ReportName].SemanticModel/       # DirectLake TMDL model
 [ReportName].Pipeline/            # Orchestration: DataflowNotebookRefresh
 [ReportName].pbip                 # .pbip project + PBIR v4.0 visuals
```

See [Fabric Project Guide](docs/FABRIC_PROJECT_GUIDE.md) for the full directory structure.

## Architecture

```
 ┌─────────────────┐      ┌──────────────────────┐      ┌──────────────────┐      ┌──────────────────────┐
 │     INPUT       │      │   STEP 1: EXTRACT    │      │   INTERMEDIATE   │      │   STEP 2: GENERATE   │
 │                 │      │                      │      │                  │      │                      │
 │  .twb / .twbx   ├─────>│  tableau_export/     ├─────>│  16 JSON files   ├─────>│  fabric_import/      │
 │  .tfl / .tflx   │      │                      │      │                  │      │                      │
 │                 │      │  - XML parser        │      │  worksheets      │      │  6 artifact types:   │
 │                 │      │  - DAX converter     │      │  datasources     │      │  - Lakehouse         │
 │                 │      │  - M query builder   │      │  calculations    │      │  - Dataflow Gen2     │
 │                 │      │  - Prep flow parser  │      │  parameters      │      │  - Notebook          │
 │                 │      │                      │      │  filters  ...    │      │  - Semantic Model    │
 │                 │      │                      │      │                  │      │  - Pipeline          │
 │                 │      │                      │      │                  │      │  - Power BI Report   │
 └─────────────────┘      └──────────────────────┘      └──────────────────┘      └──────────────────────┘
```

### End-to-End Migration Flow

```
  ┌──────────┐    ┌──────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────┐    ┌──────────┐
  │ Lakehouse│    │ Dataflow  │    │  Notebook   │    │  Semantic   │    │   PBI    │    │ Pipeline │
  │  (DDL)   │    │  Gen2 (M) │    │  (PySpark)  │    │   Model     │    │  Report  │    │  (orch)  │
  └────┬─────┘    └─────┬─────┘    └──────┬──────┘    └──────┬──────┘    └────┬─────┘    └────┬─────┘
       │                │                 │                   │                │               │
       │   Delta table  │  Ingest data    │  Transform &      │  DirectLake    │  Visuals &    │  Run
       │   definitions  │  into Lakehouse │  write to Delta   │  reads Delta   │  pages from   │  Dataflow
       │                │                 │                   │  tables        │  model        │  + Notebook
       v                v                 v                   v                v               v
  ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                              Microsoft Fabric Workspace                                        │
  └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

| Layer | Directory | Purpose |
|-------|-----------|---------|
| CLI | `migrate.py` | Entry point, batch mode, flags |
| Extraction | `tableau_export/` | TWB/TWBX parser, DAX converter, M query builder, Prep flow parser |
| Generation | `fabric_import/` | 6 artifact generators, assessment, strategy advisor, validator |
| Legacy | `conversion/` | Per-object converters (retained for compatibility) |
| Deployment | `scripts/` | PowerShell scripts for Fabric workspace + artifact deployment |
| Tests | `tests/` | 1017 tests (pytest) |
| Docs | `docs/` | Guides, references, checklists |
| Examples | `examples/` | Sample Tableau files (3 tiers + real-world) |

See [Architecture Guide](docs/ARCHITECTURE.md) for detailed diagrams and module descriptions.

## Fabric Deployment

Deploy generated artifacts to Microsoft Fabric using the PowerShell scripts:

```powershell
# 1. Generate artifacts
python migrate.py your_workbook.twbx -o output/

# 2. Create workspace
$ws = .\scripts\New-MigrationWorkspace.ps1 `
    -WorkspaceName "Tableau Migration" `
    -CapacityId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# 3. Deploy all artifacts (dependency order)
.\scripts\Deploy-TableauMigration.ps1 `
    -WorkspaceId $ws.WorkspaceId `
    -ProjectDir "output/YourReport"

# 4. Validate
.\scripts\Validate-Deployment.ps1 `
    -WorkspaceId $ws.WorkspaceId `
    -ProjectDir "output/YourReport"
```

Deployment order: Lakehouse  Notebook  Dataflow  Semantic Model  Report  Pipeline.

See [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) for full details (dry-run, RunPipeline, skip options).

## Validation

```python
from fabric_import.validator import ArtifactValidator

result = ArtifactValidator.validate_project("artifacts/fabric_projects/MyReport")
print(result)  # {"valid": True, "files_checked": N, "errors": []}
```

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_dax_converter.py -v
```

## Examples

Sample Tableau files in `examples/tableau_samples/`:

| File | Complexity | Features |
|------|-----------|----------|
| `simple_report.twb` | Low | 1 datasource, 1 chart |
| `medium_report.twb` | Medium | Joins, parameters, 3 charts |
| `complex_report.twb` | High | 3 datasources, LOD, RLS, story |
| `real_world/*.twb` | Varied | Public repo files ([sources](examples/tableau_samples/real_world/SOURCES.md)) |

```bash
python migrate.py examples/tableau_samples/simple_report.twb
python migrate.py examples/tableau_samples/complex_report.twb --artifacts lakehouse semanticmodel pbi
python migrate.py examples/tableau_samples/real_world/sample-superstore.twb -o output/
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System architecture, modules, data flow |
| [Deployment Guide](docs/DEPLOYMENT_GUIDE.md) | Fabric deployment scripts & options |
| [Migration Checklist](docs/MIGRATION_CHECKLIST.md) | Post-migration verification steps |
| [Fabric Project Guide](docs/FABRIC_PROJECT_GUIDE.md) | Output directory structure |
| [Calculated Columns Guide](docs/CALCULATED_COLUMNS_GUIDE.md) | Calc column materialisation |
| [Mapping Reference](docs/MAPPING_REFERENCE.md) | Tableau  Fabric mapping tables |
| [DAX Reference](docs/TABLEAU_TO_DAX_REFERENCE.md) | 172 Tableau  DAX functions |
| [Power Query Reference](docs/TABLEAU_TO_POWERQUERY_REFERENCE.md) | 108 Tableau  M properties |
| [Prep Flow Reference](docs/TABLEAU_PREP_TO_POWERQUERY_REFERENCE.md) | 165 Prep  M operations |
| [Version Compatibility](docs/TABLEAU_VERSION_COMPATIBILITY.md) | Tableau version support matrix |
| [Known Limitations](docs/KNOWN_LIMITATIONS.md) | Unsupported features & workarounds |
| [FAQ](docs/FAQ.md) | Frequently asked questions |
| [Contributing](CONTRIBUTING.md) | Development setup & contribution guide |
| [Changelog](CHANGELOG.md) | Version history |

## Known Limitations

- `MAKEPOINT()` (spatial) has no DAX equivalent
- `PREVIOUS_VALUE()` / `LOOKUP()` require manual conversion
- Data source paths must be reconfigured in Dataflow Gen2 after migration
- DirectLake mode requires all calculated columns to be physically materialised

See [Known Limitations](docs/KNOWN_LIMITATIONS.md) for the full list.

## License

MIT
