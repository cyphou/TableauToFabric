# Architecture — Tableau to Microsoft Fabric Migration Tool

## Pipeline Overview

The migration follows a **2-step pipeline**: Extraction → Generation.

```
              +-------------------------------+
              |           INPUT               |
              |  .twb / .twbx  (workbook)     |
              |  .tfl / .tflx  (Prep, opt.)   |
              +---------------+---------------+
                              |
                              v
              +-------------------------------+
              |    STEP 1 - EXTRACTION        |
              |   tableau_export/             |
              |                               |
              |  extract_tableau_data.py       |
              |    +-- datasource_extractor.py |
              |    +-- dax_converter.py        |
              |        172+ DAX conversions    |
              |    +-- m_query_builder.py      |
              |        26 connectors           |
              |        40+ transforms          |
              |    +-- prep_flow_parser.py     |
              +---------------+---------------+
                              |
                              v
              +-------------------------------+
              |      16 INTERMEDIATE JSON     |
              |                               |
              |  worksheets    calculations   |
              |  dashboards    parameters     |
              |  datasources   filters        |
              |  stories       actions        |
              |  sets/groups   bins            |
              |  hierarchies   sort_orders    |
              |  aliases       custom_sql     |
              |  user_filters  ds_filters     |
              +---------------+---------------+
                              |
                              v
              +-------------------------------+
              |    STEP 2 - GENERATION        |
              |   fabric_import/              |
              |                               |
              |  import_to_fabric.py          |
              |    +-- lakehouse_generator.py  |
              |        Lakehouse definition    |
              |    +-- dataflow_generator.py   |
              |        Dataflow Gen2 queries   |
              |    +-- notebook_generator.py   |
              |        PySpark Notebook        |
              |    +-- semantic_model_gen.py   |
              |        TMDL Semantic Model     |
              |    +-- pipeline_generator.py   |
              |        Data Pipeline           |
              |    +-- pbip_generator.py       |
              |        .pbip + PBIR report     |
              |    +-- tmdl_generator.py       |
              |        tables, columns,        |
              |        measures, RLS roles     |
              |    +-- visual_generator.py     |
              |        60+ visual types        |
              |    +-- validator.py            |
              |        JSON + TMDL + DAX       |
              +---------------+---------------+
                              |
                              v
              +-------------------------------+
              |           OUTPUT              |
              |                               |
              |  6 Fabric Artifacts:          |
              |  - Lakehouse definition       |
              |  - Dataflow Gen2              |
              |  - PySpark Notebook           |
              |  - Semantic Model (TMDL)      |
              |  - Data Pipeline              |
              |  - Power BI Report (.pbip)    |
              +-------------------------------+
```

## Module Responsibilities

### `tableau_export/` — Extraction Layer

| Module | Responsibility |
|--------|---------------|
| `extract_tableau_data.py` | Main orchestrator — parses TWB/TWBX XML, extracts 16+ object types |
| `datasource_extractor.py` | Datasource extraction (connections, tables, columns, calculations, relationships) |
| `dax_converter.py` | 172 Tableau → DAX formula conversions (LOD, table calcs, security, etc.) |
| `m_query_builder.py` | Power Query M generator (26 connector types + 40+ transformation generators) |
| `prep_flow_parser.py` | Tableau Prep flow parser (.tfl/.tflx → Power Query M) |

### `fabric_import/` — Generation Layer

| Module | Responsibility |
|--------|---------------|
| `import_to_fabric.py` | Generation pipeline orchestrator |
| `lakehouse_generator.py` | Lakehouse definition (Delta tables) |
| `dataflow_generator.py` | Dataflow Gen2 with M queries |
| `notebook_generator.py` | PySpark Notebook for complex ETL |
| `semantic_model_generator.py` | Standalone Semantic Model (DirectLake) |
| `pipeline_generator.py` | Data Pipeline (orchestration) |
| `pbip_generator.py` | .pbip project generator (PBIR v4.0 report, visuals, filters, bookmarks, slicers) |
| `tmdl_generator.py` | Unified semantic model generator (TMDL: tables, columns, measures, relationships) |
| `visual_generator.py` | Visual container generator (60+ visual types, data roles, config templates) |
| `validator.py` | Artifact validator (JSON, TMDL, DAX semantic validation) |
| `auth.py` | Azure AD authentication (Service Principal + Managed Identity) |
| `client.py` | Fabric REST API client with retry logic |
| `deployer.py` | Fabric deployment orchestrator |
| `strategy_advisor.py` | Auto ETL strategy advisor (Dataflow vs Notebook) |
| `assessment.py` | Pre-migration assessment and scoring |
| `config/settings.py` | Centralized config via env vars |
| `config/environments.py` | Per-environment configs (dev/staging/production) |

### `conversion/` — High-Level Converters

Per-object-type converters for worksheets, dashboards, calculations, parameters, etc.

### `scripts/` — Automation Scripts

PowerShell and Python scripts for CI/CD, deployment validation, and confidence reporting.

## Data Flow Detail

### Step 1: Extraction

```
Tableau XML → ET.parse → 16+ extract_*() methods → JSON files
                                    ↓
                        datasource_extractor.py
                          (connections, tables, columns, joins)
                                    ↓
                           dax_converter.py
                          (Tableau formula → DAX)
                                    ↓
                          m_query_builder.py
                         (connection → Power Query M)
```

### Step 2: Generation

```
JSON files → FabricImporter.import_all()
                        ↓
              ├── LakehouseGenerator.generate()     → Lakehouse definition
              ├── DataflowGenerator.generate()      → Dataflow Gen2 M queries
              ├── NotebookGenerator.generate()      → PySpark Notebook
              ├── SemanticModelGenerator.generate()  → TMDL files
              │   └── generate_tmdl()
              │       ├── model.tmdl                → model config, culture ref
              │       ├── database.tmdl             → compatibility level
              │       ├── relationships.tmdl        → table relationships
              │       ├── expressions.tmdl          → shared M expressions
              │       ├── roles.tmdl                → RLS roles
              │       ├── perspectives.tmdl         → auto-generated perspective
              │       ├── cultures/*.tmdl           → locale config
              │       ├── tables/*.tmdl             → tables, columns, measures
              │       └── diagramLayout.json        → empty (PBI auto-fills)
              ├── PipelineGenerator.generate()      → Data Pipeline
              └── FabricPBIPGenerator.generate()    → .pbip Report
                  ├── create_report_structure()     → .pbip, .platform, definition.pbir
                  ├── create_report_json()          → report.json
                  ├── create_theme()                → TableauMigrationTheme.json
                  ├── create_pages()                → pages/*/page.json + visuals/*/visual.json
                  │   ├── tooltip pages             → pageType: "Tooltip"
                  │   ├── mobile pages              → 320×568
                  │   └── drill-through pages       → pageType: "Drillthrough"
                  └── create_bookmarks()            → bookmarks from stories
```

## Output Structure

```
{ProjectName}/
├── {ProjectName}.pbip
├── .gitignore
├── migration_metadata.json
├── {ProjectName}.Lakehouse/
│   └── lakehouse_definition.json
├── {ProjectName}.Dataflow/
│   ├── dataflow_definition.json
│   └── queries/*.m
├── {ProjectName}.Notebook/
│   └── etl_pipeline.ipynb
├── {ProjectName}.Pipeline/
│   └── pipeline_definition.json
├── {ProjectName}.Report/
│   ├── definition.pbir
│   ├── report.json
│   └── definition/
│       ├── pages/
│       │   ├── pages.json
│       │   └── ReportSection*/
│       │       ├── page.json
│       │       └── visuals/
│       │           └── {visual_id}/
│       │               └── visual.json
│       └── RegisteredResources/
│           └── TableauMigrationTheme.json
└── {ProjectName}.SemanticModel/
    ├── .platform
    ├── definition.pbism
    └── definition/
        ├── model.tmdl
        ├── database.tmdl
        ├── relationships.tmdl
        ├── expressions.tmdl
        ├── roles.tmdl
        ├── perspectives.tmdl
        ├── diagramLayout.json
        ├── cultures/
        │   └── {locale}.tmdl
        └── tables/
            └── {TableName}.tmdl
```

## TMDL Generation Phases

The semantic model is built in 12 sequential phases:

1. **Table deduplication** — remove duplicate table definitions
2. **Main table identification** — identify primary table, build column metadata + DAX context
3. **Tables with columns** — emit table/column/measure/entity-partition TMDL (DirectLake mode)
4. **Relationships** — cross-datasource dedup, validation, type mismatch fixing
5. **Sets/Groups/Bins** — calculated columns for set membership, grouping, binning
6. **Auto date table** — M-partition calendar with Date Hierarchy (configurable range via `--calendar-start`/`--calendar-end`)
7. **Hierarchies** — drill-paths from Tableau
8. **What-If parameters** — GENERATESERIES/DATATABLE parameter tables
9. **RLS roles** — user filters → USERPRINCIPALNAME-based role expressions
9b. **Quick table calc measures** — pcto/running_sum/rank field detection
10. **Infer missing relationships** — from cross-table DAX references
10b. **Cardinality detection** — manyToOne vs manyToMany
10c. **RELATED→LOOKUPVALUE** — fix cross-table refs for M2M relationships
11. **Deactivate ambiguous paths** — Union-Find cycle detection
12. **Auto-generate perspectives** — "Full Model" perspective
