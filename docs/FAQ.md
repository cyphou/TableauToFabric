# Frequently Asked Questions

## Migration Workflow

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        MIGRATION WORKFLOW                                     │
│                                                                               │
│  ┌──────────┐     ┌───────────┐     ┌────────────┐     ┌──────────────────┐  │
│  │  PREPARE  │────>│  MIGRATE  │────>│  VALIDATE  │────>│  DEPLOY          │  │
│  └──────────┘     └───────────┘     └────────────┘     └──────────────────┘  │
│                                                                               │
│  1. Gather .twb/   2. Run           3. Check          4. Run deployment     │
│     .twbx files       migrate.py       generated         scripts or         │
│  2. Install           (CLI)            artifacts         Python deployer    │
│     Python 3.9+    3. Review logs      (validator)    5. Wire data sources  │
│  3. Clone repo     4. Optionally run                  6. Trigger pipeline   │
│                       --assess first                                        │
│                                        in PBI Desktop                       │
└───────────────────────────────────────────────────────────────────────────────┘
```

## General

### What files does the tool accept?
Tableau workbooks (`.twb` / `.twbx`) and, optionally, Tableau Prep flows (`.tfl` / `.tflx`).

### What does the tool produce?
Six Microsoft Fabric artifact types:

| Artifact | Description |
|----------|-------------|
| **Lakehouse** | Delta table DDL scripts + metadata |
| **Dataflow Gen2** | Power Query M mashup definitions |
| **Notebook** | PySpark ETL notebooks (`.ipynb`) |
| **Semantic Model** | Standalone DirectLake TMDL model |
| **Data Pipeline** | Orchestration: Dataflow → Notebook → Model refresh |
| **Power BI Report** | `.pbip` project with DirectLake TMDL + PBIR visuals |

### Do I need external Python packages?
No. The core migration uses **only the Python standard library**.
Optional packages for deployment:
- `azure-identity` + `requests` — Fabric REST API deployment
- `pydantic-settings` — `.env` configuration support

### Where does the output go?
By default, inside `artifacts/fabric_projects/<ReportName>/`.
Use `--output-dir` (or `-o`) to override.

---

## Migration

### How do I migrate a single workbook?

```bash
python migrate.py your_workbook.twbx
```

### How do I run a pre-migration assessment?

```bash
python migrate.py your_workbook.twbx --assess
```

This runs an **8-category readiness checklist** and produces a colour-coded console report + JSON file. No artifacts are generated — use this to evaluate migration complexity before committing.

Categories checked: datasource compatibility, calculation readiness, visual coverage, filter/parameter complexity, data model complexity, interactivity, packaging, and migration scope (with effort estimate).

Overall readiness: **GREEN** (ready), **YELLOW** (warnings to review), **RED** (blockers found).

### How do I let the tool pick the best ETL strategy?

```bash
python migrate.py your_workbook.twbx --auto
```

The `--auto` flag analyses workbook complexity across 7 signals and auto-selects **Dataflow Gen2** (simple PQ transforms), **PySpark Notebook** (heavy transforms), or **both** with Pipeline orchestration.

### How do I choose which artifacts to generate?

```bash
# Only Lakehouse + Notebook + Pipeline
python migrate.py workbook.twb --artifacts lakehouse notebook pipeline

# All artifacts (default)
python migrate.py workbook.twb
```

Available artifact names: `lakehouse`, `dataflow`, `notebook`, `semanticmodel`, `pipeline`, `pbi`.

### How do I batch-migrate a folder?

```bash
python migrate.py "path/to/folder/" -o output/
```

All `.twb` / `.twbx` files in the directory are migrated.

### What about Tableau formulas — are they converted?
Yes. **~130 Tableau functions** are converted to DAX for use in the Semantic Model (measures and calculated columns).
See the [DAX reference](TABLEAU_TO_DAX_REFERENCE.md) for the complete mapping.

### What DAX conversions are approximate or manual?
| Status | Count | Examples |
|--------|-------|---------|
| ✅ Automatic | 133 | `IF`, `SUM`, `CONTAINS`, `DATEDIFF`, LOD expressions |
| ⚠️ Approximate | 11 | `ATAN2`, `FINDNTH`, `PROPER`, `REGEXP_*` |
| 🔧 Manual | 12 | `SPLIT`, `CORR`, `LOOKUP`, `SCRIPT_*` |
| ❌ No equivalent | 15 | `HEXBINX`, `MAKEPOINT`, spatial functions |

### Are LOD expressions supported?
Yes. All three types:
- `{FIXED [dim] : AGG}` → `CALCULATE(AGG, ALLEXCEPT(...))`
- `{INCLUDE [dim] : AGG}` → `CALCULATE(AGG)`
- `{EXCLUDE [dim] : AGG}` → `CALCULATE(AGG, REMOVEFILTERS(...))`

### What about `SUM(IF ...)` patterns?
Converted to iterator functions automatically:
```
SUM(IF [status] != "Cancelled" THEN [qty] * [price] ELSE 0 END)
→ SUMX('Orders', IF('Orders'[status] != "Cancelled", 'Orders'[qty] * 'Orders'[price], 0))
```
This extends to `AVG(IF)→AVERAGEX`, `MIN(IF)→MINX`, `MAX(IF)→MAXX`, `COUNT(IF)→COUNTX`.

### Are Row-Level Security (RLS) rules migrated?
Yes. Tableau user filters and security functions are converted to TMDL RLS roles:
- User filter mappings → `USERPRINCIPALNAME()` role filters
- `USERNAME()` / `FULLNAME()` → `USERPRINCIPALNAME()`
- `ISMEMBEROF("group")` → separate RLS role (assign Azure AD group)

### Are Tableau parameters migrated?
Yes. Parameters become "What-If" tables:
- **Integer/real range** → `GENERATESERIES(min, max, step)` table + `SELECTEDVALUE` measure
- **String list** → `DATATABLE("Value", STRING, {{"val1"}, ...})` table + `SELECTEDVALUE` measure

### Do I need to reconfigure data sources?
Yes. After migration, you must connect the Dataflow Gen2 queries (or Notebook cells) to real data sources in the Fabric workspace. Tableau connection strings cannot be transferred directly.

---

## Technical

### What is DirectLake mode?
DirectLake is a Fabric-specific storage mode where the Semantic Model reads data directly from Delta tables in a Lakehouse, without importing data into the model. This gives near-Import performance with near-DirectQuery freshness.

### How are calculated columns handled?
Calculated columns are **materialised in the Lakehouse** — they exist as physical Delta table columns rather than DAX expressions in the Semantic Model. This is required by DirectLake mode.

The materialisation happens in four artifacts:

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ 1. LAKEHOUSE │──>│ 2. DATAFLOW  │──>│ 3. NOTEBOOK  │──>│ 4. SEMANTIC  │
│              │   │    GEN2      │   │              │   │    MODEL     │
│ DDL declares │   │ M query      │   │ PySpark      │   │ sourceColumn │
│ physical col │   │ computes val │   │ computes val │   │ (NOT DAX)    │
└──────────────┘   └──────────────┘   └──────────────┘   └──────────────┘
```

See the [Calculated Columns Guide](CALCULATED_COLUMNS_GUIDE.md) for details.

### What is the difference between measures and calculated columns?
| | Measure | Calculated Column |
|---|---------|------------------|
| **Definition** | DAX expression evaluated at query time | Physical value stored in Lakehouse |
| **TMDL** | `expression = DAX(...)` | `sourceColumn = ColumnName` |
| **Where computed** | Semantic Model engine | Dataflow Gen2 / Notebook |
| **DirectLake** | Fully supported | Requires physical column |

### How does `RELATED()` work across tables?
Cross-table references are detected automatically:
- **manyToOne**: `RELATED('OtherTable'[column])`
- **manyToMany**: `LOOKUPVALUE('OtherTable'[column], ...)`

### What is TMDL?
**Tabular Model Definition Language** — a text-based, human-readable format for defining Semantic Models. It replaces `model.bim` (JSON) and is compatible with Fabric Git integration.

Example:
```
table Orders
    column OrderID
        dataType: int64
        sourceColumn: OrderID
        summarizeBy: none

    measure TotalRevenue = SUM('Orders'[Revenue])

    partition Orders = entity
        entityName: Orders
        schemaName: dbo
        expressionSource: DatabaseQuery
```

### How are relationships represented?
In `relationships.tmdl`:
```
relationship rel_Orders_Customers
    fromColumn: Orders.CustomerID
    toColumn: Customers.CustomerID
    crossFilteringBehavior: oneDirection
```

---

## Fabric Artifacts

### What does the Lakehouse artifact contain?
- `lakehouse_definition.json` — Lakehouse item metadata
- `table_metadata.json` — table/column inventory for programmatic use
- `ddl/*.sql` — one SQL DDL file per table with Delta-compatible types

### What does the Dataflow Gen2 artifact contain?
- `dataflow_definition.json` — Dataflow item metadata with Lakehouse destination config
- `mashup.pq` — combined Power Query M document
- `queries/*.m` — one M query file per table

### What does the Notebook artifact contain?
- `etl_pipeline.ipynb` — PySpark cells for data ingestion into Delta tables
- `transformations.ipynb` — calculated column / transform logic
- Fabric notebook metadata (Lakehouse attachment, Spark pool config)

### What does the Pipeline artifact contain?
A Fabric Data Pipeline with three stages:
1. Dataflow Gen2 refresh (data ingestion)
2. Notebook execution (PySpark ETL → Delta tables)
3. Semantic Model refresh (DirectLake picks up fresh data)

### What does the Semantic Model artifact contain?
A standalone Fabric SemanticModel item with DirectLake mode:
- `model.tmdl` — model-level config
- `tables/*.tmdl` — table definitions with `partition = entity` (DirectLake)
- `relationships.tmdl` — relationship definitions
- `.platform` — Fabric Git integration manifest

### What does the PBI Report artifact contain?
A `.pbip` project compatible with Power BI Desktop:
- DirectLake TMDL semantic model (same structure as standalone)
- PBIR v4.0 visual definitions (60+ visual type mappings)
- ~130 DAX conversion points from Tableau calculated fields

---

## Validation & Deployment

### How do I validate the generated artifacts?

```python
from fabric_import.validator import ArtifactValidator

result = ArtifactValidator.validate_project("output/MyReport")
print(result)  # {"valid": True, "files_checked": N, "errors": []}
```

The validator checks:
- Lakehouse DDL files exist and are valid SQL
- Dataflow mashup and query files exist
- Notebook `.ipynb` are valid JSON
- SemanticModel TMDL files are well-formed
- Report `.pbir`, page and visual JSON files

### How do I deploy to a Fabric workspace?

```bash
# Set environment variables
export FABRIC_WORKSPACE_ID="your-workspace-guid"
export FABRIC_TENANT_ID="your-tenant-guid"
export FABRIC_CLIENT_ID="your-app-client-id"
export FABRIC_CLIENT_SECRET="your-app-secret"
export FABRIC_LAKEHOUSE_NAME="MyLakehouse"
```

```python
from fabric_import.deployer import FabricDeployer

deployer = FabricDeployer()
deployer.deploy_lakehouse(workspace_id, 'MyLakehouse', config)
deployer.deploy_dataflow(workspace_id, 'MyDataflow', config)
deployer.deploy_notebook(workspace_id, 'MyNotebook', config)
deployer.deploy_semantic_model(workspace_id, 'MyModel', config)
deployer.deploy_pipeline(workspace_id, 'MyPipeline', config)
deployer.deploy_report(workspace_id, 'MyReport', config)
```

### Can I batch-migrate and validate?

```bash
# Migrate entire folder
python migrate.py "path/to/folder/" -o output/

# Validate all generated artifacts
python -c "
from fabric_import.validator import ArtifactValidator
results = ArtifactValidator.validate_directory('output/')
for name, result in results.items():
    print(f'{name}: {\"OK\" if result[\"valid\"] else \"FAIL\"}')"
```

### How do I run the tests?

```bash
# All 961 tests
python -m pytest tests/ -v

# Specific module
python -m pytest tests/test_lakehouse_generator.py -v
python -m pytest tests/test_dataflow_generator.py -v
python -m pytest tests/test_calc_column_utils.py -v
```

| Test File | Coverage |
|-----------|---------|
| `test_lakehouse_generator.py` | Lakehouse DDL, table metadata, Delta types |
| `test_dataflow_generator.py` | Dataflow M queries, mashup, connectors |
| `test_notebook_generator.py` | PySpark notebook, ETL cells, transforms |
| `test_semantic_model_generator.py` | TMDL model, tables, DirectLake partitions |
| `test_pipeline_generator.py` | Pipeline stages, dependencies, retry |
| `test_import_to_fabric.py` | Orchestrator, artifact routing |
| `test_migrate.py` | CLI, batch, flags |
| `test_validator.py` | Artifact validation |
| `test_deployer.py` | Deployment orchestration |
| `test_auth.py` | Azure AD authentication |
| `test_client.py` | HTTP client, retry logic |
| `test_config.py` | Settings, environments |
| `test_utils.py` | Deployment report, cache |
| `test_calc_column_utils.py` | Calc column classification & conversion |
| `test_assessment.py` | Pre-migration assessment (8 categories, scoring) |
| `test_strategy_advisor.py` | Auto ETL strategy advisor |
