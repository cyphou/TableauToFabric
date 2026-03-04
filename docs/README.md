# Documentation

## Project Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TableauToFabric                                      │
│                                                                                 │
│  ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐  │
│  │  migrate.py      │────>│  tableau_export/  │────>│  fabric_import/          │  │
│  │  (CLI entry)     │     │  (16 JSON files)  │     │  (6 artifact generators) │  │
│  └─────────────────┘     └──────────────────┘     └──────────────────────────┘  │
│           │                   │                       │                          │
│           │              extraction layer          generation layer              │
│           │                   │                       │                          │
│           │               ┌───┘                       └───┐                     │
│           │               ▼                               ▼                     │
│           │   ┌──────────────────────┐   ┌──────────────────────────────┐       │
│           │   │  dax_converter.py     │   │  lakehouse_generator.py      │       │
│           │   │  m_query_builder.py   │   │  dataflow_generator.py       │       │
│           │   │  prep_flow_parser.py  │   │  notebook_generator.py       │       │
│           │   │  datasource_extractor │   │  semantic_model_generator.py  │       │
│           │   └──────────────────────┘   │  pipeline_generator.py        │       │
│           │                              │  pbip_generator.py            │       │
│           │                              │  tmdl_generator.py            │       │
│           │                              │  visual_generator.py          │       │
│           │                              └──────────────────────────────┘       │
│           │                                                                     │
│           └───────────── conversion/ ──────────────────────────────────────     │
│               ┌──────────────────────────────────────────────────────┐          │
│               │  worksheet_converter    dashboard_converter          │          │
│               │  datasource_converter   filter_converter             │          │
│               │  parameter_converter    calculation_converter         │          │
│               │  story_converter                                     │          │
│               └──────────────────────────────────────────────────────┘          │
│                                                                                 │
│  tests/  ─── 775 tests, 21 files, 0 failures                                  │
│  scripts/ ── PowerShell deployment (New-Workspace, Deploy, Validate)           │
│  docs/   ─── 7 guides + FAQ                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Guides

- [FABRIC_PROJECT_GUIDE.md](FABRIC_PROJECT_GUIDE.md) — Understanding Fabric artifacts and DirectLake projects
- [MAPPING_REFERENCE.md](MAPPING_REFERENCE.md) — Tableau ↔ Fabric mappings (60+ visuals, formulas, interactions)
- [CALCULATED_COLUMNS_GUIDE.md](CALCULATED_COLUMNS_GUIDE.md) — Calculated column materialisation in Lakehouse
- [TABLEAU_TO_DAX_REFERENCE.md](TABLEAU_TO_DAX_REFERENCE.md) — Complete 172-function Tableau → DAX mapping
- [TABLEAU_TO_POWERQUERY_REFERENCE.md](TABLEAU_TO_POWERQUERY_REFERENCE.md) — Complete 108-property Tableau → Power Query M mapping (25 connectors)
- [TABLEAU_PREP_TO_POWERQUERY_REFERENCE.md](TABLEAU_PREP_TO_POWERQUERY_REFERENCE.md) — Complete 165-operation Tableau Prep → Power Query M transformation mapping
- [FAQ.md](FAQ.md) — Frequently asked questions

## Quick Reference

### CLI Options

```bash
python migrate.py file.twbx                                        # All artifacts
python migrate.py file.twbx -o output/                              # Custom output
python migrate.py file.twbx --artifacts lakehouse notebook pipeline  # Specific artifacts
python migrate.py "path/to/folder/" -o output/                      # Batch migration
python migrate.py file.twbx --verbose --log-file m.log              # Verbose + log file
```

### Project Structure

| Module | Purpose |
|--------|---------|
| `migrate.py` | CLI entry point, batch support, logging |
| `tableau_export/` | Tableau XML parsing, DAX conversion, Power Query M generation |
| `fabric_import/` | Fabric artifact generation (6 types), validation, deployment |
| `tests/` | 775 tests (21 files), 0 failures |
| `artifacts/` | Generated Fabric artifacts |
| `docs/` | Documentation (7 guides + FAQ) |
