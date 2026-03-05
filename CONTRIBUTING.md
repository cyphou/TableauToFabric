# Contributing to Tableau to Microsoft Fabric Migration Tool

Thank you for your interest in contributing! This guide covers the development setup, coding standards, and contribution workflow.

---

## Development Setup

### Prerequisites

- Python 3.8+ (tested on 3.9–3.14)
- Power BI Desktop (December 2025+) for validating report output
- Microsoft Fabric workspace (for deployment testing)
- Git

### Getting Started

```bash
# Clone the repository
git clone https://github.com/cyphou/TableauToFabric.git
cd TableauToFabric

# Create a virtual environment
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS/Linux

# Install development dependencies
pip install -r requirements.txt

# Run tests
python -m unittest discover -s tests -v
```

### Project Structure

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for a detailed architecture overview.

```
tableau_export/   → Extraction layer (Tableau XML → JSON)
fabric_import/    → Generation layer (JSON → 6 Fabric artifacts)
conversion/       → High-level per-object converters
tests/            → Unit and integration tests (960+ tests)
docs/             → Documentation
examples/         → Sample Tableau workbooks
scripts/          → PowerShell and Python automation scripts
artifacts/        → Migration output
```

## Coding Standards

### No External Dependencies

The core migration pipeline uses **Python standard library only**. This is a strict design requirement:

- `xml.etree`, `json`, `os`, `re`, `uuid`, `zipfile`, `argparse`, `datetime`, `copy`, `logging`, `glob`
- Optional: `azure-identity` (deployment auth), `requests` (HTTP client), `pydantic-settings` (typed config)

If your change requires a new dependency, it must be behind a `try/except ImportError` guard.

### Style

- Follow PEP 8 with `flake8` (errors only: E9, F63, F7, F82)
- `ruff` is also configured in CI
- Maximum line length: 120 characters (soft limit)
- Use type hints where practical

### Naming Conventions

- Module-level functions for `tmdl_generator.py` (not a class)
- Class-based for `FabricPBIPGenerator`, `TableauExtractor`, `ArtifactValidator`
- Private methods prefixed with `_`
- Constants as `UPPER_SNAKE_CASE`

### DAX Formulas

- All DAX output must be single-line (multi-line formulas condensed)
- Apostrophes in table names escaped: `'Name'` → `''Name''`
- Use `SELECTEDVALUE()` for scalar references (not `VALUES()`)
- Cross-table refs use `RELATED()` for manyToOne, `LOOKUPVALUE()` for manyToMany

### Fabric Artifacts

All 6 artifact types must remain consistent:

1. **Lakehouse** — Delta table definitions
2. **Dataflow Gen2** — Power Query M ingestion
3. **Notebook** — PySpark ETL alternative
4. **Semantic Model** — TMDL with DirectLake mode
5. **Pipeline** — Orchestration of Dataflow/Notebook
6. **Power BI Report** — PBIR v4.0 interactive report

## Testing

### Running Tests

```bash
# All tests
python -m unittest discover -s tests -v

# Single test file
python -m unittest tests.test_dax_converter -v

# Single test method
python -m unittest tests.test_dax_converter.TestDaxConverter.test_isnull_to_isblank -v
```

### Test Structure

| File | Focus |
|------|-------|
| `test_dax_converter.py` | DAX formula conversion |
| `test_m_query_builder.py` | M query generation |
| `test_tmdl_generator.py` | TMDL semantic model |
| `test_visual_generator.py` | Visual container generation |
| `test_pbip_generator.py` | .pbip project structure |
| `test_conversion_modules.py` | Per-object conversion |
| `test_assessment.py` | Pre-migration assessment |
| `test_deployer.py` | Fabric deployer |
| `test_config.py` | Configuration validation |
| `test_lakehouse_generator.py` | Lakehouse generation |
| `test_dataflow_generator.py` | Dataflow generation |
| `test_notebook_generator.py` | Notebook generation |
| `test_pipeline_generator.py` | Pipeline generation |
| `test_end_to_end.py` | End-to-end migration |
| `test_migrate.py` | CLI and main flow |

### Writing Tests

- Use `unittest.TestCase` (not pytest)
- Tests write to `tempfile.mkdtemp()` and clean up in `tearDown`
- No mocking of file I/O — tests use real temp directories
- Each test should be independent and self-contained

## Contribution Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Follow the coding standards above
- Add tests for any new functionality
- Update documentation if adding new features

### 3. Run Tests

```bash
python -m unittest discover -s tests -v
```

All existing tests must pass. New features should include tests.

### 4. Validate Sample Migrations

```bash
# Migrate all samples and validate
python migrate.py --batch examples/tableau_samples/ --output-dir /tmp/test_output
```

### 5. Submit a Pull Request

- Provide a clear description of the change
- Reference any related issues
- Include before/after screenshots for visual changes

## Areas for Contribution

### High Priority

- Additional DAX conversion patterns
- Additional connector types for M queries
- Performance optimization for large workbooks
- Fabric REST API deployment improvements

### Medium Priority

- New visual type mappings
- Enhanced formatting migration
- Integration tests with Fabric workspace
- DirectLake optimization

### Low Priority

- API documentation generation (sphinx/pdoc)
- Property-based testing for formula conversion
- PBIR schema validation against Microsoft's published schemas

## Release Process

1. Update `CHANGELOG.md` with the new version
2. Run full test suite: `python -m unittest discover -s tests -v`
3. Validate all sample migrations
4. Create a Git tag: `git tag v1.x.x`
5. Push to main: `git push origin main --tags`
