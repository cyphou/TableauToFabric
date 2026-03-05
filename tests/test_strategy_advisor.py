"""Tests for the ETL Strategy Advisor module."""

import pytest
from fabric_import.strategy_advisor import (
    recommend_etl_strategy,
    StrategyRecommendation,
    StrategySignal,
    print_recommendation,
)


# ── Helpers ─────────────────────────────────────────────────────────

def _make_extracted(
    *,
    conn_type='SQL Server',
    tables=None,
    columns_per_table=5,
    calculations=None,
    custom_sql=None,
    num_datasources=1,
):
    """Build a minimal ``extracted`` dict for testing."""
    if tables is None:
        tables = [{'name': 'Orders', 'columns': [{'name': f'col{i}'} for i in range(columns_per_table)]}]
    datasources = []
    for i in range(num_datasources):
        datasources.append({
            'name': f'DS_{i}',
            'connection': {'type': conn_type, 'details': {}},
            'tables': tables,
            'columns': [],
        })
    return {
        'datasources': datasources,
        'calculations': calculations or [],
        'custom_sql': custom_sql or [],
        'worksheets': [],
        'dashboards': [],
        'parameters': [],
        'filters': [],
    }


# ── Tests ───────────────────────────────────────────────────────────

class TestStrategyRecommendation:
    """StrategyRecommendation data class."""

    def test_artifacts_dataflow(self):
        rec = StrategyRecommendation(strategy='dataflow')
        assert 'dataflow' in rec.artifacts
        assert 'notebook' not in rec.artifacts
        assert 'lakehouse' in rec.artifacts

    def test_artifacts_notebook(self):
        rec = StrategyRecommendation(strategy='notebook')
        assert 'notebook' in rec.artifacts
        assert 'dataflow' not in rec.artifacts

    def test_artifacts_both(self):
        rec = StrategyRecommendation(strategy='both')
        assert 'dataflow' in rec.artifacts
        assert 'notebook' in rec.artifacts

    def test_base_artifacts_always_present(self):
        for s in ('dataflow', 'notebook', 'both'):
            rec = StrategyRecommendation(strategy=s)
            for a in ('lakehouse', 'semanticmodel', 'pipeline', 'pbi'):
                assert a in rec.artifacts


class TestSimpleWorkbook:
    """Simple workbook → should recommend Dataflow."""

    def test_simple_sql_server(self):
        extracted = _make_extracted(conn_type='SQL Server')
        rec = recommend_etl_strategy(extracted)
        assert rec.strategy == 'dataflow'
        assert rec.dataflow_score > rec.notebook_score

    def test_simple_excel(self):
        extracted = _make_extracted(conn_type='Excel')
        rec = recommend_etl_strategy(extracted)
        assert rec.strategy == 'dataflow'

    def test_simple_csv(self):
        extracted = _make_extracted(conn_type='CSV')
        rec = recommend_etl_strategy(extracted)
        assert rec.strategy == 'dataflow'


class TestComplexWorkbook:
    """Complex workbook → should recommend Notebook."""

    def test_bigquery_connector(self):
        extracted = _make_extracted(conn_type='BigQuery')
        rec = recommend_etl_strategy(extracted)
        # BigQuery signal detected as favouring notebook
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'connectors_complex' for s in signals_nb)

    def test_bigquery_with_many_tables(self):
        """BigQuery + many tables + many columns → notebook wins."""
        tables = [{'name': f'T{i}', 'columns': [{'name': f'c{j}'} for j in range(20)]} for i in range(8)]
        calcs = [
            {'name': 'lod', 'formula': '{FIXED [Dim] : SUM([Val])}',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(
            conn_type='BigQuery', tables=tables,
            calculations=calcs,
            custom_sql=[{'datasource': 'DS', 'sql': 'SELECT 1'}],
        )
        rec = recommend_etl_strategy(extracted)
        assert rec.strategy == 'notebook'

    def test_custom_sql_present(self):
        extracted = _make_extracted(
            custom_sql=[{'datasource': 'DS', 'sql': 'SELECT * FROM big_table'}],
        )
        rec = recommend_etl_strategy(extracted)
        # custom SQL signal detected as favouring notebook
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'custom_sql' for s in signals_nb)

    def test_custom_sql_with_complexity(self):
        """Custom SQL + LOD calcs → scores close → both."""
        calcs = [
            {'name': 'lod', 'formula': '{FIXED [Region] : SUM([Sales])}',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(
            custom_sql=[{'datasource': 'DS', 'sql': 'SELECT 1'}],
            calculations=calcs,
        )
        rec = recommend_etl_strategy(extracted)
        # Scores are close (DF=5 NB=4) → "both" is the correct recommendation
        assert rec.strategy == 'both'
        assert 'notebook' in rec.artifacts
        assert 'dataflow' in rec.artifacts

    def test_many_tables(self):
        tables = [{'name': f'Table_{i}', 'columns': [{'name': 'c1'}]} for i in range(10)]
        extracted = _make_extracted(tables=tables)
        rec = recommend_etl_strategy(extracted)
        # many_tables signal should favour notebook
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'many_tables' for s in signals_nb)

    def test_many_columns(self):
        tables = [{'name': 'Wide', 'columns': [{'name': f'c{i}'} for i in range(80)]}]
        extracted = _make_extracted(tables=tables)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'many_columns' for s in signals_nb)

    def test_lod_calculations(self):
        calcs = [
            {'name': 'lod_calc', 'formula': '{FIXED [Region] : SUM([Sales])}',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'complex_calcs' for s in signals_nb)

    def test_table_calcs(self):
        calcs = [
            {'name': 'running', 'formula': 'RUNNING_SUM(SUM([Sales]))',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'complex_calcs' for s in signals_nb)

    def test_window_calcs(self):
        calcs = [
            {'name': 'win', 'formula': 'WINDOW_AVG(SUM([Profit]), -3, 0)',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'complex_calcs' for s in signals_nb)

    def test_regex_calcs(self):
        calcs = [
            {'name': 'rx', 'formula': 'REGEXP_MATCH([Name], "^A.*")',
             'role': 'dimension', 'datatype': 'boolean'},
        ]
        extracted = _make_extracted(calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'complex_calcs' for s in signals_nb)

    def test_many_calc_columns(self):
        calcs = [
            {'name': f'calc_{i}', 'formula': f'[Col{i}] + 1',
             'role': 'dimension', 'datatype': 'integer'}
            for i in range(15)
        ]
        extracted = _make_extracted(calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'many_calc_cols' for s in signals_nb)

    def test_prep_flow(self):
        extracted = _make_extracted()
        rec = recommend_etl_strategy(extracted, prep_flow=True)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'prep_flow' for s in signals_nb)

    def test_oracle_connector(self):
        extracted = _make_extracted(conn_type='Oracle')
        rec = recommend_etl_strategy(extracted)
        # Oracle detected as complex connector
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'connectors_complex' for s in signals_nb)

    def test_snowflake_connector(self):
        extracted = _make_extracted(conn_type='Snowflake')
        rec = recommend_etl_strategy(extracted)
        # Snowflake detected as complex connector
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'connectors_complex' for s in signals_nb)

    def test_multiple_complex_signals(self):
        """Oracle + many tables + LOD calcs → notebook wins decisively."""
        tables = [{'name': f'T{i}', 'columns': [{'name': f'c{j}'} for j in range(15)]} for i in range(8)]
        calcs = [
            {'name': 'lod', 'formula': '{FIXED [Dim] : AVG([Val])}',
             'role': 'measure', 'datatype': 'real'},
        ]
        extracted = _make_extracted(conn_type='Oracle', tables=tables, calculations=calcs)
        rec = recommend_etl_strategy(extracted)
        assert rec.strategy == 'notebook'


class TestMixedWorkbook:
    """Mixed signals → should recommend Both."""

    def test_simple_connector_but_custom_sql(self):
        """PQ-friendly connector but custom SQL → scores close → both."""
        extracted = _make_extracted(
            conn_type='SQL Server',
            custom_sql=[{'datasource': 'DS', 'sql': 'SELECT 1'}],
        )
        rec = recommend_etl_strategy(extracted, margin=10)
        # With high margin, should be "both"
        assert rec.strategy == 'both'

    def test_tight_margin_triggers_both(self):
        """When scores are within margin → strategy is 'both'."""
        extracted = _make_extracted(conn_type='SQL Server')
        rec = recommend_etl_strategy(extracted, margin=100)
        assert rec.strategy == 'both'


class TestEdgeCases:
    """Edge cases."""

    def test_empty_extracted(self):
        rec = recommend_etl_strategy({})
        assert rec.strategy in ('dataflow', 'notebook', 'both')

    def test_no_datasources(self):
        extracted = {'datasources': [], 'calculations': [], 'custom_sql': []}
        rec = recommend_etl_strategy(extracted)
        assert isinstance(rec, StrategyRecommendation)

    def test_unknown_connector(self):
        extracted = _make_extracted(conn_type='SOME_EXOTIC_DB')
        rec = recommend_etl_strategy(extracted)
        # Unknown connector is neither PQ-friendly nor Spark-friendly
        assert isinstance(rec, StrategyRecommendation)

    def test_custom_thresholds(self):
        tables = [{'name': f'T{i}', 'columns': [{'name': 'c'}]} for i in range(3)]
        extracted = _make_extracted(tables=tables)
        rec = recommend_etl_strategy(extracted, table_threshold=2)
        signals_nb = [s for s in rec.signals if s.favours == 'notebook']
        assert any(s.name == 'many_tables' for s in signals_nb)


class TestPrintRecommendation:
    """print_recommendation runs without errors."""

    def test_print_dataflow(self, capsys):
        rec = StrategyRecommendation(
            strategy='dataflow', dataflow_score=5, notebook_score=1,
            signals=[StrategySignal('test', 'Test signal', 'dataflow')],
        )
        print_recommendation(rec)
        captured = capsys.readouterr()
        assert 'DATAFLOW' in captured.out

    def test_print_notebook(self, capsys):
        rec = StrategyRecommendation(
            strategy='notebook', dataflow_score=1, notebook_score=6,
            signals=[StrategySignal('test', 'Test signal', 'notebook')],
        )
        print_recommendation(rec)
        captured = capsys.readouterr()
        assert 'NOTEBOOK' in captured.out

    def test_print_both(self, capsys):
        rec = StrategyRecommendation(
            strategy='both', dataflow_score=3, notebook_score=3,
            signals=[],
        )
        print_recommendation(rec)
        captured = capsys.readouterr()
        assert 'BOTH' in captured.out

    def test_print_long_description(self, capsys):
        """Long signal descriptions are truncated gracefully."""
        rec = StrategyRecommendation(
            strategy='dataflow', dataflow_score=1, notebook_score=0,
            signals=[StrategySignal('x', 'A' * 200, 'dataflow')],
        )
        print_recommendation(rec)
        captured = capsys.readouterr()
        assert '...' in captured.out
