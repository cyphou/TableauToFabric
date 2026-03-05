"""
ETL Strategy Advisor for Tableau-to-Fabric migration.

Automatically recommends the optimal ETL approach — **Dataflow Gen2**,
**PySpark Notebook**, or **both** — based on workbook characteristics
extracted from the Tableau file.

Decision signals
~~~~~~~~~~~~~~~~
+----------------------------------+------------+----------+
| Signal                           | Dataflow   | Notebook |
+----------------------------------+------------+----------+
| Simple connectors (SQL, Excel…)  | ✓          |          |
| Complex connectors (BigQuery…)   |            | ✓        |
| Few tables (≤ 5)                 | ✓          |          |
| Many tables (> 5)                |            | ✓        |
| Few columns (≤ 50 total)        | ✓          |          |
| Many columns (> 50)             |            | ✓        |
| No Custom SQL                    | ✓          |          |
| Custom SQL present               |            | ✓        |
| Simple calcs only                | ✓          |          |
| LOD / table calcs / REGEX        |            | ✓        |
| Few calculated columns (≤ 10)   | ✓          |          |
| Many calculated columns (> 10)  |            | ✓        |
| Prep flow transforms             |            | ✓        |
+----------------------------------+------------+----------+

Scoring
~~~~~~~
Each signal awards points to either ``dataflow`` or ``notebook``.
The strategy with the higher score wins.  When scores are close
(within a configurable margin), **both** artifacts are recommended so
the Pipeline can orchestrate Dataflow for ingestion and Notebook for
heavy computation.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Connector classification ────────────────────────────────────────

# Connectors that Power Query M handles well → favour Dataflow
_PQ_FRIENDLY_CONNECTORS = frozenset({
    'Excel', 'CSV', 'SQL Server', 'PostgreSQL', 'MySQL',
    'GeoJSON', 'OData', 'Azure Blob', 'ADLS',
})

# Connectors that need JDBC / custom Spark → favour Notebook
_SPARK_FRIENDLY_CONNECTORS = frozenset({
    'BigQuery', 'Oracle', 'SAP BW', 'Snowflake',
    'Google Analytics',
})

# LOD / table-calc / complex formula patterns
_COMPLEX_FORMULA_PATTERN = re.compile(
    r'\{?\s*(FIXED|INCLUDE|EXCLUDE)\s+.*?:'
    r'|RUNNING_(SUM|AVG|COUNT|MAX|MIN)\s*\('
    r'|WINDOW_(SUM|AVG|MAX|MIN|COUNT)\s*\('
    r'|LOOKUP\s*\('
    r'|PREVIOUS_VALUE\s*\('
    r'|RANK\s*\('
    r'|INDEX\s*\('
    r'|RAWSQL',
    re.IGNORECASE,
)

# Regex / string-heavy patterns that PySpark handles better
_REGEX_PATTERN = re.compile(
    r'REGEXP_(MATCH|EXTRACT|REPLACE)\s*\(', re.IGNORECASE,
)


# ── Scoring thresholds ──────────────────────────────────────────────

TABLE_THRESHOLD = 5        # ≤ 5 → dataflow, > 5 → notebook
COLUMN_THRESHOLD = 50      # ≤ 50 → dataflow, > 50 → notebook
CALC_COL_THRESHOLD = 10    # ≤ 10 → dataflow, > 10 → notebook
MARGIN = 2                 # score gap ≤ margin → recommend both


# ── Data classes ────────────────────────────────────────────────────

@dataclass
class StrategySignal:
    """A single decision signal contributing to the recommendation."""
    name: str
    description: str
    favours: str            # 'dataflow' | 'notebook'
    weight: int = 1         # how many score points this signal awards


@dataclass
class StrategyRecommendation:
    """Result of the ETL strategy analysis."""
    strategy: str                       # 'dataflow' | 'notebook' | 'both'
    dataflow_score: int = 0
    notebook_score: int = 0
    signals: List[StrategySignal] = field(default_factory=list)
    summary: str = ''

    @property
    def artifacts(self) -> List[str]:
        """Return the recommended artifact list (always includes lakehouse,
        semanticmodel, pipeline, pbi)."""
        base = ['lakehouse', 'semanticmodel', 'pipeline', 'pbi']
        if self.strategy == 'dataflow':
            return base + ['dataflow']
        elif self.strategy == 'notebook':
            return base + ['notebook']
        else:
            return base + ['dataflow', 'notebook']


# ── Main advisor function ──────────────────────────────────────────

def recommend_etl_strategy(
    extracted: Dict,
    *,
    prep_flow: bool = False,
    table_threshold: int = TABLE_THRESHOLD,
    column_threshold: int = COLUMN_THRESHOLD,
    calc_col_threshold: int = CALC_COL_THRESHOLD,
    margin: int = MARGIN,
) -> StrategyRecommendation:
    """Analyse extracted Tableau data and recommend an ETL strategy.

    Args:
        extracted: dict loaded by ``FabricImporter._load_extracted_objects()``
                   — must contain ``datasources``, ``calculations``,
                   ``custom_sql`` keys (others optional).
        prep_flow: True if a Tableau Prep flow is being merged.
        table_threshold: Tables above this favour Notebook.
        column_threshold: Columns above this favour Notebook.
        calc_col_threshold: Calculated columns above this favour Notebook.
        margin: When ``|dataflow_score − notebook_score| ≤ margin``,
                recommend *both*.

    Returns:
        ``StrategyRecommendation`` with strategy, scores, and signals.
    """
    signals: List[StrategySignal] = []

    datasources = extracted.get('datasources', [])
    calculations = extracted.get('calculations', [])
    custom_sql = extracted.get('custom_sql', [])

    # ── 1. Connector analysis ───────────────────────────────────
    connector_types = set()
    for ds in datasources:
        conn = ds.get('connection', {})
        conn_type = conn.get('type', 'Unknown')
        connector_types.add(conn_type)

    pq_count = len(connector_types & _PQ_FRIENDLY_CONNECTORS)
    spark_count = len(connector_types & _SPARK_FRIENDLY_CONNECTORS)

    if pq_count > 0 and spark_count == 0:
        signals.append(StrategySignal(
            'connectors_simple',
            f'All connectors Power Query-friendly ({", ".join(sorted(connector_types & _PQ_FRIENDLY_CONNECTORS))})',
            'dataflow', weight=2,
        ))
    elif spark_count > 0:
        signals.append(StrategySignal(
            'connectors_complex',
            f'Complex connectors present ({", ".join(sorted(connector_types & _SPARK_FRIENDLY_CONNECTORS))})',
            'notebook', weight=2,
        ))

    # ── 2. Table count ──────────────────────────────────────────
    total_tables = sum(len(ds.get('tables', [])) for ds in datasources)
    if total_tables <= table_threshold:
        signals.append(StrategySignal(
            'few_tables',
            f'{total_tables} table(s) — small dataset',
            'dataflow',
        ))
    else:
        signals.append(StrategySignal(
            'many_tables',
            f'{total_tables} table(s) — large dataset',
            'notebook',
        ))

    # ── 3. Column count ─────────────────────────────────────────
    total_columns = 0
    for ds in datasources:
        for tbl in ds.get('tables', []):
            total_columns += len(tbl.get('columns', []))
        total_columns += len(ds.get('columns', []))

    if total_columns <= column_threshold:
        signals.append(StrategySignal(
            'few_columns',
            f'{total_columns} column(s) — manageable schema',
            'dataflow',
        ))
    else:
        signals.append(StrategySignal(
            'many_columns',
            f'{total_columns} column(s) — wide schema',
            'notebook',
        ))

    # ── 4. Custom SQL ───────────────────────────────────────────
    if custom_sql:
        signals.append(StrategySignal(
            'custom_sql',
            f'{len(custom_sql)} custom SQL query/queries',
            'notebook', weight=2,
        ))
    else:
        signals.append(StrategySignal(
            'no_custom_sql',
            'No custom SQL',
            'dataflow',
        ))

    # ── 5. Calculation complexity ───────────────────────────────
    complex_calc_count = 0
    regex_calc_count = 0
    calc_col_count = 0

    from .calc_column_utils import classify_calculations
    calc_columns, measures = classify_calculations(calculations)
    calc_col_count = len(calc_columns)

    for calc in calculations:
        formula = calc.get('formula', '')
        if _COMPLEX_FORMULA_PATTERN.search(formula):
            complex_calc_count += 1
        if _REGEX_PATTERN.search(formula):
            regex_calc_count += 1

    if complex_calc_count == 0 and regex_calc_count == 0:
        signals.append(StrategySignal(
            'simple_calcs',
            'All calculations are simple (no LOD/table calcs/REGEX)',
            'dataflow',
        ))
    else:
        parts = []
        if complex_calc_count:
            parts.append(f'{complex_calc_count} LOD/table calc(s)')
        if regex_calc_count:
            parts.append(f'{regex_calc_count} REGEX calc(s)')
        signals.append(StrategySignal(
            'complex_calcs',
            f'Complex calculations: {", ".join(parts)}',
            'notebook', weight=2,
        ))

    # ── 6. Calculated column volume ─────────────────────────────
    if calc_col_count <= calc_col_threshold:
        signals.append(StrategySignal(
            'few_calc_cols',
            f'{calc_col_count} calculated column(s)',
            'dataflow',
        ))
    else:
        signals.append(StrategySignal(
            'many_calc_cols',
            f'{calc_col_count} calculated column(s) — heavy materialisation',
            'notebook',
        ))

    # ── 7. Prep flow ────────────────────────────────────────────
    if prep_flow:
        signals.append(StrategySignal(
            'prep_flow',
            'Tableau Prep flow transforms present',
            'notebook', weight=2,
        ))

    # ── Scoring ─────────────────────────────────────────────────
    df_score = sum(s.weight for s in signals if s.favours == 'dataflow')
    nb_score = sum(s.weight for s in signals if s.favours == 'notebook')

    gap = abs(df_score - nb_score)
    if gap <= margin:
        strategy = 'both'
    elif df_score > nb_score:
        strategy = 'dataflow'
    else:
        strategy = 'notebook'

    # ── Summary ─────────────────────────────────────────────────
    strategy_label = {
        'dataflow': 'Dataflow Gen2 only',
        'notebook': 'PySpark Notebook only',
        'both': 'Both (Dataflow for ingestion + Notebook for transforms)',
    }
    summary_lines = [
        f'Recommended ETL strategy: {strategy_label[strategy]}',
        f'  Dataflow score: {df_score}   Notebook score: {nb_score}',
        '',
        '  Signals:',
    ]
    for s in signals:
        arrow = '→ Dataflow' if s.favours == 'dataflow' else '→ Notebook'
        summary_lines.append(f'    [{arrow:>12}] {s.description} (weight={s.weight})')

    summary = '\n'.join(summary_lines)

    recommendation = StrategyRecommendation(
        strategy=strategy,
        dataflow_score=df_score,
        notebook_score=nb_score,
        signals=signals,
        summary=summary,
    )

    logger.info('ETL strategy recommendation: %s (DF=%d NB=%d)',
                strategy, df_score, nb_score)

    return recommendation


def print_recommendation(rec: StrategyRecommendation) -> None:
    """Pretty-print the strategy recommendation to stdout."""
    print()
    print('┌' + '─' * 68 + '┐')
    print('│' + ' ETL STRATEGY RECOMMENDATION'.center(68) + '│')
    print('├' + '─' * 68 + '┤')
    print(f'│  Strategy:  {rec.strategy.upper():<54} │')
    print(f'│  Dataflow score: {rec.dataflow_score:<6}  '
          f'Notebook score: {rec.notebook_score:<22} │')
    print('├' + '─' * 68 + '┤')
    for s in rec.signals:
        arrow = 'DF' if s.favours == 'dataflow' else 'NB'
        line = f'  [{arrow}] {s.description}'
        if s.weight > 1:
            line += f' (×{s.weight})'
        # Truncate long lines for the box
        if len(line) > 66:
            line = line[:63] + '...'
        print(f'│{line:<68}│')
    print('├' + '─' * 68 + '┤')
    artifacts_str = ', '.join(rec.artifacts)
    print(f'│  Artifacts: {artifacts_str:<54} │')
    print('└' + '─' * 68 + '┘')
    print()
