"""
Shared constants for all Fabric artifact generators.

Centralises values that were previously duplicated across multiple modules
(magic numbers, type maps, artifact lists, regex patterns).  Importers
should prefer these constants over local copies to avoid silent divergence.
"""

from __future__ import annotations

import re
import uuid

# ── Artifact types ──────────────────────────────────────────────────

ALL_ARTIFACTS: list[str] = [
    'lakehouse', 'dataflow', 'notebook',
    'semanticmodel', 'pipeline', 'pbi',
]

# ── Page / layout defaults ──────────────────────────────────────────

DEFAULT_PAGE_WIDTH = 1280
DEFAULT_PAGE_HEIGHT = 720

Z_INDEX_MULTIPLIER = 1000
"""Spacing between z-index / tab-order values for visual stacking."""

# ── Visual ID generation ───────────────────────────────────────────

VISUAL_ID_LENGTH = 20
"""Length of truncated hex UUIDs used for PBIR visual/page identifiers."""


def new_visual_id() -> str:
    """Generate a unique 20-char hex identifier for a PBIR visual."""
    return uuid.uuid4().hex[:VISUAL_ID_LENGTH]


# ── Truncation limits ──────────────────────────────────────────────

MAX_FILTER_VALUES = 100
"""Maximum number of discrete filter values kept in a slicer / filter card."""

MAX_SET_MEMBERS = 50
"""Maximum number of set members exported to TMDL."""

MAX_FIELD_PROJECTIONS = 10
"""Maximum number of field projections per visual."""

MAX_COLOR_ASSIGNMENTS = 20
"""Maximum distinct series colour assignments per visual."""

MAX_WORKSHEETS_PER_REPORT = 20
"""Maximum worksheets converted to visuals per report page."""

# ── Tableau → Spark SQL type mapping (canonical) ──────────────────

SPARK_TYPE_MAP: dict[str, str] = {
    'string':     'STRING',
    'integer':    'INT',
    'int64':      'BIGINT',
    'real':       'DOUBLE',
    'double':     'DOUBLE',
    'number':     'DOUBLE',
    'boolean':    'BOOLEAN',
    'date':       'DATE',
    'datetime':   'TIMESTAMP',
    'time':       'STRING',
    'spatial':    'STRING',
    'binary':     'BINARY',
    'currency':   'DECIMAL(19,4)',
    'percentage': 'DOUBLE',
}


def map_to_spark_type(tableau_type: str) -> str:
    """Map a Tableau data type string to the corresponding Spark SQL type."""
    return SPARK_TYPE_MAP.get(tableau_type.lower(), 'STRING')


# PySpark StructType wrappers — needed by notebook_generator
PYSPARK_TYPE_MAP: dict[str, str] = {
    'string':     'StringType()',
    'integer':    'IntegerType()',
    'int64':      'LongType()',
    'real':       'DoubleType()',
    'double':     'DoubleType()',
    'number':     'DoubleType()',
    'boolean':    'BooleanType()',
    'date':       'DateType()',
    'datetime':   'TimestampType()',
    'time':       'StringType()',
    'spatial':    'StringType()',
    'binary':     'BinaryType()',
    'currency':   'DecimalType(19, 4)',
    'percentage': 'DoubleType()',
}

# ── Aggregation detection ──────────────────────────────────────────

AGG_PATTERN = re.compile(
    r'\b(SUM|COUNT|COUNTA|COUNTD|COUNTROWS|AVERAGE|AVG|MIN|MAX|MEDIAN|'
    r'STDEV|STDEVP|VAR|VARP|PERCENTILE|DISTINCTCOUNT|CALCULATE|'
    r'TOTALYTD|SAMEPERIODLASTYEAR|RANKX|SUMX|AVERAGEX|MINX|MAXX|COUNTX|'
    r'CORR|COVAR|COVARP|'
    r'RUNNING_SUM|RUNNING_AVG|RUNNING_COUNT|RUNNING_MAX|RUNNING_MIN|'
    r'WINDOW_SUM|WINDOW_AVG|WINDOW_MAX|WINDOW_MIN|WINDOW_COUNT|'
    r'WINDOW_MEDIAN|WINDOW_STDEV|WINDOW_STDEVP|WINDOW_VAR|WINDOW_VARP|'
    r'WINDOW_CORR|WINDOW_COVAR|WINDOW_COVARP|WINDOW_PERCENTILE|'
    r'RANK|RANK_UNIQUE|RANK_DENSE|RANK_MODIFIED|RANK_PERCENTILE|'
    r'ATTR|SELECTEDVALUE)\s*\(',
    re.IGNORECASE,
)
"""Pre-compiled regex matching Tableau/DAX aggregation function calls.

Used to decide whether a calculation is a *measure* (has aggregation)
or a *calculated column* (pure row-level).
"""

# ── Literal-expression helper ──────────────────────────────────────


def literal_expr(value: str) -> dict:
    """Build a PBIR literal-value expression object.

    Replaces the duplicated ``_L = lambda v: …`` pattern used in
    visual_generator and pbip_generator.
    """
    return {"expr": {"Literal": {"Value": value}}}
