"""
Utility functions for classifying and converting Tableau calculated columns.

Shared logic used by Lakehouse, Dataflow, Notebook, and TMDL generators to
determine which calculations should be **materialized as physical columns**
in the Lakehouse (loaded by Dataflows or Notebooks) versus kept as
**DAX measures** in the Semantic Model.

Architecture:
    Calculated columns → physical columns in Lakehouse Delta tables
    Measures           → DAX expressions in Semantic Model
"""

import re


# ── Aggregation pattern (calculations matching this are MEASURES) ──

_AGG_PATTERN = re.compile(
    r'\b(SUM|COUNT|COUNTA|COUNTD|COUNTROWS|AVERAGE|AVG|MIN|MAX|MEDIAN|'
    r'STDEV|STDEVP|VAR|VARP|PERCENTILE|DISTINCTCOUNT|CALCULATE|'
    r'TOTALYTD|SAMEPERIODLASTYEAR|RANKX|SUMX|AVERAGEX|MINX|MAXX|COUNTX|'
    r'RUNNING_SUM|RUNNING_AVG|RUNNING_COUNT|RUNNING_MAX|RUNNING_MIN|'
    r'WINDOW_SUM|WINDOW_AVG|WINDOW_MAX|WINDOW_MIN|WINDOW_COUNT)\s*\(',
    re.IGNORECASE,
)

# ── Spark type mapping for materialized calc columns ───────────────

_CALC_SPARK_TYPE = {
    'string': 'STRING',
    'integer': 'INT',
    'int64': 'BIGINT',
    'real': 'DOUBLE',
    'double': 'DOUBLE',
    'number': 'DOUBLE',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'datetime': 'TIMESTAMP',
}


# ═══════════════════════════════════════════════════════════════════
#  CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════

def classify_calculations(calculations):
    """Split Tableau calculations into *calculated columns* vs *measures*.

    Calculated columns are row-level expressions without aggregation and
    are materialized in the Lakehouse.  Measures use aggregation functions
    and remain as DAX in the Semantic Model.

    Args:
        calculations: list of dicts from ``extracted['calculations']``.

    Returns:
        ``(calc_columns, measures)`` — two lists.
        Each ``calc_column`` dict carries an extra ``spark_type`` key.
    """
    calc_columns = []
    measures = []

    for calc in calculations:
        formula = calc.get('formula', '').strip()
        if not formula:
            continue

        role = calc.get('role', 'measure')
        datatype = calc.get('datatype', 'string')

        is_literal = '[' not in formula
        has_aggregation = bool(_AGG_PATTERN.search(formula))

        # A calculation is a "calculated column" if:
        #   • It references columns (not a literal constant), AND
        #   • role == 'dimension'  OR  no aggregation function present
        is_calc_col = (not is_literal) and (
            role == 'dimension' or not has_aggregation
        )

        if is_calc_col:
            cc = dict(calc)
            cc['spark_type'] = _CALC_SPARK_TYPE.get(datatype, 'STRING')
            calc_columns.append(cc)
        else:
            measures.append(calc)

    return calc_columns, measures


# ═══════════════════════════════════════════════════════════════════
#  SANITISATION HELPERS
# ═══════════════════════════════════════════════════════════════════

def sanitize_calc_col_name(name):
    """Sanitize a calculated-column name for Delta Lake / Spark."""
    name = name.replace('[', '').replace(']', '')
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    name = re.sub(r'^[0-9]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    return name.lower() or 'calc_col'


# ═══════════════════════════════════════════════════════════════════
#  FORMULA CONVERSION — Power Query M
# ═══════════════════════════════════════════════════════════════════

def tableau_formula_to_m(formula):
    """Best-effort conversion of a Tableau formula to a Power Query M
    row expression (for use inside ``each …``).

    Handles:
      - Column references ``[Col]`` → M row field ``[Col]`` (same syntax)
      - Arithmetic operators (``+  -  *  /``)
      - ``IF … THEN … ELSE … END`` → ``if … then … else``
      - ``AND / OR / NOT`` → ``and / or / not``
      - String concatenation ``+`` (works the same in M)

    Returns the M expression string.
    """
    m = formula.strip()

    # IF / THEN / ELSE / END
    m = re.sub(r'\bIF\b', 'if', m, flags=re.IGNORECASE)
    m = re.sub(r'\bTHEN\b', 'then', m, flags=re.IGNORECASE)
    m = re.sub(r'\bELSE\b', 'else', m, flags=re.IGNORECASE)
    m = re.sub(r'\bEND\b', '', m, flags=re.IGNORECASE)

    # Boolean operators
    m = re.sub(r'\bAND\b', 'and', m, flags=re.IGNORECASE)
    m = re.sub(r'\bOR\b', 'or', m, flags=re.IGNORECASE)
    m = re.sub(r'\bNOT\b', 'not', m, flags=re.IGNORECASE)

    # Tableau string functions → M equivalents
    m = re.sub(r'\bLEFT\s*\(', 'Text.Start(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bRIGHT\s*\(', 'Text.End(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bUPPER\s*\(', 'Text.Upper(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bLOWER\s*\(', 'Text.Lower(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bLEN\s*\(', 'Text.Length(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bTRIM\s*\(', 'Text.Trim(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bROUND\s*\(', 'Number.Round(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bABS\s*\(', 'Number.Abs(', m, flags=re.IGNORECASE)
    m = re.sub(r'\bINT\s*\(', 'Number.IntegerDivide(', m, flags=re.IGNORECASE)

    return m.strip()


def make_m_add_column_step(formula, col_name, prev_step):
    """Return a Power Query M ``Table.AddColumn`` step string.

    Args:
        formula: Tableau formula.
        col_name: Display name for the new column.
        prev_step: Name of the previous M step to chain from.

    Returns:
        ``(m_line, step_name)`` tuple.
    """
    safe_col = col_name.replace('"', '""')
    m_expr = tableau_formula_to_m(formula)
    step_name = f'CalcCol_{sanitize_calc_col_name(col_name)}'
    line = f'    {step_name} = Table.AddColumn({prev_step}, "{safe_col}", each {m_expr})'
    return line, step_name


# ═══════════════════════════════════════════════════════════════════
#  FORMULA CONVERSION — PySpark
# ═══════════════════════════════════════════════════════════════════

def tableau_formula_to_pyspark(formula, col_name):
    """Best-effort conversion of a Tableau formula to PySpark
    ``.withColumn()`` code.

    Returns a code string ready for a notebook code cell.
    """
    safe_name = sanitize_calc_col_name(col_name)

    def _col_ref(m):
        return f'F.col("{m.group(1)}")'

    # IF … THEN … ELSE … END
    if_match = re.match(
        r'^\s*IF\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END\s*$',
        formula, re.IGNORECASE | re.DOTALL,
    )
    if if_match:
        cond = re.sub(r'\[([^\]]+)\]', _col_ref, if_match.group(1).strip())
        then_v = re.sub(r'\[([^\]]+)\]', _col_ref, if_match.group(2).strip())
        else_v = re.sub(r'\[([^\]]+)\]', _col_ref, if_match.group(3).strip())
        return f'df = df.withColumn("{col_name}", F.when({cond}, {then_v}).otherwise({else_v}))'

    # Simple column reference [Col]
    if re.match(r'^\[[^\]]+\]$', formula.strip()):
        inner = formula.strip().strip('[]')
        return f'df = df.withColumn("{col_name}", F.col("{inner}"))'

    # General arithmetic / column expressions
    pyspark_expr = re.sub(r'\[([^\]]+)\]', _col_ref, formula)
    return f'df = df.withColumn("{col_name}", {pyspark_expr})'
