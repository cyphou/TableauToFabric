"""
Name sanitisation helpers for Fabric artifact generation.

Consolidates the 7+ variants of ``_sanitize_*`` / ``_clean_*`` that
were scattered across generator modules.  Each function targets a
specific artifact context but shares a common base pipeline.
"""

from __future__ import annotations

import re

# ── Pre-compiled patterns (avoids per-call re.compile overhead) ────

_BRACKETS = re.compile(r'[\[\]]')
_NON_ALNUM_UNDER = re.compile(r'[^a-zA-Z0-9_]')
_NON_ALNUM_UNDER_SPACE = re.compile(r'[^a-zA-Z0-9_ ]')
_LEADING_DIGITS = re.compile(r'^[0-9]+')
_MULTI_UNDER = re.compile(r'_+')
_DERIVATION_PREFIX = re.compile(
    r'^(none|sum|avg|count|min|max|usr|yr|mn|dy|qr|wk|attr|md|mdy|hms|hr|mt|sc|thr|trunc|tmn):',
)
_DERIVATION_SUFFIX = re.compile(r':(nk|qk|ok|fn|tn)$')


# ── Base pipeline ──────────────────────────────────────────────────

def _base_sanitize(name: str, *, allow_spaces: bool = False,
                   lowercase: bool = False, fallback: str = 'name') -> str:
    """Core sanitisation shared by all variants.

    1. Strip brackets ``[]``
    2. Replace disallowed chars with ``_``
    3. Collapse consecutive underscores
    4. Strip leading/trailing underscores
    """
    name = _BRACKETS.sub('', name)
    pattern = _NON_ALNUM_UNDER_SPACE if allow_spaces else _NON_ALNUM_UNDER
    name = pattern.sub('_', name)
    name = _MULTI_UNDER.sub('_', name).strip('_')
    if lowercase:
        name = name.lower()
    return name or fallback


# ── Public API ─────────────────────────────────────────────────────

def sanitize_table_name(name: str) -> str:
    """Sanitise for Lakehouse / Delta Lake table names.

    Strips schema prefixes (``[dbo].[Table]``), lowercases, removes
    leading digits.
    """
    if '.' in name:
        name = name.rsplit('.', 1)[-1]
    name = _base_sanitize(name, lowercase=True, fallback='table')
    name = _LEADING_DIGITS.sub('', name)
    return _MULTI_UNDER.sub('_', name).strip('_') or 'table'


def sanitize_column_name(name: str) -> str:
    """Sanitise for Delta Lake / Spark column names."""
    name = _base_sanitize(name, fallback='column')
    name = _LEADING_DIGITS.sub('_', name)
    return _MULTI_UNDER.sub('_', name).strip('_') or 'column'


def sanitize_query_name(name: str) -> str:
    """Sanitise for Dataflow Gen2 query names (spaces allowed)."""
    return _base_sanitize(name, allow_spaces=True, fallback='Query')


def sanitize_tmdl_table_name(name: str) -> str:
    """Sanitise for TMDL table identifiers (lowercase, prefix if digit-leading)."""
    safe = _base_sanitize(name, lowercase=True, fallback='table')
    if safe and safe[0].isdigit():
        safe = f"tbl_{safe}"
    return safe


def sanitize_pipeline_name(name: str) -> str:
    """Sanitise for Pipeline activity / reference names."""
    return _base_sanitize(name, fallback='activity')


def make_python_var(name: str) -> str:
    """Convert a table/column name to a valid Python variable name."""
    name = _base_sanitize(name, lowercase=True, fallback='table')
    name = _LEADING_DIGITS.sub('', name)
    return _MULTI_UNDER.sub('_', name).strip('_') or 'table'


def sanitize_filesystem_name(name: str) -> str:
    """Sanitise a name for filesystem paths (keeps spaces, wider charset)."""
    safe = re.sub(r'[<>:"/\\|?*]', '_', name)
    return safe.strip().strip('.')


def clean_field_name(name: str) -> str:
    """Strip Tableau derivation prefixes/suffixes from a field name.

    Examples: ``sum:Sales`` → ``Sales``, ``Region:nk`` → ``Region``.
    """
    name = _DERIVATION_PREFIX.sub('', name)
    name = _DERIVATION_SUFFIX.sub('', name)
    return name
