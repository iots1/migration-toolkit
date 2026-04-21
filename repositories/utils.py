"""Shared low-level helpers for repository functions.

All helpers are pure functions with no side effects — safe to import anywhere.
"""
from __future__ import annotations

import json


def rows_to_dicts(result) -> list[dict]:
    """Convert a SQLAlchemy result-set to a list of plain dicts."""
    keys = list(result.keys())
    return [dict(zip(keys, row)) for row in result.fetchall()]


def row_to_dict(result) -> dict | None:
    """Fetch the first row from a result and return it as a dict, or None."""
    row = result.fetchone()
    if row is None:
        return None
    return dict(zip(result.keys(), row))


def parse_json_field(data: dict, field: str = "json_data") -> None:
    """Parse a JSON string field in-place. Falls back to {} on decode error."""
    raw = data.get(field)
    try:
        data[field] = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (json.JSONDecodeError, TypeError):
        data[field] = {}
