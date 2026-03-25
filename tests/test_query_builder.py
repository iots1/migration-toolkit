import pandas as pd
import pytest
from services.query_builder import build_select_query, build_dtype_map

# --- build_select_query ---

def test_build_select_query_no_mappings():
    config = {}
    q = build_select_query(config, "patients")
    assert q == "SELECT * FROM patients"

def test_build_select_query_basic_columns():
    config = {
        "mappings": [
            {"source": "col_a", "target": "col_a", "transformers": [], "ignore": False},
            {"source": "col_b", "target": "col_b", "transformers": [], "ignore": False},
        ]
    }
    q = build_select_query(config, "patients")
    assert '"col_a"' in q
    assert '"col_b"' in q
    assert "FROM patients" in q

def test_build_select_query_skips_ignored():
    config = {
        "mappings": [
            {"source": "col_a", "target": "col_a", "transformers": [], "ignore": False},
            {"source": "col_b", "target": "col_b", "transformers": [], "ignore": True},
        ]
    }
    q = build_select_query(config, "patients")
    assert '"col_a"' in q
    assert '"col_b"' not in q

def test_build_select_query_skips_generate_hn():
    config = {
        "mappings": [
            {"source": "hn", "target": "hn", "transformers": ["GENERATE_HN"], "ignore": False},
            {"source": "name", "target": "name", "transformers": [], "ignore": False},
        ]
    }
    q = build_select_query(config, "patients")
    assert '"hn"' not in q
    assert '"name"' in q

def test_build_select_query_mssql_trim():
    config = {
        "mappings": [
            {"source": "col_a", "target": "col_a", "transformers": ["TRIM"], "ignore": False},
        ]
    }
    q = build_select_query(config, "patients", db_type="Microsoft SQL Server")
    assert 'TRIM("col_a") AS "col_a"' in q

def test_build_select_query_only_generate_hn_fallback():
    """When all columns are GENERATE_HN, should fallback to selecting the first column."""
    config = {
        "mappings": [
            {"source": "hn", "target": "hn", "transformers": ["GENERATE_HN"], "ignore": False},
        ]
    }
    q = build_select_query(config, "patients")
    assert "hn" in q
    assert "FROM patients" in q

# --- build_dtype_map ---

def test_build_dtype_map_empty():
    df = pd.DataFrame({"a": [1]})
    assert build_dtype_map([], df, "PostgreSQL") == {}

def test_build_dtype_map_postgresql():
    df = pd.DataFrame({"flag": ["1"]})
    result = build_dtype_map(["flag"], df, "PostgreSQL")
    assert "flag" in result

def test_build_dtype_map_mysql():
    df = pd.DataFrame({"flag": ["1"]})
    result = build_dtype_map(["flag"], df, "MySQL")
    assert "flag" in result

def test_build_dtype_map_skips_missing_cols():
    df = pd.DataFrame({"other": [1]})
    result = build_dtype_map(["flag"], df, "PostgreSQL")
    assert "flag" not in result
