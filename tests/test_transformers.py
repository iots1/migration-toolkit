"""
Tests for DataTransformer — Bug #2 fix (fallback to target_col).

Regression tests for the fix where transformers (TRIM, BIT_CAST, etc.)
were skipped when generate_sql aliases columns to target names, because
source_col was not found in the DataFrame but target_col was.
"""
import pandas as pd
import numpy as np
import pytest
from services.transformers import DataTransformer


def test_bug2_trim_fallback_when_source_missing_target_present():
    """Bug #2: TRIM should run on target_col when source_col is missing.

    Scenario: generate_sql alias HospitalNumber AS old_hn → DataFrame has 'old_hn'.
    Config mapping: source='HospitalNumber', target='old_hn', transformers=['TRIM'].
    source_col='HospitalNumber' not in df, target_col='old_hn' is → fallback to target_col.
    """
    config = {
        "mappings": [
            {
                "source": "HospitalNumber",
                "target": "old_hn",
                "transformers": ["TRIM"],
                "ignore": False,
            }
        ]
    }
    df = pd.DataFrame({"old_hn": ["660008276    ", "660008277  "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert "old_hn" in result.columns
    assert result["old_hn"].iloc[0] == "660008276", "TRIM should strip trailing spaces"
    assert result["old_hn"].iloc[1] == "660008277", "TRIM should strip trailing spaces"


def test_bug2_trim_with_source_present_no_fallback_needed():
    """When source_col is present, TRIM works normally (no fallback needed)."""
    config = {
        "mappings": [
            {
                "source": "HospitalNumber",
                "target": "old_hn",
                "transformers": ["TRIM"],
                "ignore": False,
            }
        ]
    }
    df = pd.DataFrame({"HospitalNumber": ["660008276    "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert "old_hn" in result.columns
    assert result["old_hn"].iloc[0] == "660008276"


def test_bug2_no_transformers_target_present_skip():
    """When source_col missing, target_col present, no transformers → should skip gracefully."""
    config = {
        "mappings": [
            {
                "source": "HospitalNumber",
                "target": "old_hn",
                "transformers": [],
                "ignore": False,
            }
        ]
    }
    df = pd.DataFrame({"old_hn": ["660008276"]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert "old_hn" in result.columns
    assert result["old_hn"].iloc[0] == "660008276"


def test_bug2_both_source_and_target_missing():
    """When both source_col and target_col are missing → skip the mapping."""
    config = {
        "mappings": [
            {
                "source": "NonExistent",
                "target": "also_missing",
                "transformers": ["TRIM"],
                "ignore": False,
            }
        ]
    }
    df = pd.DataFrame({"other_col": ["val"]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert "also_missing" not in result.columns
    assert "NonExistent" not in result.columns
    assert "other_col" in result.columns


def test_trim_preserves_none_values():
    """TRIM should not convert None/NaN to strings."""
    config = {
        "mappings": [
            {"source": "val", "target": "val", "transformers": ["TRIM"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"val": ["  hello  ", None, "  world  "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["val"].iloc[0] == "hello"
    assert pd.isna(result["val"].iloc[1])
    assert result["val"].iloc[2] == "world"


def test_trim_vectorized_strips_all_whitespace():
    """TRIM should strip leading, trailing, and internal excess spaces."""
    config = {
        "mappings": [
            {"source": "val", "target": "val", "transformers": ["TRIM"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"val": ["  foo  ", "\tbar\t", " baz "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["val"].iloc[0] == "foo"
    assert result["val"].iloc[1] == "bar"
    assert result["val"].iloc[2] == "baz"


def test_upper_trim_fallback():
    """UPPER_TRIM should work with target_col fallback."""
    config = {
        "mappings": [
            {"source": "Name", "target": "name", "transformers": ["UPPER_TRIM"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"name": ["  john  "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["name"].iloc[0] == "JOHN"


def test_lower_trim_fallback():
    """LOWER_TRIM should work with target_col fallback."""
    config = {
        "mappings": [
            {"source": "Name", "target": "name", "transformers": ["LOWER_TRIM"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"name": ["  JOHN  "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["name"].iloc[0] == "john"


def test_multiple_transformers_fallback():
    """Multiple transformers should all apply via target_col fallback."""
    config = {
        "mappings": [
            {"source": "Name", "target": "name", "transformers": ["TRIM", "UPPER_TRIM"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"name": ["  john  "]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["name"].iloc[0] == "JOHN"


def test_default_value_fallback():
    """default_value should fill NaN when source_col is present."""
    config = {
        "mappings": [
            {
                "source": "phone",
                "target": "phone",
                "transformers": ["TRIM"],
                "ignore": False,
                "default_value": "N/A",
            }
        ]
    }
    df = pd.DataFrame({"phone": [None, " 1234 ", ""]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["phone"].iloc[0] == "N/A"
    assert result["phone"].iloc[1] == "1234"
    assert result["phone"].iloc[2] == "N/A"


def test_generate_hn_creates_column():
    """GENERATE_HN should create a new column even if source doesn't exist."""
    DataTransformer.reset_hn_counter(0)
    config = {
        "mappings": [
            {"source": "hn", "target": "hn", "transformers": ["GENERATE_HN"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"other": [1, 2, 3]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert "hn" in result.columns
    assert result["hn"].iloc[0] == "HN000000001"
    assert result["hn"].iloc[2] == "HN000000003"


def test_empty_dataframe_returns_unchanged():
    """Empty DataFrame should return unchanged."""
    config = {"mappings": [{"source": "a", "target": "b", "transformers": ["TRIM"], "ignore": False}]}
    df = pd.DataFrame()
    result = DataTransformer.apply_transformers_to_batch(df, config)
    assert result.empty


def test_empty_config_returns_unchanged():
    """Empty config should return DataFrame unchanged."""
    df = pd.DataFrame({"a": [1]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), {})
    assert list(result.columns) == ["a"]


def test_to_number_transformer():
    """TO_NUMBER should strip non-digit characters."""
    config = {
        "mappings": [
            {"source": "phone", "target": "phone", "transformers": ["TO_NUMBER"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"phone": ["02-123-4567", "081-234-5678"]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["phone"].iloc[0] == "021234567"
    assert result["phone"].iloc[1] == "0812345678"


def test_replace_empty_with_null_transformer():
    """REPLACE_EMPTY_WITH_NULL should convert empty/whitespace strings to NaN."""
    config = {
        "mappings": [
            {"source": "val", "target": "val", "transformers": ["REPLACE_EMPTY_WITH_NULL"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"val": ["hello", "", "   ", "world"]})
    result = DataTransformer.apply_transformers_to_batch(df.copy(), config)

    assert result["val"].iloc[0] == "hello"
    assert pd.isna(result["val"].iloc[1])
    assert pd.isna(result["val"].iloc[2])
    assert result["val"].iloc[3] == "world"
