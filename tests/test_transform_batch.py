"""
Tests for transform_batch — Bug #1 fix (drop ignored columns logic).

Regression tests for the 2-step drop fix that prevents active columns
from being dropped when generate_sql aliases columns to target names
and ignored identity mappings exist.
"""
import pandas as pd
import pytest
from services.query_builder import transform_batch


def _config_with_generate_sql_and_ignored():
    """Config mimicking cnPatient_patients_config with generate_sql + ignored identity mappings."""
    return {
        "mappings": [
            {"source": "HospitalNumber", "target": "old_hn", "transformers": ["TRIM"], "ignore": False},
            {"source": "IdCardNo", "target": "national_id", "transformers": [], "ignore": False},
            {"source": "FirstName", "target": "first_name", "transformers": [], "ignore": False},
            {"source": "Death", "target": "death", "transformers": ["BIT_CAST"], "ignore": False},
            {"source": "old_hn", "target": "old_hn", "transformers": [], "ignore": True},
            {"source": "national_id", "target": "national_id", "transformers": [], "ignore": True},
            {"source": "first_name", "target": "first_name", "transformers": [], "ignore": True},
            {"source": "death", "target": "death", "transformers": [], "ignore": True},
        ]
    }


def test_bug1_generate_sql_with_ignored_identity_preserves_all_active_columns():
    """Bug #1: generate_sql aliases to target names + ignored identity mappings.

    Before fix: all columns dropped → 0 columns → 0 rows inserted.
    After fix: only truly ignored columns dropped, active columns preserved.
    """
    config = _config_with_generate_sql_and_ignored()
    df = pd.DataFrame({
        "old_hn": ["660008276", "660008277"],
        "national_id": ["1101234567890", "1109876543210"],
        "first_name": ["John", "Jane"],
        "death": ["0", "1"],
    })
    result, bit_columns, _ = transform_batch(df.copy(), config)

    assert "old_hn" in result.columns, "active column 'old_hn' was incorrectly dropped"
    assert "national_id" in result.columns, "active column 'national_id' was incorrectly dropped"
    assert "first_name" in result.columns, "active column 'first_name' was incorrectly dropped"
    assert "death" in result.columns, "active column 'death' was incorrectly dropped"
    assert len(result) == 2
    assert "death" in bit_columns


def test_bug1_generate_sql_active_data_preserved():
    """Data values should be preserved after transform_batch with generate_sql + ignored."""
    config = _config_with_generate_sql_and_ignored()
    df = pd.DataFrame({
        "old_hn": ["AA", "BB"],
        "national_id": ["111", "222"],
        "first_name": ["X", "Y"],
        "death": ["0", "1"],
    })
    result, _, _ = transform_batch(df.copy(), config)

    assert list(result["old_hn"]) == ["AA", "BB"]
    assert list(result["national_id"]) == ["111", "222"]
    assert list(result["first_name"]) == ["X", "Y"]


def test_transform_batch_dynamic_select_no_generate_sql():
    """Without generate_sql (dynamic SELECT), columns come as source names → rename."""
    config = {
        "mappings": [
            {"source": "HospitalNumber", "target": "old_hn", "transformers": ["TRIM"], "ignore": False},
            {"source": "IdCardNo", "target": "national_id", "transformers": [], "ignore": False},
            {"source": "secret_col", "target": "secret_col", "transformers": [], "ignore": True},
        ]
    }
    df = pd.DataFrame({
        "HospitalNumber": ["  660008276  "],
        "IdCardNo": ["1234"],
        "secret_col": ["hidden"],
    })
    result, _, _ = transform_batch(df.copy(), config)

    assert "old_hn" in result.columns
    assert "national_id" in result.columns
    assert "secret_col" not in result.columns, "ignored column should be dropped"
    assert result["old_hn"].iloc[0] == "660008276", "TRIM should have stripped spaces"


def test_transform_batch_no_ignored_mappings():
    """With only active mappings and no ignored, everything should work normally."""
    config = {
        "mappings": [
            {"source": "col_a", "target": "col_a", "transformers": [], "ignore": False},
            {"source": "col_b", "target": "col_b", "transformers": [], "ignore": False},
        ]
    }
    df = pd.DataFrame({"col_a": [1], "col_b": [2]})
    result, _, _ = transform_batch(df.copy(), config)

    assert list(result.columns) == ["col_a", "col_b"]
    assert len(result) == 1


def test_transform_batch_ignored_source_neq_target():
    """Ignored mapping where source != target — source column should be dropped before rename."""
    config = {
        "mappings": [
            {"source": "src_col", "target": "tgt_col", "transformers": [], "ignore": False},
            {"source": "internal_id", "target": "excluded_id", "transformers": [], "ignore": True},
        ]
    }
    df = pd.DataFrame({
        "src_col": ["val1"],
        "internal_id": ["val2"],
    })
    result, _, _ = transform_batch(df.copy(), config)

    assert "tgt_col" in result.columns
    assert "excluded_id" not in result.columns
    assert "internal_id" not in result.columns


def test_transform_batch_ignored_target_same_as_active_target():
    """Ignored mapping target == active mapping target → should NOT drop the active target."""
    config = {
        "mappings": [
            {"source": "A", "target": "x", "transformers": [], "ignore": False},
            {"source": "x", "target": "x", "transformers": [], "ignore": True},
        ]
    }
    df = pd.DataFrame({"x": ["hello"]})
    result, _, _ = transform_batch(df.copy(), config)

    assert "x" in result.columns, "column 'x' should be preserved (active mapping uses it)"
    assert result["x"].iloc[0] == "hello"


def test_transform_batch_drops_extra_columns_not_in_active_targets():
    """Extra columns not in any active mapping should be dropped."""
    config = {
        "mappings": [
            {"source": "col_a", "target": "col_a", "transformers": [], "ignore": False},
        ]
    }
    df = pd.DataFrame({"col_a": [1], "extra_junk": [99]})
    result, _, _ = transform_batch(df.copy(), config)

    assert "col_a" in result.columns
    assert "extra_junk" not in result.columns


def test_transform_batch_empty_dataframe():
    """Empty DataFrame (with columns) should return empty without error."""
    config = {"mappings": [{"source": "a", "target": "b", "transformers": [], "ignore": False}]}
    df = pd.DataFrame(columns=["a"])
    result, bit_columns, warnings = transform_batch(df, config)
    assert result.empty


def test_transform_batch_empty_config():
    """Empty config should return DataFrame with lowered columns."""
    df = pd.DataFrame({"ColA": [1], "ColB": [2]})
    result, _, _ = transform_batch(df.copy(), {})
    assert list(result.columns) == ["cola", "colb"]


def test_transform_batch_rename_source_to_target():
    """Verify rename from source to target column names."""
    config = {
        "mappings": [
            {"source": "FirstName", "target": "first_name", "transformers": [], "ignore": False},
            {"source": "LastName", "target": "last_name", "transformers": [], "ignore": False},
        ]
    }
    df = pd.DataFrame({"FirstName": ["John"], "LastName": ["Doe"]})
    result, _, _ = transform_batch(df.copy(), config)

    assert "first_name" in result.columns
    assert "last_name" in result.columns
    assert "FirstName" not in result.columns


def test_transform_batch_bit_cast_converts_values():
    """BIT_CAST transformer should convert truthy values to '1'/'0'."""
    config = {
        "mappings": [
            {"source": "flag", "target": "flag", "transformers": ["BIT_CAST"], "ignore": False},
        ]
    }
    df = pd.DataFrame({"flag": [True, False, 1, 0, "true", "false", "1", "0"]})
    result, bit_columns, _ = transform_batch(df.copy(), config)

    assert "flag" in bit_columns
    assert list(result["flag"]) == ["1", "0", "1", "0", "1", "0", "1", "0"]


def test_transform_batch_columns_lowercased():
    """All column names should be lowercased after transform."""
    config = {
        "mappings": [
            {"source": "ColA", "target": "col_a", "transformers": [], "ignore": False},
        ]
    }
    df = pd.DataFrame({"ColA": [1]})
    result, _, _ = transform_batch(df.copy(), config)

    assert all(c == c.lower() for c in result.columns)


def test_transform_batch_duplicate_columns_deduped():
    """Duplicate columns after rename should keep only the first occurrence."""
    config = {
        "mappings": [
            {"source": "a", "target": "x", "transformers": [], "ignore": False},
            {"source": "b", "target": "x", "transformers": [], "ignore": False},
        ]
    }
    df = pd.DataFrame({"a": [1], "b": [2]})
    result, _, _ = transform_batch(df.copy(), config)

    assert list(result.columns) == ["x"]
