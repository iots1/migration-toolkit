import pandas as pd
import pytest
from services.encoding_helper import clean_value, clean_dataframe

def test_clean_value_none():
    assert clean_value(None) is None

def test_clean_value_bytes_utf8():
    assert clean_value(b"hello") == "hello"

def test_clean_value_bytes_latin1():
    assert isinstance(clean_value(b"\xe9l\xe8ve"), str)

def test_clean_value_strips_null_bytes():
    result = clean_value("hello\x00world")
    assert "\x00" not in result
    assert "hello" in result

def test_clean_value_replaces_nbsp():
    result = clean_value("hello\xa0world")
    assert "\xa0" not in result
    assert " " in result

def test_clean_value_passthrough_normal_string():
    assert clean_value("normal text") == "normal text"

def test_clean_dataframe():
    df = pd.DataFrame({"a": ["hello\x00", None, "ok"], "b": [1, 2, 3]})
    result = clean_dataframe(df)
    assert "\x00" not in result["a"].iloc[0]
    assert result["b"].tolist() == [1, 2, 3]  # numeric unchanged

def test_clean_dataframe_only_touches_object_columns():
    df = pd.DataFrame({"nums": [1, 2, 3], "text": ["a\x00", "b", "c"]})
    result = clean_dataframe(df)
    assert result["nums"].tolist() == [1, 2, 3]
