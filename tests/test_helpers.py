import pandas as pd
import pytest
from utils.helpers import safe_str, to_snake_case, to_camel_case, format_row_count, safe_filename, resolve_dbname

def test_safe_str_none():
    assert safe_str(None) == ""

def test_safe_str_nan():
    assert safe_str(float("nan")) == ""

def test_safe_str_normal():
    assert safe_str("  hello  ") == "hello"

def test_to_snake_case_camel():
    assert to_snake_case("FirstName") == "first_name"

def test_to_snake_case_already_snake():
    assert to_snake_case("first_name") == "first_name"

def test_to_snake_case_spaces():
    assert to_snake_case("first name") == "first_name"

def test_to_camel_case():
    assert to_camel_case("first_name") == "firstName"

def test_to_camel_case_single():
    assert to_camel_case("name") == "name"

def test_format_row_count():
    assert format_row_count(1234) == "1,234 rows"
    assert format_row_count(0) == "0 rows"

def test_safe_filename_removes_special():
    result = safe_filename("my config/v2")
    assert "/" not in result
    assert " " not in result

def test_resolve_dbname_found():
    df = pd.DataFrame({"name": ["MyDB"], "dbname": ["actual_db"]})
    assert resolve_dbname("MyDB", df) == "actual_db"

def test_resolve_dbname_not_found():
    df = pd.DataFrame({"name": ["OtherDB"], "dbname": ["other"]})
    assert resolve_dbname("MyDB", df) == "MyDB"

def test_resolve_dbname_empty_name():
    df = pd.DataFrame({"name": ["MyDB"], "dbname": ["actual_db"]})
    assert resolve_dbname("", df) == ""

def test_resolve_dbname_none_df():
    assert resolve_dbname("MyDB", None) == "MyDB"
