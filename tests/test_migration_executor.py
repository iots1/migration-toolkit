"""
Tests for migration_executor — Bug #3 fix + helper functions.

Regression tests for the fix where _count_source_rows used PostgreSQL-specific
`SET statement_timeout` on MSSQL connections, causing errors and incorrect
fallback counts.
"""
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call
from sqlalchemy import text

from services.migration_executor import (
    _count_source_rows,
    _qualify_table_name,
    _qualify_sql_tables,
    _split_sql_statements,
    _quote_identifier,
    _detect_pk_columns,
    _build_offset_query,
    _strip_pagination_artifacts,
    _load_last_seen_pk,
)


# ---------------------------------------------------------------------------
# Bug #3: _count_source_rows dialect check
# ---------------------------------------------------------------------------


def test_bug3_mssql_engine_no_statement_timeout():
    """Bug #3: MSSQL engine should NOT execute SET statement_timeout.

    Before fix: SET statement_timeout was always executed → error on MSSQL.
    After fix: only set timeout for PostgreSQL dialect.
    """
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = 72167
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.dialect.name = "mssql"
    mock_engine.connect.return_value = mock_conn

    count = _count_source_rows(mock_engine, "SELECT * FROM dbo.cnPatient", "dbo.cnPatient")

    assert count == 72167
    for c in mock_conn.execute.call_args_list:
        sql_arg = c[0][0]
        sql_str = str(sql_arg)
        assert "statement_timeout" not in sql_str, (
            f"MSSQL should not set statement_timeout, but got: {sql_str}"
        )


def test_bug3_postgresql_engine_sets_statement_timeout():
    """PostgreSQL engine should set statement_timeout before counting."""
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = 5000
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.dialect.name = "postgresql"
    mock_engine.connect.return_value = mock_conn

    count = _count_source_rows(mock_engine, "SELECT * FROM patients", "patients")

    assert count == 5000
    first_call = mock_conn.execute.call_args_list[0]
    sql_str = str(first_call[0][0])
    assert "statement_timeout" in sql_str


def test_bug3_count_subquery_fails_falls_back_to_table_count():
    """When subquery count fails, should fall back to direct table count."""
    call_count = [0]

    def mock_execute(stmt):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("timeout expired")
        mock_result = MagicMock()
        mock_result.scalar.return_value = 86361
        return mock_result

    mock_conn = MagicMock()
    mock_conn.execute.side_effect = mock_execute
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.dialect.name = "mssql"
    mock_engine.connect.return_value = mock_conn

    count = _count_source_rows(mock_engine, "SELECT * FROM complex_join", "dbo.source_table")

    assert count == 86361


def test_bug3_both_queries_fail_returns_zero():
    """When both subquery and table count fail, should return 0."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("connection lost")
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_engine = MagicMock()
    mock_engine.dialect.name = "mssql"
    mock_engine.connect.return_value = mock_conn

    count = _count_source_rows(mock_engine, "SELECT * FROM x", "dbo.x")
    assert count == 0


# ---------------------------------------------------------------------------
# Helper: _qualify_table_name
# ---------------------------------------------------------------------------


def test_qualify_table_name_mssql_no_schema():
    assert _qualify_table_name("patients", "Microsoft SQL Server") == "dbo.patients"


def test_qualify_table_name_mssql_already_qualified():
    assert _qualify_table_name("dbo.patients", "Microsoft SQL Server") == "dbo.patients"


def test_qualify_table_name_postgresql():
    assert _qualify_table_name("patients", "PostgreSQL") == "patients"


def test_qualify_table_name_empty():
    assert _qualify_table_name("", "Microsoft SQL Server") == ""


# ---------------------------------------------------------------------------
# Helper: _qualify_sql_tables
# ---------------------------------------------------------------------------


def test_qualify_sql_tables_mssql_adds_dbo():
    sql = "SELECT * FROM patients WHERE 1=1"
    result = _qualify_sql_tables(sql, "Microsoft SQL Server")
    assert "dbo.patients" in result


def test_qualify_sql_tables_mssql_join():
    sql = "SELECT a.* FROM tableA a JOIN tableB b ON a.id = b.id"
    result = _qualify_sql_tables(sql, "Microsoft SQL Server")
    assert "dbo.tableA" in result
    assert "dbo.tableB" in result


def test_qualify_sql_tables_non_mssql_unchanged():
    sql = "SELECT * FROM patients"
    result = _qualify_sql_tables(sql, "PostgreSQL")
    assert result == sql


def test_qualify_sql_tables_already_qualified():
    sql = "SELECT * FROM dbo.patients"
    result = _qualify_sql_tables(sql, "Microsoft SQL Server")
    assert result == sql


# ---------------------------------------------------------------------------
# Helper: _quote_identifier
# ---------------------------------------------------------------------------


def test_quote_identifier_simple():
    assert _quote_identifier("patients") == '"patients"'


def test_quote_identifier_schema_qualified():
    assert _quote_identifier("public.patients") == '"public"."patients"'


# ---------------------------------------------------------------------------
# Helper: _split_sql_statements
# ---------------------------------------------------------------------------


def test_split_sql_multiple_statements():
    sql = "SELECT 1; SELECT 2;"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2
    assert stmts[0] == "SELECT 1"
    assert stmts[1] == "SELECT 2"


def test_split_sql_single_statement():
    sql = "SELECT 1"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 1
    assert stmts[0] == "SELECT 1"


def test_split_sql_dollar_quotes():
    sql = "CREATE FUNCTION foo() RETURNS void AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql; SELECT 1;"
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 2


def test_split_sql_empty():
    assert _split_sql_statements("") == []
    assert _split_sql_statements("   ") == []


# ---------------------------------------------------------------------------
# Helper: _strip_pagination_artifacts
# ---------------------------------------------------------------------------


def test_strip_pagination_artifacts_removes_ctid():
    df = pd.DataFrame({"ctid": [1], "col_a": ["val"]})
    result = _strip_pagination_artifacts(df)
    assert "ctid" not in result.columns
    assert "col_a" in result.columns


def test_strip_pagination_artifacts_removes_surrogate_row_num():
    df = pd.DataFrame({"_surrogate_row_num": [1, 2], "col_a": ["a", "b"]})
    result = _strip_pagination_artifacts(df)
    assert "_surrogate_row_num" not in result.columns


def test_strip_pagination_artifacts_no_artifacts():
    df = pd.DataFrame({"col_a": [1], "col_b": [2]})
    result = _strip_pagination_artifacts(df)
    assert list(result.columns) == ["col_a", "col_b"]


# ---------------------------------------------------------------------------
# Helper: _load_last_seen_pk
# ---------------------------------------------------------------------------


def test_load_last_seen_pk_valid():
    checkpoint = {"last_seen_pk": [1, "ABC"]}
    assert _load_last_seen_pk(checkpoint, "test") == (1, "ABC")


def test_load_last_seen_pk_empty_list():
    checkpoint = {"last_seen_pk": []}
    assert _load_last_seen_pk(checkpoint, "test") is None


def test_load_last_seen_pk_none():
    assert _load_last_seen_pk(None, "test") is None


def test_load_last_seen_pk_missing_key():
    checkpoint = {"other_key": 123}
    assert _load_last_seen_pk(checkpoint, "test") is None


# ---------------------------------------------------------------------------
# Helper: _build_offset_query
# ---------------------------------------------------------------------------


def test_build_offset_query_postgresql():
    mock_engine = MagicMock()
    mock_engine.dialect.name = "postgresql"
    log = MagicMock()

    result = _build_offset_query(mock_engine, "SELECT * FROM t", log)
    assert "LIMIT :batch_size OFFSET :offset" in result


def test_build_offset_query_mssql():
    mock_engine = MagicMock()
    mock_engine.dialect.name = "mssql"
    log = MagicMock()

    result = _build_offset_query(mock_engine, "SELECT * FROM t", log)
    assert "ROW_NUMBER()" in result
    assert "_surrogate_row_num" in result
    log.assert_called()


# ---------------------------------------------------------------------------
# Helper: _detect_pk_columns
# ---------------------------------------------------------------------------


def test_detect_pk_columns_found():
    mock_engine = MagicMock()
    mock_insp = MagicMock()
    mock_insp.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
    with patch("sqlalchemy.inspect", return_value=mock_insp):
        result = _detect_pk_columns(mock_engine, "patients")
    assert result == ["id"]


def test_detect_pk_columns_fallback_to_unique():
    mock_engine = MagicMock()
    mock_insp = MagicMock()
    mock_insp.get_pk_constraint.return_value = {}
    mock_insp.get_unique_constraints.return_value = [
        {"column_names": ["email", "org_id"]},
        {"column_names": ["code"]},
    ]
    with patch("sqlalchemy.inspect", return_value=mock_insp):
        result = _detect_pk_columns(mock_engine, "patients")
    assert result == ["code"]


def test_detect_pk_columns_none():
    mock_engine = MagicMock()
    mock_insp = MagicMock()
    mock_insp.get_pk_constraint.side_effect = Exception("no table")
    with patch("sqlalchemy.inspect", return_value=mock_insp):
        result = _detect_pk_columns(mock_engine, "patients")
    assert result is None
