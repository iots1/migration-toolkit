"""
Query Builder — builds SELECT queries and handles batch ETL operations.

Responsibility (SRP): SQL generation + DataFrame transformation pipeline
for migration batches. No Streamlit dependencies.
"""
import io
import csv as _csv

import pandas as pd
from sqlalchemy import text

from services.transformers import DataTransformer


# ---------------------------------------------------------------------------
# Query Generation
# ---------------------------------------------------------------------------

def build_select_query(config: dict, source_table: str, db_type: str = "MySQL") -> str:
    """
    Generate a SELECT query from a mapping config.

    - Skips ignored columns and GENERATE_HN columns (generated in-process).
    - Applies TRIM at SQL level for MSSQL CHAR columns to remove padding.
    """
    try:
        if not config or "mappings" not in config:
            return f"SELECT * FROM {source_table}"

        selected_cols = []
        for mapping in config.get("mappings", []):
            if mapping.get("ignore", False) or "GENERATE_HN" in mapping.get("transformers", []):
                continue
            col = mapping["source"]
            if db_type == "Microsoft SQL Server" and "TRIM" in mapping.get("transformers", []):
                selected_cols.append(f'TRIM("{col}") AS "{col}"')
            else:
                selected_cols.append(f'"{col}"')

        if not selected_cols:
            has_hn = any(
                "GENERATE_HN" in m.get("transformers", [])
                for m in config.get("mappings", [])
                if not m.get("ignore", False)
            )
            if has_hn:
                first = next(
                    (m["source"] for m in config.get("mappings", []) if not m.get("ignore", False)),
                    None,
                )
                if first:
                    return f'SELECT "{first}" FROM {source_table}'
            return f"SELECT * FROM {source_table}"

        return f"SELECT {', '.join(selected_cols)} FROM {source_table}"
    except Exception:
        return f"SELECT * FROM {source_table}"


# ---------------------------------------------------------------------------
# Cursor-Based Pagination
# ---------------------------------------------------------------------------

def build_paginated_select(
    base_query: str,
    pk_columns: list[str],
    last_seen_pk: tuple | None = None,
    batch_size: int = 1000,
) -> tuple[text, dict]:
    """Wrap a SELECT query with cursor-based pagination (row-value comparison).

    Uses ``WHERE (col1, col2) > (:pk_0, :pk_1) ORDER BY pk LIMIT :batch_size``.
    Works on PostgreSQL, MySQL 8+, MariaDB 10.3+.

    Returns (sqlalchemy.text query, parameter dict).
    """
    order_clause = ", ".join(f'"{c}"' for c in pk_columns)
    pk_params: dict = {"batch_size": batch_size}

    if last_seen_pk is not None:
        for i, v in enumerate(last_seen_pk):
            pk_params[f"pk_{i}"] = v

        pk_placeholders = ", ".join(f":pk_{i}" for i in range(len(pk_columns)))
        where_clause = f"WHERE ({order_clause}) > ({pk_placeholders})"
    else:
        where_clause = ""

    return text(
        f"SELECT * FROM ({base_query}) AS _paginated_src "
        f"{where_clause} "
        f"ORDER BY {order_clause} "
        f"LIMIT :batch_size"
    ), pk_params


def build_paginated_select_expanded(
    base_query: str,
    pk_columns: list[str],
    last_seen_pk: tuple | None = None,
    batch_size: int = 1000,
) -> tuple[text, dict]:
    """Cross-dialect cursor pagination using expanded OR-chain.

    For composite PK (a, b), generates::

        WHERE a > :pk_0 OR (a = :pk_0 AND b > :pk_1)

    Works on ALL databases including MySQL 5.x and MSSQL.
    """
    pk_params: dict = {"batch_size": batch_size}

    if last_seen_pk is not None:
        for i, v in enumerate(last_seen_pk):
            pk_params[f"pk_{i}"] = v

        conditions = []
        for depth in range(len(pk_columns)):
            eq_parts = []
            for i in range(depth):
                eq_parts.append(f'"{pk_columns[i]}" = :pk_{i}')
            gt_part = f'"{pk_columns[depth]}" > :pk_{depth}'
            if eq_parts:
                conditions.append(f"({' AND '.join(eq_parts)} AND {gt_part})")
            else:
                conditions.append(gt_part)
        where_clause = f"WHERE ({' OR '.join(conditions)})"
    else:
        where_clause = ""

    order_clause = ", ".join(f'"{c}"' for c in pk_columns)
    return text(
        f"SELECT * FROM ({base_query}) AS _paginated_src "
        f"{where_clause} "
        f"ORDER BY {order_clause} "
        f"LIMIT :batch_size"
    ), pk_params


def select_pagination_builder(engine) -> callable:
    """Return the correct pagination builder for the engine's dialect.

    PostgreSQL uses row-value comparison; MySQL/MSSQL use expanded OR-chain.
    """
    dialect = engine.dialect.name if hasattr(engine, "dialect") else ""
    if dialect == "postgresql":
        return build_paginated_select
    return build_paginated_select_expanded


# ---------------------------------------------------------------------------
# Batch Transformation
# ---------------------------------------------------------------------------

def transform_batch(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Apply transformers, rename source→target columns, drop ignored columns,
    then run validators and collect warnings.

    Returns:
        (transformed DataFrame, list of BIT column names, list of validation warning strings)
    """
    df = DataTransformer.apply_transformers_to_batch(df, config)

    rename_map: dict[str, str] = {}
    transformer_created: list[str] = []

    for m in config.get("mappings", []):
        if m.get("ignore", False) or "target" not in m:
            continue
        src, tgt = m["source"], m["target"]
        if src not in df.columns or src == tgt:
            continue
        if tgt in df.columns:
            transformer_created.append(src)
        else:
            rename_map[src] = tgt

    if transformer_created:
        df = df.drop(columns=transformer_created, errors="ignore")
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    ignored = [m["target"] for m in config.get("mappings", []) if m.get("ignore", False)]
    df = df.drop(columns=[c for c in ignored if c in df.columns], errors="ignore")

    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    bit_columns = [
        m.get("target", "").lower()
        for m in config.get("mappings", [])
        if "BIT_CAST" in m.get("transformers", []) and m.get("target")
    ]
    for col in bit_columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: "1" if x in (True, 1, "1") or str(x).lower() == "true" else "0"
            )

    validation_warnings: list[str] = []
    _run_validators(df, config, validation_warnings)

    return df, bit_columns, validation_warnings


def _run_validators(df: pd.DataFrame, config: dict, warnings: list[str]) -> None:
    """Run registered validators for each mapping that has validators configured.

    Appends human-readable warning strings to *warnings* in-place.
    Never raises — validator errors are captured as warnings so migration continues.
    """
    import validators as _vld_pkg
    from validators.registry import get_validator

    for m in config.get("mappings", []):
        if m.get("ignore", False):
            continue
        validator_names: list[str] = m.get("validators", [])
        if not validator_names:
            continue
        col = m.get("target", m.get("source", "")).lower()
        if col not in df.columns:
            continue
        for v_name in validator_names:
            try:
                validator_fn = get_validator(v_name)
                result = validator_fn(df[col])
                if not result.get("valid", True):
                    errs = "; ".join(result.get("errors", []))
                    invalid_count = result.get("invalid_count", 0)
                    warnings.append(
                        f"[{col}] {v_name}: {errs} ({invalid_count:,} invalid rows)"
                    )
            except ValueError:
                warnings.append(f"[{col}] Unknown validator '{v_name}' — skipped")
            except Exception as exc:
                warnings.append(f"[{col}] {v_name} error: {exc}")


# ---------------------------------------------------------------------------
# Dtype Mapping
# ---------------------------------------------------------------------------

def build_dtype_map(bit_columns: list[str], df: pd.DataFrame, db_type: str) -> dict:
    """Return SQLAlchemy dtype overrides for BIT columns per target DB dialect."""
    if not bit_columns:
        return {}

    dtype_map: dict = {}
    if db_type == "PostgreSQL":
        from sqlalchemy.dialects.postgresql import BIT
        for col in bit_columns:
            if col in df.columns:
                dtype_map[col] = BIT(1)
    elif db_type == "MySQL":
        from sqlalchemy.types import Integer
        for col in bit_columns:
            if col in df.columns:
                dtype_map[col] = Integer()
    elif db_type == "Microsoft SQL Server":
        from sqlalchemy.dialects.mssql import BIT as MSSQL_BIT
        for col in bit_columns:
            if col in df.columns:
                dtype_map[col] = MSSQL_BIT()
    return dtype_map


# ---------------------------------------------------------------------------
# Batch Insert
# ---------------------------------------------------------------------------

BATCH_STATEMENT_TIMEOUT_MS = 300_000


def _make_pg_copy_method(target_table: str):
    """
    Returns a pandas to_sql 'method' callable that uses PostgreSQL COPY FROM STDIN.

    COPY streams data as text — no SQL parameter limit, 5-10x faster than INSERT.
    Uses CSV format so Python's csv module handles all quoting/escaping automatically.
    NULL values (Python None) are written as unquoted empty fields, which COPY CSV
    maps to SQL NULL.
    """
    quoted = ".".join(
        f'"{p.strip().strip(chr(34))}"' for p in target_table.split(".")
    )

    def _pg_copy(table, conn, keys, data_iter):
        buf = io.StringIO()
        writer = _csv.writer(buf, lineterminator="\n")
        writer.writerows(data_iter)
        buf.seek(0)
        cols = ", ".join(f'"{k}"' for k in keys)
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            cur.copy_expert(f"COPY {quoted} ({cols}) FROM STDIN WITH CSV", buf)

    return _pg_copy


def batch_insert(
    df: pd.DataFrame,
    target_table: str,
    engine,
    dtype_map: dict = None,
    insert_strategy: str = "append",
    pk_columns: list[str] | None = None,
) -> int:
    """
    Bulk-insert a DataFrame batch into the target table.

    Strategies:
        "append"       — plain INSERT / COPY (default, backward compatible)
        "upsert"       — PostgreSQL INSERT ... ON CONFLICT DO UPDATE
        "upsert_ignore" — PostgreSQL INSERT ... ON CONFLICT DO NOTHING

    PostgreSQL uses COPY FROM STDIN wrapped in an explicit transaction with
    per-connection ``statement_timeout``.  On failure the transaction is fully
    rolled back — no partial rows.

    MySQL / MSSQL use multi-row INSERT inside ``engine.begin()`` (implicit tx).
    """
    if df.empty:
        return 0

    is_pg = "postgresql" in str(engine.url)

    if is_pg:
        if insert_strategy in ("upsert", "upsert_ignore") and pk_columns:
            return _pg_upsert(df, target_table, engine, dtype_map, pk_columns, insert_strategy)

        quoted = ".".join(
            f'"{p.strip().strip(chr(34))}"' for p in target_table.split(".")
        )
        dbapi_conn = None
        conn = None
        try:
            conn = engine.connect()
            dbapi_conn = conn.connection
            dbapi_conn.autocommit = False

            with dbapi_conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {BATCH_STATEMENT_TIMEOUT_MS}")

            buf = io.StringIO()
            writer = _csv.writer(buf, lineterminator="\n")
            cols = list(df.columns)
            writer.writerows(df[cols].itertuples(index=False, name=None))
            buf.seek(0)

            col_list = ", ".join(f'"{c}"' for c in cols)
            with dbapi_conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY {quoted} ({col_list}) FROM STDIN WITH CSV",
                    buf,
                )
            dbapi_conn.commit()
        except Exception:
            if dbapi_conn:
                try:
                    dbapi_conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if dbapi_conn:
                try:
                    dbapi_conn.autocommit = True
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    else:
        with engine.begin() as conn:
            df.to_sql(
                name=target_table, con=conn, if_exists="append",
                index=False, method="multi", dtype=dtype_map or None,
            )

    return len(df)


def _pg_upsert(
    df: pd.DataFrame,
    target_table: str,
    engine,
    dtype_map: dict,
    pk_columns: list[str],
    insert_strategy: str,
) -> int:
    """COPY into a temp staging table, then INSERT ... ON CONFLICT.

    The temp table is ``ON COMMIT DROP`` so it auto-cleans even on error.
    Staging columns use TEXT to avoid type mismatch during COPY — PostgreSQL
    casts to target column types during the INSERT.
    """
    quoted = ".".join(
        f'"{p.strip().strip(chr(34))}"' for p in target_table.split(".")
    )
    pk_cols = ", ".join(f'"{k}"' for k in pk_columns)

    dbapi_conn = None
    conn = None
    try:
        conn = engine.connect()
        dbapi_conn = conn.connection
        dbapi_conn.autocommit = False

        with dbapi_conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {BATCH_STATEMENT_TIMEOUT_MS}")

        buf = io.StringIO()
        writer = _csv.writer(buf, lineterminator="\n")
        keys = list(df.columns)
        writer.writerows(df[keys].itertuples(index=False, name=None))
        buf.seek(0)

        cols = ", ".join(f'"{k}"' for k in keys)
        col_defs = ", ".join(f'"{k}" TEXT' for k in keys)
        updates = ", ".join(
            f'"{k}" = EXCLUDED."{k}"' for k in keys if k not in pk_columns
        )

        with dbapi_conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMP TABLE IF NOT EXISTS _upsert_staging "
                f"({col_defs}) ON COMMIT DROP"
            )
            cur.execute("TRUNCATE _upsert_staging")
            cur.copy_expert(
                f"COPY _upsert_staging({cols}) FROM STDIN WITH CSV", buf
            )

            if insert_strategy == "upsert":
                sql = (
                    f"INSERT INTO {quoted} ({cols}) "
                    f"SELECT {cols} FROM _upsert_staging "
                    f"ON CONFLICT ({pk_cols}) DO UPDATE SET {updates}"
                )
            else:
                sql = (
                    f"INSERT INTO {quoted} ({cols}) "
                    f"SELECT {cols} FROM _upsert_staging "
                    f"ON CONFLICT ({pk_cols}) DO NOTHING"
                )
            cur.execute(sql)

        dbapi_conn.commit()
    except Exception:
        if dbapi_conn:
            try:
                dbapi_conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if dbapi_conn:
            try:
                dbapi_conn.autocommit = True
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

    return len(df)
