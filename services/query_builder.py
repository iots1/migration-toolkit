"""
Query Builder — builds SELECT queries and handles batch ETL operations.

Responsibility (SRP): SQL generation + DataFrame transformation pipeline
for migration batches. No Streamlit dependencies.
"""
import pandas as pd
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
            # Edge case: only GENERATE_HN mappings — select one anchor column
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

    # Build rename map; skip columns already created by a transformer
    rename_map: dict[str, str] = {}
    transformer_created: list[str] = []

    for m in config.get("mappings", []):
        if m.get("ignore", False) or "target" not in m:
            continue
        src, tgt = m["source"], m["target"]
        if src not in df.columns or src == tgt:
            continue
        if tgt in df.columns:
            transformer_created.append(src)  # transformer already wrote target col
        else:
            rename_map[src] = tgt

    if transformer_created:
        df = df.drop(columns=transformer_created, errors="ignore")
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Drop ignored target columns
    ignored = [m["target"] for m in config.get("mappings", []) if m.get("ignore", False)]
    df = df.drop(columns=[c for c in ignored if c in df.columns], errors="ignore")

    # Normalise column names (lowercase, deduplicate)
    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    # Identify BIT columns and normalise their values to "0"/"1"
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

    # ---------------------------------------------------------------------------
    # Validators — run after transformers so we validate the final output value
    # ---------------------------------------------------------------------------
    validation_warnings: list[str] = []
    _run_validators(df, config, validation_warnings)

    return df, bit_columns, validation_warnings


def _run_validators(df: pd.DataFrame, config: dict, warnings: list[str]) -> None:
    """Run registered validators for each mapping that has validators configured.

    Appends human-readable warning strings to *warnings* in-place.
    Never raises — validator errors are captured as warnings so migration continues.
    """
    import validators as _vld_pkg  # noqa: F401 — side-effect: registers all validators
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


def _make_pg_copy_method(target_table: str):
    """
    Returns a pandas to_sql 'method' callable that uses PostgreSQL COPY FROM STDIN.

    COPY streams data as text — no SQL parameter limit, 5-10x faster than INSERT.
    Uses CSV format so Python's csv module handles all quoting/escaping automatically.
    NULL values (Python None) are written as unquoted empty fields, which COPY CSV
    maps to SQL NULL.

    target_table is captured in the closure so the COPY statement uses the correct
    schema-qualified, double-quoted identifier even if pandas strips the schema.
    """
    import io
    import csv as _csv

    quoted = ".".join(
        f'"{p.strip().strip(chr(34))}"' for p in target_table.split(".")
    )

    def _pg_copy(table, conn, keys, data_iter):
        buf = io.StringIO()
        writer = _csv.writer(buf, lineterminator="\n")
        writer.writerows(data_iter)
        buf.seek(0)
        cols = ", ".join(f'"{k}"' for k in keys)
        dbapi_conn = conn.connection  # psycopg2 DBAPI connection
        with dbapi_conn.cursor() as cur:
            cur.copy_expert(f"COPY {quoted} ({cols}) FROM STDIN WITH CSV", buf)

    return _pg_copy


def batch_insert(df: pd.DataFrame, target_table: str, engine, dtype_map: dict = None) -> int:
    """
    Bulk-insert a DataFrame batch into the target table.

    PostgreSQL → COPY FROM STDIN (CSV): streams data directly, no parameter limit,
    5-10x faster than INSERT. dtype_map is not used (PG casts text inputs automatically).

    MySQL / MSSQL → multi-row INSERT with parameter-safe chunksize:
    PostgreSQL's 65,535 parameter limit doesn't apply, but we guard MySQL/MSSQL too.

    Returns number of rows inserted (0 if DataFrame is empty).
    """
    if df.empty:
        return 0

    is_pg = "postgresql" in str(engine.url)

    if is_pg:
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method=_make_pg_copy_method(target_table),
            dtype=dtype_map or None,
        )
    else:
        # Guard against parameter-count limits on MySQL / MSSQL
        n_cols = max(len(df.columns), 1)
        safe_chunksize = max(1, 60_000 // n_cols)
        df.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=min(safe_chunksize, 2000),
            dtype=dtype_map or None,
        )

    return len(df)
