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

def transform_batch(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list[str]]:
    """
    Apply transformers, rename source→target columns, drop ignored columns.

    Returns:
        (transformed DataFrame, list of BIT column names in target schema)
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

    return df, bit_columns


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

def batch_insert(df: pd.DataFrame, target_table: str, engine, dtype_map: dict = None) -> int:
    """
    Bulk-insert a DataFrame batch using pandas multi-row INSERT.

    Returns number of rows inserted (0 if DataFrame is empty).
    """
    if df.empty:
        return 0
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
        dtype=dtype_map or None,
    )
    return len(df)
