"""
Encoding Helper — cleans charset issues from source database values.

Responsibility (SRP): byte/string encoding normalisation for ETL pipelines.
Used when source DB has CHAR padding, mixed encodings, or control characters.
"""
import pandas as pd


def clean_value(value) -> object:
    """Decode bytes and strip non-printable characters from a single value."""
    if value is None:
        return value
    if isinstance(value, bytes):
        for enc in ("utf-8", "latin-1"):
            try:
                value = value.decode(enc)
                break
            except UnicodeDecodeError:
                pass
        else:
            value = str(value)
    if isinstance(value, str):
        value = value.replace("\xa0", " ").replace("\x00", "").replace("\x85", "...")
        value = "".join(c if c in "\t\n\r" or ord(c) >= 32 else "" for c in value)
    return value


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply clean_value to all object-typed columns in a DataFrame batch."""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(clean_value)
    return df


def _try_fix_single(value: str) -> str:
    """
    Attempt to re-decode a string that was mis-read as latin1 but was actually
    TIS-620/CP874 (Thai legacy encoding). Returns the fixed string if the result
    is fully printable, otherwise returns the original.
    """
    try:
        fixed = value.encode("latin1").decode("cp874")
        if all(c.isprintable() or c in "\t\n\r" for c in fixed):
            return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    return value


def fix_thai_encoding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Heuristic fix for DataFrames where Thai text (TIS-620/CP874) was fetched
    via a latin1 MySQL connection and appears garbled.

    Applies only to object columns. Each cell is tested: if re-encoding
    latin1→cp874 yields a fully-printable string, the fixed value is used.
    Safe to call on already-correct UTF-8 data (the heuristic is conservative).
    """
    df = df.copy()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda v: _try_fix_single(v) if isinstance(v, str) else v
        )
    return df
