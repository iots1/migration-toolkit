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
