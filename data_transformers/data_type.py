"""Data type transformers - Type conversions and null handling."""
import pandas as pd
import numpy as np
from data_transformers.registry import register_transformer


@register_transformer("TO_NUMBER", "To Number", "Convert to numeric", has_params=True)
def to_number(series: pd.Series, params=None) -> pd.Series:
    """
    Convert series to numeric type.

    Params:
        decimal: Decimal separator (default: '.')
        errors: 'coerce', 'raise', or 'ignore' (default: 'coerce')
    """
    if params is None:
        params = {}
    decimal = params.get("decimal", ".")
    errors = params.get("errors", "coerce")

    # Replace decimal separator if needed
    if decimal != ".":
        series = series.astype(str).str.replace(decimal, ".")

    return pd.to_numeric(series, errors=errors)


@register_transformer("REPLACE_EMPTY_WITH_NULL", "Empty to Null", "Replace empty strings with NaN")
def replace_empty_with_null(series: pd.Series, params=None) -> pd.Series:
    """Replace empty strings with NaN."""
    return series.replace(r'^\s*$', np.nan, regex=True)


@register_transformer("BIT_CAST", "Bit Cast", "Convert to integer using bit operations", has_params=True)
def bit_cast(series: pd.Series, params=None) -> pd.Series:
    """
    Convert to integer using bitwise operations (for binary flags).

    Params:
        as_type: Target type ('int', 'bool')
    """
    if params is None:
        params = {}
    as_type = params.get("as_type", "int")

    if as_type == "bool":
        return series.notna().astype(int)
    return pd.to_numeric(series, errors="coerce").astype("Int64")
