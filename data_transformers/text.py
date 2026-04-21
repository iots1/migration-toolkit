"""Text transformers - String manipulation transformations."""
import pandas as pd
import re
from data_transformers.registry import register_transformer


@register_transformer("TRIM", "Trim", "Remove leading/trailing whitespace")
def trim(series: pd.Series, params=None) -> pd.Series:
    """Remove leading and trailing whitespace from strings."""
    return series.astype(str).str.strip()


@register_transformer("UPPER", "Upper Case", "Convert to uppercase")
def upper(series: pd.Series, params=None) -> pd.Series:
    """Convert strings to uppercase."""
    return series.astype(str).str.upper()


@register_transformer("UPPER_TRIM", "Upper & Trim", "Trim and convert to uppercase")
def upper_trim(series: pd.Series, params=None) -> pd.Series:
    """Trim whitespace then convert to uppercase."""
    return series.astype(str).str.strip().str.upper()


@register_transformer("LOWER_TRIM", "Lower & Trim", "Trim and convert to lowercase")
def lower_trim(series: pd.Series, params=None) -> pd.Series:
    """Trim whitespace then convert to lowercase."""
    return series.astype(str).str.strip().str.lower()


@register_transformer("CLEAN_SPACES", "Clean Spaces", "Remove extra whitespace")
def clean_spaces(series: pd.Series, params=None) -> pd.Series:
    """Replace multiple spaces with single space and trim."""
    return series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()


@register_transformer("CONCAT", "Concatenate", "Concatenate with prefix/suffix", has_params=True)
def concat(series: pd.Series, params=None) -> pd.Series:
    """
    Concatenate strings with prefix and/or suffix.

    Params:
        prefix: String to prepend
        suffix: String to append
    """
    if params is None:
        params = {}
    prefix = params.get("prefix", "")
    suffix = params.get("suffix", "")
    return prefix + series.astype(str) + suffix


@register_transformer("DEFAULT_VALUE", "Default Value", "Fill null/empty values with default", has_params=True)
def default_value(series: pd.Series, params=None) -> pd.Series:
    """
    Fill null and empty string values with a default value.

    Params:
        value: Default value to use for null/empty cells
    """
    if params is None:
        params = {}
    default_val = params.get("value", None)

    def fill_null(val):
        if pd.isna(val) or val == '':
            return default_val
        return val

    return series.apply(fill_null)
