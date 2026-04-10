"""Name transformers - Name parsing and formatting."""
import pandas as pd
from transformers.registry import register_transformer


@register_transformer("REMOVE_PREFIX", "Remove Prefix", "Remove name prefixes (Mr., Ms., etc.)", has_params=True)
def remove_prefix(series: pd.Series, params=None) -> pd.Series:
    """
    Remove common name prefixes.

    Params:
        prefixes: List of prefixes to remove (default: Thai and English titles)
        language: 'thai', 'english', or 'both' (default: 'both')
    """
    if params is None:
        params = {}

    thai_prefixes = ['นาย', 'นาง', 'นางสาว', 'ด.ช.', 'ด.ญ.', 'ศ.', 'ด.ศ.', 'พ.ต.', 'พ.ต.ท.']
    english_prefixes = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Prof.', 'Assoc.', 'Asst.']

    prefixes = params.get("prefixes", thai_prefixes + english_prefixes)

    def remove_prefix_val(val):
        if pd.isna(val):
            return val
        val = str(val).strip()
        for prefix in prefixes:
            if val.startswith(prefix):
                return val[len(prefix):].strip()
        return val

    return series.apply(remove_prefix_val)


@register_transformer("SPLIT_THAI_NAME", "Split Thai Name", "Split Thai name into first/last", has_params=True)
def split_thai_name(series: pd.Series, params=None) -> pd.Series:
    """
    Split Thai name into first name and last name.

    Params:
        output: 'first', 'last', or 'both' (default: 'first')
    """
    if params is None:
        params = {}
    output = params.get("output", "first")

    def split_name(val):
        if pd.isna(val):
            return val
        parts = str(val).strip().split()
        if len(parts) >= 2:
            if output == "first":
                return parts[0]
            elif output == "last":
                return " ".join(parts[1:])
            else:  # both
                return {"first": parts[0], "last": " ".join(parts[1:])}
        return val

    return series.apply(split_name)


@register_transformer("SPLIT_ENG_NAME", "Split English Name", "Split English name into first/last", has_params=True)
def split_eng_name(series: pd.Series, params=None) -> pd.Series:
    """
    Split English name into first name and last name.

    Params:
        output: 'first', 'last', or 'both' (default: 'first')
    """
    if params is None:
        params = {}
    output = params.get("output", "first")

    def split_name(val):
        if pd.isna(val):
            return val
        parts = str(val).strip().split()
        if len(parts) >= 2:
            if output == "first":
                return parts[0]
            elif output == "last":
                return " ".join(parts[1:])
            else:  # both
                return {"first": parts[0], "last": " ".join(parts[1:])}
        return val

    return series.apply(split_name)
