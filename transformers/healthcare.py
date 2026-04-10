"""Healthcare transformers - Healthcare-specific transformations."""
import pandas as pd
import random
from transformers.registry import register_transformer


@register_transformer("GENERATE_HN", "Generate HN", "Generate hospital number", has_params=True)
def generate_hn(series: pd.Series, params=None) -> pd.Series:
    """
    Generate Hospital Numbers (HN).

    Params:
        prefix: HN prefix (default: '')
        format: Format pattern (default: 'digits')
        length: Number of digits (default: 9)
    """
    if params is None:
        params = {}
    prefix = params.get("prefix", "")
    length = params.get("length", 9)

    def generate_hn_value(val):
        if pd.notna(val) and str(val).strip() != '':
            return val  # Keep existing value
        digits = ''.join([str(random.randint(0, 9)) for _ in range(length)])
        return f"{prefix}{digits}"

    return series.apply(generate_hn_value)


@register_transformer("MAP_GENDER", "Map Gender", "Map gender codes to standard values", has_params=True)
def map_gender(series: pd.Series, params=None) -> pd.Series:
    """
    Map gender codes to standard values (M/F/Other).

    Params:
        mapping: Dict mapping input values to output values
        Default maps: 1, M, Male, ชาย → M
                      2, F, Female, หญิง → F
    """
    if params is None:
        params = {}

    default_mapping = {
        '1': 'M', 'M': 'M', 'Male': 'M', 'MALE': 'M', 'male': 'M',
        'ชาย': 'M', 'ผู้ชาย': 'M',
        '2': 'F', 'F': 'F', 'Female': 'F', 'FEMALE': 'F', 'female': 'F',
        'หญิง': 'F', 'ผู้หญิง': 'F',
    }

    mapping = params.get("mapping", default_mapping)
    mapping_upper = {str(k).upper(): v for k, v in mapping.items()}

    def map_value(val):
        if pd.isna(val):
            return val
        val_upper = str(val).upper()
        return mapping_upper.get(val_upper, val)

    return series.apply(map_value)


@register_transformer("FORMAT_PHONE", "Format Phone", "Format phone numbers", has_params=True)
def format_phone(series: pd.Series, params=None) -> pd.Series:
    """
    Format phone numbers to standard format.

    Params:
        country_code: Country code to add (default: None)
        format: Format pattern (default: 'digits_only')
    """
    if params is None:
        params = {}

    def clean_phone(val):
        if pd.isna(val):
            return val
        # Keep only digits
        digits = ''.join(c for c in str(val) if c.isdigit())
        if len(digits) >= 9:
            return digits
        return val

    return series.apply(clean_phone)
