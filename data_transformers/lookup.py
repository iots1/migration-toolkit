"""Lookup transformers - Value lookup and replacement."""
import pandas as pd
from data_transformers.registry import register_transformer


@register_transformer("LOOKUP_REPLACE", "Lookup Replace", "Replace values using lookup table", has_params=True)
def lookup_replace(series: pd.Series, params=None) -> pd.Series:
    """
    Replace values using a lookup table.

    Params:
        lookup_table: Dict mapping old values to new values (required)
        default: Default value if not found in lookup (optional)
    """
    if params is None:
        params = {}

    lookup_table = params.get("lookup_table", {})
    default = params.get("default")

    def replace_val(val):
        if pd.isna(val):
            return val
        return lookup_table.get(str(val), default if default is not None else val)

    return series.apply(replace_val)


@register_transformer("VALUE_MAP", "Value Map", "Map values using rules", has_params=True)
def value_map(series: pd.Series, params=None) -> pd.Series:
    """
    Map values based on rules.

    Params:
        rules: List of dicts with 'condition' and 'value'
        Example: [
            {"condition": lambda x: x > 100, "value": "High"},
            {"condition": lambda x: x > 50, "value": "Medium"},
            {"default": "Low"}
        ]
    """
    if params is None:
        params = {}
    rules = params.get("rules", [])

    def apply_rules(val):
        if pd.isna(val):
            return val
        for rule in rules:
            condition = rule.get("condition")
            if callable(condition) and condition(val):
                return rule.get("value")
            elif "default" in rule:
                return rule["default"]
        return val

    return series.apply(apply_rules)
