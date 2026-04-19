"""Required field validator."""
import pandas as pd
from validators.registry import register_validator


@register_validator("REQUIRED", "Required", "Field must not be null or empty")
def validate_required(series: pd.Series, params=None) -> dict:
    """Validate that series has no null or empty values."""
    null_count = series.isna().sum()
    empty_count = (series == "").sum() if series.dtype == "object" else 0

    invalid_count = int(null_count + empty_count)

    if invalid_count > 0:
        invalid_indices = series[series.isna() | (series == "")].index.tolist()
        return {
            "valid": False,
            "errors": [f"Found {invalid_count} null or empty values"],
            "invalid_count": invalid_count,
            "invalid_indices": invalid_indices[:100]  # Limit to 100
        }

    return {
        "valid": True,
        "errors": [],
        "invalid_count": 0,
        "invalid_indices": []
    }
