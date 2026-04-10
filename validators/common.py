"""Common validators."""
import pandas as pd
from validators.registry import register_validator


@register_validator("MIN_LENGTH_13", "Min Length 13", "Minimum length 13 characters")
def validate_min_length_13(series: pd.Series, params=None) -> dict:
    """Validate minimum length of 13."""
    invalid_mask = series.astype(str).str.len() < 13
    invalid_count = int(invalid_mask.sum())

    if invalid_count > 0:
        invalid_indices = series[invalid_mask].index.tolist()
        return {
            "valid": False,
            "errors": [f"Found {invalid_count} values with length < 13"],
            "invalid_count": invalid_count,
            "invalid_indices": invalid_indices[:100]
        }

    return {
        "valid": True,
        "errors": [],
        "invalid_count": 0,
        "invalid_indices": []
    }


@register_validator("IS_EMAIL", "Is Email", "Validate email format")
def validate_is_email(series: pd.Series, params=None) -> dict:
    """Validate email format."""
    import re

    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    def is_valid_email(val):
        if pd.isna(val):
            return True  # Null values are ok (use REQUIRED validator separately)
        return bool(re.match(email_pattern, str(val)))

    invalid_mask = ~series.apply(is_valid_email)
    invalid_count = int(invalid_mask.sum())

    if invalid_count > 0:
        invalid_indices = series[invalid_mask].index.tolist()
        return {
            "valid": False,
            "errors": [f"Found {invalid_count} invalid email addresses"],
            "invalid_count": invalid_count,
            "invalid_indices": invalid_indices[:100]
        }

    return {
        "valid": True,
        "errors": [],
        "invalid_count": 0,
        "invalid_indices": []
    }
