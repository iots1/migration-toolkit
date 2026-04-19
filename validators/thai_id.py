"""Thai ID validator."""
import pandas as pd
from validators.registry import register_validator


@register_validator("THAI_ID", "Thai ID", "Validate Thai Citizen ID (13 digits)")
def validate_thai_id(series: pd.Series, params=None) -> dict:
    """Validate Thai Citizen ID format."""
    import re

    def is_valid_thai_id(id_val):
        if pd.isna(id_val):
            return False
        id_str = str(id_val).strip()
        if not re.match(r'^\d{13}$', id_str):
            return False
        # Checksum validation
        digits = [int(d) for d in id_str]
        weighted_sum = sum(digits[i] * (13 - i) for i in range(12))
        checksum = (11 - (weighted_sum % 11)) % 10
        return checksum == digits[12]

    invalid_mask = ~series.apply(is_valid_thai_id)
    invalid_count = int(invalid_mask.sum())

    if invalid_count > 0:
        invalid_indices = series[invalid_mask].index.tolist()
        return {
            "valid": False,
            "errors": [f"Found {invalid_count} invalid Thai IDs"],
            "invalid_count": invalid_count,
            "invalid_indices": invalid_indices[:100]
        }

    return {
        "valid": True,
        "errors": [],
        "invalid_count": 0,
        "invalid_indices": []
    }
