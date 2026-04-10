"""
Validators package - Pluggable data validation (OCP).

This package provides pluggable validator functions following the
Open/Closed Principle (OCP).

Usage:
    >>> from validators.registry import get_validator
    >>> validator = get_validator("NOT_NULL")
    >>> result = validator.validate(series)
"""
from validators.registry import (
    get_validator,
    get_validator_options,
    register_validator,
)

# Import all validator modules to register them
import validators.required
import validators.thai_id
import validators.common

__all__ = [
    "get_validator",
    "get_validator_options",
    "register_validator",
]
