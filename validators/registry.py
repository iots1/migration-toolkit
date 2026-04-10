"""Validator registry - Central registry for data validators."""
from typing import Dict, Callable
import pandas as pd

_validators: Dict[str, Callable] = {}
_labels: Dict[str, str] = {}
_descriptions: Dict[str, str] = {}


def register_validator(name: str, label: str, description: str = ""):
    """Decorator to register a validator function."""
    def decorator(fn):
        _validators[name] = fn
        _labels[name] = label
        _descriptions[name] = description
        return fn
    return decorator


def get_validator(name: str) -> Callable:
    """Get a registered validator by name."""
    if name not in _validators:
        available = ", ".join(_validators.keys())
        raise ValueError(f"Unknown validator: {name}. Available: {available}")
    return _validators[name]


def get_validator_options() -> list[dict]:
    """Get list of all registered validators."""
    return [
        {"name": name, "label": _labels[name], "description": _descriptions[name]}
        for name in _validators
    ]
