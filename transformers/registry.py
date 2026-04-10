"""
Transformer registry - Central registry for data transformers.

Provides decorator-based registration pattern for transformers.
"""
from typing import Dict, Callable, Any
import pandas as pd

# Internal transformer storage
_transformers: Dict[str, Callable] = {}
_labels: Dict[str, str] = {}
_descriptions: Dict[str, str] = {}
_has_params: Dict[str, bool] = {}


def register_transformer(
    name: str,
    label: str,
    description: str = "",
    has_params: bool = False
):
    """
    Decorator to register a transformer function.

    Args:
        name: Unique transformer name (e.g., 'TRIM', 'BUDDHIST_TO_ISO')
        label: Human-readable label for UI
        description: Description of what transformer does
        has_params: Whether transformer accepts parameters

    Example:
        >>> @register_transformer("TRIM", "Trim", "Remove leading/trailing whitespace")
        ... def trim(series, params=None):
        ...     return series.astype(str).str.strip()
    """
    def decorator(fn: Callable[[Any, Any | None], Any]) -> Callable[[Any, Any | None], Any]:
        _transformers[name] = fn
        _labels[name] = label
        _descriptions[name] = description
        _has_params[name] = has_params
        return fn
    return decorator


def get_transformer(name: str) -> Callable:
    """
    Get a registered transformer by name.

    Args:
        name: Transformer name

    Returns:
        Callable: Transformer function

    Raises:
        ValueError: If transformer not found

    Example:
        >>> trim = get_transformer("TRIM")
        >>> result = trim(series)
    """
    if name not in _transformers:
        available = ", ".join(_transformers.keys())
        raise ValueError(f"Unknown transformer: {name}. Available: {available}")
    return _transformers[name]


def get_transformer_options() -> list[Dict]:
    """
    Get list of all registered transformers.

    Returns:
        list[dict]: List of transformer info dictionaries
            [
                {"name": "TRIM", "label": "Trim", "description": "...", "has_params": False},
                ...
            ]

    Example:
        >>> options = get_transformer_options()
        >>> for opt in options:
        ...     print(f"{opt['label']}: {opt['description']}")
    """
    return [
        {
            "name": name,
            "label": _labels[name],
            "description": _descriptions[name],
            "has_params": _has_params[name]
        }
        for name in _transformers
    ]


def transform_batch(series: pd.Series, transformer_names: list[str], params_dict: Dict[str, Any] | None = None) -> pd.Series:
    """
    Apply multiple transformers to a series in batch.

    Args:
        series: pandas Series to transform
        transformer_names: List of transformer names to apply
        params_dict: Optional dict mapping transformer names to their parameters

    Returns:
        Transformed pandas Series

    Example:
        >>> result = transform_batch(
        ...     df['column'],
        ...     ["TRIM", "UPPER", "BUDDHIST_TO_ISO"],
        ...     {"BUDDHIST_TO_ISO": {"source_format": "BE"}}
        ... )
    """
    result = series
    params_dict = params_dict or {}

    for name in transformer_names:
        transformer = get_transformer(name)
        params = params_dict.get(name)
        result = transformer(result, params)

    return result
