"""
Transformers package - Pluggable data transformations (OCP).

This package provides pluggable transformer functions following the
Open/Closed Principle (OCP).

Usage:
    >>> from transformers.registry import get_transformer
    >>> transformer = get_transformer("TRIM")
    >>> result = transformer.transform(series)
"""
from transformers.registry import (
    get_transformer,
    get_transformer_options,
    register_transformer,
)
from transformers.base import DataTransformer

# Import all transformer modules to register them
import transformers.text
import transformers.dates
import transformers.healthcare
import transformers.names
import transformers.data_type
import transformers.lookup

__all__ = [
    "get_transformer",
    "get_transformer_options",
    "register_transformer",
    "DataTransformer",
]
