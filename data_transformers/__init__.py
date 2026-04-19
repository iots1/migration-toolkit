"""
Transformers package - Pluggable data transformations (OCP).

This package provides pluggable transformer functions following the
Open/Closed Principle (OCP).

Usage:
    >>> from data_transformers.registry import get_transformer
    >>> transformer = get_transformer("TRIM")
    >>> result = transformer.transform(series)
"""
from data_transformers.registry import (
    get_transformer,
    get_transformer_options,
    register_transformer,
)
from data_transformers.base import DataTransformer

# Import all transformer modules to register them
import data_transformers.text
import data_transformers.dates
import data_transformers.healthcare
import data_transformers.names
import data_transformers.data_type
import data_transformers.lookup

__all__ = [
    "get_transformer",
    "get_transformer_options",
    "register_transformer",
    "DataTransformer",
]
