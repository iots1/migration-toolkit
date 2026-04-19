"""
Protocols package - Abstract interfaces for dependency inversion.

This package defines Protocol interfaces following DIP (Dependency Inversion Principle).
All controllers depend on these protocols, not concrete implementations.
"""
from protocols.repository import (
    DatasourceRepository,
    ConfigRepository,
    PipelineRepository,
    PipelineRunRepository,
)

__all__ = [
    "DatasourceRepository",
    "ConfigRepository",
    "PipelineRepository",
    "PipelineRunRepository",
]
