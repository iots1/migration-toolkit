"""
Models — pure Python dataclasses representing domain objects.

No Streamlit, no SQLAlchemy, no side effects.
These are the shapes that flow between Service → View layers.
"""
from models.migration_config import MigrationConfig, MappingItem
from models.datasource import Datasource

__all__ = ["MigrationConfig", "MappingItem", "Datasource"]
