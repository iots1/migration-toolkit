"""
MigrationConfig — domain model for a mapping configuration.

Parsed from the JSON blob stored in SQLite `configs` table.
"""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class MappingItem:
    source: str
    target: str
    transformers: list[str] = field(default_factory=list)
    validators: list[str] = field(default_factory=list)
    ignore: bool = False
    transformer_params: dict = field(default_factory=dict)
    default_value: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "MappingItem":
        return cls(
            source=d.get("source", ""),
            target=d.get("target", ""),
            transformers=d.get("transformers", []),
            validators=d.get("validators", []),
            ignore=d.get("ignore", False),
            transformer_params=d.get("transformer_params", {}),
            default_value=d.get("default_value", ""),
        )

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "transformers": self.transformers,
            "validators": self.validators,
            "ignore": self.ignore,
            "transformer_params": self.transformer_params,
            "default_value": self.default_value,
        }


@dataclass
class MigrationConfig:
    config_name: str
    source_database: str
    source_table: str
    target_database: str
    target_table: str
    mappings: list[MappingItem] = field(default_factory=list)
    batch_size: int = 1000

    @classmethod
    def from_dict(cls, d: dict) -> "MigrationConfig":
        return cls(
            config_name=d.get("config_name", ""),
            source_database=d.get("source", {}).get("database", ""),
            source_table=d.get("source", {}).get("table", ""),
            target_database=d.get("target", {}).get("database", ""),
            target_table=d.get("target", {}).get("table", ""),
            mappings=[MappingItem.from_dict(m) for m in d.get("mappings", [])],
            batch_size=d.get("batch_size", 1000),
        )

    def to_dict(self) -> dict:
        return {
            "config_name": self.config_name,
            "source": {"database": self.source_database, "table": self.source_table},
            "target": {"database": self.target_database, "table": self.target_table},
            "mappings": [m.to_dict() for m in self.mappings],
            "batch_size": self.batch_size,
        }
