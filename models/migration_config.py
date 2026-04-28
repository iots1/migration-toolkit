"""
MigrationConfig — domain models for mapping configuration.

MappingItem / MigrationConfig: domain model for the mapping JSON blob.
ConfigRecord: single source of truth for configs table columns.
              Pass this object to config_repo.save() instead of flat kwargs.
              Adding a new DB column = add field here only.
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
    source_datasource_id: int | None = None
    source_datasource_name: str = ""
    target_datasource_id: int | None = None
    target_datasource_name: str = ""
    config_type: str = "std"
    script: str = ""
    generate_sql: bool = False
    condition: str = ""
    lookup: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "MigrationConfig":
        src = d.get("source", {})
        tgt = d.get("target", {})
        return cls(
            config_name=d.get("config_name", ""),
            source_database=src.get("database", ""),
            source_table=src.get("table", ""),
            target_database=tgt.get("database", ""),
            target_table=tgt.get("table", ""),
            mappings=[MappingItem.from_dict(m) for m in d.get("mappings", [])],
            batch_size=d.get("batch_size", 1000),
            source_datasource_id=src.get("datasource_id"),
            source_datasource_name=src.get("datasource_name", ""),
            target_datasource_id=tgt.get("datasource_id"),
            target_datasource_name=tgt.get("datasource_name", ""),
            config_type=d.get("config_type", "std"),
            script=d.get("script", ""),
            generate_sql=d.get("generate_sql", False),
            condition=d.get("condition", ""),
            lookup=d.get("lookup", ""),
        )

    def to_dict(self) -> dict:
        source: dict = {"database": self.source_database, "table": self.source_table}
        if self.source_datasource_id is not None:
            source["datasource_id"] = self.source_datasource_id
        if self.source_datasource_name:
            source["datasource_name"] = self.source_datasource_name

        target: dict = {"database": self.target_database, "table": self.target_table}
        if self.target_datasource_id is not None:
            target["datasource_id"] = self.target_datasource_id
        if self.target_datasource_name:
            target["datasource_name"] = self.target_datasource_name

        result: dict = {
            "config_name": self.config_name,
            "source": source,
            "target": target,
            "mappings": [m.to_dict() for m in self.mappings],
            "batch_size": self.batch_size,
            "config_type": self.config_type,
            "generate_sql": self.generate_sql,
        }
        if self.script:
            result["script"] = self.script
        if self.condition:
            result["condition"] = self.condition
        if self.lookup:
            result["lookup"] = self.lookup
        return result


@dataclass
class ConfigRecord:
    """
    Single source of truth for the `configs` table columns.

    Pass this to config_repo.save() instead of flat keyword arguments.
    When a new DB column is needed, add the field here — the repo and
    API layer read their column list from this class.

    Fields that map to DB columns (NOT including audit/soft-delete fields
    which are managed by the DB itself):
    """

    config_name: str
    table_name: str = ""
    json_data: str | dict = field(default_factory=dict)
    datasource_source_id: str | None = None
    datasource_target_id: str | None = None
    config_type: str = "std"
    script: str | None = None
    # Stores generated SQL text (not a boolean despite old DDL naming).
    generate_sql: str | None = None
    condition: str | None = None
    lookup: str | None = None
    pk_columns: str | None = None
