"""Configs service — handles CRUD for configs table."""

from __future__ import annotations

import json
from api.base.service import BaseService
from api.base.query_params import QueryParams
from api.base.exceptions import BusinessRuleValidationException
from repositories import config_repo
from repositories import pipeline_node_repo
from models.migration_config import ConfigRecord


class ConfigsService(BaseService):
    """Service for configs CRUD operations."""

    resource_type = "configs"
    allowed_fields = [
        "id",
        "config_name",
        "table_name",
        "json_data",
        "datasource_source_id",
        "datasource_target_id",
        "config_type",
        "script",
        "generate_sql",
        "condition",
        "lookup",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "is_deleted",
        "deleted_at",
        "deleted_by",
        "deleted_reason",
        "datasource_source_name",
        "datasource_source_db_type",
        "datasource_source_dbname",
        "datasource_target_name",
        "datasource_target_db_type",
        "datasource_target_dbname",
    ]

    def _count_all(self) -> int:
        return config_repo.count_all()

    def _list_all(self) -> list[dict]:
        return config_repo.get_all_list()

    def find_by_id(self, id: str) -> dict:
        """Get config by UUID."""
        result = self.execute_db_operation(lambda: config_repo.get_by_id_raw(id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def create(self, data: dict) -> dict:
        """Create new config."""
        record = self._to_record(data)
        ok, msg = self.execute_db_operation(lambda: config_repo.save(record))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(
            lambda: config_repo.get_content(record.config_name)
        )
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update config (patch — existing values are preserved for missing fields)."""
        existing = self.find_by_id(id)
        record = self._to_record(
            data,
            existing=existing,
        )
        ok, msg = self.execute_db_operation(lambda: config_repo.save(record, id))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(lambda: config_repo.get_by_id_raw(id))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """
        Delete config with validation check.

        Validates that the config is not being used in any active pipelines
        before proceeding with deletion.
        """
        existing = self.find_by_id(id)

        # Check if config is in use by any pipelines
        pipelines = self.execute_db_operation(
            lambda: pipeline_node_repo.get_pipelines_using_config(id)
        )

        if pipelines:
            # Build error message with pipeline details
            pipeline_details = []
            for pipeline in pipelines:
                pipeline_details.append(
                    f"  - {pipeline['pipeline_name']} (ID: {pipeline['pipeline_id']})"
                )

            error_msg = (
                "Config is in use by the following pipelines:\n"
                + "\n".join(pipeline_details)
                + "\nPlease remove it from these pipelines first."
            )
            raise BusinessRuleValidationException(error_msg)

        # Proceed with deletion
        ok, msg = self.execute_db_operation(
            lambda: config_repo.delete(existing["config_name"])
        )
        self._assert_success(ok, msg)

    def duplicate(self, id: str) -> dict:
        existing = self.find_by_id(id)
        new_name = f"{existing['config_name']} copy"
        return self.create({
            "config_name": new_name,
            "table_name": existing.get("table_name", ""),
            "json_data": existing.get("json_data", {}),
            "datasource_source_id": existing.get("datasource_source_id"),
            "datasource_target_id": existing.get("datasource_target_id"),
            "config_type": existing.get("config_type", "std"),
            "script": existing.get("script"),
            "generate_sql": existing.get("generate_sql"),
            "condition": existing.get("condition"),
            "lookup": existing.get("lookup"),
        })

    def get_history(self, config_name: str) -> list[dict]:
        """Get config version history."""
        df = self.execute_db_operation(lambda: config_repo.get_history(config_name))
        return self._df_to_list(df)

    def get_version(self, config_name: str, version: int) -> dict:
        """Get specific config version."""
        result = self.execute_db_operation(
            lambda: config_repo.get_version(config_name, version)
        )
        self._assert_found(result, f"{config_name}:v{version}")
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_record(
        self,
        data: dict,
        *,
        existing: dict | None = None,
        config_name_override: str | None = None,
    ) -> ConfigRecord:
        """Build a ConfigRecord from request data, falling back to existing values."""
        ex = existing or {}

        json_data = data.get("json_data") if "json_data" in data else ex.get("json_data", "{}")
        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        return ConfigRecord(
            config_name=config_name_override or data.get("config_name", ""),
            table_name=data.get("table_name") if "table_name" in data else ex.get("table_name", ""),
            json_data=json_data,
            datasource_source_id=data.get("datasource_source_id")
            if "datasource_source_id" in data
            else ex.get("datasource_source_id"),
            datasource_target_id=data.get("datasource_target_id")
            if "datasource_target_id" in data
            else ex.get("datasource_target_id"),
            config_type=data.get("config_type") if "config_type" in data else ex.get("config_type", "std"),
            script=data.get("script") if "script" in data else ex.get("script"),
            generate_sql=data.get("generate_sql") if "generate_sql" in data else ex.get("generate_sql"),
            condition=data.get("condition") if "condition" in data else ex.get("condition"),
            lookup=data.get("lookup") if "lookup" in data else ex.get("lookup"),
        )
