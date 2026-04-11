"""Configs service — handles CRUD for configs table."""

from __future__ import annotations

import json
from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import config_repo


class ConfigsService(BaseService):
    """Service for configs CRUD operations."""

    resource_type = "configs"
    allowed_fields = [
        "id", "config_name", "table_name", "json_data",
        "created_at", "created_by", "updated_at", "updated_by",
        "is_deleted", "deleted_at", "deleted_by", "deleted_reason",
    ]

    def find_all(self, params: QueryParams) -> dict:
        """List all configs with pagination."""
        data = self.execute_db_operation(lambda: config_repo.get_all_list())
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)

        return {
            "data": page_data,
            "total": total,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str) -> dict:
        """Get config by name (id is treated as config_name)."""
        result = self.execute_db_operation(lambda: config_repo.get_content(id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def create(self, data: dict) -> dict:
        """Create new config."""
        config_name = data.get("config_name", "")
        table_name = data.get("table_name", "")
        json_data = data.get("json_data", "{}")

        # Convert dict to JSON string if needed
        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        ok, msg = self.execute_db_operation(
            lambda: config_repo.save(config_name, table_name, json_data)
        )
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: config_repo.get_content(config_name))
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update config (upsert)."""
        existing = self.find_by_id(id)

        table_name = data.get("table_name") or existing.get("table_name", "")
        json_data = data.get("json_data") or existing.get("json_data", "{}")

        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        ok, msg = self.execute_db_operation(
            lambda: config_repo.save(id, table_name, json_data)
        )
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: config_repo.get_content(id))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """Delete config."""
        self.find_by_id(id)
        ok, msg = self.execute_db_operation(lambda: config_repo.delete(id))
        self._assert_success(ok, msg)

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
