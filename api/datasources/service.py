"""Datasources service — handles CRUD for datasources table."""

from __future__ import annotations

from fastapi import HTTPException

from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import datasource_repo
from models.datasource import DatasourceRecord
from services.schema_inspector import (
    get_tables_from_datasource,
    get_columns_from_table,
    _safe_id,
)


class DatasourcesService(BaseService):
    """Service for datasources CRUD operations."""

    resource_type = "datasources"
    allowed_fields = ["id", "name", "db_type", "host", "port", "dbname", "username"]

    def _count_all(self) -> int:
        return datasource_repo.count_all()

    def _list_all(self) -> list[dict]:
        return datasource_repo.get_all_list()

    def _strip_password(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k != "password"}

    def find_by_id(self, id: str | int) -> dict:
        """Get datasource by ID (public API — strips password)."""
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        self._assert_found(result, id)
        return self._sanitize_response(self._strip_password(result))

    def resolve_datasource(self, datasource_id: str) -> dict:
        """Get full datasource record including password (for internal use)."""
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(datasource_id))
        self._assert_found(result, datasource_id)
        return result

    def create(self, data: dict) -> dict:
        """Create new datasource."""
        record = DatasourceRecord(**self._merge_fields(data, {}, [
            "name", "db_type", "host", "port", "dbname", "username", "password",
        ]))
        new_id = self.execute_db_operation(lambda: datasource_repo.save(record))
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(new_id))
        return self._sanitize_response(self._strip_password(result))

    def update(self, id: str | int, data: dict) -> dict:
        """Update datasource (patch — missing fields fall back to existing values)."""
        existing = self.find_by_id(id)
        merged = self._merge_fields(data, existing, [
            "name", "db_type", "host", "port", "dbname", "username", "password",
        ])
        record = DatasourceRecord(**merged)
        ok, msg = self.execute_db_operation(lambda: datasource_repo.update(id, record))
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        return self._sanitize_response(self._strip_password(result))

    def delete(self, id: str | int) -> None:
        """Delete datasource."""
        self.find_by_id(id)
        self.execute_db_operation(lambda: datasource_repo.delete(id))

    def get_tables(self, datasource_id: str) -> list[dict]:
        """Get list of tables for a datasource."""
        ds = self.resolve_datasource(datasource_id)
        kw = self._to_connection_kwargs(ds)
        ok, result = get_tables_from_datasource(**kw)
        if not ok:
            raise HTTPException(status_code=500, detail=result)
        return [{"name": t} for t in result]

    def get_columns(self, datasource_id: str, table_name: str) -> list[dict]:
        """Get column details for a table in a datasource."""
        ds = self.resolve_datasource(datasource_id)
        _safe_id(table_name)
        kw = self._to_connection_kwargs(ds)
        ok, result = get_columns_from_table(**kw, table_name=table_name)
        if not ok:
            raise HTTPException(status_code=500, detail=result)
        return result

    @staticmethod
    def _to_connection_kwargs(ds: dict) -> dict:
        return {
            "db_type": ds["db_type"],
            "host": ds["host"],
            "port": ds["port"],
            "db_name": ds["dbname"],
            "user": ds["username"],
            "password": ds["password"],
        }
