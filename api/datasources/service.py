"""Datasources service — handles CRUD for datasources table."""

from __future__ import annotations

from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import datasource_repo
from models.datasource import DatasourceRecord


class DatasourcesService(BaseService):
    """Service for datasources CRUD operations."""

    resource_type = "datasources"
    allowed_fields = ["id", "name", "db_type", "host", "port", "dbname", "username"]

    def _strip_password(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k != "password"}

    def find_all(self, params: QueryParams) -> dict:
        """List all datasources with pagination."""
        total_records = self.execute_db_operation(lambda: datasource_repo.count_all())
        data = self.execute_db_operation(lambda: datasource_repo.get_all_list())
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)

        return {
            "data": page_data,
            "total": total,
            "total_records": total_records,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str | int) -> dict:
        """Get datasource by ID."""
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        self._assert_found(result, id)
        return self._sanitize_response(self._strip_password(result))

    def create(self, data: dict) -> dict:
        """Create new datasource."""
        record = DatasourceRecord(
            name=data.get("name", ""),
            db_type=data.get("db_type", ""),
            host=data.get("host", ""),
            port=data.get("port", ""),
            dbname=data.get("dbname", ""),
            username=data.get("username", ""),
            password=data.get("password", ""),
        )
        new_id = self.execute_db_operation(lambda: datasource_repo.save(record))
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(new_id))
        return self._sanitize_response(self._strip_password(result))

    def update(self, id: str | int, data: dict) -> dict:
        """Update datasource (patch — missing fields fall back to existing values)."""
        existing = self.find_by_id(id)
        record = DatasourceRecord(
            name=data.get("name") if "name" in data else existing.get("name", ""),
            db_type=data.get("db_type") if "db_type" in data else existing.get("db_type", ""),
            host=data.get("host") if "host" in data else existing.get("host", ""),
            port=data.get("port") if "port" in data else existing.get("port", ""),
            dbname=data.get("dbname") if "dbname" in data else existing.get("dbname", ""),
            username=data.get("username") if "username" in data else existing.get("username", ""),
            password=data.get("password") if "password" in data else existing.get("password", ""),
        )
        ok, msg = self.execute_db_operation(lambda: datasource_repo.update(id, record))
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        return self._sanitize_response(self._strip_password(result))

    def delete(self, id: str | int) -> None:
        """Delete datasource."""
        self.find_by_id(id)
        self.execute_db_operation(lambda: datasource_repo.delete(id))
