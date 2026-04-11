"""Datasources service — handles CRUD for datasources table."""

from __future__ import annotations

from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import datasource_repo


class DatasourcesService(BaseService):
    """Service for datasources CRUD operations."""

    resource_type = "datasources"
    allowed_fields = ["id", "name", "db_type", "host", "port", "dbname", "username"]

    def _strip_password(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k != "password"}

    def find_all(self, params: QueryParams) -> dict:
        """List all datasources with pagination."""
        data = self.execute_db_operation(lambda: datasource_repo.get_all_list())
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

    def find_by_id(self, id: str | int) -> dict:
        """Get datasource by ID."""
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        self._assert_found(result, id)
        return self._sanitize_response(self._strip_password(result))

    def create(self, data: dict) -> dict:
        """Create new datasource."""
        ok, msg = self.execute_db_operation(
            lambda: datasource_repo.save(
                name=data.get("name", ""),
                db_type=data.get("db_type", ""),
                host=data.get("host", ""),
                port=data.get("port", ""),
                dbname=data.get("dbname", ""),
                username=data.get("username", ""),
                password=data.get("password", ""),
            )
        )
        self._assert_success(ok, msg)

        # Return created record
        result = self.execute_db_operation(
            lambda: datasource_repo.get_by_name(data["name"])
        )
        return self._sanitize_response(self._strip_password(result))

    def update(self, id: str | int, data: dict) -> dict:
        """Update datasource."""
        # Get existing record first
        existing = self.find_by_id(id)

        # Merge with updates
        updated_data = {**existing, **{k: v for k, v in data.items() if v is not None}}

        ok, msg = self.execute_db_operation(
            lambda: datasource_repo.update(
                ds_id=id,
                name=updated_data.get("name", ""),
                db_type=updated_data.get("db_type", ""),
                host=updated_data.get("host", ""),
                port=updated_data.get("port", ""),
                dbname=updated_data.get("dbname", ""),
                username=updated_data.get("username", ""),
                password=updated_data.get("password", ""),
            )
        )
        self._assert_success(ok, msg)

        # Return updated record
        result = self.execute_db_operation(lambda: datasource_repo.get_by_id(id))
        return self._sanitize_response(self._strip_password(result))

    def delete(self, id: str | int) -> None:
        """Delete datasource."""
        existing = self.find_by_id(id)
        self.execute_db_operation(lambda: datasource_repo.delete(id))
