"""
DatasourceRepository — single entry point for all datasource operations.

Eliminates the repeated pattern of:
    ds = db.get_datasource_by_name(name)
    connector.some_function(ds['db_type'], ds['host'], ds['port'], ...)

Usage:
    from services.datasource_repository import DatasourceRepository as DSRepo

    ok, msg = DSRepo.test_connection("MyPostgres")
    engine   = DSRepo.get_engine("MyPostgres")
    ok, tbls = DSRepo.get_tables("MyPostgres")
    ok, cols = DSRepo.get_columns("MyPostgres", "patients")
"""
from __future__ import annotations
from typing import Optional
import database as db
import services.db_connector as connector


class DatasourceRepository:
    """Facade that combines datasource lookup + connector calls."""

    @staticmethod
    def get_by_name(name: str) -> Optional[dict]:
        """Return datasource dict from SQLite, or None if not found."""
        return db.get_datasource_by_name(name)

    @staticmethod
    def test_connection(name: str) -> tuple[bool, str]:
        """
        Test connectivity for a named datasource.
        Returns (True, "") on success, (False, error_msg) on failure.
        """
        ds = db.get_datasource_by_name(name)
        if not ds:
            return False, f"Datasource '{name}' not found."
        return connector.test_db_connection(
            ds["db_type"], ds["host"], ds["port"],
            ds["dbname"], ds["username"], ds["password"],
        )

    @staticmethod
    def get_engine(name: str, charset: Optional[str] = None):
        """
        Create and return a SQLAlchemy Engine for a named datasource.
        Raises ValueError if datasource not found.
        """
        ds = db.get_datasource_by_name(name)
        if not ds:
            raise ValueError(f"Datasource '{name}' not found.")
        return connector.create_sqlalchemy_engine(
            ds["db_type"], ds["host"], ds["port"],
            ds["dbname"], ds["username"], ds["password"],
            charset=charset,
        )

    @staticmethod
    def get_tables(name: str) -> tuple[bool, list]:
        """
        Fetch table list for a named datasource.
        Returns (True, [table_names]) or (False, []).
        """
        ds = db.get_datasource_by_name(name)
        if not ds:
            return False, []
        return connector.get_tables_from_datasource(
            ds["db_type"], ds["host"], ds["port"],
            ds["dbname"], ds["username"], ds["password"],
        )

    @staticmethod
    def get_columns(name: str, table: str) -> tuple[bool, list]:
        """
        Fetch column list for a table in a named datasource.
        Returns (True, [{"name": ..., "type": ...}]) or (False, []).
        """
        ds = db.get_datasource_by_name(name)
        if not ds:
            return False, []
        return connector.get_columns_from_table(
            ds["db_type"], ds["host"], ds["port"],
            ds["dbname"], ds["username"], ds["password"], table,
        )
