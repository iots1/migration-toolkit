"""
DbType — canonical database type enum.

Single source of truth for all db_type string values stored in the datasources table
and used throughout the codebase. Using str+Enum means:
  - DbType.MYSQL == "MySQL"            → True  (backward-compatible with DB strings)
  - DbType.MSSQL.value                 → "Microsoft SQL Server"
  - db_type in [DbType.PG, DbType.MSSQL]  → works normally
"""

from enum import Enum


class DbType(str, Enum):
    MYSQL = "MySQL"
    POSTGRESQL = "PostgreSQL"
    MSSQL = "Microsoft SQL Server"
