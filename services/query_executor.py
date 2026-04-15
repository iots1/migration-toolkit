"""
query_executor.py — Orchestrates SQL query execution for Data Explorer.

DIP: Depends on SqlValidator (composition) and BaseDialect (polymorphism)
     for validation and dialect-specific behavior.
Reuses DatabaseConnectionPool for connection management and
dialects/ registry for dialect-specific limit/timeout handling.
"""

from __future__ import annotations

import logging
import time
import uuid

from services.connection_pool import DatabaseConnectionPool
from services.sql_validator import SqlValidator, SqlValidationError
from dialects.registry import get as get_dialect
from repositories.datasource_repo import get_by_id as get_datasource

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
QUERY_TIMEOUT_SECONDS = 30


class QueryExecutor:
    def __init__(
        self,
        validator: SqlValidator | None = None,
        connection_pool: DatabaseConnectionPool | None = None,
    ):
        self._validator = validator or SqlValidator()
        self._pool = connection_pool or DatabaseConnectionPool()

    def execute(self, datasource_id: str, cmd: str) -> dict:
        sql = self._validator.validate(cmd)

        ds = get_datasource(datasource_id)
        if not ds:
            raise SqlValidationError(f"Datasource '{datasource_id}' not found")

        dialect = get_dialect(ds["db_type"])

        wrapped_sql = dialect.wrap_query_with_limit(sql, MAX_ROWS)

        cursor = None
        try:
            conn, cursor = self._pool.get_connection(
                db_type=ds["db_type"],
                host=ds["host"],
                port=ds["port"],
                db_name=ds["dbname"],
                user=ds["username"],
                password=ds["password"],
            )

            timeout_stmt = dialect.get_timeout_statement(QUERY_TIMEOUT_SECONDS)
            if timeout_stmt:
                cursor.execute(timeout_stmt)

            start = time.monotonic()
            cursor.execute(wrapped_sql)
            elapsed = time.monotonic() - start

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            rows = cursor.fetchmany(MAX_ROWS + 1)
            truncated = len(rows) > MAX_ROWS
            rows = rows[:MAX_ROWS]

            row_dicts = [dict(zip(columns, row)) for row in rows]

            logger.info(
                "DataExplorer: datasource=%s rows=%d elapsed=%.2fs sql=%.200s",
                datasource_id,
                len(row_dicts),
                elapsed,
                sql,
            )

            return {
                "id": str(uuid.uuid4()),
                "columns": columns,
                "rows": row_dicts,
                "row_count": len(row_dicts),
                "limit": MAX_ROWS,
                "truncated": truncated,
            }
        except SqlValidationError:
            raise
        except Exception as exc:
            logger.error(
                "DataExplorer error: datasource=%s error=%s",
                datasource_id,
                exc,
            )
            raise SqlValidationError(
                f"Query execution failed: {str(exc)}"
            )
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
