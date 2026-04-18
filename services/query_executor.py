"""
query_executor.py — Orchestrates SQL query execution for Data Explorer.

DIP: Depends on SqlValidator (composition) and BaseDialect (polymorphism)
     for validation and dialect-specific behavior.
Reuses DatabaseConnectionPool for connection management and
dialects/ registry for dialect-specific limit/timeout handling.
"""

from __future__ import annotations

import logging
import re
import time
import uuid

from services.connection_pool import DatabaseConnectionPool
from services.sql_validator import SqlValidator, SqlValidationError
from dialects.registry import get as get_dialect
from repositories.datasource_repo import get_by_id as get_datasource

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
QUERY_TIMEOUT_SECONDS = 30


def _extract_table_name(sql: str) -> str | None:
    """
    Extract table name from SELECT query.

    Handles simple queries like:
    - SELECT * FROM table_name
    - SELECT * FROM table_name LIMIT 10
    - SELECT * FROM schema.table_name
    - SELECT * FROM database.schema.table_name

    Returns None for complex queries (joins, subqueries, CTEs, etc.)
    """
    try:
        # Remove newlines and extra spaces
        sql_clean = re.sub(r'\s+', ' ', sql.strip())

        # Detect complex queries that should return None
        complex_indicators = [
            r'\bJOIN\b',                    # JOINs
            r'\bLEFT\s+JOIN\b',             # LEFT JOIN
            r'\bRIGHT\s+JOIN\b',            # RIGHT JOIN
            r'\bINNER\s+JOIN\b',            # INNER JOIN
            r'\bOUTER\s+JOIN\b',            # OUTER JOIN
            r'\bCROSS\s+JOIN\b',            # CROSS JOIN
            r'\bUNION\b',                   # UNION
            r'\bWITH\b.*?\bAS\b',           # CTEs (WITH ... AS)
            r'\bFROM\b.*?\(.*?\bFROM\b',     # Subquery with FROM
            r'\bFROM\b.*?\(.*?\bSELECT\b',   # Subquery
        ]

        for indicator in complex_indicators:
            if re.search(indicator, sql_clean, re.IGNORECASE):
                logger.debug("Complex query detected, skipping table extraction: %s", indicator)
                return None

        # Pattern: SELECT ... FROM table_name [WHERE/LIMIT/ORDER BY/GROUP BY/HAVING...]
        # This captures table names with optional schema/database prefixes
        # The pattern ensures we capture the LAST FROM clause (not in subqueries)
        pattern = r'SELECT\s+.+?\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*)\s*(?:WHERE|LIMIT|ORDER BY|GROUP BY|HAVING|;|$)'

        match = re.search(pattern, sql_clean, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return None
    except Exception:
        return None


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

        # Extract table name to build COUNT query
        table_name = _extract_table_name(sql)

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

            # Build COUNT query directly on table (more efficient)
            start = time.monotonic()
            if table_name:
                # Handle schema.table format by quoting each part
                if '.' in table_name:
                    parts = table_name.split('.')
                    quoted_parts = [dialect.quote_identifier(p.strip()) for p in parts]
                    quoted_table = '.'.join(quoted_parts)
                else:
                    quoted_table = dialect.quote_identifier(table_name)

                count_sql = f"SELECT COUNT(1) AS total_row FROM {quoted_table}"
                cursor.execute(count_sql)
                result = cursor.fetchone()
                total_row = result[0] if result and cursor.description else 0
            else:
                # Fallback: use wrapped query for complex queries
                count_sql = dialect.wrap_query_with_count(sql)
                cursor.execute(count_sql)
                result = cursor.fetchone()
                total_row = result[0] if result and cursor.description else 0

            # Execute SELECT query with LIMIT
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
                "DataExplorer: datasource=%s rows=%d total=%d elapsed=%.2fs sql=%.200s",
                datasource_id,
                len(row_dicts),
                total_row,
                elapsed,
                sql,
            )

            return {
                "id": str(uuid.uuid4()),
                "columns": columns,
                "rows": row_dicts,
                "row_count": len(row_dicts),
                "total_row": total_row,
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
