"""
sql_validator.py — SQL validation for Data Explorer.

SRP: This module is solely responsible for validating user-provided SQL queries.
It performs security checks to ensure only safe, read-only SELECT queries pass through.
"""

from __future__ import annotations

import re


class SqlValidationError(ValueError):
    pass


class SqlValidator:
    _FORBIDDEN_PATTERN = re.compile(
        r"\b("
        # DDL
        r"CREATE|ALTER|DROP|TRUNCATE|RENAME|COMMENT|"
        # DML
        r"INSERT|UPDATE|DELETE|MERGE|REPLACE|UPSERT|"
        # DCL
        r"GRANT|REVOKE|DENY|"
        # TCL
        r"BEGIN|COMMIT|ROLLBACK|SAVEPOINT|"
        r"START\s+TRANSACTION|"
        # Exec / Procedures
        r"EXEC|EXECUTE|CALL|DECLARE|"
        # File operations
        r"INTO\s+(OUTFILE|DUMPFILE)|"
        r"LOAD\s+(DATA|FILE|XML)|COPY|BULK\s+INSERT|"
        # Locking
        r"LOCK\s+TABLE|UNLOCK\s+TABLE|"
        r"FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE|"
        # Other unsafe
        r"PREPARE|DEALLOCATE|SIGNAL|RESIGNAL|"
        r"GET\s+DIAGNOSTICS|HANDLER"
        r")\b",
        re.IGNORECASE,
    )
    _BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
    _LINE_COMMENT_PATTERN = re.compile(r"--.*$", re.MULTILINE)

    def validate(self, sql: str) -> str:
        stripped = self._strip_comments(sql)
        self._reject_stacked_queries(stripped)
        self._reject_forbidden_keywords(stripped)
        self._enforce_select_only(stripped)
        return stripped

    def _strip_comments(self, sql: str) -> str:
        sql = self._BLOCK_COMMENT_PATTERN.sub("", sql)
        sql = self._LINE_COMMENT_PATTERN.sub("", sql)
        return sql

    def _reject_stacked_queries(self, sql: str) -> None:
        if ";" in sql.rstrip(";"):
            raise SqlValidationError(
                "Semicolons are not allowed (stacked queries blocked)"
            )

    def _reject_forbidden_keywords(self, sql: str) -> None:
        if self._FORBIDDEN_PATTERN.search(sql):
            raise SqlValidationError(
                "Query contains forbidden keywords or unsafe patterns"
            )

    _SELECT_ONLY_PATTERN = re.compile(r"^\s*(SELECT|WITH\s)", re.IGNORECASE)

    def _enforce_select_only(self, sql: str) -> None:
        if not self._SELECT_ONLY_PATTERN.match(sql):
            raise SqlValidationError("Only SELECT queries are allowed")
