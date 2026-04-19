"""
Base dialect class - Abstract base class for all database dialects.

All dialect implementations must inherit from BaseDialect and implement
all abstract methods.
"""

from abc import ABC, abstractmethod
from sqlalchemy import URL


class BaseDialect(ABC):
    """
    Abstract base class for database dialects.

    All dialect implementations (MySQL, PostgreSQL, MSSQL) must
    inherit from this class and implement all abstract methods.

    Example:
        >>> class MySQLDialect(BaseDialect):
        ...     name = "MySQL"
        ...     default_port = "3306"
        ...     # ... implement abstract methods
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable dialect name."""
        ...

    @property
    @abstractmethod
    def default_port(self) -> str:
        """Default port number as string."""
        ...

    @property
    @abstractmethod
    def default_charset(self) -> str:
        """Default character set."""
        ...

    @abstractmethod
    def build_url(
        self,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str,
        charset: str | None = None,
    ) -> str:
        """
        Build SQLAlchemy connection URL.

        Must return a URL string that SQLAlchemy can use to create an engine.
        """
        ...

    def get_schema_default(self) -> str:
        """
        Get default schema name for this dialect.

        Default implementation returns 'public'.
        Override for dialects with different defaults.

        Returns:
            str: Default schema name
        """
        return "public"

    def quote_identifier(self, name: str) -> str:
        """
        Quote an identifier (table/column name) for this dialect.

        Default implementation uses double quotes (PostgreSQL style).
        Override for dialects with different quoting.

        Args:
            name: Identifier to quote

        Returns:
            str: Quoted identifier
        """
        return f'"{name}"'

    def get_limit_offset_syntax(self, limit: int, offset: int = 0) -> str:
        """
        Get LIMIT/OFFSET syntax for this dialect.

        Default implementation uses standard SQL syntax.
        Override for dialects with different syntax (e.g., MSSQL).

        Args:
            limit: Number of rows
            offset: Number of rows to skip

        Returns:
            str: SQL fragment
        """
        if offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"

    def wrap_query_with_count(self, sql: str) -> str:
        """
        Wrap a SELECT query to get total row count using subquery.

        Default implementation uses COUNT(1) with alias for clarity.
        Override for dialects with different optimization requirements.

        Args:
            sql: The SELECT query to wrap

        Returns:
            str: COUNT query
        """
        return f"SELECT COUNT(1) AS total_row FROM ({sql}) AS _data_explorer_count_subq"

    def wrap_query_with_limit(self, sql: str, limit: int) -> str:
        """
        Wrap a SELECT query with a row limit using dialect-specific syntax.

        Default implementation uses subquery + LIMIT (PostgreSQL/MySQL style).
        Override for dialects that use TOP (e.g., MSSQL).

        Args:
            sql: The SELECT query to wrap
            limit: Maximum number of rows to return

        Returns:
            str: Wrapped SQL query
        """
        return f"SELECT * FROM ({sql}) AS _data_explorer_subq LIMIT {limit}"

    def get_timeout_statement(self, timeout_seconds: int) -> str | None:
        """
        Get SQL statement to set query execution timeout.

        Default implementation returns None (no timeout support).
        Override for dialects that support statement-level timeouts.

        Args:
            timeout_seconds: Timeout in seconds

        Returns:
            str | None: SQL statement to execute, or None if not supported
        """
        return None
