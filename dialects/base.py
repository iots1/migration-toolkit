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
        charset: str | None = None
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
