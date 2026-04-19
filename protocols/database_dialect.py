"""
Database dialect protocols - Abstract interfaces for database-specific behavior.

Different databases (MySQL, PostgreSQL, MSSQL) have different:
- Connection URL formats
- Default ports
- Schema names
- Identifier quoting (backtick vs double quote vs bracket)
- SQL dialect differences

This protocol defines the contract for database dialect implementations.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class DatabaseDialect(Protocol):
    """
    Protocol for database-specific behavior.

    Implementations:
    - MySQLDialect
    - PostgreSQLDialect
    - MSSQLDialect
    """

    @property
    def name(self) -> str:
        """Human-readable dialect name (e.g., 'MySQL', 'PostgreSQL')."""
        ...

    @property
    def default_port(self) -> str:
        """Default port number as string."""
        ...

    @property
    def default_charset(self) -> str:
        """Default character set."""
        ...

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
        Build SQLAlchemy connection URL for this dialect.

        Args:
            host: Database host
            port: Database port
            dbname: Database name
            username: Database username
            password: Database password
            charset: Character set (optional)

        Returns:
            str: SQLAlchemy connection URL

        Example:
            >>> dialect = MySQLDialect()
            >>> url = dialect.build_url("localhost", "3306", "mydb", "user", "pass")
            >>> print(url)
            mysql+pymysql://user:pass@localhost:3306/mydb?charset=utf8mb4
        """
        ...

    def get_schema_default(self) -> str:
        """
        Get default schema name for this dialect.

        Returns:
            str: Default schema (e.g., 'public', 'dbo', 'database_name')

        Example:
            >>> dialect = PostgreSQLDialect()
            >>> dialect.get_schema_default()
            'public'
        """
        ...

    def quote_identifier(self, name: str) -> str:
        """
        Quote an identifier (table name, column name) for this dialect.

        Args:
            name: Identifier to quote

        Returns:
            str: Quoted identifier

        Example:
            >>> MySQLDialect().quote_identifier("table_name")
            '`table_name`'
            >>> PostgreSQLDialect().quote_identifier("table_name")
            '"table_name"'
        """
        ...

    def get_limit_offset_syntax(self, limit: int, offset: int = 0) -> str:
        """
        Get LIMIT/OFFSET syntax for this dialect.

        Args:
            limit: Number of rows
            offset: Number of rows to skip

        Returns:
            str: SQL fragment for LIMIT/OFFSET

        Example:
            >>> MySQLDialect().get_limit_offset_syntax(10, 5)
            'LIMIT 10 OFFSET 5'
            >>> MSSQLDialect().get_limit_offset_syntax(10, 5)
            'OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY'
        """
        ...


# Concrete implementations will be in dialects/ package
# (See Phase 4 for implementation)
