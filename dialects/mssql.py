"""Microsoft SQL Server dialect implementation."""

from sqlalchemy import URL
from dialects.base import BaseDialect
from models.db_type import DbType


class MSSQLDialect(BaseDialect):
    """Microsoft SQL Server dialect implementation."""

    name = DbType.MSSQL
    default_port = "1433"
    default_charset = "utf8"

    def build_url(
        self,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str,
        charset: str | None = None,
    ) -> str:
        """Build MSSQL connection URL with pymssql driver."""
        url = URL.create(
            "mssql+pymssql",
            username=username,
            password=password,
            host=host,
            port=int(port),
            database=dbname,
        )
        return str(url)

    def get_schema_default(self) -> str:
        """MSSQL uses 'dbo' as default schema."""
        return "dbo"

    def quote_identifier(self, name: str) -> str:
        """MSSQL uses brackets for identifier quoting."""
        return f"[{name}]"

    def get_limit_offset_syntax(self, limit: int, offset: int = 0) -> str:
        """MSSQL uses OFFSET/FETCH syntax (SQL Server 2012+)."""
        if offset == 0:
            return f"OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        return f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"

    def wrap_query_with_limit(self, sql: str, limit: int) -> str:
        """MSSQL uses TOP clause before column list."""
        return f"SELECT TOP {limit} * FROM ({sql}) AS _data_explorer_subq"
