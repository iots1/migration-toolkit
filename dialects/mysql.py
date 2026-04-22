"""MySQL dialect implementation."""

from sqlalchemy import URL
from dialects.base import BaseDialect
from models.db_type import DbType


class MySQLDialect(BaseDialect):
    """MySQL dialect implementation."""

    name = DbType.MYSQL
    default_port = "3306"
    default_charset = "utf8mb4"

    def build_url(
        self,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str,
        charset: str | None = None,
    ) -> str:
        """Build MySQL connection URL with pymysql driver."""
        charset = charset or self.default_charset
        url = URL.create(
            "mysql+pymysql",
            username=username,
            password=password,
            host=host,
            port=int(port),
            database=dbname,
            query={"charset": charset},
        )
        return str(url)

    def get_schema_default(self) -> str:
        """MySQL uses 'dbo' or database name as default schema."""
        return "dbo"

    def quote_identifier(self, name: str) -> str:
        """MySQL uses backticks for identifier quoting."""
        return f"`{name}`"

    def get_limit_offset_syntax(self, limit: int, offset: int = 0) -> str:
        """MySQL uses standard LIMIT/OFFSET syntax."""
        if offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {offset}, {limit}"

    def wrap_query_with_limit(self, sql: str, limit: int) -> str:
        """MySQL uses subquery + LIMIT."""
        return f"SELECT * FROM ({sql}) AS _data_explorer_subq LIMIT {limit}"

    def get_timeout_statement(self, timeout_seconds: int) -> str | None:
        """MySQL uses max_execution_time (milliseconds)."""
        return f"SET SESSION max_execution_time = {timeout_seconds * 1000}"
