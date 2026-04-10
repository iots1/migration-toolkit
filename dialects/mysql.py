"""MySQL dialect implementation."""
from sqlalchemy import URL
from dialects.base import BaseDialect


class MySQLDialect(BaseDialect):
    """MySQL dialect implementation."""

    name = "MySQL"
    default_port = "3306"
    default_charset = "utf8mb4"

    def build_url(
        self,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str,
        charset: str | None = None
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
            query={"charset": charset}
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
