"""PostgreSQL dialect implementation."""
from sqlalchemy import URL
from dialects.base import BaseDialect


class PostgreSQLDialect(BaseDialect):
    """PostgreSQL dialect implementation."""

    name = "PostgreSQL"
    default_port = "5432"
    default_charset = "utf8"

    def build_url(
        self,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str,
        charset: str | None = None
    ) -> str:
        """Build PostgreSQL connection URL with psycopg2 driver."""
        url = URL.create(
            "postgresql+psycopg2",
            username=username,
            password=password,
            host=host,
            port=int(port),
            database=dbname
        )
        return str(url)

    def get_schema_default(self) -> str:
        """PostgreSQL uses 'public' as default schema."""
        return "public"

    def quote_identifier(self, name: str) -> str:
        """PostgreSQL uses double quotes for identifier quoting."""
        return f'"{name}"'

    def get_limit_offset_syntax(self, limit: int, offset: int = 0) -> str:
        """PostgreSQL uses standard LIMIT/OFFSET syntax."""
        if offset == 0:
            return f"LIMIT {limit}"
        return f"LIMIT {limit} OFFSET {offset}"
