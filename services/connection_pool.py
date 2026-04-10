"""
connection_pool.py — Database connection pool manager.

Extracted from db_connector.py in Phase 9 (SRP - Single Responsibility Principle).
Manages a pool of reusable database connections to avoid repeatedly opening/closing connections.

Thread-safety: Each thread should get its own connection via get_connection().
The pool manages connections but connection objects themselves are NOT thread-safe.
"""
from __future__ import annotations
import hashlib
from typing import Dict, Any


class DatabaseConnectionPool:
    """
    Singleton connection pool manager for database connections.
    Maintains a pool of reusable connections to avoid repeatedly opening/closing connections.

    Thread-safety note: This class is thread-safe for get_connection() calls,
    but the returned connection objects are NOT thread-safe. Each thread must
    get its own connection.
    """
    _instance = None
    _connections: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionPool, cls).__new__(cls)
        return cls._instance

    @staticmethod
    def _generate_key(db_type: str, host: str, port: str, db_name: str, user: str) -> str:
        """Generate unique key for connection based on connection parameters."""
        key_data = f"{db_type}:{host}:{port}:{db_name}:{user}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def get_connection(self, db_type: str, host: str, port: str, db_name: str, user: str, password: str):
        """Get or create a database connection."""
        conn_key = self._generate_key(db_type, host, port, db_name, user)

        # Check existing connection
        if conn_key in self._connections:
            try:
                conn = self._connections[conn_key]
                if self._is_connection_alive(conn, db_type):
                    return conn, conn.cursor()
                else:
                    del self._connections[conn_key]
            except:
                if conn_key in self._connections:
                    del self._connections[conn_key]

        # Create new connection
        conn = self._create_connection(db_type, host, port, db_name, user, password)
        self._connections[conn_key] = conn
        return conn, conn.cursor()

    def _is_connection_alive(self, conn, db_type: str) -> bool:
        """Check if connection is still alive."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False

    def _create_connection(self, db_type: str, host: str, port: str, db_name: str, user: str, password: str):
        """Create a new database connection (Low-level drivers).

        Supports MySQL, PostgreSQL, and Microsoft SQL Server.
        Uses native drivers (pymysql, psycopg2, pymssql) for raw DBAPI connections.
        """
        try:
            port_int = int(port) if port and str(port).strip() else None
        except ValueError:
            raise ValueError(f"Invalid port number: {port}")

        if db_type == "MySQL":
            try:
                import pymysql
                connect_args = {
                    "host": host, "user": user, "password": password,
                    "database": db_name, "connect_timeout": 5, "autocommit": True,
                    "charset": "utf8mb4"
                }
                if port_int: connect_args["port"] = port_int
                return pymysql.connect(**connect_args)
            except ImportError:
                raise ImportError("Library 'pymysql' not found. Run: pip install pymysql")

        elif db_type == "Microsoft SQL Server":
            try:
                import pymssql
                connect_args = {
                    "server": host, "user": user, "password": password,
                    "database": db_name, "timeout": 5, "autocommit": True
                }
                if port_int: connect_args["port"] = port_int
                return pymssql.connect(**connect_args)
            except ImportError:
                raise ImportError("Library 'pymssql' not found. Run: pip install pymssql")

        elif db_type == "PostgreSQL":
            try:
                import psycopg2
                connect_args = {
                    "host": host, "database": db_name, "user": user,
                    "password": password, "connect_timeout": 5
                }
                if port_int: connect_args["port"] = port_int
                conn = psycopg2.connect(**connect_args)
                conn.autocommit = True
                return conn
            except ImportError:
                raise ImportError("Library 'psycopg2' not found. Run: pip install psycopg2-binary")

        raise ValueError(f"Unknown Database Type: {db_type}")

    def close_connection(self, db_type: str, host: str, port: str, db_name: str, user: str):
        """Close a specific connection in the pool."""
        conn_key = self._generate_key(db_type, host, port, db_name, user)
        if conn_key in self._connections:
            try:
                self._connections[conn_key].close()
            except: pass
            del self._connections[conn_key]

    def close_all(self):
        """Close all connections in the pool."""
        for conn in self._connections.values():
            try:
                conn.close()
            except: pass
        self._connections.clear()


# Global singleton instance (for backward compatibility)
_connection_pool = DatabaseConnectionPool()


def close_connection(db_type: str, host: str, port: str, db_name: str, user: str) -> None:
    """Close a specific connection. Convenience function for backward compatibility."""
    _connection_pool.close_connection(db_type, host, port, db_name, user)


def close_all_connections() -> None:
    """Close all connections. Convenience function for backward compatibility."""
    _connection_pool.close_all()
