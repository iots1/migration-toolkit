import sqlite3
from typing import Dict, Any
import hashlib


class DatabaseConnectionPool:
    """
    Singleton connection pool manager for database connections.
    Maintains a pool of reusable connections to avoid repeatedly opening/closing connections.
    """
    _instance = None
    _connections: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionPool, cls).__new__(cls)
        return cls._instance

    @staticmethod
    def _generate_key(db_type: str, host: str, port: str, db_name: str, user: str) -> str:
        """Generate unique key for connection based on connection parameters (excluding password)."""
        key_data = f"{db_type}:{host}:{port}:{db_name}:{user}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def get_connection(self, db_type: str, host: str, port: str, db_name: str, user: str, password: str):
        """
        Get or create a database connection.
        Returns: (connection, cursor) tuple or raises exception
        """
        conn_key = self._generate_key(db_type, host, port, db_name, user)

        # Check if connection exists and is alive
        if conn_key in self._connections:
            try:
                conn = self._connections[conn_key]
                # Test if connection is still alive
                if self._is_connection_alive(conn, db_type):
                    cursor = conn.cursor()
                    return conn, cursor
                else:
                    # Connection is dead, remove it
                    del self._connections[conn_key]
            except:
                # Connection is invalid, remove it
                if conn_key in self._connections:
                    del self._connections[conn_key]

        # Create new connection
        conn = self._create_connection(db_type, host, port, db_name, user, password)
        self._connections[conn_key] = conn
        cursor = conn.cursor()
        return conn, cursor

    def _is_connection_alive(self, conn, db_type: str) -> bool:
        """Check if connection is still alive."""
        try:
            cursor = conn.cursor()
            if db_type == "MySQL":
                cursor.execute("SELECT 1")
            elif db_type == "Microsoft SQL Server":
                cursor.execute("SELECT 1")
            elif db_type == "PostgreSQL":
                cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except:
            return False

    def _create_connection(self, db_type: str, host: str, port: str, db_name: str, user: str, password: str):
        """Create a new database connection."""
        try:
            port_int = int(port) if port and str(port).strip() else None
        except ValueError:
            raise ValueError(f"Invalid port number: {port}")

        if db_type == "MySQL":
            try:
                import pymysql
                connect_args = {
                    "host": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "connect_timeout": 5,
                    "autocommit": True
                }
                if port_int:
                    connect_args["port"] = port_int
                return pymysql.connect(**connect_args)
            except ImportError:
                raise ImportError("Library 'pymysql' not found. Run: pip install pymysql")

        elif db_type == "Microsoft SQL Server":
            try:
                import pymssql
                connect_args = {
                    "server": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "timeout": 5,
                    "autocommit": True
                }
                if port_int:
                    connect_args["port"] = port_int
                return pymssql.connect(**connect_args)
            except ImportError:
                raise ImportError("Library 'pymssql' not found. Run: pip install pymssql")

        elif db_type == "PostgreSQL":
            try:
                import psycopg2
                connect_args = {
                    "host": host,
                    "database": db_name,
                    "user": user,
                    "password": password,
                    "connect_timeout": 5
                }
                if port_int:
                    connect_args["port"] = port_int
                conn = psycopg2.connect(**connect_args)
                conn.autocommit = True
                return conn
            except ImportError:
                raise ImportError("Library 'psycopg2' not found. Run: pip install psycopg2-binary")

        raise ValueError(f"Unknown Database Type: {db_type}")

    def close_connection(self, db_type: str, host: str, port: str, db_name: str, user: str):
        """Close a specific connection."""
        conn_key = self._generate_key(db_type, host, port, db_name, user)
        if conn_key in self._connections:
            try:
                self._connections[conn_key].close()
            except:
                pass
            del self._connections[conn_key]

    def close_all(self):
        """Close all connections in the pool."""
        for conn in self._connections.values():
            try:
                conn.close()
            except:
                pass
        self._connections.clear()


# Global connection pool instance
_connection_pool = DatabaseConnectionPool()


def test_db_connection(db_type, host, port, db_name, user, password):
    """
    Test connection to external database sources with Port support.
    Uses connection pool to reuse connections.
    """
    try:
        # Try to get connection from pool
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        cursor.close()
        # Connection successful - keep it in pool for reuse
        return True, f"Successfully connected to {db_type}!"
    except ImportError as e:
        return False, str(e)
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Connection Error: {str(e)}"


def get_tables_from_datasource(db_type, host, port, db_name, user, password):
    """
    Retrieves list of tables from a datasource.
    Uses connection pool to reuse connections.
    Returns: (success: bool, result: list or error_message: str)
    """
    try:
        # Get connection from pool
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        # Execute appropriate query based on database type
        if db_type == "MySQL":
            cursor.execute("SHOW TABLES")
        elif db_type == "Microsoft SQL Server":
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
        elif db_type == "PostgreSQL":
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        else:
            return False, f"Unknown Database Type: {db_type}"

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return True, tables

    except ImportError as e:
        return False, str(e)
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Error retrieving tables: {str(e)}"


def get_columns_from_table(db_type, host, port, db_name, user, password, table_name):
    """
    Retrieves column information from a specific table.
    Uses connection pool to reuse connections.
    Returns: (success: bool, result: list of dicts [{'name': str, 'type': str}] or error_message: str)
    """
    try:
        # Get connection from pool
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        # Execute appropriate query based on database type
        if db_type == "MySQL":
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        elif db_type == "Microsoft SQL Server":
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        elif db_type == "PostgreSQL":
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        else:
            return False, f"Unknown Database Type: {db_type}"

        cursor.close()
        return True, columns

    except ImportError as e:
        return False, str(e)
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Error retrieving columns: {str(e)}"


def close_connection(db_type, host, port, db_name, user):
    """
    Close a specific connection from the pool.
    Useful when you want to force reconnection or clean up.
    """
    _connection_pool.close_connection(db_type, host, port, db_name, user)


def close_all_connections():
    """
    Close all connections in the pool.
    Useful for cleanup on application shutdown.
    """
    _connection_pool.close_all()