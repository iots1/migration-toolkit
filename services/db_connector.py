import sqlite3
from typing import Dict, Any, Optional
import hashlib
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

# ==========================================
#  PART 1: SQLAlchemy Integration
#  Used for Migration Engine (Pandas read_sql/to_sql)
# ==========================================

def create_sqlalchemy_engine(db_type, host, port, db_name, user, password) -> Optional[Engine]:
    """
    Creates a SQLAlchemy Engine using URL object construction.
    This prevents errors when passwords contain special characters (@, :, /).
    """
    try:
        # Convert port to int if exists
        port_int = int(port) if port and str(port).strip() else None

        if db_type == "MySQL":
            # Requires: pip install pymysql
            connection_url = URL.create(
                "mysql+pymysql",
                username=user,
                password=password,
                host=host,
                port=port_int or 3306,
                database=db_name,
                query={"charset": "utf8mb4"}
            )
            
        elif db_type == "PostgreSQL":
            # Requires: pip install psycopg2-binary
            connection_url = URL.create(
                "postgresql+psycopg2",
                username=user,
                password=password,
                host=host,
                port=port_int or 5432,
                database=db_name,
                query={"client_encoding": "utf8"}
            )
            
        elif db_type == "Microsoft SQL Server":
            # Requires: pip install pymssql
            connection_url = URL.create(
                "mssql+pymssql",
                username=user,
                password=password,
                host=host,
                port=port_int or 1433,
                database=db_name,
                query={"charset": "utf8"}
            )
        
        else:
            raise ValueError(f"Unsupported DB Type for Engine: {db_type}")

        # Create Engine with pool settings
        engine = create_engine(connection_url)
        return engine

    except Exception as e:
        print(f"Error creating engine: {e}")
        raise e


# ==========================================
#  PART 2: Connection Pool Class
# ==========================================

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
        """Create a new database connection (Low-level drivers)."""
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
        conn_key = self._generate_key(db_type, host, port, db_name, user)
        if conn_key in self._connections:
            try:
                self._connections[conn_key].close()
            except: pass
            del self._connections[conn_key]

    def close_all(self):
        for conn in self._connections.values():
            try:
                conn.close()
            except: pass
        self._connections.clear()


# ==========================================
#  PART 3: Global Instance & Functions
#  CRITICAL: _connection_pool MUST be defined here
# ==========================================

# Global connection pool instance
_connection_pool = DatabaseConnectionPool()


def test_db_connection(db_type, host, port, db_name, user, password):
    """Test connection to external database sources."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        cursor.close()
        return True, f"Successfully connected to {db_type}!"
    except Exception as e:
        return False, str(e)


# --- INSPECTION FUNCTIONS (Originals Restored) ---

def get_tables_from_datasource(db_type, host, port, db_name, user, password, schema=None):
    """Retrieves list of tables from a datasource."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        if db_type == "MySQL":
            cursor.execute("SHOW TABLES")
        elif db_type == "Microsoft SQL Server":
            schema_filter = schema if schema else 'dbo'
            cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema_filter}' ORDER BY TABLE_NAME")
        elif db_type == "PostgreSQL":
            schema_filter = schema if schema else 'public'
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_filter}' ORDER BY table_name")
        else:
            return False, f"Unknown Database Type: {db_type}"

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return True, tables
    except Exception as e:
        return False, str(e)

def get_columns_from_table(db_type, host, port, db_name, user, password, table_name, schema=None):
    """Retrieves column information from a specific table."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        if db_type == "MySQL":
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        elif db_type == "Microsoft SQL Server":
            schema_filter = schema if schema else 'dbo'
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_filter}' ORDER BY ORDINAL_POSITION")
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        elif db_type == "PostgreSQL":
            schema_filter = schema if schema else 'public'
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = '{schema_filter}' ORDER BY ordinal_position")
            columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        
        cursor.close()
        return True, columns
    except Exception as e:
        return False, str(e)

def get_foreign_keys(db_type, host, port, db_name, user, password, schema=None):
    """Retrieves foreign key relationships."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        relationships = []

        if db_type == "MySQL":
            query = f"""
                SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE REFERENCED_TABLE_SCHEMA = '{db_name}' 
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0], "col": row[1],
                    "ref_table": row[2], "ref_col": row[3]
                })

        elif db_type == "PostgreSQL":
            schema_filter = schema if schema else 'public'
            query = f"""
                SELECT
                    tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = '{schema_filter}'
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0], "col": row[1],
                    "ref_table": row[2], "ref_col": row[3]
                })

        elif db_type == "Microsoft SQL Server":
            query = """
                SELECT 
                    tp.name, cp.name, tr.name, cr.name
                FROM 
                    sys.foreign_keys fk
                INNER JOIN 
                    sys.tables tp ON fk.parent_object_id = tp.object_id
                INNER JOIN 
                    sys.tables tr ON fk.referenced_object_id = tr.object_id
                INNER JOIN 
                    sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                INNER JOIN 
                    sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
                INNER JOIN 
                    sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0], "col": row[1],
                    "ref_table": row[2], "ref_col": row[3]
                })

        cursor.close()
        return True, relationships
    except Exception as e:
        return False, str(e)

def get_table_sample_data(db_type, host, port, db_name, user, password, table_name, limit=50, schema=None):
    """Retrieves a sample of data from a table."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        table_ref = table_name
        if schema:
            if db_type == "Microsoft SQL Server":
                table_ref = f"[{schema}].[{table_name}]"
            elif db_type == "PostgreSQL":
                table_ref = f'"{schema}"."{table_name}"'

        query = ""
        if db_type == "Microsoft SQL Server":
            query = f"SELECT TOP {limit} * FROM {table_ref}"
        else:
            query = f"SELECT * FROM {table_ref} LIMIT {limit}"

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        cursor.close()
        return True, rows, columns
    except Exception as e:
        return False, str(e), []

def get_column_sample_values(db_type, host, port, db_name, user, password, table_name, column_name, limit=20, schema=None):
    """Retrieves distinct sample values from a specific column."""
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        table_ref = table_name
        if schema:
            if db_type == "Microsoft SQL Server":
                table_ref = f"[{schema}].[{table_name}]"
            elif db_type == "PostgreSQL":
                table_ref = f'"{schema}"."{table_name}"'

        if db_type == "MySQL":
            query = f"SELECT DISTINCT `{column_name}` FROM {table_ref} WHERE `{column_name}` IS NOT NULL AND CAST(`{column_name}` AS CHAR) <> '' LIMIT {limit}"
        elif db_type == "PostgreSQL":
            query = f'SELECT DISTINCT "{column_name}" FROM {table_ref} WHERE "{column_name}" IS NOT NULL AND CAST("{column_name}" AS TEXT) <> \'\' LIMIT {limit}'
        elif db_type == "Microsoft SQL Server":
            query = f"SELECT DISTINCT TOP {limit} [{column_name}] FROM {table_ref} WHERE [{column_name}] IS NOT NULL AND CAST([{column_name}] AS NVARCHAR(MAX)) <> ''"
        else:
            return False, f"Unknown Database Type: {db_type}"

        cursor.execute(query)
        values = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return True, values
    except Exception as e:
        return False, str(e)

def close_connection(db_type, host, port, db_name, user):
    _connection_pool.close_connection(db_type, host, port, db_name, user)

def close_all_connections():
    _connection_pool.close_all()