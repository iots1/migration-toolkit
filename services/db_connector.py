import sqlite3

def test_db_connection(db_type, host, port, db_name, user, password):
    """
    Test connection to external database sources with Port support
    """
    # Convert port to int, default if empty
    try:
        port = int(port) if port and str(port).strip() else None
    except ValueError:
        return False, f"Invalid port number: {port}"

    try:
        if db_type == "MySQL":
            try:
                import pymysql
                # PyMySQL defaults to 3306 if port is None
                connect_args = {
                    "host": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "connect_timeout": 5
                }
                if port: connect_args["port"] = port
                
                conn = pymysql.connect(**connect_args)
                conn.close()
                return True, "Successfully connected to MySQL!"
            except ImportError:
                return False, "Library 'pymysql' not found. Run: pip install pymysql"
            
        elif db_type == "Microsoft SQL Server":
            try:
                import pymssql
                # Pymssql uses 'port' argument
                connect_args = {
                    "server": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "timeout": 5
                }
                if port: connect_args["port"] = port

                conn = pymssql.connect(**connect_args)
                conn.close()
                return True, "Successfully connected to MSSQL!"
            except ImportError:
                return False, "Library 'pymssql' not found. Run: pip install pymssql"

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
                if port: connect_args["port"] = port

                conn = psycopg2.connect(**connect_args)
                conn.close()
                return True, "Successfully connected to PostgreSQL!"
            except ImportError:
                return False, "Library 'psycopg2' not found. Run: pip install psycopg2-binary"
                
    except Exception as e:
        return False, f"Connection Error: {str(e)}"

    return False, f"Unknown Database Type: {db_type}"


def get_tables_from_datasource(db_type, host, port, db_name, user, password):
    """
    Retrieves list of tables from a datasource.
    Returns: (success: bool, result: list or error_message: str)
    """
    try:
        port = int(port) if port and str(port).strip() else None
    except ValueError:
        return False, f"Invalid port number: {port}"

    try:
        tables = []

        if db_type == "MySQL":
            try:
                import pymysql
                connect_args = {
                    "host": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "connect_timeout": 5
                }
                if port: connect_args["port"] = port

                conn = pymysql.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
                return True, tables
            except ImportError:
                return False, "Library 'pymysql' not found. Run: pip install pymysql"

        elif db_type == "Microsoft SQL Server":
            try:
                import pymssql
                connect_args = {
                    "server": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "timeout": 5
                }
                if port: connect_args["port"] = port

                conn = pymssql.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
                return True, tables
            except ImportError:
                return False, "Library 'pymssql' not found. Run: pip install pymssql"

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
                if port: connect_args["port"] = port

                conn = psycopg2.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
                return True, tables
            except ImportError:
                return False, "Library 'psycopg2' not found. Run: pip install psycopg2-binary"

    except Exception as e:
        return False, f"Error retrieving tables: {str(e)}"

    return False, f"Unknown Database Type: {db_type}"


def get_columns_from_table(db_type, host, port, db_name, user, password, table_name):
    """
    Retrieves column information from a specific table.
    Returns: (success: bool, result: list of dicts [{'name': str, 'type': str}] or error_message: str)
    """
    try:
        port = int(port) if port and str(port).strip() else None
    except ValueError:
        return False, f"Invalid port number: {port}"

    try:
        columns = []

        if db_type == "MySQL":
            try:
                import pymysql
                connect_args = {
                    "host": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "connect_timeout": 5
                }
                if port: connect_args["port"] = port

                conn = pymysql.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE `{table_name}`")
                for row in cursor.fetchall():
                    columns.append({"name": row[0], "type": row[1]})
                cursor.close()
                conn.close()
                return True, columns
            except ImportError:
                return False, "Library 'pymysql' not found. Run: pip install pymysql"

        elif db_type == "Microsoft SQL Server":
            try:
                import pymssql
                connect_args = {
                    "server": host,
                    "user": user,
                    "password": password,
                    "database": db_name,
                    "timeout": 5
                }
                if port: connect_args["port"] = port

                conn = pymssql.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                for row in cursor.fetchall():
                    columns.append({"name": row[0], "type": row[1]})
                cursor.close()
                conn.close()
                return True, columns
            except ImportError:
                return False, "Library 'pymssql' not found. Run: pip install pymssql"

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
                if port: connect_args["port"] = port

                conn = psycopg2.connect(**connect_args)
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                for row in cursor.fetchall():
                    columns.append({"name": row[0], "type": row[1]})
                cursor.close()
                conn.close()
                return True, columns
            except ImportError:
                return False, "Library 'psycopg2' not found. Run: pip install psycopg2-binary"

    except Exception as e:
        return False, f"Error retrieving columns: {str(e)}"

    return False, f"Unknown Database Type: {db_type}"