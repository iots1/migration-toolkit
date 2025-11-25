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