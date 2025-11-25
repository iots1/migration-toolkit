import sqlite3
import pandas as pd
import json
from datetime import datetime
from config import DB_FILE

def get_connection():
    """Creates a database connection to the SQLite database specified by DB_FILE."""
    return sqlite3.connect(DB_FILE)

def init_db():
    """Initializes the database tables if they do not exist."""
    conn = get_connection()
    c = conn.cursor()
    
    # Table: Datasources
    c.execute('''CREATE TABLE IF NOT EXISTS datasources
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT UNIQUE,
                  db_type TEXT,
                  host TEXT,
                  port TEXT,
                  dbname TEXT,
                  username TEXT,
                  password TEXT)''')
    
    # Table: Configs
    c.execute('''CREATE TABLE IF NOT EXISTS configs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  config_name TEXT UNIQUE,
                  table_name TEXT,
                  json_data TEXT,
                  updated_at TIMESTAMP)''')
    conn.commit()
    conn.close()

# --- Datasource CRUD Operations ---

def get_datasources():
    """Retrieves all datasources from the database."""
    conn = get_connection()
    try:
        # Select specific columns to display in the UI
        df = pd.read_sql_query("SELECT id, name, db_type, host, dbname, username FROM datasources", conn)
    except:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def get_datasource_by_id(id):
    """Retrieves a specific datasource by its ID."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM datasources WHERE id=?", (id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0], "name": row[1], "db_type": row[2], 
            "host": row[3], "port": row[4], "dbname": row[5], 
            "username": row[6], "password": row[7]
        }
    return None

def get_datasource_by_name(name):
    """Retrieves a specific datasource by its unique name."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM datasources WHERE name=?", (name,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "id": row[0], "name": row[1], "db_type": row[2], 
            "host": row[3], "port": row[4], "dbname": row[5], 
            "username": row[6], "password": row[7]
        }
    return None

def save_datasource(name, db_type, host, port, dbname, username, password):
    """Saves a new datasource to the database."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''INSERT INTO datasources (name, db_type, host, port, dbname, username, password)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                  (name, db_type, host, port, dbname, username, password))
        conn.commit()
        return True, "Saved successfully"
    except sqlite3.IntegrityError:
        return False, f"Datasource name '{name}' already exists."
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def update_datasource(id, name, db_type, host, port, dbname, username, password):
    """Updates an existing datasource in the database."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute('''UPDATE datasources 
                     SET name=?, db_type=?, host=?, port=?, dbname=?, username=?, password=?
                     WHERE id=?''', 
                  (name, db_type, host, port, dbname, username, password, id))
        conn.commit()
        return True, "Updated successfully"
    except sqlite3.IntegrityError:
        return False, f"Datasource name '{name}' already exists."
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def delete_datasource(id):
    """Deletes a datasource from the database by ID."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM datasources WHERE id=?", (id,))
    conn.commit()
    conn.close()

# --- Config CRUD Operations ---

def save_config_to_db(config_name, table_name, json_data):
    """Saves or updates a JSON configuration in the database."""
    conn = get_connection()
    c = conn.cursor()
    try:
        # Using INSERT OR REPLACE to handle updates seamlessly based on unique config_name
        c.execute('''INSERT OR REPLACE INTO configs (config_name, table_name, json_data, updated_at)
                     VALUES (?, ?, ?, ?)''', 
                  (config_name, table_name, json.dumps(json_data), datetime.now()))
        conn.commit()
        return True, "Config saved!"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_configs_list():
    """Retrieves a list of saved configurations, sorted by update time."""
    conn = get_connection()
    try:
        df = pd.read_sql_query("SELECT config_name, table_name, updated_at FROM configs ORDER BY updated_at DESC", conn)
    except:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def get_config_content(config_name):
    """Retrieves the JSON content of a specific configuration."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT json_data FROM configs WHERE config_name=?", (config_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None