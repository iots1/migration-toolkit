import sqlite3
import pandas as pd
import json
from datetime import datetime
from config import DB_FILE
import uuid

def get_connection():
    """Creates a database connection to the SQLite database specified by DB_FILE."""
    return sqlite3.connect(DB_FILE)

def ensure_config_histories_table():
    """Ensures config_histories table exists with correct schema."""
    conn = get_connection()
    c = conn.cursor()
    try:
        # Check if old table exists and migrate
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='config_history'")
        old_exists = c.fetchone()

        if old_exists:
            # Migrate old data to new table
            try:
                c.execute("ALTER TABLE config_history RENAME TO config_histories")
            except:
                # If rename fails, just drop old table
                c.execute("DROP TABLE IF EXISTS config_history")

        # Create table with correct schema
        c.execute('''CREATE TABLE IF NOT EXISTS config_histories
                     (id TEXT PRIMARY KEY,
                      config_id TEXT,
                      version INTEGER,
                      json_data TEXT,
                      created_at TIMESTAMP,
                      FOREIGN KEY(config_id) REFERENCES configs(id) ON DELETE CASCADE)''')
        conn.commit()
    except Exception as e:
        print(f"Error ensuring config_histories table: {e}")
    finally:
        conn.close()

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
                 (id TEXT PRIMARY KEY,
                  config_name TEXT UNIQUE,
                  table_name TEXT,
                  json_data TEXT,
                  updated_at TIMESTAMP)''')

    # Table: Config Histories (renamed from config_history)
    c.execute('''CREATE TABLE IF NOT EXISTS config_histories
                 (id TEXT PRIMARY KEY,
                  config_id TEXT,
                  version INTEGER,
                  json_data TEXT,
                  created_at TIMESTAMP,
                  FOREIGN KEY(config_id) REFERENCES configs(id) ON DELETE CASCADE)''')
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
    """Saves or updates a JSON configuration in the database and tracks history."""
    # Ensure config_histories table exists with correct schema
    ensure_config_histories_table()

    conn = get_connection()
    c = conn.cursor()
    try:
        json_str = json.dumps(json_data)

        # Check if config already exists
        c.execute("SELECT id FROM configs WHERE config_name=?", (config_name,))
        existing = c.fetchone()
        config_id = existing[0] if existing else str(uuid.uuid4())

        # Get next version number
        c.execute("SELECT MAX(version) FROM config_histories WHERE config_id=?", (config_id,))
        max_version = c.fetchone()[0]
        next_version = (max_version + 1) if max_version else 1

        # Save to configs table (INSERT OR REPLACE)
        c.execute('''INSERT OR REPLACE INTO configs (id, config_name, table_name, json_data, updated_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (config_id, config_name, table_name, json_str, datetime.now()))

        # Save to config_histories table
        history_id = str(uuid.uuid4())
        c.execute('''INSERT INTO config_histories (id, config_id, version, json_data, created_at)
                     VALUES (?, ?, ?, ?, ?)''',
                  (history_id, config_id, next_version, json_str, datetime.now()))

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
        # Fetch json_data to extract target table info
        df = pd.read_sql_query("SELECT config_name, table_name, json_data, updated_at FROM configs ORDER BY updated_at DESC", conn)
        
        # Helper to extract target table from JSON string
        def extract_target(json_str):
            try:
                data = json.loads(json_str)
                return data.get('target', {}).get('table', '-')
            except:
                return '-'

        if not df.empty:
            df['destination_table'] = df['json_data'].apply(extract_target)
            # Remove json_data column to keep DF lightweight for UI
            df = df.drop(columns=['json_data'])
            
            # Rename for clarity
            df = df.rename(columns={'table_name': 'source_table'})
            
    except Exception as e:
        # Return empty DF with expected columns if error
        df = pd.DataFrame(columns=['config_name', 'source_table', 'destination_table', 'updated_at'])
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

def delete_config(config_name):
    """Deletes a configuration from the database by name."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM configs WHERE config_name=?", (config_name,))
        conn.commit()
        return True, "Config deleted successfully"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_config_history(config_name):
    """Retrieves all versions of a configuration."""
    # Ensure config_histories table exists with correct schema
    ensure_config_histories_table()

    conn = get_connection()
    try:
        # Get config_id from config_name first
        c = conn.cursor()
        c.execute("SELECT id FROM configs WHERE config_name=?", (config_name,))
        result = c.fetchone()

        if result:
            config_id = result[0]
            df = pd.read_sql_query(
                "SELECT id, version, created_at FROM config_histories WHERE config_id=? ORDER BY version DESC",
                conn,
                params=(config_id,)
            )
        else:
            df = pd.DataFrame(columns=['id', 'version', 'created_at'])
    except:
        df = pd.DataFrame(columns=['id', 'version', 'created_at'])
    finally:
        conn.close()
    return df

def get_config_version(config_name, version):
    """Retrieves a specific version of a configuration."""
    # Ensure config_histories table exists with correct schema
    ensure_config_histories_table()

    conn = get_connection()
    c = conn.cursor()
    try:
        # Get config_id first
        c.execute("SELECT id FROM configs WHERE config_name=?", (config_name,))
        result = c.fetchone()

        if result:
            config_id = result[0]
            c.execute("SELECT json_data FROM config_histories WHERE config_id=? AND version=?", (config_id, version))
            row = c.fetchone()
            if row:
                return json.loads(row[0])
        return None
    except:
        return None
    finally:
        conn.close()

def compare_config_versions(config_name, version1, version2):
    """Compares two versions of a configuration and returns the differences."""
    config_v1 = get_config_version(config_name, version1)
    config_v2 = get_config_version(config_name, version2)

    if not config_v1 or not config_v2:
        return None

    diff = {
        'mappings_added': [],
        'mappings_removed': [],
        'mappings_modified': []
    }

    mappings_v1 = {m['source']: m for m in config_v1.get('mappings', [])}
    mappings_v2 = {m['source']: m for m in config_v2.get('mappings', [])}

    # Find added and modified mappings
    for source, mapping_v2 in mappings_v2.items():
        if source not in mappings_v1:
            diff['mappings_added'].append(mapping_v2)
        elif mappings_v1[source] != mapping_v2:
            diff['mappings_modified'].append({
                'source': source,
                'old': mappings_v1[source],
                'new': mapping_v2
            })

    # Find removed mappings
    for source, mapping_v1 in mappings_v1.items():
        if source not in mappings_v2:
            diff['mappings_removed'].append(mapping_v1)

    return diff