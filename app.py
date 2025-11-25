import streamlit as st
import pandas as pd
import json
import os
import glob
import time
import sqlite3
from datetime import datetime

# --- CONSTANTS ---
TRANSFORMER_OPTIONS = [
    "TRIM", "UPPER_TRIM", "LOWER_TRIM", 
    "BUDDHIST_TO_ISO", "ENG_DATE_TO_ISO", 
    "SPLIT_THAI_NAME", "SPLIT_ENG_NAME", 
    "FORMAT_PHONE", "MAP_GENDER", 
    "TO_NUMBER", "CLEAN_SPACES",
    "REMOVE_PREFIX", "REPLACE_EMPTY_WITH_NULL",
    "LOOKUP_VISIT_ID", "LOOKUP_PATIENT_ID", "LOOKUP_DOCTOR_ID"
]

VALIDATOR_OPTIONS = [
    "REQUIRED", "THAI_ID", "HN_FORMAT", 
    "VALID_DATE", "POSITIVE_NUMBER", "IS_EMAIL", "IS_PHONE",
    "NOT_EMPTY", "MIN_LENGTH_13", "NUMERIC_ONLY"
]

DB_TYPES = ["MySQL", "Microsoft SQL Server", "PostgreSQL"]

# --- CONFIGURATION ---
st.set_page_config(page_title="HIS Migration Toolkit", layout="wide", page_icon="🏥")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_report")
MIGRATION_REPORT_DIR = os.path.join(ANALYSIS_DIR, "migration_report")
DB_FILE = os.path.join(BASE_DIR, "migration_tool.db")

# --- DATABASE MANAGEMENT (SQLite) ---

def init_db():
    conn = sqlite3.connect(DB_FILE)
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

def save_datasource(name, db_type, host, port, dbname, username, password):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute('''INSERT OR REPLACE INTO datasources (name, db_type, host, port, dbname, username, password)
                     VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                  (name, db_type, host, port, dbname, username, password))
        conn.commit()
        return True, "Saved successfully"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def get_datasources():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT id, name, db_type, host, dbname, username FROM datasources", conn)
    conn.close()
    return df

def get_datasource_by_name(name):
    conn = sqlite3.connect(DB_FILE)
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

def delete_datasource(id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM datasources WHERE id=?", (id,))
    conn.commit()
    conn.close()

def save_config_to_db(config_name, table_name, json_data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
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
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT config_name, table_name, updated_at FROM configs ORDER BY updated_at DESC", conn)
    conn.close()
    return df

def get_config_content(config_name):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT json_data FROM configs WHERE config_name=?", (config_name,))
    row = c.fetchone()
    conn.close()
    if row:
        return json.loads(row[0])
    return None

# Initialize DB on load
init_db()

# --- HELPER FUNCTIONS ---

def safe_str(val):
    if pd.isna(val) or val is None: return ""
    return str(val).strip()

def get_report_folders():
    if not os.path.exists(MIGRATION_REPORT_DIR): return []
    folders = glob.glob(os.path.join(MIGRATION_REPORT_DIR, "*"))
    folders.sort(reverse=True)
    return folders

@st.cache_data
def load_data_profile(report_folder):
    csv_path = os.path.join(report_folder, "data_profile", "data_profile.csv")
    if os.path.exists(csv_path): 
        try:
            return pd.read_csv(csv_path, on_bad_lines='skip')
        except:
            return None
    return None

def to_camel_case(snake_str):
    s = safe_str(snake_str)
    if not s: return ""
    components = s.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def init_editor_state(df, table_name):
    """Initialize session state for a new table"""
    state_key = f"df_{table_name}"
    if state_key not in st.session_state:
        if "last_selected_row" in st.session_state:
            del st.session_state["last_selected_row"]
            
        editor_data = []
        for _, row in df.iterrows():
            src_col = row.get('Column', '')
            dtype = row.get('DataType', '')
            target_col = to_camel_case(src_col)
            
            transformers = []
            validators = []
            
            dtype_str = safe_str(dtype).lower()
            if "char" in dtype_str or "text" in dtype_str: transformers.append("TRIM")
            if "date" in dtype_str:
                transformers.append("BUDDHIST_TO_ISO")
                validators.append("VALID_DATE")
            
            src_lower = safe_str(src_col).lower()
            if src_lower == "hn": 
                transformers = ["UPPER_TRIM"]; validators = ["HN_FORMAT"]
            if src_lower == "cid":
                transformers = ["TRIM"]; validators = ["THAI_ID"]
            
            editor_data.append({
                "Source Column": src_col,
                "Type": dtype,
                "Target Column": target_col,
                "Transformers": ", ".join(transformers),
                "Validators": ", ".join(validators),
                "Required": False,
                "Ignore": False,
                "Lookup Table": "",
                "Lookup By": "",
                "Sample": safe_str(row.get('Sample_Values', ''))[:50]
            })
        st.session_state[state_key] = pd.DataFrame(editor_data)

def generate_json_config(params, mappings_df):
    """Generate Config as JSON Object"""
    config_data = {
        "name": params['config_name'], # Use config name
        "module": params['module'],
        "priority": 50,
        "source": {
            "database": params['source_db'],
            "table": params['table_name']
        },
        "target": {
            "database": params['target_db'],
            "table": params['target_table']
        },
        "batchSize": 5000,
        "dependencies": params['dependencies'] if params['dependencies'] else [],
        "mappings": []
    }
    for _, row in mappings_df.iterrows():
        if row['Ignore']: continue
        mapping_item = {"source": row['Source Column'], "target": row['Target Column']}
        
        t_val = row.get('Transformers')
        if t_val and safe_str(t_val):
            t_list = [t.strip() for t in str(t_val).split(',') if t.strip()]
            if t_list: mapping_item["transformers"] = t_list

        v_val = row.get('Validators')
        if v_val and safe_str(v_val):
            v_list = [v.strip() for v in str(v_val).split(',') if v.strip()]
            if v_list: mapping_item["validators"] = v_list

        if row.get('Lookup Table') and safe_str(row.get('Lookup Table')):
             mapping_item["lookupTable"] = row['Lookup Table']
        if row.get('Lookup By') and safe_str(row.get('Lookup By')):
             mapping_item["lookupBy"] = row['Lookup By']

        if row.get('Required'): mapping_item["required"] = True
            
        config_data["mappings"].append(mapping_item)
    return config_data

def test_db_connection(db_type, host, db_name, user, password):
    try:
        if db_type == "MySQL":
            import pymysql
            conn = pymysql.connect(host=host, user=user, password=password, database=db_name, connect_timeout=5)
            conn.close()
            return True, "Successfully connected to MySQL!"
        elif db_type == "Microsoft SQL Server":
            import pymssql
            conn = pymssql.connect(server=host, user=user, password=password, database=db_name, timeout=5)
            conn.close()
            return True, "Successfully connected to MSSQL!"
        elif db_type == "PostgreSQL":
            import psycopg2
            conn = psycopg2.connect(host=host, database=db_name, user=user, password=password, connect_timeout=5)
            conn.close()
            return True, "Successfully connected to PostgreSQL!"
    except ImportError as e:
        return False, f"Driver Error: {str(e)}"
    except Exception as e:
        return False, f"Connection Error: {str(e)}"
    return False, "Unknown Database Type"

# --- SAFETY WRAPPER ---
def safe_data_editor(df, **kwargs):
    try:
        return st.data_editor(df, **kwargs)
    except TypeError:
        unsafe_args = ['selection_mode', 'on_select']
        clean_kwargs = {k: v for k, v in kwargs.items() if k not in unsafe_args}
        return st.data_editor(df, **clean_kwargs)

# --- UI LAYOUT ---

st.title("🏥 HIS Migration Toolkit Center")

with st.sidebar:
    st.header("Navigate")
    page = st.radio("Go to", ["📊 Schema Mapper", "🚀 Migration Engine", "📁 File Explorer", "⚙️ Datasource & Config"])
    st.divider()
    st.caption(f"📂 Root: {BASE_DIR}")
    st.caption("💾 Storage: SQLite (migration_tool.db)")

if page == "📊 Schema Mapper":
    report_folders = get_report_folders()
    
    c1, c2 = st.columns([1, 3])
    with c1:
        if not report_folders:
            st.warning("No reports found.")
            st.stop()
        if "selected_folder_idx" not in st.session_state: st.session_state.selected_folder_idx = 0
        
        def update_folder(): st.session_state.selected_folder_idx = report_folders.index(st.session_state.folder_select)
        
        selected_folder = st.selectbox(
            "Run ID", 
            report_folders, 
            format_func=lambda x: os.path.basename(x),
            key="folder_select",
            index=st.session_state.selected_folder_idx,
            on_change=update_folder
        )
        
        df_raw = load_data_profile(selected_folder)
        if df_raw is None: st.stop()
        
    with c2:
        tables = df_raw['Table'].unique()
        if "selected_table_idx" not in st.session_state: st.session_state.selected_table_idx = 0
        if st.session_state.selected_table_idx >= len(tables): st.session_state.selected_table_idx = 0
        
        def update_table(): 
            try: st.session_state.selected_table_idx = list(tables).index(st.session_state.table_select)
            except: st.session_state.selected_table_idx = 0

        selected_table = st.selectbox(
            "Select Table to Map", 
            tables,
            key="table_select",
            index=st.session_state.selected_table_idx,
            on_change=update_table
        )

    if selected_table:
        st.markdown("---")
        init_editor_state(df_raw[df_raw['Table'] == selected_table], selected_table)
        
        with st.expander("⚙️ Table Configuration", expanded=True):
            conf_c1, conf_c2, conf_c3 = st.columns(3)
            with conf_c1:
                module_input = st.text_input("Module", value="patient", key=f"mod_{selected_table}")
                source_db_input = st.text_input("Source DB", value="hos_db", key=f"src_{selected_table}")
            with conf_c2:
                target_db_input = st.text_input("Target DB", value="hospital_new", key=f"tgt_{selected_table}")
                target_table_input = st.text_input("Target Table", value=selected_table, key=f"tbl_{selected_table}")
            with conf_c3:
                all_tables = df_raw['Table'].unique().tolist()
                dependencies_input = st.multiselect("Dependencies", options=all_tables, key=f"dep_{selected_table}")

        st.markdown("### 📋 Field Mapping")
        
        col_main, col_detail = st.columns([2, 1])
        columns_list = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
        
        # --- EDITOR & LOGIC (Same as before) ---
        default_sb_index = 0 
        editor_key = f"editor_{selected_table}"
        if editor_key in st.session_state and st.session_state[editor_key].get("selection", {}).get("rows"):
            default_sb_index = st.session_state[editor_key]["selection"]["rows"][0]
            if default_sb_index >= len(columns_list): default_sb_index = 0

        with col_main:
            edited_df = safe_data_editor(
                st.session_state[f"df_{selected_table}"],
                column_config={
                    "Source Column": st.column_config.TextColumn(disabled=True),
                    "Type": st.column_config.TextColumn(disabled=True, width="small"),
                    "Sample": st.column_config.TextColumn(disabled=True, width="medium"),
                },
                use_container_width=True, hide_index=True, num_rows="fixed", key=editor_key, height=500, selection_mode="single-row", on_select="rerun"
            )
            if not edited_df.equals(st.session_state[f"df_{selected_table}"]):
                st.session_state[f"df_{selected_table}"] = edited_df
                st.rerun()

        with col_detail:
            st.subheader("✏️ Field Settings")
            col_to_edit = st.selectbox("Select Field", columns_list, index=default_sb_index, key="sb_field_selector")
            if col_to_edit:
                row_idx = st.session_state[f"df_{selected_table}"].index[st.session_state[f"df_{selected_table}"]['Source Column'] == col_to_edit].tolist()[0]
                row_data = st.session_state[f"df_{selected_table}"].iloc[row_idx]
                st.info(f"Target: `{row_data['Target Column']}`")
                
                # ... (Tabs for transformers/validators same as before) ...
                t1, t2 = st.tabs(["Pipeline", "Lookup"])
                with t1:
                    curr_trans = row_data.get('Transformers', '')
                    def_trans = [t.strip() for t in str(curr_trans).split(', ') if t.strip() in TRANSFORMER_OPTIONS]
                    new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_t_{row_idx}")
                    curr_val = row_data.get('Validators', '')
                    def_val = [v.strip() for v in str(curr_val).split(', ') if v.strip() in VALIDATOR_OPTIONS]
                    new_val = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_val, key=f"ms_v_{row_idx}")
                with t2:
                    new_lookup_table = st.text_input("Lookup Table", value=safe_str(row_data.get('Lookup Table', '')), key=f"txt_lt_{row_idx}")
                    new_lookup_by = st.text_input("Lookup By", value=safe_str(row_data.get('Lookup By', '')), key=f"txt_lb_{row_idx}")

                if st.button("Apply", type="primary", use_container_width=True, key=f"btn_{row_idx}"):
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Transformers'] = ", ".join(new_trans)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Validators'] = ", ".join(new_val)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup Table'] = new_lookup_table
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup By'] = new_lookup_by
                    st.rerun()

        st.markdown("---")
        st.markdown("### 💻 Generated Registry Config (JSON)")
        
        # Save to DB Section
        col_name, col_act = st.columns([2, 1])
        with col_name:
            config_name_input = st.text_input("Config Name (Unique)", value=f"{selected_table}_config", help="Name to save in SQLite")
        
        params = {
            "config_name": config_name_input,
            "table_name": selected_table,
            "module": module_input,
            "source_db": source_db_input,
            "target_db": target_db_input,
            "target_table": target_table_input,
            "dependencies": dependencies_input
        }
        
        json_data = generate_json_config(params, st.session_state[f"df_{selected_table}"])
        json_str = json.dumps(json_data, indent=2, ensure_ascii=False)
        
        with col_act:
            st.write("") # Spacer
            st.write("")
            if st.button("💾 Save to Project DB", type="secondary", use_container_width=True):
                success, msg = save_config_to_db(config_name_input, selected_table, json_data)
                if success: st.success(msg)
                else: st.error(msg)

        # Download & Preview
        ac_left, ac_right = st.columns([1, 1])
        with ac_left:
            st.download_button("📥 Download JSON File", json_str, f"{selected_table}.json", "application/json", type="primary", use_container_width=True)
        with ac_right:
            is_expanded = st.toggle("Expand JSON Tree", value=True)

        t_tree, t_raw = st.tabs(["🌳 Tree View", "📄 Raw / Copy"])
        with t_tree: st.json(json_data, expanded=is_expanded)
        with t_raw: st.code(json_str, language="json")

# --- MIGRATION ENGINE ---
elif page == "🚀 Migration Engine":
    st.subheader("🚀 Data Migration Execution Engine")
    
    # 1. Connection Selection
    with st.expander("🔌 Database Connection Settings", expanded=True):
        datasources = get_datasources()
        
        col_src, col_tgt = st.columns(2)
        
        # Source Selection
        with col_src:
            st.markdown("#### Source Database")
            src_options = ["Custom Connection"] + datasources['name'].tolist()
            src_select = st.selectbox("Select Source Profile", src_options, key="src_sel")
            
            if src_select != "Custom Connection":
                ds = get_datasource_by_name(src_select)
                src_type = st.text_input("Type", ds['db_type'], key="src_t", disabled=True)
                src_host = st.text_input("Host", ds['host'], key="src_h", disabled=True)
                src_db = st.text_input("DB Name", ds['dbname'], key="src_d", disabled=True)
                src_user = ds['username']
                src_pass = ds['password']
                st.caption(f"User: {src_user}")
            else:
                src_type = st.selectbox("Database Type", DB_TYPES, key="src_t_c")
                src_host = st.text_input("Host", "192.168.1.10", key="src_h_c")
                src_db = st.text_input("Database Name", "hos_db", key="src_d_c")
                src_user = st.text_input("User", "sa", key="src_u_c")
                src_pass = st.text_input("Password", type="password", key="src_p_c")

            if st.button("Test Source", key="btn_src"):
                with st.spinner("Connecting..."):
                    ok, msg = test_db_connection(src_type, src_host, src_db, src_user, src_pass)
                    if ok: st.success(msg)
                    else: st.error(msg)

        # Target Selection
        with col_tgt:
            st.markdown("#### Target Database")
            tgt_options = ["Custom Connection"] + datasources['name'].tolist()
            tgt_select = st.selectbox("Select Target Profile", tgt_options, key="tgt_sel")
            
            if tgt_select != "Custom Connection":
                ds = get_datasource_by_name(tgt_select)
                tgt_type = st.text_input("Type", ds['db_type'], key="tgt_t", disabled=True)
                tgt_host = st.text_input("Host", ds['host'], key="tgt_h", disabled=True)
                tgt_db = st.text_input("DB Name", ds['dbname'], key="tgt_d", disabled=True)
                tgt_user = ds['username']
                tgt_pass = ds['password']
                st.caption(f"User: {tgt_user}")
            else:
                tgt_type = st.selectbox("Database Type", DB_TYPES, index=2, key="tgt_t_c")
                tgt_host = st.text_input("Host", "10.0.0.5", key="tgt_h_c")
                tgt_db = st.text_input("Database Name", "hospital_new", key="tgt_d_c")
                tgt_user = st.text_input("User", "admin", key="tgt_u_c")
                tgt_pass = st.text_input("Password", type="password", key="tgt_p_c")
                
            if st.button("Test Target", key="btn_tgt"):
                with st.spinner("Connecting..."):
                    ok, msg = test_db_connection(tgt_type, tgt_host, tgt_db, tgt_user, tgt_pass)
                    if ok: st.success(msg)
                    else: st.error(msg)

    # 2. Config Selection
    st.divider()
    st.markdown("#### 📄 Load Configuration")
    
    tab_db, tab_file = st.tabs(["📚 From Project DB", "📂 Upload File"])
    config_data = None
    
    with tab_db:
        configs_df = get_configs_list()
        if not configs_df.empty:
            sel_config = st.selectbox("Select Saved Config", configs_df['config_name'])
            if sel_config:
                config_data = get_config_content(sel_config)
                st.info(f"Loaded: {sel_config} (Updated: {configs_df[configs_df['config_name']==sel_config]['updated_at'].values[0]})")
        else:
            st.warning("No configs saved in database yet.")

    with tab_file:
        uploaded = st.file_uploader("Upload .json", type=["json"])
        if uploaded:
            config_data = json.load(uploaded)

    if config_data:
        with st.expander("Preview Selected Config"):
            st.json(config_data, expanded=False)

    # 3. Execution (Simulation)
    st.divider()
    st.markdown("#### ⚙️ Execution")
    if st.button("🚀 Start Migration", type="primary", disabled=(config_data is None), use_container_width=True):
        st.success("Migration Started... (See logs)")
        # ... (Same simulation logic as before) ...
        progress_bar = st.progress(0)
        time.sleep(1)
        progress_bar.progress(100)
        st.balloons()

# --- DATASOURCE & CONFIG MANAGER ---
elif page == "⚙️ Datasource & Config":
    st.subheader("🛠️ Project Settings (SQLite)")
    
    tab_ds, tab_conf = st.tabs(["🔌 Datasources", "📄 Saved Configs"])
    
    with tab_ds:
        st.markdown("#### Add New Datasource")
        with st.form("add_ds_form"):
            c1, c2 = st.columns(2)
            ds_name = c1.text_input("Profile Name (Unique)")
            ds_type = c2.selectbox("Type", DB_TYPES)
            c3, c4 = st.columns(2)
            ds_host = c3.text_input("Host")
            ds_port = c4.text_input("Port (Optional)")
            c5, c6, c7 = st.columns(3)
            ds_db = c5.text_input("DB Name")
            ds_user = c6.text_input("Username")
            ds_pass = c7.text_input("Password", type="password")
            
            if st.form_submit_button("Save Datasource"):
                if ds_name and ds_host:
                    ok, msg = save_datasource(ds_name, ds_type, ds_host, ds_port, ds_db, ds_user, ds_pass)
                    if ok: st.success("Saved!"); st.rerun()
                    else: st.error(msg)
                else:
                    st.error("Name and Host are required.")

        st.markdown("#### Existing Datasources")
        ds_df = get_datasources()
        if not ds_df.empty:
            st.dataframe(ds_df, use_container_width=True)
            # Simple delete UI
            del_id = st.selectbox("Select ID to Delete", ds_df['id'])
            if st.button("Delete Datasource"):
                delete_datasource(del_id)
                st.rerun()
        else:
            st.info("No datasources defined.")

    with tab_conf:
        st.markdown("#### Existing Saved Configs")
        cf_df = get_configs_list()
        st.dataframe(cf_df, use_container_width=True)

elif page == "📁 File Explorer":
    st.subheader("Project Files")
    if os.path.exists(ANALYSIS_DIR): st.code("\n".join(os.listdir(ANALYSIS_DIR)))