import streamlit as st
import pandas as pd
import json
import os
import glob

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

# --- CONFIGURATION ---
st.set_page_config(page_title="HIS Migration Toolkit", layout="wide", page_icon="🏥")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis_report")
MIGRATION_REPORT_DIR = os.path.join(ANALYSIS_DIR, "migration_report")
CONFIG_FILE = os.path.join(ANALYSIS_DIR, "config.json")

# --- HELPER FUNCTIONS ---

def safe_str(val):
    if pd.isna(val) or val is None: return ""
    return str(val).strip()

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f: return json.load(f)
        except: return {}
    return {}

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
    """Generate Config as JSON Object (Dictionary)"""
    
    # Base structure
    config_data = {
        "name": params['table_name'],
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

    # Generate Mappings Array
    for _, row in mappings_df.iterrows():
        if row['Ignore']: continue
            
        mapping_item = {
            "source": row['Source Column'],
            "target": row['Target Column']
        }
        
        # Transformers
        transformers_val = row.get('Transformers')
        if transformers_val and safe_str(transformers_val):
            t_list = [t.strip() for t in str(transformers_val).split(',') if t.strip()]
            if t_list:
                # Use 'transformers' (plural) and list
                mapping_item["transformers"] = t_list

        # Validators
        validators_val = row.get('Validators')
        if validators_val and safe_str(validators_val):
            v_list = [v.strip() for v in str(validators_val).split(',') if v.strip()]
            if v_list:
                mapping_item["validators"] = v_list

        # Lookups
        if row.get('Lookup Table') and safe_str(row.get('Lookup Table')):
             mapping_item["lookupTable"] = row['Lookup Table']
        if row.get('Lookup By') and safe_str(row.get('Lookup By')):
             mapping_item["lookupBy"] = row['Lookup By']

        if row.get('Required'):
            mapping_item["required"] = True
            
        config_data["mappings"].append(mapping_item)

    return config_data

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
    page = st.radio("Go to", ["📊 Schema Mapper", "📁 File Explorer", "⚙️ Configuration"])
    st.divider()
    st.caption(f"📂 Root: {BASE_DIR}")

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
        
        # --- CONFIGURATION FORM ---
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
                default_deps = []
                current_cols = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
                if 'vn' in current_cols and selected_table != 'opd_visit': default_deps.append('visits')
                if 'hn' in current_cols and selected_table != 'patient': default_deps.append('patients')
                valid_defaults = [t for t in default_deps if t in all_tables]
                
                dependencies_input = st.multiselect("Dependencies", options=all_tables, default=valid_defaults, key=f"dep_{selected_table}")

        st.markdown("### 📋 Field Mapping")
        
        col_main, col_detail = st.columns([2, 1])
        
        columns_list = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
        default_sb_index = 0 
        editor_key = f"editor_{selected_table}"
        
        if editor_key in st.session_state:
            selection = st.session_state[editor_key].get("selection", {})
            selected_rows = selection.get("rows", [])
            if selected_rows and selected_rows[0] < len(columns_list):
                default_sb_index = selected_rows[0]

        with col_main:
            edited_df = safe_data_editor(
                st.session_state[f"df_{selected_table}"],
                column_config={
                    "Source Column": st.column_config.TextColumn(disabled=True),
                    "Type": st.column_config.TextColumn(disabled=True, width="small"),
                    "Transformers": st.column_config.TextColumn(disabled=True),
                    "Validators": st.column_config.TextColumn(disabled=True),
                    "Lookup Table": st.column_config.TextColumn(width="small"),
                    "Lookup By": st.column_config.TextColumn(width="small"),
                    "Sample": st.column_config.TextColumn(disabled=True, width="medium"),
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key=editor_key,
                height=500,
                selection_mode="single-row",
                on_select="rerun"
            )
            
            if not edited_df.equals(st.session_state[f"df_{selected_table}"]):
                st.session_state[f"df_{selected_table}"] = edited_df
                st.rerun()

        with col_detail:
            st.subheader("✏️ Field Settings")
            col_to_edit = st.selectbox("Select Field", columns_list, index=default_sb_index, key="sb_field_selector")
            
            if col_to_edit:
                row_idx = st.session_state[f"df_{selected_table}"].index[
                    st.session_state[f"df_{selected_table}"]['Source Column'] == col_to_edit
                ].tolist()[0]
                
                row_data = st.session_state[f"df_{selected_table}"].iloc[row_idx]
                
                st.info(f"Target: `{row_data['Target Column']}`")
                
                tab1, tab2 = st.tabs(["Pipeline", "Lookup"])
                
                with tab1:
                    curr_trans = row_data.get('Transformers', '')
                    def_trans = [t.strip() for t in str(curr_trans).split(', ') if t.strip() and t.strip() in TRANSFORMER_OPTIONS]
                    new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_t_{row_idx}")
                    
                    curr_val = row_data.get('Validators', '')
                    def_val = [v.strip() for v in str(curr_val).split(', ') if v.strip() and v.strip() in VALIDATOR_OPTIONS]
                    new_val = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_val, key=f"ms_v_{row_idx}")

                with tab2:
                    new_lookup_table = st.text_input("Lookup Table", value=safe_str(row_data.get('Lookup Table', '')), key=f"txt_lt_{row_idx}")
                    new_lookup_by = st.text_input("Lookup By Field", value=safe_str(row_data.get('Lookup By', '')), key=f"txt_lb_{row_idx}")

                if st.button("Apply Changes", type="primary", use_container_width=True, key=f"btn_apply_{row_idx}"):
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Transformers'] = ", ".join(new_trans)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Validators'] = ", ".join(new_val)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup Table'] = new_lookup_table
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup By'] = new_lookup_by
                    st.toast("Saved!", icon="💾")
                    st.rerun()

        st.markdown("---")
        st.markdown("### 💻 Generated Registry Config (JSON)")
        
        params = {
            "table_name": selected_table,
            "module": module_input,
            "source_db": source_db_input,
            "target_db": target_db_input,
            "target_table": target_table_input,
            "dependencies": dependencies_input
        }
        
        # Logic for JSON Generation
        json_data = generate_json_config(params, st.session_state[f"df_{selected_table}"])
        json_str = json.dumps(json_data, indent=2, ensure_ascii=False)
        
        # --- ACTION AREA ---
        ac_left, ac_right = st.columns([1, 1])
        
        with ac_left:
            st.download_button(
                label="📥 Download JSON Config",
                data=json_str,
                file_name=f"{selected_table}.json",
                mime="application/json",
                type="primary",
                use_container_width=True
            )
            
        with ac_right:
            # Toggle for Tree View (Default is True as requested)
            is_expanded = st.toggle("Expand JSON Tree", value=True)

        # --- PREVIEW AREA (Tabs for Tree vs Raw/Copy) ---
        tab_tree, tab_raw = st.tabs(["🌳 Tree View", "📄 Raw / Copy"])
        
        with tab_tree:
            # st.json expanded defaults to is_expanded (which is True)
            st.json(json_data, expanded=is_expanded)
            
        with tab_raw:
            st.caption("คลิกปุ่มไอคอน 📄 มุมขวาบนเพื่อ Copy")
            st.code(json_str, language="json")

    else:
        st.info("Please select a table.")

elif page == "📁 File Explorer":
    st.subheader("Project Files Structure")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📂 Analysis Report")
        if os.path.exists(ANALYSIS_DIR): st.code("\n".join(os.listdir(ANALYSIS_DIR)))
    with col2:
        st.markdown("### 📂 Mini HIS (Mockup)")
        mini_his_dir = os.path.join(BASE_DIR, "mini_his")
        if os.path.exists(mini_his_dir): st.code("\n".join(os.listdir(mini_his_dir)))

elif page == "⚙️ Configuration":
    st.subheader("Database Configuration")
    config = load_config()
    if config: st.json(config)