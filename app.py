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
            # Try reading with standard settings first
            return pd.read_csv(csv_path, encoding='utf-8')
        except pd.errors.ParserError:
            # If parsing fails, use more lenient settings
            st.warning("⚠️ CSV parsing issue detected. Using fallback parser. Some rows may be skipped.")
            return pd.read_csv(
                csv_path,
                on_bad_lines='skip',
                encoding='utf-8',
                engine='python',
                quoting=1,
                escapechar='\\'
            )
    return None

def to_camel_case(snake_str):
    if pd.isna(snake_str): return ""
    components = str(snake_str).split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def init_editor_state(df, table_name):
    """Initialize session state for a new table"""
    if f"df_{table_name}" not in st.session_state:
        # Reset selection state
        if "last_selected_row" in st.session_state:
            del st.session_state["last_selected_row"]
            
        # Prepare initial data
        editor_data = []
        for _, row in df.iterrows():
            src_col = row['Column']
            dtype = row['DataType']
            target_col = to_camel_case(src_col)
            
            transformers = []
            validators = []
            
            dtype_str = str(dtype).lower()
            if "char" in dtype_str or "text" in dtype_str:
                transformers.append("TRIM")
            if "date" in dtype_str:
                transformers.append("BUDDHIST_TO_ISO")
                validators.append("VALID_DATE")
            if src_col == "hn": 
                transformers = ["UPPER_TRIM"]
                validators = ["HN_FORMAT"]
            if src_col == "cid":
                transformers = ["TRIM"]
                validators = ["THAI_ID"]
            
            editor_data.append({
                "Source Column": src_col,
                "Type": dtype,
                "Target Column": target_col,
                "Transformers": ", ".join(transformers),
                "Validators": ", ".join(validators),
                "Required": False,
                "Ignore": False,
                "Lookup Table": "", # New Field for FK
                "Lookup By": "",    # New Field for FK
                "Sample": str(row.get('Sample_Values', ''))[:50]
            })
        st.session_state[f"df_{table_name}"] = pd.DataFrame(editor_data)

def generate_ts_config(params, mappings_df):
    """Generate Full TypeScript Config (Registry Pattern)"""
    
    # Extract params
    table_name = params['table_name']
    module = params['module']
    source_db = params['source_db']
    target_db = params['target_db']
    target_table = params['target_table']
    dependencies = params['dependencies']

    # Generate Mappings Array
    mappings_str = ""
    for _, row in mappings_df.iterrows():
        if row['Ignore']: continue
            
        props = []
        props.append(f"          source: '{row['Source Column']}'")
        props.append(f"          target: '{row['Target Column']}'")
        
        # Transformers
        transformers_val = row.get('Transformers')
        if transformers_val and str(transformers_val).strip():
            t_list = [t.strip() for t in str(transformers_val).split(',') if t.strip()]
            if t_list:
                if len(t_list) == 1:
                     props.append(f"          transformer: '{t_list[0]}'")
                else:
                     t_str = "', '".join(t_list)
                     props.append(f"          transformers: ['{t_str}']")

        # Validators
        validators_val = row.get('Validators')
        if validators_val and str(validators_val).strip():
            v_list = [v.strip() for v in str(validators_val).split(',') if v.strip()]
            if v_list:
                if len(v_list) == 1:
                    props.append(f"          validator: '{v_list[0]}'")
                else:
                    v_str = "', '".join(v_list)
                    props.append(f"          validators: ['{v_str}']")

        # Lookups
        if row.get('Lookup Table') and str(row.get('Lookup Table')).strip():
             props.append(f"          lookupTable: '{row['Lookup Table']}'")
        if row.get('Lookup By') and str(row.get('Lookup By')).strip():
             props.append(f"          lookupBy: '{row['Lookup By']}'")

        if row['Required']: props.append(f"          required: true")
            
        mappings_str += "        {\n" + ",\n".join(props) + "\n        },\n"

    # Dependencies Array String
    deps_str = str(dependencies).replace("'", "'") if dependencies else "[]"

    # Generate Final Block
    return f"""    // {table_name}
    this.register({{
      name: '{table_name}',
      module: '{module}',
      priority: 50,
      source: {{ database: '{source_db}', table: '{table_name}' }},
      target: {{ database: '{target_db}', table: '{target_table}' }},
      batchSize: 5000,
      dependencies: {deps_str},
      mappings: [
{mappings_str}      ]
    }});"""

# --- SAFETY WRAPPER ---
def safe_data_editor(df, **kwargs):
    try:
        return st.data_editor(df, **kwargs)
    except TypeError:
        unsafe_args = ['selection_mode', 'on_select']
        clean_kwargs = {k: v for k, v in kwargs.items() if k not in unsafe_args}
        if "ver_warn" not in st.session_state:
            st.warning(f"⚠️ Running in compatibility mode. Update Streamlit for better experience.")
            st.session_state["ver_warn"] = True
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
    
    # Top Bar
    c1, c2 = st.columns([1, 3])
    with c1:
        if not report_folders:
            st.warning("No reports found.")
            st.stop()
        selected_folder = st.selectbox("Run ID", report_folders, format_func=lambda x: os.path.basename(x))
        df_raw = load_data_profile(selected_folder)
        if df_raw is None: st.stop()
        
    with c2:
        tables = df_raw['Table'].unique()
        selected_table = st.selectbox("Select Table to Map", tables)

    if selected_table:
        st.markdown("---")
        
        init_editor_state(df_raw[df_raw['Table'] == selected_table], selected_table)
        
        # --- CONFIGURATION FORM ---
        with st.expander("⚙️ Table Configuration", expanded=True):
            conf_c1, conf_c2, conf_c3 = st.columns(3)
            with conf_c1:
                module_input = st.text_input("Module", value="patient", help="e.g. patient, pharmacy, ipd")
                source_db_input = st.text_input("Source DB", value="hos_db")
            with conf_c2:
                target_db_input = st.text_input("Target DB", value="hospital_new")
                target_table_input = st.text_input("Target Table", value=selected_table)
            with conf_c3:
                # Dependencies multiselect
                all_tables = df_raw['Table'].unique().tolist()
                # Try to guess deps if possible (e.g. if has vn -> visits)
                default_deps = []
                current_cols = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
                if 'vn' in current_cols and selected_table != 'opd_visit': default_deps.append('visits')
                if 'hn' in current_cols and selected_table != 'patient': default_deps.append('patients')
                
                # Filter only existing tables in list
                valid_defaults = [t for t in default_deps if t in all_tables]
                
                dependencies_input = st.multiselect("Dependencies", options=all_tables, default=valid_defaults)

        st.markdown("### 📋 Field Mapping")
        
        col_main, col_detail = st.columns([2, 1])
        
        # Check selection logic
        columns_list = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
        default_sb_index = 0 
        editor_key = f"editor_{selected_table}"
        
        if editor_key in st.session_state:
            selection = st.session_state[editor_key].get("selection", {})
            selected_rows = selection.get("rows", [])
            if selected_rows and selected_rows[0] < len(columns_list):
                default_sb_index = selected_rows[0]

        with col_main:
            # MAIN TABLE EDITOR
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
                
                # ใช้ Tabs เพื่อแยกส่วนการทำงาน
                tab1, tab2 = st.tabs(["Pipeline", "Lookup"])
                
                with tab1:
                    # Transformers
                    curr_trans = row_data.get('Transformers', '')
                    def_trans = [t.strip() for t in str(curr_trans).split(', ') if t.strip() and t.strip() in TRANSFORMER_OPTIONS]
                    new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_t_{row_idx}")
                    
                    # Validators
                    curr_val = row_data.get('Validators', '')
                    def_val = [v.strip() for v in str(curr_val).split(', ') if v.strip() and v.strip() in VALIDATOR_OPTIONS]
                    new_val = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_val, key=f"ms_v_{row_idx}")

                with tab2:
                    # Lookup settings for FK
                    new_lookup_table = st.text_input("Lookup Table", value=str(row_data.get('Lookup Table', '')), key=f"txt_lt_{row_idx}")
                    new_lookup_by = st.text_input("Lookup By Field", value=str(row_data.get('Lookup By', '')), key=f"txt_lb_{row_idx}")

                if st.button("Apply Changes", type="primary", use_container_width=True):
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Transformers'] = ", ".join(new_trans)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Validators'] = ", ".join(new_val)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup Table'] = new_lookup_table
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup By'] = new_lookup_by
                    st.toast("Saved!", icon="💾")
                    st.rerun()

        # Code Gen Section
        st.markdown("---")
        st.subheader("💻 Generated Registry Config")
        
        params = {
            "table_name": selected_table,
            "module": module_input,
            "source_db": source_db_input,
            "target_db": target_db_input,
            "target_table": target_table_input,
            "dependencies": dependencies_input
        }
        
        if st.button("⚡ Generate Config"):
            ts_code = generate_ts_config(params, st.session_state[f"df_{selected_table}"])
            st.code(ts_code, language="typescript")

    else:
        st.info("Please select a table.")

# (File Explorer & Config pages remain the same...)
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