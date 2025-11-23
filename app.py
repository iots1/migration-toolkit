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
    "REMOVE_PREFIX", "REPLACE_EMPTY_WITH_NULL"
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
    if os.path.exists(csv_path): return pd.read_csv(csv_path)
    return None

def to_camel_case(snake_str):
    if pd.isna(snake_str): return ""
    components = str(snake_str).split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def init_editor_state(df, table_name):
    """Initialize session state for a new table"""
    if f"df_{table_name}" not in st.session_state:
        # Prepare initial data
        editor_data = []
        for _, row in df.iterrows():
            src_col = row['Column']
            dtype = row['DataType']
            target_col = to_camel_case(src_col)
            
            # Auto-Guess Logic
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
                "Validators": ", ".join(validators),  # Now storing as comma-sep string
                "Required": False,
                "Ignore": False,
                "Sample": str(row.get('Sample_Values', ''))[:50]
            })
        st.session_state[f"df_{table_name}"] = pd.DataFrame(editor_data)

def generate_ts_definition(table_name, target_table, mappings_df):
    mappings_str = ""
    for _, row in mappings_df.iterrows():
        if row['Ignore']: continue
            
        props = [f"      source: '{row['Source Column']}'", f"      target: '{row['Target Column']}'"]
        
        # Handle Transformers (Array)
        transformers_val = row.get('Transformers')
        if transformers_val and str(transformers_val).strip():
            t_list = [t.strip() for t in str(transformers_val).split(',') if t.strip()]
            if t_list:
                t_str = "', '".join(t_list)
                props.append(f"      transformers: ['{t_str}']")

        # Handle Validators (Array)
        validators_val = row.get('Validators')
        if validators_val and str(validators_val).strip():
            v_list = [v.strip() for v in str(validators_val).split(',') if v.strip()]
            if v_list:
                v_str = "', '".join(v_list)
                props.append(f"      validators: ['{v_str}']")

        if row['Required']: props.append(f"      required: true")
            
        mappings_str += "    {\n" + ",\n".join(props) + "\n    },\n"

    class_name = str(table_name).capitalize() + "Definition"
    return f"""import {{ TableDefinition }} from '../../../types';

export const {class_name}: TableDefinition = {{
  name: '{table_name}',
  targetTable: '{target_table}',
  description: 'Auto-generated definition for {table_name}',
  
  defaultBatchSize: 5000,
  defaultPriority: 50,
  
  commonMappings: [
{mappings_str}  ]
}};"""

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
            st.error("No reports found!")
            st.stop()
        selected_folder = st.selectbox("Run ID", report_folders, format_func=lambda x: os.path.basename(x))
        df_raw = load_data_profile(selected_folder)
        if df_raw is None: st.stop()
        
    with c2:
        tables = df_raw['Table'].unique()
        selected_table = st.selectbox("Select Table to Map", tables)

    if selected_table:
        st.markdown("---")
        
        # Initialize State
        init_editor_state(df_raw[df_raw['Table'] == selected_table], selected_table)
        
        # Layout: Main Editor + Detail Panel
        col_main, col_detail = st.columns([2, 1])
        
        with col_main:
            st.subheader(f"📋 Mapping Table: {selected_table}")
            
            # MAIN TABLE EDITOR
            edited_df = st.data_editor(
                st.session_state[f"df_{selected_table}"],
                column_config={
                    "Source Column": st.column_config.TextColumn(disabled=True),
                    "Type": st.column_config.TextColumn(disabled=True, width="small"),
                    "Transformers": st.column_config.TextColumn(disabled=True, help="Use 'Advanced Editor' to edit"),
                    "Validators": st.column_config.TextColumn(disabled=True, help="Use 'Advanced Editor' to edit"),
                },
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key=f"editor_{selected_table}",
                height=500
            )
            
            # Sync manual table edits back to session state
            if not edited_df.equals(st.session_state[f"df_{selected_table}"]):
                st.session_state[f"df_{selected_table}"] = edited_df
                st.rerun()

        with col_detail:
            st.subheader("✏️ Advanced Editor")
            st.info("Select field to edit Pipelines")
            
            # 1. Select Column to Edit
            columns_list = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
            col_to_edit = st.selectbox("Select Field", columns_list)
            
            if col_to_edit:
                # Get current row data
                current_row_idx = st.session_state[f"df_{selected_table}"].index[
                    st.session_state[f"df_{selected_table}"]['Source Column'] == col_to_edit
                ].tolist()[0]
                
                current_row = st.session_state[f"df_{selected_table}"].iloc[current_row_idx]
                
                # --- TRANSFORMERS ---
                st.caption("🛠️ Transformers Pipeline")
                current_trans_str = current_row.get('Transformers', '')
                default_trans = [t.strip() for t in str(current_trans_str).split(', ') if t.strip()]
                default_trans = [t for t in default_trans if t in TRANSFORMER_OPTIONS] # Filter valid

                new_transformers = st.multiselect(
                    "Select Transformers",
                    options=TRANSFORMER_OPTIONS,
                    default=default_trans,
                    key="ms_trans",
                    label_visibility="collapsed"
                )

                # --- VALIDATORS ---
                st.caption("🛡️ Validators Pipeline")
                current_val_str = current_row.get('Validators', '')
                default_val = [v.strip() for v in str(current_val_str).split(', ') if v.strip()]
                default_val = [v for v in default_val if v in VALIDATOR_OPTIONS] # Filter valid

                new_validators = st.multiselect(
                    "Select Validators",
                    options=VALIDATOR_OPTIONS,
                    default=default_val,
                    key="ms_val",
                    label_visibility="collapsed"
                )
                
                # 3. Apply Button
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("Update Pipeline", type="primary", use_container_width=True):
                    # Update DataFrame
                    trans_str = ", ".join(new_transformers)
                    val_str = ", ".join(new_validators)
                    
                    st.session_state[f"df_{selected_table}"].at[current_row_idx, 'Transformers'] = trans_str
                    st.session_state[f"df_{selected_table}"].at[current_row_idx, 'Validators'] = val_str
                    
                    st.toast(f"Updated {col_to_edit}!", icon="✅")
                    st.rerun()

        # Code Gen Section
        st.markdown("---")
        st.subheader("💻 Result")
        target_table_input = st.text_input("Target Table Name", value=selected_table)
        
        if st.button("⚡ Generate Config Code"):
            ts_code = generate_ts_definition(selected_table, target_table_input, st.session_state[f"df_{selected_table}"])
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