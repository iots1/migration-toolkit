import streamlit as st
import pandas as pd
import os
import json
import time
from config import TRANSFORMER_OPTIONS, VALIDATOR_OPTIONS
import utils.helpers as helpers
import database as db
from services.db_connector import get_tables_from_datasource, get_columns_from_table, test_db_connection
from utils.ui_components import inject_global_css # Import CSS

# --- AgGrid Imports ---
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode

# ==========================================
# DIALOGS (Define at module level)
# ==========================================
@st.dialog("Preview Configuration JSON")
def show_json_preview(json_data):
    st.caption("This is the JSON structure that will be saved.")
    # Convert dict to formatted string for raw view
    json_str = json.dumps(json_data, indent=2, ensure_ascii=False)
    st.code(json_str, language="json")

# ==========================================
# MAIN RENDER
# ==========================================
def render_schema_mapper_page():
    # 1. Inject Global CSS (Makes Primary buttons Green)
    inject_global_css()

    # --- HEADER & CONTROLS ---
    c_title, c_mode = st.columns([3, 1])
    with c_title:
        st.markdown("## 🗂️ Schema Mapper (AgGrid Edition)")
    
    if "mapper_focus_mode" not in st.session_state:
        st.session_state.mapper_focus_mode = False

    with c_mode:
        mode_btn_text = "🔍 Enter Focus Mode" if not st.session_state.mapper_focus_mode else "🔙 Exit Focus Mode"
        mode_btn_type = "secondary" if not st.session_state.mapper_focus_mode else "primary"
        if st.button(mode_btn_text, type=mode_btn_type, use_container_width=True):
            st.session_state.mapper_focus_mode = not st.session_state.mapper_focus_mode
            st.rerun()

    # Load datasources
    datasources_df = db.get_datasources()
    datasource_names = ["-- Select Datasource --"] + (datasources_df['name'].tolist() if not datasources_df.empty else [])

    # Session Init
    if "source_mode" not in st.session_state: st.session_state.source_mode = "Run ID"
    
    # Temporary variables
    selected_table = None
    df_raw = None
    source_db_input = None
    source_table_name = None
    loaded_config_json = None 
    
    # ==================== CONFIGURATION SECTION ====================
    if not st.session_state.mapper_focus_mode:
        
        with st.expander("📥 Source & Configuration", expanded=True):
            col_src_mode, col_src_sel = st.columns([1, 2])
            with col_src_mode:
                source_mode = st.radio(
                    "Source Mode", 
                    ["Run ID", "Datasource", "Saved Config", "Upload File"], 
                    horizontal=True
                )
                st.session_state.source_mode = source_mode

            # --- MODE 1: Run ID ---
            if source_mode == "Run ID":
                report_folders = helpers.get_report_folders()
                if report_folders:
                    with col_src_sel:
                        c1, c2 = st.columns(2)
                        sel_folder = c1.selectbox("Run ID", report_folders, format_func=os.path.basename)
                        df_profile = load_data_profile(sel_folder)
                        
                        if df_profile is not None:
                            tables = df_profile['Table'].unique()
                            if "sm_sel_table_idx" not in st.session_state: st.session_state.sm_sel_table_idx = 0
                            try:
                                sel_table = c2.selectbox("Source Table", tables, index=st.session_state.sm_sel_table_idx)
                            except:
                                sel_table = c2.selectbox("Source Table", tables, index=0)

                            if sel_table in tables:
                                st.session_state.sm_sel_table_idx = list(tables).index(sel_table)

                            df_raw = df_profile[df_profile['Table'] == sel_table].copy()
                            selected_table = sel_table
                            source_table_name = sel_table
                            source_db_input = "Run ID (CSV)"

            # --- MODE 2: Datasource ---
            elif source_mode == "Datasource":
                if datasource_names:
                    with col_src_sel:
                        src_ds_name = st.selectbox("Source DB", datasource_names, key="src_ds")
                        
                        if src_ds_name != "-- Select Datasource --":
                            src_ds = db.get_datasource_by_name(src_ds_name)
                            
                            if src_ds:
                                c_status, c_btn = st.columns([3, 1])
                                status_key = f"conn_status_{src_ds_name}"
                                if status_key not in st.session_state: st.session_state[status_key] = "unknown"
                                
                                with c_status:
                                    if st.session_state[status_key] == "success":
                                        st.success(f"🟢 Connected: {src_ds['host']}")
                                    elif st.session_state[status_key] == "fail":
                                        st.error(f"🔴 Connection Failed: {src_ds['host']}")
                                    else:
                                        st.info(f"⚪ Ready to connect: {src_ds['host']}")
                                
                                with c_btn:
                                    if st.button("📡 Test", key="btn_test_conn"):
                                        ok, msg = test_db_connection(
                                            src_ds['db_type'], src_ds['host'], src_ds['port'], 
                                            src_ds['dbname'], src_ds['username'], src_ds['password']
                                        )
                                        if ok:
                                            st.session_state[status_key] = "success"
                                            st.rerun()
                                        else:
                                            st.session_state[status_key] = "fail"
                                            st.toast(f"Connection Failed: {msg}")

                                if st.session_state[status_key] == "success":
                                    success, tables = get_tables_from_datasource(
                                        src_ds['db_type'], src_ds['host'], src_ds['port'], 
                                        src_ds['dbname'], src_ds['username'], src_ds['password']
                                    )
                                    if success:
                                        if "sm_src_tbl_idx" not in st.session_state: st.session_state.sm_src_tbl_idx = 0
                                        try:
                                            sel_table = st.selectbox("Source Table", tables, index=st.session_state.sm_src_tbl_idx, key="src_tbl")
                                        except:
                                            sel_table = st.selectbox("Source Table", tables, index=0, key="src_tbl")
                                        
                                        if sel_table in tables:
                                            st.session_state.sm_src_tbl_idx = list(tables).index(sel_table)

                                        source_db_input = src_ds_name
                                        source_table_name = sel_table
                                        
                                        ok, cols = get_columns_from_table(
                                            src_ds['db_type'], src_ds['host'], src_ds['port'], 
                                            src_ds['dbname'], src_ds['username'], src_ds['password'], sel_table
                                        )
                                        if ok:
                                            df_raw = pd.DataFrame({
                                                'Table': [sel_table]*len(cols),
                                                'Column': [c['name'] for c in cols],
                                                'DataType': [c['type'] for c in cols],
                                                'Sample_Values': [''] * len(cols)
                                            })
                                            selected_table = sel_table

            # --- MODE 3 & 4: Saved Config / Upload File ---
            elif source_mode in ["Saved Config", "Upload File"]:
                with col_src_sel:
                    config_data = None
                    if source_mode == "Saved Config":
                        configs_df = db.get_configs_list()
                        if not configs_df.empty:
                            sel_config = st.selectbox("Select Config", configs_df['config_name'])
                            if sel_config: config_data = db.get_config_content(sel_config)
                        else:
                            st.warning("No saved configurations found.")
                    else: 
                        uploaded_file = st.file_uploader("Upload JSON Config", type=["json"])
                        if uploaded_file:
                            try: config_data = json.load(uploaded_file)
                            except: st.error("Invalid JSON file")

                    if config_data:
                        loaded_config_json = config_data
                        src_info = config_data.get('source', {})
                        src_db_name = src_info.get('database')
                        src_tbl_name = src_info.get('table')
                        
                        if src_db_name and src_tbl_name:
                            src_ds = db.get_datasource_by_name(src_db_name)
                            schema_fetched = False
                            if src_ds:
                                ok, cols = get_columns_from_table(
                                    src_ds['db_type'], src_ds['host'], src_ds['port'], 
                                    src_ds['dbname'], src_ds['username'], src_ds['password'], src_tbl_name
                                )
                                if ok:
                                    st.success(f"✅ Loaded & Synced: {src_tbl_name} (from {src_db_name})")
                                    df_raw = pd.DataFrame({
                                        'Table': [src_tbl_name]*len(cols),
                                        'Column': [c['name'] for c in cols],
                                        'DataType': [c['type'] for c in cols],
                                        'Sample_Values': [''] * len(cols)
                                    })
                                    schema_fetched = True
                            
                            if not schema_fetched:
                                st.warning(f"⚠️ Offline Mode: Datasource '{src_db_name}' not reachable. Using saved mapping.")
                                mappings = config_data.get('mappings', [])
                                if mappings:
                                    df_raw = pd.DataFrame({
                                        'Table': [src_tbl_name]*len(mappings),
                                        'Column': [m['source'] for m in mappings],
                                        'DataType': ['Unknown'] * len(mappings), 
                                        'Sample_Values': [''] * len(mappings)
                                    })

                            if df_raw is not None:
                                selected_table = src_tbl_name
                                source_db_input = src_db_name
                                source_table_name = src_tbl_name

        # --- Context Switch Detection ---
        if selected_table:
            config_sig = loaded_config_json.get('name', '') if loaded_config_json else ''
            current_signature = f"{st.session_state.source_mode}|{source_db_input}|{source_table_name}|{config_sig}"
            last_signature = st.session_state.get("last_mapper_signature", "")
            
            if current_signature != last_signature:
                state_key = f"df_{selected_table}"
                if state_key in st.session_state:
                    del st.session_state[state_key]
                
                # Reset Target State if not loading a config
                if not loaded_config_json:
                    st.session_state.mapper_tgt_db = None
                    st.session_state.mapper_tgt_tbl = None
                    st.session_state.mapper_real_tgt_cols = []

                st.session_state.mapper_editor_ver = time.time()
                st.session_state.last_mapper_signature = current_signature

            st.session_state.mapper_active_table = selected_table
            st.session_state.mapper_df_raw = df_raw
            st.session_state.mapper_source_db = source_db_input
            st.session_state.mapper_source_tbl = source_table_name
            
            if loaded_config_json:
                st.session_state.mapper_loaded_config = loaded_config_json
                st.session_state.mapper_tgt_db = loaded_config_json.get('target', {}).get('database')
                st.session_state.mapper_tgt_tbl = loaded_config_json.get('target', {}).get('table')
            else:
                 st.session_state.mapper_loaded_config = None

    # ==================== MAPPING LOGIC (AgGrid) ====================
    active_table = st.session_state.get("mapper_active_table")
    active_df_raw = st.session_state.get("mapper_df_raw")
    loaded_config = st.session_state.get("mapper_loaded_config")

    if active_table and active_df_raw is not None:
        
        target_db_input = None
        target_table_input = None
        real_target_columns = []
        
        default_tgt_db = st.session_state.get("mapper_tgt_db")
        default_tgt_tbl = st.session_state.get("mapper_tgt_tbl")

        # --- Target Configuration ---
        if not st.session_state.mapper_focus_mode:
            st.markdown("---")
            with st.expander("📤 Target Table Configuration", expanded=True):
                c_tgt_1, c_tgt_2 = st.columns(2)
                
                tgt_idx = 0
                if default_tgt_db in datasource_names:
                    tgt_idx = datasource_names.index(default_tgt_db)
                
                target_db_input = c_tgt_1.selectbox("Target Datasource", datasource_names, index=tgt_idx, key="tgt_ds")
                
                target_tables = []
                if target_db_input and target_db_input != "-- Select Datasource --":
                    tgt_ds = db.get_datasource_by_name(target_db_input)
                    if tgt_ds:
                        ok, res = get_tables_from_datasource(
                            tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], 
                            tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password']
                        )
                        if ok: 
                            target_tables = res
                            # Select Table Logic
                            def_tbl_idx = target_tables.index(default_tgt_tbl) if (default_tgt_tbl and default_tgt_tbl in target_tables) else (target_tables.index(active_table) if active_table in target_tables else 0)
                            target_table_input = c_tgt_2.selectbox("Target Table", target_tables, index=def_tbl_idx, key="tgt_tbl_sel")
                        else:
                            target_table_input = c_tgt_2.text_input("Target Table", value=default_tgt_tbl if default_tgt_tbl else active_table, key="tgt_tbl_txt")
                    
                    if target_table_input:
                        ok, cols = get_columns_from_table(
                            tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], 
                            tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password'], target_table_input
                        )
                        if ok: real_target_columns = [c['name'] for c in cols]
                else:
                    target_table_input = c_tgt_2.text_input("Target Table", value="", placeholder="Please select datasource first", disabled=True, key="tgt_tbl_disabled")
            
            st.session_state.mapper_tgt_db = target_db_input
            st.session_state.mapper_tgt_tbl = target_table_input
            st.session_state.mapper_real_tgt_cols = real_target_columns
        else:
            target_db_input = st.session_state.get("mapper_tgt_db")
            target_table_input = st.session_state.get("mapper_tgt_tbl")
            real_target_columns = st.session_state.get("mapper_real_tgt_cols", [])
            st.info(f"🔎 Focus Mode: `{active_table}` -> `{target_table_input}`")

        # Initialize Data
        init_editor_state(active_df_raw, active_table, loaded_config)

        # ------------------ 1. AGGRID TABLE ------------------
        if not st.session_state.mapper_focus_mode:
            st.markdown("### 📋 Field Mapping")
            st.caption("Select a row to edit details below.")
        
        # Prepare DataFrame
        df_to_edit = st.session_state[f"df_{active_table}"].copy()
        
        # Configure Grid Options
        gb = GridOptionsBuilder.from_dataframe(df_to_edit)
        
        gb.configure_column("Status", editable=False, width=90, cellStyle={'textAlign': 'center'})
        gb.configure_column("Source Column", editable=False, width=200)
        gb.configure_column("Type", editable=False, width=120)
        
        if real_target_columns:
            gb.configure_column(
                "Target Column", editable=True, width=250,
                cellEditor='agRichSelectCellEditor',
                cellEditorParams={'values': real_target_columns, 'searchDebounceDelay': 200, 'allowTyping': True, 'filterList': True}
            )
        else:
            gb.configure_column("Target Column", editable=True, width=250)

        gb.configure_column("Transformers", editable=False, width=200)
        gb.configure_column("Validators", editable=False, width=200)
        gb.configure_column("Ignore", editable=True, cellRenderer='agCheckboxCellRenderer', cellEditor='agCheckboxCellEditor', width=80)

        gb.configure_selection('single')
        gb.configure_grid_options(suppressColumnVirtualisation=True)
        
        gridOptions = gb.build()

        grid_height = 500 if st.session_state.mapper_focus_mode else 400
        editor_ver = st.session_state.get("mapper_editor_ver", "v1")
        source_context = st.session_state.mapper_source_db if st.session_state.mapper_source_db else "unknown"
        unique_key = f"aggrid_{source_context}_{active_table}_{editor_ver}"

        grid_response = AgGrid(
            df_to_edit,
            gridOptions=gridOptions,
            height=grid_height,
            width='100%',
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED, 
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            key=unique_key
        )

        # Sync Grid Changes (Target/Ignore)
        if grid_response['data'] is not None:
            updated_df = pd.DataFrame(grid_response['data'])
            if not updated_df.equals(st.session_state[f"df_{active_table}"]):
                st.session_state[f"df_{active_table}"] = updated_df

        # ------------------ 2. QUICK EDIT PANEL ------------------
        selected_rows = grid_response['selected_rows']
        
        if selected_rows is not None and len(selected_rows) > 0:
            if isinstance(selected_rows, pd.DataFrame):
                sel_row = selected_rows.iloc[0].to_dict()
            else:
                sel_row = selected_rows[0]
            
            src_col = sel_row.get('Source Column')
            df_state = st.session_state[f"df_{active_table}"]
            row_idx = df_state.index[df_state['Source Column'] == src_col].tolist()
            
            if row_idx:
                idx = row_idx[0]
                with st.container(border=True):
                    st.markdown(f"#### ✏️ Edit: `{src_col}`")
                    
                    c_edit_1, c_edit_2, c_edit_3 = st.columns([1, 1, 1])
                    
                    current_tgt = sel_row.get('Target Column', '')
                    target_opts = [current_tgt] + [c for c in real_target_columns if c != current_tgt] if real_target_columns else [current_tgt]
                    target_opts = list(dict.fromkeys(target_opts))
                    
                    with c_edit_1:
                        new_target = st.selectbox("Target Column", target_opts, index=0, key=f"sb_tgt_{src_col}")
                    
                    current_trans = sel_row.get('Transformers', '')
                    def_trans = [t.strip() for t in str(current_trans).split(',') if t.strip() and t.strip() in TRANSFORMER_OPTIONS]
                    
                    with c_edit_2:
                        new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_tf_{src_col}")
                    
                    current_val = sel_row.get('Validators', '')
                    def_vals = [v.strip() for v in str(current_val).split(',') if v.strip() and v.strip() in VALIDATOR_OPTIONS]
                    
                    with c_edit_3:
                        new_vals = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_vals, key=f"ms_vd_{src_col}")
                    
                    # Update Button (Green because Primary)
                    if st.button("✅ Update Row", type="primary"):
                        st.session_state[f"df_{active_table}"].at[idx, 'Target Column'] = new_target
                        st.session_state[f"df_{active_table}"].at[idx, 'Transformers'] = ", ".join(new_trans)
                        st.session_state[f"df_{active_table}"].at[idx, 'Validators'] = ", ".join(new_vals)
                        st.session_state.mapper_editor_ver = time.time()
                        st.rerun()

        # ==================== BOTTOM CONTROLS ====================
        st.markdown("---")
        default_config_name = f"{active_table}_config"
        is_edit_existing = False
        if loaded_config and loaded_config.get('name'):
             default_config_name = loaded_config.get('name')
             is_edit_existing = (st.session_state.source_mode == "Saved Config")
        
        col_validate, col_preview, col_save = st.columns([1, 1, 2.5])
        
        # 1. Validate
        with col_validate:
            st.write("") 
            if st.button("🔍 Validate Targets", use_container_width=True):
                if not target_db_input or target_db_input == "-- Select Datasource --":
                     st.warning("⚠️ Please select a Target Datasource first.")
                else:
                    tgt_ds = db.get_datasource_by_name(target_db_input)
                    if tgt_ds:
                        with st.spinner(f"Connecting to Target..."):
                            is_connected, conn_msg = test_db_connection(
                                tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], 
                                tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password']
                            )
                        
                        if not is_connected:
                             st.error(f"❌ Cannot connect to Target DB: {conn_msg}")
                        else:
                            with st.spinner(f"Fetching columns for '{target_table_input}'..."):
                                ok, cols = get_columns_from_table(
                                    tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], 
                                    tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password'], target_table_input
                                )
                                if ok:
                                    real_target_columns = [c['name'] for c in cols]
                                    updated_df = validate_mapping_in_table(st.session_state[f"df_{active_table}"], real_target_columns)
                                    st.session_state[f"df_{active_table}"] = updated_df
                                    st.session_state.mapper_editor_ver = time.time() 
                                    st.rerun()
                                else:
                                    st.error(f"❌ Cannot fetch columns: {cols}")
                    else:
                        st.error("Target Datasource configuration not found.")

        # 2. Preview JSON (New Button)
        with col_preview:
            st.write("")
            if st.button("👁️ Preview JSON", use_container_width=True):
                current_df = st.session_state[f"df_{active_table}"]
                preview_config_name = default_config_name 
                
                params = {
                    "config_name": preview_config_name,
                    "table_name": active_table,
                    "module": loaded_config.get('module', 'patient') if loaded_config else 'patient',
                    "source_db": st.session_state.get("mapper_source_db"),
                    "target_db": target_db_input,
                    "target_table": target_table_input,
                    "dependencies": []
                }
                json_data = generate_json_config(params, current_df)
                show_json_preview(json_data)

        # 3. Save Controls
        with col_save:
            def do_save(save_name):
                current_df = st.session_state[f"df_{active_table}"]
                params = {
                    "config_name": save_name,
                    "table_name": active_table,
                    "module": loaded_config.get('module', 'patient') if loaded_config else 'patient',
                    "source_db": st.session_state.get("mapper_source_db"),
                    "target_db": target_db_input,
                    "target_table": target_table_input,
                    "dependencies": []
                }
                json_data = generate_json_config(params, current_df)
                success, msg = db.save_config_to_db(params['config_name'], active_table, json_data)
                if success: st.success(msg)
                else: st.error(msg)
            
            if is_edit_existing:
                c_ovr, c_new = st.columns([1, 1.5])
                with c_ovr:
                     st.write("") 
                     st.write("") 
                     # Save (Overwrite) -> Primary -> Green
                     if st.button(f"💾 Save (Overwrite)", type="primary", use_container_width=True, help=f"Update '{default_config_name}'"):
                        do_save(default_config_name)
                with c_new:
                    new_name_input = st.text_input("New Config Name", value=f"{default_config_name}_copy", label_visibility="visible")
                    if st.button("🆕 Save as New", use_container_width=True):
                         do_save(new_name_input)
            else:
                c_input, c_btn = st.columns([2, 1])
                with c_input:
                    save_name_input = st.text_input("Config Name", value=default_config_name, label_visibility="visible")
                with c_btn:
                    st.write("") 
                    st.write("") 
                    # Save Config -> Primary -> Green
                    if st.button("💾 Save Configuration", type="primary", use_container_width=True):
                        do_save(save_name_input)

# --- Helpers ---

def init_editor_state(df, table_name, config_json=None):
    state_key = f"df_{table_name}"
    if state_key not in st.session_state:
        mapping_dict = {}
        if config_json:
            for m in config_json.get('mappings', []):
                mapping_dict[m['source']] = m
        
        editor_data = []
        for _, row in df.iterrows():
            src_col = row.get('Column', '')
            dtype = row.get('DataType', '')
            
            target_col = helpers.to_snake_case(src_col)
            transformers = []
            validators = []
            ignore = False
            
            if src_col in mapping_dict:
                rule = mapping_dict[src_col]
                target_col = rule.get('target', target_col)
                if 'transformers' in rule:
                    transformers = rule['transformers']
                if 'validators' in rule:
                    validators = rule['validators']
            elif not config_json: 
                if "date" in str(dtype).lower(): 
                    transformers.append("BUDDHIST_TO_ISO")
                    validators.append("VALID_DATE")
            
            editor_data.append({
                "Status": "",
                "Source Column": src_col, 
                "Type": dtype, 
                "Target Column": target_col,
                "Transformers": ", ".join(transformers), 
                "Validators": ", ".join(validators),
                "Required": False, 
                "Ignore": ignore
            })
        st.session_state[state_key] = pd.DataFrame(editor_data)

def validate_mapping_in_table(df_mapping, real_columns):
    if not real_columns:
        return df_mapping

    df_mapping['Status'] = df_mapping['Status'].astype(str)
    
    valid_count = 0
    invalid_count = 0
    
    for idx, row in df_mapping.iterrows():
        tgt = row['Target Column']
        ignore = row.get('Ignore', False)
        
        if ignore:
            df_mapping.at[idx, 'Status'] = "⚪ Skip"
            continue
            
        if not tgt:
             df_mapping.at[idx, 'Status'] = "⚠️ Empty"
             continue

        if tgt in real_columns:
            df_mapping.at[idx, 'Status'] = "✅ OK"
            valid_count += 1
        else:
            df_mapping.at[idx, 'Status'] = "❌ Invalid"
            invalid_count += 1
    
    if invalid_count > 0:
        st.toast(f"Validation Finished: {invalid_count} errors found.", icon="❌")
    else:
        st.toast(f"Validation Finished: All {valid_count} columns valid.", icon="✅")
        
    return df_mapping

def load_data_profile(report_folder):
    csv_path = os.path.join(report_folder, "data_profile", "data_profile.csv")
    if os.path.exists(csv_path): 
        try: return pd.read_csv(csv_path, on_bad_lines='skip')
        except: return None
    return None

def generate_json_config(params, mappings_df):
    config_data = {
        "name": params['config_name'],
        "module": params['module'],
        "source": {"database": params['source_db'], "table": params['table_name']},
        "target": {"database": params['target_db'], "table": params['target_table']},
        "mappings": []
    }
    for _, row in mappings_df.iterrows():
        if row.get('Ignore', False): continue
        
        mapping_item = {
            "source": row['Source Column'], 
            "target": row['Target Column']
        }
        
        tf_val = row.get('Transformers')
        if tf_val:
            if isinstance(tf_val, list):
                mapping_item["transformers"] = tf_val
            elif isinstance(tf_val, str) and tf_val.strip():
                mapping_item["transformers"] = [t.strip() for t in tf_val.split(',') if t.strip()]

        vd_val = row.get('Validators')
        if vd_val:
            if isinstance(vd_val, list):
                mapping_item["validators"] = vd_val
            elif isinstance(vd_val, str) and vd_val.strip():
                mapping_item["validators"] = [v.strip() for v in vd_val.split(',') if v.strip()]
            
        config_data["mappings"].append(mapping_item)
    return config_data