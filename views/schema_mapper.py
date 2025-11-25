import streamlit as st
import pandas as pd
import os
import json
from config import TRANSFORMER_OPTIONS, VALIDATOR_OPTIONS
import utils.helpers as helpers
import database as db
from services.db_connector import get_tables_from_datasource, get_columns_from_table

def render_schema_mapper_page():
    report_folders = helpers.get_report_folders()
    
    c1, c2 = st.columns([1, 3])
    with c1:
        if not report_folders:
            st.warning("No reports found.")
            return

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
        if df_raw is None: return
        
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
            # Load datasources
            datasources_df = db.get_datasources()
            if datasources_df.empty:
                st.warning("⚠️ No datasources configured. Please add datasources in Settings page.")
                datasource_names = []
            else:
                datasource_names = datasources_df['name'].tolist()

            conf_c1, conf_c2, conf_c3 = st.columns(3)
            with conf_c1:
                module_input = st.text_input("Module", value="patient", key=f"mod_{selected_table}")

                # Source DB as datasource dropdown
                source_db_index = 0
                if f"source_db_{selected_table}" in st.session_state and st.session_state[f"source_db_{selected_table}"] in datasource_names:
                    source_db_index = datasource_names.index(st.session_state[f"source_db_{selected_table}"])

                if datasource_names:
                    source_db_input = st.selectbox(
                        "Source DB",
                        options=datasource_names,
                        index=source_db_index,
                        key=f"src_{selected_table}",
                        help="Select datasource from configured list"
                    )

                    # Show available tables from source datasource
                    source_datasource = db.get_datasource_by_name(source_db_input)
                    if source_datasource:
                        success, result = get_tables_from_datasource(
                            source_datasource['db_type'],
                            source_datasource['host'],
                            source_datasource['port'],
                            source_datasource['dbname'],
                            source_datasource['username'],
                            source_datasource['password']
                        )
                        if success and result:
                            st.caption(f"📊 {len(result)} tables available")
                else:
                    source_db_input = st.text_input("Source DB", value="", key=f"src_{selected_table}", disabled=True)

            with conf_c2:
                # Target DB as datasource dropdown
                target_db_index = 0
                if f"target_db_{selected_table}" in st.session_state and st.session_state[f"target_db_{selected_table}"] in datasource_names:
                    target_db_index = datasource_names.index(st.session_state[f"target_db_{selected_table}"])

                if datasource_names:
                    target_db_input = st.selectbox(
                        "Target DB",
                        options=datasource_names,
                        index=target_db_index,
                        key=f"tgt_{selected_table}",
                        help="Select datasource from configured list"
                    )

                    # Load tables from target datasource
                    target_datasource = db.get_datasource_by_name(target_db_input)
                    if target_datasource:
                        success, result = get_tables_from_datasource(
                            target_datasource['db_type'],
                            target_datasource['host'],
                            target_datasource['port'],
                            target_datasource['dbname'],
                            target_datasource['username'],
                            target_datasource['password']
                        )

                        if success and result:
                            target_tables = result
                            target_table_index = 0
                            if selected_table in target_tables:
                                target_table_index = target_tables.index(selected_table)

                            target_table_input = st.selectbox(
                                "Target Table",
                                options=target_tables,
                                index=target_table_index,
                                key=f"tbl_{selected_table}",
                                help="Select target table from database"
                            )
                        else:
                            st.warning(f"⚠️ Could not load tables: {result}")
                            target_table_input = st.text_input("Target Table", value=selected_table, key=f"tbl_{selected_table}")
                    else:
                        target_table_input = st.text_input("Target Table", value=selected_table, key=f"tbl_{selected_table}")
                else:
                    target_db_input = st.text_input("Target DB", value="", key=f"tgt_{selected_table}", disabled=True)
                    target_table_input = st.text_input("Target Table", value="", key=f"tbl_{selected_table}", disabled=True)

            with conf_c3:
                all_tables = df_raw['Table'].unique().tolist()
                dependencies_input = st.multiselect("Dependencies", options=all_tables, key=f"dep_{selected_table}")

        st.markdown("### 📋 Field Mapping")
        
        col_main, col_detail = st.columns([2, 1])
        columns_list = st.session_state[f"df_{selected_table}"]['Source Column'].tolist()
        
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

                # Get target column suggestions from target table
                target_column_suggestions = []
                if datasource_names and target_db_input and target_table_input:
                    target_datasource = db.get_datasource_by_name(target_db_input)
                    if target_datasource:
                        success, columns_result = get_columns_from_table(
                            target_datasource['db_type'],
                            target_datasource['host'],
                            target_datasource['port'],
                            target_datasource['dbname'],
                            target_datasource['username'],
                            target_datasource['password'],
                            target_table_input
                        )
                        if success and columns_result:
                            target_column_suggestions = [col['name'] for col in columns_result]

                # Target Column input with suggestions
                current_target = row_data.get('Target Column', '')
                if target_column_suggestions:
                    if current_target not in target_column_suggestions:
                        target_column_suggestions.insert(0, current_target)

                    target_col_index = 0
                    if current_target in target_column_suggestions:
                        target_col_index = target_column_suggestions.index(current_target)

                    new_target_column = st.selectbox(
                        "Target Column",
                        options=target_column_suggestions,
                        index=target_col_index,
                        key=f"tgt_col_{row_idx}",
                        help="Select from target table columns"
                    )
                else:
                    new_target_column = st.text_input(
                        "Target Column",
                        value=current_target,
                        key=f"tgt_col_{row_idx}"
                    )

                t1, t2 = st.tabs(["Pipeline", "Lookup"])
                with t1:
                    curr_trans = row_data.get('Transformers', '')
                    def_trans = [t.strip() for t in str(curr_trans).split(', ') if t.strip() in TRANSFORMER_OPTIONS]
                    new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_t_{row_idx}")
                    curr_val = row_data.get('Validators', '')
                    def_val = [v.strip() for v in str(curr_val).split(', ') if v.strip() in VALIDATOR_OPTIONS]
                    new_val = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_val, key=f"ms_v_{row_idx}")
                with t2:
                    new_lookup_table = st.text_input("Lookup Table", value=helpers.safe_str(row_data.get('Lookup Table', '')), key=f"txt_lt_{row_idx}")
                    new_lookup_by = st.text_input("Lookup By", value=helpers.safe_str(row_data.get('Lookup By', '')), key=f"txt_lb_{row_idx}")

                if st.button("Apply", type="primary", use_container_width=True, key=f"btn_{row_idx}"):
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Target Column'] = new_target_column
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Transformers'] = ", ".join(new_trans)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Validators'] = ", ".join(new_val)
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup Table'] = new_lookup_table
                    st.session_state[f"df_{selected_table}"].at[row_idx, 'Lookup By'] = new_lookup_by
                    st.rerun()

        st.markdown("---")
        st.markdown("### 💻 Generated Registry Config (JSON)")
        
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
            st.write("") 
            st.write("")
            if st.button("💾 Save to Project DB", type="secondary", use_container_width=True):
                success, msg = db.save_config_to_db(config_name_input, selected_table, json_data)
                if success: st.success(msg)
                else: st.error(msg)

        ac_left, ac_right = st.columns([1, 1])
        with ac_left:
            st.download_button("📥 Download JSON File", json_str, f"{selected_table}.json", "application/json", type="primary", use_container_width=True)
        with ac_right:
            is_expanded = st.toggle("Expand JSON Tree", value=True)

        t_tree, t_raw = st.tabs(["🌳 Tree View", "📄 Raw / Copy"])
        with t_tree: st.json(json_data, expanded=is_expanded)
        with t_raw: st.code(json_str, language="json")

# --- Internal Helpers for this View ---
@st.cache_data
def load_data_profile(report_folder):
    csv_path = os.path.join(report_folder, "data_profile", "data_profile.csv")
    if os.path.exists(csv_path): 
        try:
            return pd.read_csv(csv_path, on_bad_lines='skip')
        except:
            return None
    return None

def init_editor_state(df, table_name):
    state_key = f"df_{table_name}"
    if state_key not in st.session_state:
        editor_data = []
        for _, row in df.iterrows():
            src_col = row.get('Column', '')
            dtype = row.get('DataType', '')
            target_col = helpers.to_camel_case(src_col)
            
            transformers = []
            validators = []
            
            dtype_str = helpers.safe_str(dtype).lower()
            if "char" in dtype_str or "text" in dtype_str: transformers.append("TRIM")
            if "date" in dtype_str:
                transformers.append("BUDDHIST_TO_ISO")
                validators.append("VALID_DATE")
            
            src_lower = helpers.safe_str(src_col).lower()
            if src_lower == "hn": transformers = ["UPPER_TRIM"]; validators = ["HN_FORMAT"]
            if src_lower == "cid": transformers = ["TRIM"]; validators = ["THAI_ID"]
            
            editor_data.append({
                "Source Column": src_col, "Type": dtype, "Target Column": target_col,
                "Transformers": ", ".join(transformers), "Validators": ", ".join(validators),
                "Required": False, "Ignore": False, "Lookup Table": "", "Lookup By": "",
                "Sample": helpers.safe_str(row.get('Sample_Values', ''))[:50]
            })
        st.session_state[state_key] = pd.DataFrame(editor_data)

def safe_data_editor(df, **kwargs):
    try: return st.data_editor(df, **kwargs)
    except TypeError:
        unsafe_args = ['selection_mode', 'on_select']
        clean_kwargs = {k: v for k, v in kwargs.items() if k not in unsafe_args}
        return st.data_editor(df, **clean_kwargs)

def generate_json_config(params, mappings_df):
    config_data = {
        "name": params['config_name'],
        "module": params['module'],
        "priority": 50,
        "source": {"database": params['source_db'], "table": params['table_name']},
        "target": {"database": params['target_db'], "table": params['target_table']},
        "batchSize": 5000,
        "dependencies": params['dependencies'] if params['dependencies'] else [],
        "mappings": []
    }
    for _, row in mappings_df.iterrows():
        if row['Ignore']: continue
        mapping_item = {"source": row['Source Column'], "target": row['Target Column']}
        
        t_val = row.get('Transformers')
        if t_val and helpers.safe_str(t_val):
            t_list = [t.strip() for t in str(t_val).split(',') if t.strip()]
            if t_list: mapping_item["transformers"] = t_list

        v_val = row.get('Validators')
        if v_val and helpers.safe_str(v_val):
            v_list = [v.strip() for v in str(v_val).split(',') if v.strip()]
            if v_list: mapping_item["validators"] = v_list

        if row.get('Lookup Table') and helpers.safe_str(row.get('Lookup Table')):
             mapping_item["lookupTable"] = row['Lookup Table']
        if row.get('Lookup By') and helpers.safe_str(row.get('Lookup By')):
             mapping_item["lookupBy"] = row['Lookup By']

        if row.get('Required'): mapping_item["required"] = True
        config_data["mappings"].append(mapping_item)
    return config_data