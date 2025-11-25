import streamlit as st
import json
import time
from config import DB_TYPES
import services.db_connector as connector
import database as db

def render_migration_engine_page():
    st.subheader("🚀 Data Migration Execution Engine")
    
    with st.expander("🔌 Database Connection Settings", expanded=True):
        datasources = db.get_datasources()
        
        col_src, col_tgt = st.columns(2)
        
        # --- Source Section ---
        with col_src:
            st.markdown("#### Source Database")
            src_options = ["Custom Connection"] + datasources['name'].tolist()
            src_select = st.selectbox("Select Source Profile", src_options, key="src_sel")
            
            if src_select != "Custom Connection":
                row = datasources[datasources['name'] == src_select].iloc[0]
                ds = db.get_datasource_by_id(int(row['id']))
                
                src_type = st.text_input("Type", ds['db_type'], key="src_t", disabled=True)
                c1, c2 = st.columns([3, 1])
                src_host = c1.text_input("Host", ds['host'], key="src_h", disabled=True)
                src_port = c2.text_input("Port", ds['port'], key="src_po", disabled=True)
                
                src_db = st.text_input("DB Name", ds['dbname'], key="src_d", disabled=True)
                src_user = ds['username']
                src_pass = ds['password']
                st.caption(f"User: {src_user}")
            else:
                src_type = st.selectbox("Database Type", DB_TYPES, key="src_t_c")
                c1, c2 = st.columns([3, 1])
                src_host = c1.text_input("Host", "192.168.1.10", key="src_h_c")
                src_port = c2.text_input("Port", "", key="src_po_c")
                
                src_db = st.text_input("Database Name", "hos_db", key="src_d_c")
                src_user = st.text_input("User", "sa", key="src_u_c")
                src_pass = st.text_input("Password", type="password", key="src_p_c")

            if st.button("Test Source", key="btn_src"):
                with st.spinner(f"Connecting to {src_host}:{src_port}..."):
                    # Pass PORT to connector
                    ok, msg = connector.test_db_connection(src_type, src_host, src_port, src_db, src_user, src_pass)
                    if ok: st.success(msg)
                    else: st.error(msg)

        # --- Target Section ---
        with col_tgt:
            st.markdown("#### Target Database")
            tgt_options = ["Custom Connection"] + datasources['name'].tolist()
            tgt_select = st.selectbox("Select Target Profile", tgt_options, key="tgt_sel")
            
            if tgt_select != "Custom Connection":
                row = datasources[datasources['name'] == tgt_select].iloc[0]
                ds = db.get_datasource_by_id(int(row['id']))
                
                tgt_type = st.text_input("Type", ds['db_type'], key="tgt_t", disabled=True)
                c3, c4 = st.columns([3, 1])
                tgt_host = c3.text_input("Host", ds['host'], key="tgt_h", disabled=True)
                tgt_port = c4.text_input("Port", ds['port'], key="tgt_po", disabled=True)
                
                tgt_db = st.text_input("DB Name", ds['dbname'], key="tgt_d", disabled=True)
                tgt_user = ds['username']
                tgt_pass = ds['password']
                st.caption(f"User: {tgt_user}")
            else:
                tgt_type = st.selectbox("Database Type", DB_TYPES, index=2, key="tgt_t_c")
                c3, c4 = st.columns([3, 1])
                tgt_host = c3.text_input("Host", "10.0.0.5", key="tgt_h_c")
                tgt_port = c4.text_input("Port", "", key="tgt_po_c")
                
                tgt_db = st.text_input("Database Name", "hospital_new", key="tgt_d_c")
                tgt_user = st.text_input("User", "admin", key="tgt_u_c")
                tgt_pass = st.text_input("Password", type="password", key="tgt_p_c")
                
            if st.button("Test Target", key="btn_tgt"):
                with st.spinner(f"Connecting to {tgt_host}:{tgt_port}..."):
                    # Pass PORT to connector
                    ok, msg = connector.test_db_connection(tgt_type, tgt_host, tgt_port, tgt_db, tgt_user, tgt_pass)
                    if ok: st.success(msg)
                    else: st.error(msg)

    st.divider()
    st.markdown("#### 📄 Load Configuration")
    
    tab_db, tab_file = st.tabs(["📚 From Project DB", "📂 Upload File"])
    config_data = None
    
    with tab_db:
        configs_df = db.get_configs_list()
        if not configs_df.empty:
            sel_config = st.selectbox("Select Saved Config", configs_df['config_name'])
            if sel_config:
                config_data = db.get_config_content(sel_config)
                st.info(f"Loaded: {sel_config}")
        else:
            st.warning("No configs saved in database yet.")

    with tab_file:
        uploaded = st.file_uploader("Upload .json", type=["json"])
        if uploaded:
            config_data = json.load(uploaded)

    if config_data:
        with st.expander("Preview Selected Config"):
            st.json(config_data, expanded=False)

    st.divider()
    st.markdown("#### ⚙️ Execution")
    if st.button("🚀 Start Migration", type="primary", disabled=(config_data is None), use_container_width=True):
        st.success("Migration Started... (Simulation)")
        
        col_act, col_log = st.columns([1, 2])
        with col_act:
             progress_bar = st.progress(0)
             time.sleep(1)
             progress_bar.progress(50)
             time.sleep(1)
             progress_bar.progress(100)
             st.balloons()