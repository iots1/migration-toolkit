import streamlit as st
import time
from config import DB_TYPES
import database as db

def render_settings_page():
    st.subheader("🛠️ Project Settings (SQLite)")
    
    tab_ds, tab_conf = st.tabs(["🔌 Datasources", "📄 Saved Configs"])
    
    with tab_ds:
        render_datasource_tab()

    with tab_conf:
        st.markdown("#### Existing Saved Configs")
        cf_df = db.get_configs_list()
        st.dataframe(cf_df, use_container_width=True)

def render_datasource_tab():
    # --- RESET LOGIC ---
    if st.session_state.get("trigger_ds_reset", False):
        clear_form_state()
        st.session_state.trigger_ds_reset = False

    # State Init
    init_form_state()

    form_title = "Update Datasource" if st.session_state.is_edit_mode else "Add New Datasource"
    st.markdown(f"#### {form_title}")
    
    with st.container(border=True):
        c1, c2 = st.columns(2)
        ds_name = c1.text_input("Profile Name (Unique)", key="new_ds_name")
        ds_type = c2.selectbox("Type", DB_TYPES, index=st.session_state.ds_form_type_index, key="new_ds_type")
        c3, c4 = st.columns(2)
        ds_host = c3.text_input("Host", key="new_ds_host")
        ds_port = c4.text_input("Port (Optional)", key="new_ds_port")
        c5, c6, c7 = st.columns(3)
        ds_db = c5.text_input("DB Name", key="new_ds_db")
        ds_user = c6.text_input("Username", key="new_ds_user")
        ds_pass = c7.text_input("Password", type="password", key="new_ds_pass")
        
        b1, b2 = st.columns([1, 4])
        if st.session_state.is_edit_mode:
            if b1.button("Update Profile", type="primary", use_container_width=True):
                if ds_name and ds_host:
                    # Pass variables correctly
                    confirm_update_dialog(st.session_state.edit_ds_id, ds_name, ds_type, ds_host, ds_port, ds_db, ds_user, ds_pass)
                else:
                    st.error("Name and Host are required.")
            
            if b2.button("Cancel Edit"):
                st.session_state.trigger_ds_reset = True
                st.session_state.is_edit_mode = False
                st.session_state.edit_ds_id = None
                st.rerun()
        else:
            if st.button("Save Datasource", type="primary"):
                if ds_name and ds_host:
                    ok, msg = db.save_datasource(ds_name, ds_type, ds_host, ds_port, ds_db, ds_user, ds_pass)
                    if ok:
                        st.success("Saved!")
                        st.session_state.trigger_ds_reset = True
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.error("Name and Host are required.")

    st.markdown("#### Existing Datasources")
    ds_df = db.get_datasources()
    
    if not ds_df.empty:
        display_datasources_list(ds_df)
    else:
        st.info("No datasources defined.")

def display_datasources_list(ds_df):
    h1, h2, h3, h4 = st.columns([2, 1, 3, 2])
    h1.markdown("**Name**")
    h2.markdown("**Type**")
    h3.markdown("**Host**")
    h4.markdown("**Actions**")
    st.divider()
    
    for index, row in ds_df.iterrows():
        r1, r2, r3, r4 = st.columns([2, 1, 3, 2])
        with r1: st.write(row['name'])
        with r2: st.caption(row['db_type'])
        with r3: st.code(f"{row['username']}@{row['host']}/{row['dbname']}")
        with r4:
            c_edit, c_del = st.columns(2)
            c_edit.button("✏️", key=f"edit_{row['id']}", on_click=load_edit_data, args=(row['id'],))
            if c_del.button("🗑️", key=f"del_{row['id']}"):
                confirm_delete_dialog(row['id'], row['name'])
        st.divider()

# --- Helper State Functions ---
def init_form_state():
    defaults = {"new_ds_name": "", "new_ds_host": "", "new_ds_port": "", 
                "new_ds_db": "", "new_ds_user": "", "new_ds_pass": "",
                "ds_form_type_index": 0, "is_edit_mode": False, "edit_ds_id": None}
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v

def clear_form_state():
    st.session_state.new_ds_name = ""
    st.session_state.new_ds_host = ""
    st.session_state.new_ds_port = ""
    st.session_state.new_ds_db = ""
    st.session_state.new_ds_user = ""
    st.session_state.new_ds_pass = ""
    st.session_state.ds_form_type_index = 0

def load_edit_data(ds_id):
    full_data = db.get_datasource_by_id(ds_id)
    if full_data:
        st.session_state.new_ds_name = full_data['name']
        try:
            st.session_state.ds_form_type_index = DB_TYPES.index(full_data['db_type'])
        except:
            st.session_state.ds_form_type_index = 0
        st.session_state.new_ds_host = full_data['host']
        st.session_state.new_ds_port = full_data['port']
        st.session_state.new_ds_db = full_data['dbname']
        st.session_state.new_ds_user = full_data['username']
        st.session_state.new_ds_pass = full_data['password']
        st.session_state.is_edit_mode = True
        st.session_state.edit_ds_id = ds_id

# --- Dialogs ---
@st.dialog("Confirm Deletion")
def confirm_delete_dialog(ds_id, ds_name):
    st.warning(f"Delete profile: **{ds_name}**?")
    if st.button("Confirm Delete", type="primary", use_container_width=True):
        db.delete_datasource(ds_id)
        st.success("Deleted!")
        time.sleep(0.5)
        st.rerun()

# FIX: Changed argument 'db' to 'dbname_val' to avoid shadowing module 'db'
@st.dialog("Confirm Update")
def confirm_update_dialog(id, name, db_type, host, port, dbname_val, user, pwd):
    st.info(f"Update profile: **{name}**?")
    if st.button("Yes, Update", type="primary", use_container_width=True):
        # Now 'db' refers to the imported module correctly
        ok, msg = db.update_datasource(id, name, db_type, host, port, dbname_val, user, pwd)
        if ok:
            st.success("Updated!")
            st.session_state.trigger_ds_reset = True
            st.session_state.edit_ds_id = None 
            st.session_state.is_edit_mode = False
            time.sleep(0.5)
            st.rerun()
        else:
            st.error(msg)