import streamlit as st
import time
import json
from config import DB_TYPES
import database as db
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from utils.ui_components import inject_global_css, generic_confirm_dialog

# ==========================================
# MAIN RENDER
# ==========================================


def render_settings_page():
    # 1. Load Global Styles
    inject_global_css()

    st.subheader("🛠️ Project Settings (SQLite)")

    tab_ds, tab_conf = st.tabs(["🔌 Datasources", "📄 Saved Configs"])

    with tab_ds:
        render_datasource_tab()

    with tab_conf:
        render_configs_tab()


# ==========================================
# 1. DATASOURCES TAB
# ==========================================
def render_datasource_tab():
    # Helper for delete action (Callback for dialog)
    def execute_delete_ds(ds_id):
        db.delete_datasource(ds_id)
        st.success("Deleted Successfully!")
        st.session_state.trigger_ds_reset = True
        time.sleep(0.5)
        st.rerun()

    # --- RESET CHECK ---
    if st.session_state.get("trigger_ds_reset", False):
        reset_to_new_mode()
        st.session_state.trigger_ds_reset = False

    init_form_state()
    if "ds_grid_key" not in st.session_state:
        st.session_state.ds_grid_key = 0

    form_slot = st.empty()
    st.markdown("#### Existing Datasources")
    st.caption("Click a row to edit details above.")
    grid_slot = st.empty()

    # --- Grid Logic ---
    with grid_slot.container():
        ds_df = db.get_datasources()
        if not ds_df.empty:
            gb = GridOptionsBuilder.from_dataframe(ds_df)
            gb.configure_selection("single", use_checkbox=False)
            if "id" in ds_df.columns:
                gb.configure_column("id", hide=True)

            gb.configure_column(
                "name", header_name="Name", flex=1, filter=True, sortable=True
            )
            gb.configure_column("db_type", header_name="Type", width=120)
            gb.configure_column("host", header_name="Host", width=150)
            gb.configure_column("dbname", header_name="Database", width=150)
            gb.configure_column("username", header_name="User", width=120)

            gridOptions = gb.build()
            grid_key = f"ds_grid_{st.session_state.ds_grid_key}"

            grid_response = AgGrid(
                ds_df,
                gridOptions=gridOptions,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                fit_columns_on_grid_load=True,
                height=300,
                width="100%",
                key=grid_key,
            )

            selected = grid_response["selected_rows"]
            if selected is not None and len(selected) > 0:
                sel_row = (
                    selected[0] if isinstance(selected, list) else selected.iloc[0]
                )
                sel_id = sel_row.get("id")
                if sel_id != st.session_state.edit_ds_id:
                    load_edit_data(sel_id)
                    st.rerun()
        else:
            st.info("No datasources defined.")

    # --- Form Logic ---
    with form_slot.container():
        form_title = (
            "✏️ Edit Datasource"
            if st.session_state.is_edit_mode
            else "➕ Add New Datasource"
        )
        st.markdown(f"#### {form_title}")

        with st.container(border=True):
            c1, c2 = st.columns(2)
            ds_name = c1.text_input("Profile Name (Unique)", key="new_ds_name")
            ds_type = c2.selectbox(
                "Type",
                DB_TYPES,
                index=st.session_state.ds_form_type_index,
                key="new_ds_type",
            )

            c3, c4 = st.columns(2)
            ds_host = c3.text_input("Host", key="new_ds_host")
            ds_port = c4.text_input("Port (Optional)", key="new_ds_port")

            c5, c6, c7 = st.columns(3)
            ds_db = c5.text_input("DB Name", key="new_ds_db")
            ds_user = c6.text_input("Username", key="new_ds_user")
            ds_pass = c7.text_input("Password", type="password", key="new_ds_pass")

            st.divider()

            if st.session_state.is_edit_mode:
                b_col1, b_col2, b_col3 = st.columns([1, 1, 1])

                # Save (Green)
                if b_col1.button(
                    "💾 Save Changes", type="primary", use_container_width=True
                ):
                    if ds_name and ds_host:
                        ok, msg = db.update_datasource(
                            st.session_state.edit_ds_id,
                            ds_name,
                            ds_type,
                            ds_host,
                            ds_port,
                            ds_db,
                            ds_user,
                            ds_pass,
                        )
                        if ok:
                            st.success("Updated Successfully!")
                            st.session_state.trigger_ds_reset = True
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.error("Name required.")

                # Cancel
                b_col2.button(
                    "🚫 Cancel", use_container_width=True, on_click=reset_to_new_mode
                )

                # Delete (Trigger Dialog)
                if b_col3.button("🗑️ Delete Datasource", use_container_width=True):
                    generic_confirm_dialog(
                        title=f"Delete profile: {ds_name}?",
                        message="This will permanently delete this datasource configuration.",
                        confirm_label="Delete Datasource",
                        on_confirm_func=execute_delete_ds,
                        ds_id=st.session_state.edit_ds_id,
                    )
            else:
                # Add New (Green)
                if st.button(
                    "✨ Save New Datasource", type="primary", use_container_width=True
                ):
                    if ds_name and ds_host:
                        ok, msg = db.save_datasource(
                            ds_name, ds_type, ds_host, ds_port, ds_db, ds_user, ds_pass
                        )
                        if ok:
                            st.success("Saved Successfully!")
                            st.session_state.trigger_ds_reset = True
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.error("Name required.")


# ==========================================
# 2. SAVED CONFIGS TAB
# ==========================================
def render_configs_tab():
    # Helper for delete action (Callback for dialog)
    def execute_delete_config(conf_name):
        success, msg = db.delete_config(conf_name)
        if success:
            st.success(msg)
            time.sleep(0.5)
            st.rerun()
        else:
            st.error(msg)

    st.markdown("#### Existing Saved Configs")
    st.caption("Select a row to preview JSON or delete.")

    cf_df = db.get_configs_list()

    if cf_df.empty:
        st.info("No configurations saved yet.")
        return

    gb = GridOptionsBuilder.from_dataframe(cf_df)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_column(
        "config_name", header_name="Config Name", flex=1, filter=True, sortable=True
    )
    gb.configure_column(
        "source_table", header_name="Source Table", width=150, filter=True
    )
    gb.configure_column(
        "destination_table", header_name="Destination Table", width=150, filter=True
    )
    gb.configure_column(
        "updated_at", header_name="Last Updated", width=180, sortable=True
    )

    gb.configure_grid_options(domLayout="autoHeight")
    gridOptions = gb.build()

    grid_response = AgGrid(
        cf_df,
        gridOptions=gridOptions,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        height=400,
        width="100%",
        key="configs_grid",
    )

    selected = grid_response["selected_rows"]
    if selected is not None and len(selected) > 0:
        sel_row = selected[0] if isinstance(selected, list) else selected.iloc[0]
        config_name = sel_row.get("config_name")

        st.divider()
        st.markdown(f"**Selected Config:** `{config_name}`")

        c_view, c_del = st.columns([1, 1])

        with c_view:
            if st.button("👁️ Preview JSON", use_container_width=True, type="secondary"):
                preview_config_dialog(config_name)

        with c_del:
            # Delete Trigger (Reusing the generic dialog!)
            # Note: We use secondary button for trigger to avoid confusion with Save
            if st.button("🗑️ Delete Config", use_container_width=True, type="secondary"):
                generic_confirm_dialog(
                    title=f"Delete config: {config_name}?",
                    message="This action cannot be undone.",
                    confirm_label="Delete Config",
                    on_confirm_func=execute_delete_config,
                    conf_name=config_name,
                )


# ==========================================
# 3. HELPERS
# ==========================================


def init_form_state():
    defaults = {
        "new_ds_name": "",
        "new_ds_host": "",
        "new_ds_port": "",
        "new_ds_db": "",
        "new_ds_user": "",
        "new_ds_pass": "",
        "ds_form_type_index": 0,
        "is_edit_mode": False,
        "edit_ds_id": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def clear_form_state():
    st.session_state.new_ds_name = ""
    st.session_state.new_ds_host = ""
    st.session_state.new_ds_port = ""
    st.session_state.new_ds_db = ""
    st.session_state.new_ds_user = ""
    st.session_state.new_ds_pass = ""
    st.session_state.ds_form_type_index = 0


def reset_to_new_mode():
    clear_form_state()
    st.session_state.is_edit_mode = False
    st.session_state.edit_ds_id = None
    if "ds_grid_key" in st.session_state:
        st.session_state.ds_grid_key += 1


def load_edit_data(ds_id):
    full_data = db.get_datasource_by_id(ds_id)
    if full_data:
        st.session_state.new_ds_name = full_data["name"]
        try:
            st.session_state.ds_form_type_index = DB_TYPES.index(full_data["db_type"])
        except:
            st.session_state.ds_form_type_index = 0
        st.session_state.new_ds_host = full_data["host"]
        st.session_state.new_ds_port = full_data["port"]
        st.session_state.new_ds_db = full_data["dbname"]
        st.session_state.new_ds_user = full_data["username"]
        st.session_state.new_ds_pass = full_data["password"]
        st.session_state.is_edit_mode = True
        st.session_state.edit_ds_id = ds_id


# Local Preview Dialog (Still needed as it's specific UI, not generic confirm)
@st.dialog("Preview Configuration")
def preview_config_dialog(config_name):
    content = db.get_config_content(config_name)
    if content:
        st.markdown(f"### 📄 Config: {config_name}")
        st.json(content, expanded=True)
    else:
        st.error("Could not load configuration content.")
