"""
Step 1 — Configuration Selection.

Lets the user choose a saved config from the project DB or upload a JSON file.
Updates session_state:
    migration_mode     "load_db" | "upload_file"
    migration_config   dict (parsed JSON config)
    migration_step     → 2 on confirm
"""
import json
import streamlit as st
import database as db


def render_step_config() -> None:
    st.markdown("### Step 1: Select Configuration")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("📚 Load from Project DB", use_container_width=True):
            st.session_state.migration_mode = "load_db"
            st.rerun()
    with col2:
        if st.button("📂 Upload JSON File", use_container_width=True):
            st.session_state.migration_mode = "upload_file"
            st.rerun()

    st.divider()

    if st.session_state.get("migration_mode") == "load_db":
        configs_df = db.get_configs_list()
        if not configs_df.empty:
            sel_config = st.selectbox("Select Saved Config", configs_df["config_name"])
            if st.button("Proceed to Connection Test", type="primary"):
                st.session_state.migration_config = db.get_config_content(sel_config)
                st.session_state.migration_step = 2
                st.rerun()
        else:
            st.warning("No saved configurations found.")

    elif st.session_state.get("migration_mode") == "upload_file":
        uploaded = st.file_uploader("Upload .json config", type=["json"])
        if uploaded:
            st.session_state.migration_config = json.load(uploaded)
            if st.button("Proceed to Connection Test", type="primary"):
                st.session_state.migration_step = 2
                st.rerun()
