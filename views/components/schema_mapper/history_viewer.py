"""
History Viewer — Config version list and version diff comparison.

Functions:
    render_history_panel(config_name)
    render_compare_panel(config_name)
"""
import streamlit as st
import database as db
from views.components.shared.dialogs import show_json_preview, show_diff_dialog


def render_history_panel(config_name: str) -> None:
    """Show list of all saved versions for a config."""
    if not st.session_state.get("mapper_show_history", False):
        return

    st.markdown("---")
    st.markdown("### 📜 Config Version History")
    history_df = db.get_config_history(config_name)

    if history_df.empty:
        st.info(f"No history found for '{config_name}'. Save a config to create history.")
        return

    st.write(f"Found **{len(history_df)}** versions of '{config_name}'")
    for _, row in history_df.iterrows():
        with st.container(border=True):
            c_v, c_time, c_btn = st.columns([1, 2, 1])
            c_v.write(f"**Version {int(row['version'])}**")
            c_time.write(f"📅 {row['created_at']}")
            with c_btn:
                if st.button(f"👁️ View", key=f"view_v{int(row['version'])}"):
                    data = db.get_config_version(config_name, int(row["version"]))
                    if data:
                        show_json_preview(data)


def render_compare_panel(config_name: str) -> None:
    """Show version diff UI for comparing two saved versions."""
    if not st.session_state.get("mapper_show_compare", False):
        return

    st.markdown("---")
    st.markdown("### 🔄 Compare Config Versions")
    history_df = db.get_config_history(config_name)

    if history_df.empty or len(history_df) < 2:
        st.info(f"Need at least 2 versions to compare. Current versions: {len(history_df)}")
        return

    c_v1, c_v2 = st.columns(2)
    with c_v1:
        v1 = st.selectbox("Version 1", history_df["version"].tolist(), index=0, key="comp_v1")
    with c_v2:
        v2 = st.selectbox("Version 2", history_df["version"].tolist(),
                          index=1 if len(history_df) > 1 else 0, key="comp_v2")

    if st.button("📊 Show Diff", type="primary", use_container_width=True):
        diff = db.compare_config_versions(config_name, int(v1), int(v2))
        if diff:
            show_diff_dialog(config_name, int(v1), int(v2), diff)
