"""File Explorer View - Pure rendering component."""
import streamlit as st
import os


def render_file_explorer_page(view_data: dict, callbacks: dict) -> None:
    """
    Render file explorer page.

    Args:
        view_data: dict with keys:
            - analysis_dir: Path to analysis directory
            - mini_his_dir: Path to mini_his directory
            - has_analysis_dir: bool
            - has_mini_his: bool
        callbacks: dict (empty for this simple page)
    """
    st.subheader("Project Files")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📂 Analysis Report")
        if view_data["has_analysis_dir"]:
            files = os.listdir(view_data["analysis_dir"])
            st.code("\n".join(files))
        else:
            st.info("No analysis report directory found.")

    with col2:
        st.markdown("### 📂 Mini HIS (Mockup)")
        if view_data["has_mini_his"]:
            files = os.listdir(view_data["mini_his_dir"])
            st.code("\n".join(files))
        else:
            st.info("No mini_his directory found.")
