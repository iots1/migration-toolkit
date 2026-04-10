"""
Shared Dialogs — reusable @st.dialog components used across multiple pages.

Components:
    generic_confirm_dialog(title, message, confirm_label, on_confirm_func, ...)
    preview_config_dialog(config_name, content)
    show_json_preview(json_data)
    show_diff_dialog(config_name, version1, version2, diff_data)
"""
from __future__ import annotations  # Enable modern type hints

import json
import streamlit as st
import database as db


@st.dialog("Please Confirm")
def generic_confirm_dialog(
    title: str,
    message: str,
    confirm_label: str,
    on_confirm_func,
    *args,
    **kwargs,
) -> None:
    """
    Reusable confirmation dialog with a red primary confirm button.

    The on_confirm_func is responsible for any st.rerun() after the action.
    Designed to be used by controllers that pass action callbacks.
    """
    st.markdown(f"### {title}")
    st.write(message)
    st.write("")  # spacer

    c_spacer, c_cancel, c_confirm = st.columns([2, 1, 2.5])
    with c_cancel:
        if st.button("Cancel", type="secondary", use_container_width=True):
            st.rerun()
    with c_confirm:
        if st.button(confirm_label, type="primary", use_container_width=True):
            try:
                on_confirm_func(*args, **kwargs)
            except Exception as e:
                st.error(f"Error: {e}")


@st.dialog("Preview Configuration")
def preview_config_dialog(config_name: str, content: dict | None) -> None:
    """
    Renders a pre-fetched config as formatted JSON.

    The controller must fetch content via db.get_config_content() before
    calling this dialog — the view must never fetch data itself.
    """
    if content:
        st.markdown(f"### 📄 Config: `{config_name}`")
        st.json(content, expanded=True)
    else:
        st.error("Could not load configuration content.")


@st.dialog("Preview Configuration JSON")
def show_json_preview(json_data):
    st.caption("This is the JSON structure that will be saved.")
    st.code(json.dumps(json_data, indent=2, ensure_ascii=False), language="json")


@st.dialog("Compare Config Versions", width="large")
def show_diff_dialog(config_name: str, version1: int, version2: int, diff_data: dict):
    """Git-style diff view between two saved config versions."""
    st.markdown(f"### 🔄 Comparing: **{config_name}**")
    st.caption(f"Version {version1} ➡️ Version {version2}")

    config_v1 = db.get_config_version(config_name, version1)
    config_v2 = db.get_config_version(config_name, version2)

    diff_lines = [f"--- Version {version1}", f"+++ Version {version2}", ""]

    if config_v1.get("module") != config_v2.get("module"):
        diff_lines += ["@@ Metadata @@",
                       f"- module: {config_v1.get('module')}",
                       f"+ module: {config_v2.get('module')}", ""]

    v1_src, v2_src = config_v1.get("source", {}), config_v2.get("source", {})
    if v1_src != v2_src:
        diff_lines.append("@@ Source @@")
        if v1_src.get("database") != v2_src.get("database"):
            diff_lines += [f"- database: {v1_src.get('database')}", f"+ database: {v2_src.get('database')}"]
        if v1_src.get("table") != v2_src.get("table"):
            diff_lines += [f"- table: {v1_src.get('table')}", f"+ table: {v2_src.get('table')}"]
        diff_lines.append("")

    v1_tgt, v2_tgt = config_v1.get("target", {}), config_v2.get("target", {})
    if v1_tgt != v2_tgt:
        diff_lines.append("@@ Target @@")
        if v1_tgt.get("database") != v2_tgt.get("database"):
            diff_lines += [f"- database: {v1_tgt.get('database')}", f"+ database: {v2_tgt.get('database')}"]
        if v1_tgt.get("table") != v2_tgt.get("table"):
            diff_lines += [f"- table: {v1_tgt.get('table')}", f"+ table: {v2_tgt.get('table')}"]
        diff_lines.append("")

    if diff_data.get("mappings_removed"):
        diff_lines.append("@@ Removed Mappings @@")
        for m in diff_data["mappings_removed"]:
            diff_lines.append(f"- {json.dumps(m, indent=2, ensure_ascii=False)}")
        diff_lines.append("")

    if diff_data.get("mappings_added"):
        diff_lines.append("@@ Added Mappings @@")
        for m in diff_data["mappings_added"]:
            diff_lines.append(f"+ {json.dumps(m, indent=2, ensure_ascii=False)}")
        diff_lines.append("")

    if diff_data.get("mappings_modified"):
        diff_lines.append("@@ Modified Mappings @@")
        for m in diff_data["mappings_modified"]:
            diff_lines += [
                f"  Mapping: {m['source']}",
                f"- {json.dumps(m['old'], indent=2, ensure_ascii=False)}",
                f"+ {json.dumps(m['new'], indent=2, ensure_ascii=False)}",
                "",
            ]

    st.code("\n".join(diff_lines), language="diff", line_numbers=True)
