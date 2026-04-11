"""
Source Selector — mode tabs for choosing the data source to map.

Modes: Run ID | Datasource | Saved Config | Upload File

After selection, writes to session_state:
    mapper_active_table, mapper_df_raw, mapper_source_db,
    mapper_source_tbl, mapper_loaded_config,
    mapper_editor_ver, last_mapper_signature
Also handles auto-fill and context-change detection.
"""

import os
import json
import time
import pandas as pd
import streamlit as st

import database as db
from services.datasource_repository import DatasourceRepository as DSRepo
import utils.helpers as helpers


def render_source_selector(
    datasources_df: pd.DataFrame, datasource_names: list
) -> None:
    """
    Renders the '📥 Source & Configuration' expander.
    Skipped (but state preserved) when focus mode is active.
    Always runs context-change detection to update session_state.
    """
    selected_table = source_db_input = source_table_name = df_raw = (
        loaded_config_json
    ) = None

    if not st.session_state.mapper_focus_mode:
        with st.expander("📥 Source & Configuration", expanded=True):
            col_mode, col_sel = st.columns([1, 2])
            with col_mode:
                source_mode = st.radio(
                    "Source Mode",
                    ["Run ID", "Datasource", "Saved Config", "Upload File"],
                    horizontal=True,
                )
                st.session_state.source_mode = source_mode

            if source_mode == "Run ID":
                selected_table, df_raw, source_db_input, source_table_name = (
                    _mode_run_id(col_sel)
                )

            elif source_mode == "Datasource":
                selected_table, df_raw, source_db_input, source_table_name = (
                    _mode_datasource(col_sel, datasource_names, datasources_df)
                )

            elif source_mode in ["Saved Config", "Upload File"]:
                (
                    selected_table,
                    df_raw,
                    source_db_input,
                    source_table_name,
                    loaded_config_json,
                ) = _mode_config(col_sel, source_mode, datasources_df)

        # --- Auto-fill session state from loaded config ---
        if source_mode in ["Saved Config", "Upload File"] and loaded_config_json:
            _auto_fill_from_config(loaded_config_json, datasource_names)

        # --- Config Details panel (Saved Config / Upload File mode) ---
        if source_mode in ["Saved Config", "Upload File"] and loaded_config_json:
            _render_config_details(loaded_config_json, datasource_names)

    # --- Context-change detection (always runs) ---
    if selected_table:
        _handle_context_change(
            selected_table, source_db_input, source_table_name, loaded_config_json
        )
        st.session_state.mapper_active_table = selected_table
        st.session_state.mapper_df_raw = df_raw
        st.session_state.mapper_source_db = source_db_input
        st.session_state.mapper_source_tbl = source_table_name
        st.session_state.mapper_loaded_config = (
            loaded_config_json if loaded_config_json else None
        )


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------


def _mode_run_id(col_sel):
    report_folders = helpers.get_report_folders()
    if not report_folders:
        return None, None, None, None

    with col_sel:
        c1, c2 = st.columns(2)
        sel_folder = c1.selectbox(
            "Run ID", report_folders, format_func=os.path.basename
        )
        df_profile = _load_data_profile(sel_folder)
        if df_profile is None:
            return None, None, None, None

        tables = df_profile["Table"].unique()
        if "sm_sel_table_idx" not in st.session_state:
            st.session_state.sm_sel_table_idx = 0
        try:
            sel_table = c2.selectbox(
                "Source Table", tables, index=st.session_state.sm_sel_table_idx
            )
        except Exception:
            sel_table = c2.selectbox("Source Table", tables, index=0)
        if sel_table in tables:
            st.session_state.sm_sel_table_idx = list(tables).index(sel_table)

        df_raw = df_profile[df_profile["Table"] == sel_table].copy()
        return sel_table, df_raw, "Run ID (CSV)", sel_table


def _mode_datasource(col_sel, datasource_names, datasources_df):
    if not datasource_names:
        return None, None, None, None

    with col_sel:
        src_ds_name = st.selectbox("Source DB", datasource_names, key="src_ds")
        if src_ds_name == "-- Select Datasource --":
            return None, None, None, None

        src_ds = db.get_datasource_by_name(src_ds_name)
        if not src_ds:
            return None, None, None, None

        status_key = f"conn_status_{src_ds_name}"
        if status_key not in st.session_state:
            st.session_state[status_key] = "unknown"

        c_status, c_btn, c_live = st.columns([2.5, 1, 0.8])
        with c_status:
            if st.session_state[status_key] == "success":
                st.success(f"🟢 Connected: {src_ds['host']}")
            elif st.session_state[status_key] == "fail":
                st.error(f"🔴 Connection Failed: {src_ds['host']}")
            else:
                st.info(f"⚪ Ready to connect: {src_ds['host']}")

        with c_btn:
            if st.button("📡 Test", key="btn_test_conn"):
                ok, msg = DSRepo.test_connection(src_ds_name)
                st.session_state[status_key] = "success" if ok else "fail"
                if not ok:
                    st.toast(f"Connection Failed: {msg}")
                st.session_state["_mapper_needs_rerun"] = True

        with c_live:
            st.write("")
            if st.button("🔄 Live", key="btn_live_status", use_container_width=True):
                with st.spinner("Checking..."):
                    ok, msg = DSRepo.test_connection(src_ds_name)
                    st.session_state[status_key] = "success" if ok else "fail"
                    st.session_state["_mapper_needs_rerun"] = True

        if st.session_state.pop("_mapper_needs_rerun", False):
            st.rerun()

        if st.session_state[status_key] != "success":
            return None, None, None, None

        ok_t, tables = DSRepo.get_tables(src_ds_name)
        if not ok_t:
            return None, None, None, None

        if "sm_src_tbl_idx" not in st.session_state:
            st.session_state.sm_src_tbl_idx = 0
        try:
            sel_table = st.selectbox(
                "Source Table",
                tables,
                index=st.session_state.sm_src_tbl_idx,
                key="src_tbl",
            )
        except Exception:
            sel_table = st.selectbox("Source Table", tables, index=0, key="src_tbl")
        if sel_table in tables:
            st.session_state.sm_src_tbl_idx = list(tables).index(sel_table)

        ok_c, cols = DSRepo.get_columns(src_ds_name, sel_table)
        if not ok_c:
            return None, None, None, None

        df_raw = pd.DataFrame(
            {
                "Table": [sel_table] * len(cols),
                "Column": [c["name"] for c in cols],
                "DataType": [c["type"] for c in cols],
                "Sample_Values": [""] * len(cols),
            }
        )
        return sel_table, df_raw, src_ds_name, sel_table


def _mode_config(col_sel, source_mode: str, datasources_df):
    with col_sel:
        config_data = None
        if source_mode == "Saved Config":
            configs_df = db.get_configs_list()
            if not configs_df.empty:
                sel_config = st.selectbox("Select Config", configs_df["config_name"])
                if sel_config:
                    config_data = db.get_config_content(sel_config)
            else:
                st.warning("No saved configurations found.")
        else:
            uploaded = st.file_uploader("Upload JSON Config", type=["json"])
            if uploaded:
                try:
                    config_data = json.load(uploaded)
                except Exception:
                    st.error("Invalid JSON file")

        if not config_data:
            return None, None, None, None, None

        src_db_name = config_data.get("source", {}).get("database")
        src_tbl_name = config_data.get("source", {}).get("table")
        if not src_db_name or not src_tbl_name:
            return None, None, None, None, config_data

        src_ds_info = db.get_datasource_by_name(src_db_name)

        if source_mode == "Saved Config" and src_ds_info:
            conn_status_key = f"conn_status_{src_db_name}"
            if conn_status_key not in st.session_state:
                st.session_state[conn_status_key] = "unknown"

            c_status, c_btn, c_live = st.columns([2.5, 1, 0.8])
            with c_status:
                if st.session_state[conn_status_key] == "success":
                    st.success(f"🟢 Connected: {src_ds_info['host']}")
                elif st.session_state[conn_status_key] == "fail":
                    st.error(f"🔴 Connection Failed: {src_ds_info['host']}")
                else:
                    st.info(f"⚪ Ready to connect: {src_ds_info['host']}")

            with c_btn:
                if st.button("📡 Test", key="btn_test_conn_saved_cfg"):
                    ok, msg = DSRepo.test_connection(src_db_name)
                    st.session_state[conn_status_key] = "success" if ok else "fail"
                    if not ok:
                        st.toast(f"Connection Failed: {msg}")
                    st.session_state["_mapper_needs_rerun"] = True

            with c_live:
                st.write("")
                if st.button(
                    "🔄 Live", key="btn_live_status_saved_cfg", use_container_width=True
                ):
                    with st.spinner("Checking..."):
                        ok, msg = DSRepo.test_connection(src_db_name)
                        st.session_state[conn_status_key] = "success" if ok else "fail"
                        st.session_state["_mapper_needs_rerun"] = True

            if st.session_state.pop("_mapper_needs_rerun", False):
                st.rerun()

            if st.session_state[conn_status_key] != "success":
                st.warning(
                    f"⚠️ Offline Mode: Datasource '{src_db_name}' not reachable. Using saved mapping."
                )
                mappings = config_data.get("mappings", [])
                if mappings:
                    df_raw = pd.DataFrame(
                        {
                            "Table": [src_tbl_name] * len(mappings),
                            "Column": [m["source"] for m in mappings],
                            "DataType": ["Unknown"] * len(mappings),
                            "Sample_Values": [""] * len(mappings),
                        }
                    )
                return src_tbl_name, df_raw, src_db_name, src_tbl_name, config_data

        df_raw = None
        if src_db_name:
            ok, cols = DSRepo.get_columns(src_db_name, src_tbl_name)
            if ok:
                st.success(f"✅ Loaded & Synced: {src_tbl_name} (from {src_db_name})")
                df_raw = pd.DataFrame(
                    {
                        "Table": [src_tbl_name] * len(cols),
                        "Column": [c["name"] for c in cols],
                        "DataType": [c["type"] for c in cols],
                        "Sample_Values": [""] * len(cols),
                    }
                )

        if df_raw is None:
            st.warning(
                f"⚠️ Offline Mode: Datasource '{src_db_name}' not reachable. Using saved mapping."
            )
            mappings = config_data.get("mappings", [])
            if mappings:
                df_raw = pd.DataFrame(
                    {
                        "Table": [src_tbl_name] * len(mappings),
                        "Column": [m["source"] for m in mappings],
                        "DataType": ["Unknown"] * len(mappings),
                        "Sample_Values": [""] * len(mappings),
                    }
                )

        return src_tbl_name, df_raw, src_db_name, src_tbl_name, config_data


# ---------------------------------------------------------------------------
# Auto-fill & Config Details
# ---------------------------------------------------------------------------


def _auto_fill_from_config(loaded_config: dict, datasource_names: list) -> None:
    current_cfg_name = loaded_config.get("name", "")
    tgt_db_from_cfg = loaded_config.get("target", {}).get("database", "")
    tgt_tbl_from_cfg = loaded_config.get("target", {}).get("table", "")

    # Map dbname (stored in JSON) → display name (shown in selectbox)
    tgt_db_display = tgt_db_from_cfg
    if tgt_db_from_cfg and tgt_db_from_cfg not in datasource_names:
        for ds_name in datasource_names:
            if ds_name == "-- Select Datasource --":
                continue
            ds_info = db.get_datasource_by_name(ds_name)
            if ds_info and ds_info.get("dbname") == tgt_db_from_cfg:
                tgt_db_display = ds_name
                break

    if st.session_state.get("_mapper_loaded_config_name") != current_cfg_name:
        st.session_state["mapper_tgt_db"] = tgt_db_display
        st.session_state["mapper_tgt_tbl"] = tgt_tbl_from_cfg
        st.session_state.pop("mapper_tgt_tables", None)
        st.session_state.pop("mapper_real_tgt_cols", None)
        st.session_state.pop("mapper_tgt_db_edit", None)
        st.session_state.pop("mapper_tgt_tbl_edit", None)
        st.session_state["_mapper_loaded_config_name"] = current_cfg_name

        # Reset widget keys so next render picks up new values via value= param
        # (Streamlit ignores value= when session_state key already exists)
        st.session_state["config_name_edit"] = current_cfg_name
        st.session_state["config_name_input"] = current_cfg_name
        for k in [
            "saved_src_db",
            "saved_src_tbl",
            "config_detail_tgt_db",
            "config_detail_tgt_tbl",
        ]:
            st.session_state.pop(k, None)

        # Restore VALUE_MAP and default_value params from config
        for m in loaded_config.get("mappings", []):
            src_col = m.get("source")
            if "transformer_params" in m and "VALUE_MAP" in m["transformer_params"]:
                vmap = m["transformer_params"]["VALUE_MAP"]
                rules = vmap.get("rules", [])
                if rules:
                    rows = []
                    for rule in rules:
                        for col, val in rule.get("when", {}).items():
                            rows.append(
                                {
                                    "condition_column": col,
                                    "condition_value": str(val),
                                    "output": str(rule.get("then", "")),
                                }
                            )
                    st.session_state[f"vmap_rules_{src_col}"] = pd.DataFrame(rows)
                st.session_state[f"vmap_default_{src_col}"] = vmap.get("default", "")
            if "default_value" in m:
                st.session_state[f"default_value_{src_col}"] = m["default_value"]

        st.session_state.pop("mapper_condition", None)
        st.session_state.pop("mapper_lookup", None)
        st.session_state.pop("mapper_generate_sql_text", None)
        st.session_state["mapper_condition"] = loaded_config.get("condition", "")
        st.session_state["mapper_lookup"] = loaded_config.get("lookup", "")
        st.session_state["mapper_config_type"] = loaded_config.get("config_type", "std")
        st.session_state["mapper_script"] = loaded_config.get("script", "")
        st.session_state["mapper_generate_sql_text"] = loaded_config.get(
            "generate_sql", ""
        )


def _render_config_details(loaded_config: dict, datasource_names: list) -> None:
    st.markdown("---")
    st.markdown("### ⚙️ Config Details")

    cols1 = st.columns([2, 2])
    with cols1[0]:
        src_db_default = loaded_config.get("source", {}).get("database", "")
        src_db_idx = (
            datasource_names.index(src_db_default)
            if src_db_default in datasource_names
            else 0
        )
        st.selectbox(
            "Source Database", datasource_names, index=src_db_idx, key="saved_src_db"
        )
    with cols1[1]:
        st.text_input(
            "Source Table",
            value=loaded_config.get("source", {}).get("table", ""),
            key="saved_src_tbl",
        )

    cols2 = st.columns([2, 2, 2, 2])
    with cols2[0]:
        cur_tgt_db = st.session_state.get("mapper_tgt_db", "")
        tgt_db_idx = (
            datasource_names.index(cur_tgt_db) if cur_tgt_db in datasource_names else 0
        )
        selected_tgt_db = st.selectbox(
            "Target Database",
            datasource_names,
            index=tgt_db_idx,
            key="config_detail_tgt_db",
        )
        if selected_tgt_db != st.session_state.get("mapper_tgt_db"):
            st.session_state["mapper_tgt_db"] = selected_tgt_db
            st.session_state.pop("mapper_tgt_tables", None)
            st.session_state["_mapper_needs_rerun"] = True

        if selected_tgt_db and selected_tgt_db != "-- Select Datasource --":
            if "mapper_tgt_tables" not in st.session_state:
                ok, tables = DSRepo.get_tables(selected_tgt_db)
                if ok:
                    st.session_state["mapper_tgt_tables"] = tables

    with cols2[1]:
        cur_tgt_tbl = st.session_state.get("mapper_tgt_tbl", "")
        tgt_tables = st.session_state.get("mapper_tgt_tables", [])
        if tgt_tables:
            tgt_tbl_idx = (
                tgt_tables.index(cur_tgt_tbl) if cur_tgt_tbl in tgt_tables else 0
            )
            sel_tbl = st.selectbox(
                "Target Table",
                tgt_tables,
                index=tgt_tbl_idx,
                key="config_detail_tgt_tbl",
            )
            st.session_state["mapper_tgt_tbl"] = sel_tbl

            if (
                selected_tgt_db
                and selected_tgt_db != "-- Select Datasource --"
                and sel_tbl
            ):
                ok_c, cols_c = DSRepo.get_columns(selected_tgt_db, sel_tbl)
                if ok_c:
                    st.session_state["mapper_real_tgt_cols"] = [
                        c["name"] for c in cols_c
                    ]
        else:
            st.text_input(
                "Target Table",
                value=cur_tgt_tbl,
                disabled=True,
                help="Select a Target Database first",
                key="config_detail_tgt_tbl_disabled",
            )

    if st.session_state.pop("_mapper_needs_rerun", False):
        st.rerun()


# ---------------------------------------------------------------------------
# Context-change detection
# ---------------------------------------------------------------------------


def _handle_context_change(
    selected_table, source_db_input, source_table_name, loaded_config_json
) -> None:
    config_sig = loaded_config_json.get("name", "") if loaded_config_json else ""
    source_mode = st.session_state.get("source_mode", "")
    current_sig = f"{source_mode}|{source_db_input}|{source_table_name}|{config_sig}"
    last_sig = st.session_state.get("last_mapper_signature", "")

    if current_sig != last_sig:
        state_key = f"df_{selected_table}"
        if state_key in st.session_state:
            del st.session_state[state_key]

        if not loaded_config_json:
            for k in [
                "mapper_tgt_db",
                "mapper_tgt_tbl",
                "mapper_real_tgt_cols",
                "mapper_tgt_db_edit",
                "mapper_tgt_tbl_edit",
                "mapper_tgt_tables",
                "_mapper_loaded_config_name",
            ]:
                st.session_state.pop(k, None)
            st.session_state["mapper_tgt_db"] = None
            st.session_state["mapper_tgt_tbl"] = None
            st.session_state["mapper_real_tgt_cols"] = []

        st.session_state.mapper_editor_ver = time.time()
        st.session_state.last_mapper_signature = current_sig


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _load_data_profile(report_folder: str):
    csv_path = os.path.join(report_folder, "data_profile", "data_profile.csv")
    if os.path.exists(csv_path):
        try:
            return pd.read_csv(csv_path, on_bad_lines="skip")
        except Exception:
            return None
    return None
