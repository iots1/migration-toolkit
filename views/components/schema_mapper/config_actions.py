"""
Config Actions — Validate, Preview JSON, and Save bottom controls.

Also owns:
    generate_json_config(params, mappings_df) -> dict
    load_data_profile(report_folder) -> DataFrame | None
"""
import os
import json
import pandas as pd
import streamlit as st

import database as db
from services.datasource_repository import DatasourceRepository as DSRepo
from views.components.shared.dialogs import show_json_preview
from views.components.schema_mapper.mapping_editor import validate_mapping_in_table


# ---------------------------------------------------------------------------
# Bottom Controls
# ---------------------------------------------------------------------------

def render_bottom_controls(
    active_table: str,
    target_db_input: str | None,
    target_table_input: str | None,
    datasource_names: list,
    loaded_config,
    is_edit_existing: bool,
    default_config_name: str,
    active_df_raw: pd.DataFrame,
) -> None:
    """Render Validate / Preview JSON / Save Configuration buttons."""
    st.markdown("---")
    col_validate, col_preview, col_save = st.columns([1, 1, 2])

    with col_validate:
        st.write("")
        _render_validate_button(active_table, target_db_input, target_table_input)

    with col_preview:
        st.write("")
        _render_preview_button(active_table, datasource_names, loaded_config, is_edit_existing,
                               default_config_name, target_db_input, target_table_input)

    with col_save:
        st.write("")
        _render_save_button(active_table, datasource_names, loaded_config, is_edit_existing,
                            default_config_name, target_db_input, target_table_input)

    if st.session_state.pop("_mapper_needs_rerun", False):
        st.rerun()


def _render_validate_button(active_table, target_db_input, target_table_input) -> None:
    import time
    if not st.button("🔍 Validate Targets", use_container_width=True):
        return

    if not target_db_input or target_db_input == "-- Select Datasource --":
        st.warning("⚠️ Please select a Target Datasource first.")
        return

    with st.spinner("Connecting to Target..."):
        connected, conn_msg = DSRepo.test_connection(target_db_input)

    if not connected:
        st.error(f"❌ Cannot connect to Target DB: {conn_msg}")
        return

    with st.spinner(f"Fetching columns for '{target_table_input}'..."):
        ok, cols = DSRepo.get_columns(target_db_input, target_table_input)

    if not ok:
        st.error(f"❌ Cannot fetch columns: {cols}")
        return

    real_cols = [c["name"] for c in cols]
    updated_df = validate_mapping_in_table(st.session_state[f"df_{active_table}"], real_cols)
    st.session_state[f"df_{active_table}"] = updated_df
    import time
    st.session_state.mapper_editor_ver = time.time()
    st.session_state["_mapper_needs_rerun"] = True


def _render_preview_button(active_table, datasource_names, loaded_config, is_edit_existing,
                           default_config_name, target_db_input, target_table_input) -> None:
    if not st.button("👁️ Preview JSON", use_container_width=True):
        return

    config_name = st.session_state.get("mapper_config_name", default_config_name) if not is_edit_existing else default_config_name
    params = _build_params(config_name, active_table, datasource_names, loaded_config, target_db_input, target_table_input)
    json_data = generate_json_config(params, st.session_state[f"df_{active_table}"])
    show_json_preview(json_data)


def _render_save_button(active_table, datasource_names, loaded_config, is_edit_existing,
                        default_config_name, target_db_input, target_table_input) -> None:
    import time

    def do_save(save_name: str) -> None:
        params = _build_params(save_name, active_table, datasource_names, loaded_config, target_db_input, target_table_input)
        json_data = generate_json_config(params, st.session_state[f"df_{active_table}"])
        success, msg = db.save_config_to_db(params["config_name"], active_table, json_data)
        if success:
            st.toast(f"Config '{save_name}' saved successfully!", icon="✅")
            st.session_state.mapper_editor_ver = time.time()
            st.session_state["_mapper_needs_rerun"] = True
        else:
            st.toast(f"Save failed: {msg}", icon="❌")

    if is_edit_existing:
        if st.button(f"💾 Save (Overwrite)", type="primary", use_container_width=True,
                     help=f"Update '{default_config_name}'"):
            do_save(default_config_name)
    else:
        config_name = st.session_state.get("mapper_config_name", default_config_name)
        if st.button("💾 Save Configuration", type="primary", use_container_width=True):
            do_save(config_name)


# ---------------------------------------------------------------------------
# JSON Config Generation
# ---------------------------------------------------------------------------

def generate_json_config(params: dict, mappings_df: pd.DataFrame) -> dict:
    """Build the config JSON dict from params + mapping DataFrame."""
    config_data = {
        "name": params["config_name"],
        "module": params["module"],
        "source": {"database": params["source_db"], "table": params["table_name"]},
        "target": {"database": params["target_db"], "table": params["target_table"]},
        "mappings": [],
    }

    for _, row in mappings_df.iterrows():
        src_col = row["Source Column"]
        is_ignored = row.get("Ignore", False)

        item: dict = {
            "source": src_col,
            "target": row["Target Column"],
            "ignore": is_ignored,
        }

        tgt_type = row.get("Target Type")
        if tgt_type and str(tgt_type).strip():
            item["target_type"] = str(tgt_type).strip()

        # Transformers
        tf_val = row.get("Transformers")
        transformers_list: list = []
        if tf_val:
            if isinstance(tf_val, list):
                transformers_list = tf_val
            elif isinstance(tf_val, str) and tf_val.strip():
                transformers_list = [t.strip() for t in tf_val.split(",") if t.strip()]
            if transformers_list:
                item["transformers"] = transformers_list

        # Default Value
        default_val = str(row.get("Default Value", "") or "").strip()
        if not default_val:
            default_val = st.session_state.get(f"default_value_{src_col}", "").strip()
        if default_val:
            item["default_value"] = default_val

        # GENERATE_HN params
        if "GENERATE_HN" in transformers_list:
            auto_detect = st.session_state.get(f"ghn_auto_detect_{src_col}", True)
            start_from = int(st.session_state.get(f"ghn_start_from_{src_col}", 0))
            item.setdefault("transformer_params", {})["GENERATE_HN"] = {
                "auto_detect_max": auto_detect,
                "start_from": start_from,
            }

        # VALUE_MAP params
        if "VALUE_MAP" in transformers_list:
            vmap_df = st.session_state.get(f"vmap_rules_{src_col}")
            vmap_default = st.session_state.get(f"vmap_default_{src_col}", "")
            if vmap_df is not None and not vmap_df.empty:
                rules = []
                for _, rule_row in vmap_df.iterrows():
                    c_col = rule_row.get("condition_column", "")
                    c_val = rule_row.get("condition_value", "")
                    output = rule_row.get("output", "")
                    if c_col and c_val and output:
                        rules.append({"when": {c_col: c_val}, "then": output})
                if rules:
                    item["transformer_params"] = {
                        "VALUE_MAP": {
                            "rules": rules,
                            "default": vmap_default or None,
                        }
                    }

        # Validators
        vd_val = row.get("Validators")
        if vd_val:
            if isinstance(vd_val, list):
                item["validators"] = vd_val
            elif isinstance(vd_val, str) and vd_val.strip():
                item["validators"] = [v.strip() for v in vd_val.split(",") if v.strip()]

        config_data["mappings"].append(item)

    return config_data


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def load_data_profile(report_folder: str) -> pd.DataFrame | None:
    csv_path = os.path.join(report_folder, "data_profile", "data_profile.csv")
    if os.path.exists(csv_path):
        try:
            return pd.read_csv(csv_path, on_bad_lines="skip")
        except Exception:
            return None
    return None


def _build_params(
    config_name: str,
    active_table: str,
    datasource_names: list,
    loaded_config,
    target_db_input: str | None,
    target_table_input: str | None,
) -> dict:
    """Resolve display names → actual dbnames for source & target."""
    src_db_display = st.session_state.get("mapper_source_db")
    src_db_actual = _resolve_dbname(src_db_display, datasource_names)
    tgt_db_display = st.session_state.get("mapper_tgt_db", target_db_input or "")
    tgt_db_actual = _resolve_dbname(tgt_db_display, datasource_names)
    tgt_tbl_actual = st.session_state.get("mapper_tgt_tbl", target_table_input or "")

    return {
        "config_name": config_name,
        "table_name": active_table,
        "module": loaded_config.get("module", "patient") if loaded_config else "patient",
        "source_db": src_db_actual,
        "target_db": tgt_db_actual,
        "target_table": tgt_tbl_actual,
        "dependencies": [],
    }


def _resolve_dbname(display_name: str | None, datasource_names: list) -> str:
    """Convert datasource display name → actual dbname stored in config JSON."""
    if not display_name or display_name == "-- Select Datasource --":
        return display_name or ""
    if display_name in datasource_names:
        ds = DSRepo.get_by_name(display_name)
        if ds:
            return ds.get("dbname", display_name)
    return display_name
