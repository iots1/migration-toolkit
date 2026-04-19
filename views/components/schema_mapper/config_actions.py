"""
Config Actions — Validate, Preview JSON, and Save bottom controls.

Also owns:
    generate_json_config(params, mappings_df) -> dict
    build_preview_sql(config_data) -> str
    load_data_profile(report_folder) -> DataFrame | None
"""

from __future__ import annotations  # Enable modern type hints

import os
import json
import pandas as pd
import streamlit as st

from sqlalchemy import text
from repositories import config_repo
from models.migration_config import ConfigRecord
from services.datasource_repository import DatasourceRepository as DSRepo
from views.components.shared.dialogs import show_json_preview
from views.components.schema_mapper.mapping_editor import validate_mapping_in_table


# ---------------------------------------------------------------------------
# Bottom Controls
# ---------------------------------------------------------------------------


def check_unmapped_required_columns(
    mappings_df: pd.DataFrame,
    col_nullable_map: dict,
    col_defaults_map: dict | None = None,
) -> list:
    """
    Find required target columns (NOT NULL) that aren't mapped and have no default value.
    Returns list of unmapped required column names without defaults.

    Args:
        mappings_df: Mapping DataFrame
        col_nullable_map: Dict of column_name -> is_nullable
        col_defaults_map: Dict of column_name -> has_default_value (optional)
    """
    if not col_nullable_map:
        return []

    col_defaults_map = col_defaults_map or {}

    # Get mapped target columns (excluding ignored rows)
    mapped_targets = set()
    for _, row in mappings_df.iterrows():
        if not row.get("Ignore", False) and row.get("Target Column"):
            mapped_targets.add(row.get("Target Column"))

    # Find required columns not in mapped set AND without default values
    unmapped_required = []
    for col_name, is_nullable in col_nullable_map.items():
        if not is_nullable and col_name not in mapped_targets:
            # Only add if no default value exists
            has_default = col_defaults_map.get(col_name, False)
            if not has_default:
                unmapped_required.append(col_name)

    return sorted(unmapped_required)


def render_unmapped_required_check(
    active_table: str,
    col_nullable_map: dict,
    col_defaults_map: dict | None = None,
) -> bool:
    """
    Render validation for required columns (NOT NULL without defaults).
    Shows error if unmapped required columns exist, success if all mapped.
    Returns True if all required columns are mapped, False otherwise.

    Args:
        active_table: Active table name
        col_nullable_map: Dict of column_name -> is_nullable
        col_defaults_map: Dict of column_name -> has_default_value (optional)
    """
    if not col_nullable_map:
        return True

    mappings_df = st.session_state.get(f"df_{active_table}")
    if mappings_df is None or mappings_df.empty:
        return True

    unmapped_required = check_unmapped_required_columns(
        mappings_df, col_nullable_map, col_defaults_map
    )

    st.markdown("---")
    st.markdown("### 🔒 Required Columns Validation")

    with st.container(border=True):
        if unmapped_required:
            st.error(
                f"🚨 {len(unmapped_required)} NOT NULL column(s) without default - NOT mapped yet!",
                icon="❌",
            )
            st.markdown("**These columns need mapping (no default value):**")
            for col in unmapped_required:
                st.markdown(f"  - `{col}` 🔒")
            return False
        else:
            required_cols = [
                col for col, is_nullable in col_nullable_map.items() if not is_nullable
            ]
            cols_with_defaults = len(
                [
                    col
                    for col, has_def in (col_defaults_map or {}).items()
                    if has_def and not col_nullable_map.get(col, True)
                ]
            )
            st.success(
                f"✅ All {len(required_cols)} NOT NULL column(s) are safe! "
                f"({len(required_cols) - cols_with_defaults} mapped, {cols_with_defaults} have defaults)",
                icon="✅",
            )
            return True


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
        _render_preview_button(
            active_table,
            datasource_names,
            loaded_config,
            is_edit_existing,
            default_config_name,
            target_db_input,
            target_table_input,
        )

    with col_save:
        st.write("")
        _render_save_button(
            active_table,
            datasource_names,
            loaded_config,
            is_edit_existing,
            default_config_name,
            target_db_input,
            target_table_input,
        )

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

    real_cols = [c["name"] if isinstance(c, dict) else c for c in cols]
    updated_df = validate_mapping_in_table(
        st.session_state[f"df_{active_table}"], cols, show_toast=True
    )
    st.session_state[f"df_{active_table}"] = updated_df
    import time

    st.session_state.mapper_editor_ver = time.time()
    st.session_state["_mapper_needs_rerun"] = True


def _render_preview_button(
    active_table,
    datasource_names,
    loaded_config,
    is_edit_existing,
    default_config_name,
    target_db_input,
    target_table_input,
) -> None:
    if not st.button("👁️ Preview JSON", use_container_width=True):
        return

    config_name = (
        st.session_state.get("mapper_config_name", default_config_name)
        if not is_edit_existing
        else default_config_name
    )
    params = _build_params(
        config_name,
        active_table,
        datasource_names,
        loaded_config,
        target_db_input,
        target_table_input,
    )
    json_data = generate_json_config(params, st.session_state[f"df_{active_table}"])
    show_json_preview(json_data)


def _render_save_button(
    active_table,
    datasource_names,
    loaded_config,
    is_edit_existing,
    default_config_name,
    target_db_input,
    target_table_input,
) -> None:
    import time

    def do_save(save_name: str) -> None:
        params = _build_params(
            save_name,
            active_table,
            datasource_names,
            loaded_config,
            target_db_input,
            target_table_input,
        )
        json_data = generate_json_config(params, st.session_state[f"df_{active_table}"])
        record = ConfigRecord(
            config_name=params["config_name"],
            table_name=active_table,
            json_data=json_data,
            datasource_source_id=params.get("source_datasource_id"),
            datasource_target_id=params.get("target_datasource_id"),
            config_type=st.session_state.get("mapper_config_type", "std"),
            script=st.session_state.get("mapper_script") or None,
            generate_sql=st.session_state.get("mapper_generate_sql_text") or None,
            condition=st.session_state.get("mapper_condition") or None,
            lookup=st.session_state.get("mapper_lookup") or None,
        )
        success, msg = config_repo.save(record)
        if success:
            st.toast(f"Config '{save_name}' saved successfully!", icon="✅")
            st.session_state.mapper_editor_ver = time.time()
            st.session_state["_mapper_needs_rerun"] = True
        else:
            st.toast(f"Save failed: {msg}", icon="❌")

    if is_edit_existing:
        if st.button(
            f"💾 Save (Overwrite)",
            type="primary",
            use_container_width=True,
            help=f"Update '{default_config_name}'",
        ):
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
    source_obj: dict = {"database": params["source_db"], "table": params["table_name"]}
    if params.get("source_datasource_id") is not None:
        source_obj["datasource_id"] = params["source_datasource_id"]
    if params.get("source_datasource_name"):
        source_obj["datasource_name"] = params["source_datasource_name"]

    target_obj: dict = {
        "database": params["target_db"],
        "table": params["target_table"],
    }
    if params.get("target_datasource_id") is not None:
        target_obj["datasource_id"] = params["target_datasource_id"]
    if params.get("target_datasource_name"):
        target_obj["datasource_name"] = params["target_datasource_name"]

    config_data = {
        "name": params["config_name"],
        "module": params["module"],
        "source": source_obj,
        "target": target_obj,
        "mappings": [],
    }

    for _, row in mappings_df.iterrows():
        src_col = row["Source Column"]
        is_ignored = row.get("Ignore", False)
        if is_ignored:
            continue

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


def build_preview_sql(config_data: dict, limit: int = 1000, db_type: str = "") -> str:
    source = config_data.get("source", {})
    target = config_data.get("target", {})
    mappings = config_data.get("mappings", [])
    condition = config_data.get("condition", "")
    lookup = config_data.get("lookup", "")

    source_table = source.get("table", "")
    is_mssql = db_type.lower().startswith("microsoft sql server") if db_type else False

    active_mappings = [m for m in mappings if not m.get("ignore", False)]

    if not active_mappings:
        return "-- No active mappings"

    select_parts = []
    for m in active_mappings:
        src = m.get("source", "")
        tgt = m.get("target", "")
        if src and tgt:
            transformers = m.get("transformers", [])
            alias = src
            if "TRIM" in transformers:
                alias = f"LTRIM(RTRIM({src}))"
            elif "UPPER" in transformers:
                alias = f"UPPER({src})"
            elif "LOWER" in transformers:
                alias = f"LOWER({src})"
            select_parts.append(f"    {alias} AS {tgt}")

    top_clause = f"TOP {limit} " if is_mssql else ""
    sql = f"SELECT {top_clause}\n"
    sql += ",\n".join(select_parts)
    sql += f"\nFROM {source_table}"

    if lookup:
        sql += f"\n{lookup}"

    if condition:
        sql += f"\nWHERE {condition}"

    if not is_mssql:
        sql += f"\nLIMIT {limit};"
    else:
        sql += ";"
    return sql


def execute_preview_sql(
    datasource_name: str, sql: str, charset: str | None = None
) -> tuple[bool, str, "pd.DataFrame | None"]:
    try:
        engine = DSRepo.get_engine(datasource_name, charset=charset or None)
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return True, "", df
    except Exception as e:
        return False, str(e), None


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

    src_ds = (
        DSRepo.get_by_name(src_db_display)
        if src_db_display and src_db_display in datasource_names
        else None
    )
    tgt_ds = (
        DSRepo.get_by_name(tgt_db_display)
        if tgt_db_display and tgt_db_display in datasource_names
        else None
    )

    return {
        "config_name": config_name,
        "table_name": active_table,
        "module": loaded_config.get("module", "patient")
        if loaded_config
        else "patient",
        "source_db": src_db_actual,
        "target_db": tgt_db_actual,
        "target_table": tgt_tbl_actual,
        "dependencies": [],
        "source_datasource_id": str(src_ds["id"])
        if src_ds and src_ds.get("id")
        else None,
        "source_datasource_name": src_db_display if src_ds else "",
        "target_datasource_id": str(tgt_ds["id"])
        if tgt_ds and tgt_ds.get("id")
        else None,
        "target_datasource_name": tgt_db_display if tgt_ds else "",
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
