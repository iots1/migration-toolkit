"""
Schema Mapper Page — orchestrator only.

Coordinates source selection → target selection → mapping editor →
condition/lookup → SQL preview → bottom controls → history viewer.

Components:
    views/components/shared/dialogs.py
    views/components/schema_mapper/source_selector.py
    views/components/schema_mapper/metadata_editor.py
    views/components/schema_mapper/mapping_editor.py
    views/components/schema_mapper/history_viewer.py
    views/components/schema_mapper/config_actions.py
"""

import time

import pandas as pd
import streamlit as st
import database as db
from utils.ui_components import inject_global_css


@st.cache_data(ttl=30, show_spinner=False)
def _get_datasources():
    """Cached datasource list — refreshes every 30 s to avoid DB hit on every rerun."""
    return db.get_datasources()


from views.components.schema_mapper.source_selector import render_source_selector
from views.components.schema_mapper.metadata_editor import (
    render_target_selector,
    render_config_metadata,
)
from views.components.schema_mapper.mapping_editor import (
    init_editor_state,
    render_mapping_editor,
)
from views.components.schema_mapper.history_viewer import (
    render_history_panel,
    render_compare_panel,
)
from views.components.schema_mapper.config_actions import (
    render_bottom_controls,
    render_unmapped_required_check,
    generate_json_config,
    build_preview_sql,
    execute_preview_sql,
)

_DEFAULTS: dict = {
    "mapper_focus_mode": False,
    "source_mode": "Run ID",
    "mapper_show_history": False,
    "mapper_show_compare": False,
    "mapper_config_name": "",
    "mapper_condition": "",
    "mapper_lookup": "",
    "mapper_config_type": "std",
    "mapper_script": "",
    "mapper_generate_sql_text": "",
    "_mapper_condition_widget": "",
    "_mapper_lookup_widget": "",
}


def render_schema_mapper_page() -> None:
    inject_global_css()

    for key, default in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # --- Header ---
    c_title, c_mode = st.columns([3, 1])
    with c_title:
        st.markdown("## 🗂️ Schema Mapper (AI Powered 🧠)")
    with c_mode:
        btn_text = (
            "🔍 Enter Focus Mode"
            if not st.session_state.mapper_focus_mode
            else "🔙 Exit Focus Mode"
        )
        btn_type = "secondary" if not st.session_state.mapper_focus_mode else "primary"
        if st.button(btn_text, type=btn_type, use_container_width=True):
            st.session_state.mapper_focus_mode = not st.session_state.mapper_focus_mode
            st.rerun()

    # --- Datasource list (shared across components) ---
    # Cached: datasource list rarely changes; avoid DB hit on every rerun.
    datasources_df = _get_datasources()
    datasource_names = ["-- Select Datasource --"] + (
        datasources_df["name"].tolist() if not datasources_df.empty else []
    )

    # === 1. Source Selection ===
    render_source_selector(datasources_df, datasource_names)

    # === 2. Mapping Logic (only when source is selected) ===
    active_table = st.session_state.get("mapper_active_table")
    active_df_raw = st.session_state.get("mapper_df_raw")
    loaded_config = st.session_state.get("mapper_loaded_config")

    if not active_table or active_df_raw is None:
        return

    source_mode = st.session_state.get("source_mode", "Run ID")
    saved_config_mode = (
        source_mode in ["Saved Config", "Upload File"] and loaded_config is not None
    )
    source_db_input = st.session_state.get("mapper_source_db")
    source_table_name = st.session_state.get("mapper_source_tbl")

    # === 3. Target Selector (non-saved-config mode) ===
    target_db_input, target_table_input, real_target_columns = render_target_selector(
        datasource_names,
        active_table,
        saved_config_mode,
    )

    # === 4. Build maps for Required auto-check and defaults ===
    col_nullable_map = {}
    col_defaults_map = {}
    if (
        real_target_columns and isinstance(real_target_columns[0], dict)
        if real_target_columns
        else False
    ):
        for col_info in real_target_columns:
            col_nullable_map[col_info.get("name")] = col_info.get("is_nullable", True)
            col_defaults_map[col_info.get("name")] = bool(
                col_info.get("column_default")
            )

    # === 5. Init mapping DataFrame ===
    init_editor_state(
        active_df_raw,
        active_table,
        loaded_config,
        real_target_columns,
        col_nullable_map,
    )

    # === 6. Config Metadata (name, source/target display, batch size) ===
    if not st.session_state.mapper_focus_mode:
        current_config_name, is_edit_existing = render_config_metadata(
            active_table=active_table,
            datasource_names=datasource_names,
            loaded_config=loaded_config,
            source_db_input=source_db_input,
            source_table_name=source_table_name,
            saved_config_mode=saved_config_mode,
            target_db_input=target_db_input,
            target_table_input=target_table_input,
        )
    else:
        default_cfg_name = f"{active_table}_config"
        current_config_name = (
            loaded_config.get("name", default_cfg_name)
            if loaded_config
            else default_cfg_name
        )
        is_edit_existing = source_mode == "Saved Config" and loaded_config is not None

    default_config_name = current_config_name

    # === 7. AgGrid + Quick Edit ===
    render_mapping_editor(
        active_table,
        real_target_columns,
        active_df_raw,
        col_nullable_map,
        col_defaults_map,
    )

    # === 7.5 Condition, Lookup & SQL Preview ===
    if not st.session_state.mapper_focus_mode:
        _render_condition_lookup_sql(
            active_table,
            datasource_names,
            loaded_config,
            target_db_input,
            target_table_input,
        )

    # === 8. Pre-save validation — Required columns check ===
    if not st.session_state.mapper_focus_mode:
        render_unmapped_required_check(active_table, col_nullable_map, col_defaults_map)

    # === 9. Bottom Controls (Validate / Preview / Save) ===
    if not st.session_state.mapper_focus_mode:
        render_bottom_controls(
            active_table=active_table,
            target_db_input=target_db_input,
            target_table_input=target_table_input,
            datasource_names=datasource_names,
            loaded_config=loaded_config,
            is_edit_existing=is_edit_existing,
            default_config_name=default_config_name,
            active_df_raw=active_df_raw,
        )

    # === 10. History / Compare Panels ===
    render_history_panel(current_config_name)
    render_compare_panel(current_config_name)


def _render_condition_lookup_sql(
    active_table: str,
    datasource_names: list,
    loaded_config,
    target_db_input: str | None,
    target_table_input: str | None,
) -> None:
    from services.datasource_repository import DatasourceRepository as DSRepo

    st.markdown("---")
    st.markdown("### 🔧 Table Conditions & SQL Preview")

    print(st.session_state.get("mapper_condition"))
    _condition_val = st.session_state.get("mapper_condition", "")
    _lookup_val = st.session_state.get("mapper_lookup", "")
    _has_condition_or_lookup = bool(_condition_val or _lookup_val)
    with st.expander("⚙️ Condition & Lookup", expanded=_has_condition_or_lookup):
        c1, c2 = st.columns(2)
        with c1:
            _edited_condition = st.text_area(
                "WHERE Condition",
                value=_condition_val,
                placeholder="CreateDateAt > '2024-01-01' AND Sex = 1",
                height=80,
                key="_mapper_condition_widget",
                help="SQL WHERE clause to filter source data",
            )
            st.session_state["mapper_condition"] = _edited_condition
        with c2:
            _edited_lookup = st.text_area(
                "Lookup / JOIN",
                value=_lookup_val,
                placeholder="LEFT JOIN lookup_table lt ON source.cid = lt.cid",
                height=80,
                key="_mapper_lookup_widget",
                help="SQL JOIN clause for lookup tables",
            )
            st.session_state["mapper_lookup"] = _edited_lookup

    # --- SQL generation (helper) ---
    def _generate_sql() -> str:
        source_db_display = st.session_state.get("mapper_source_db")
        source_db_actual = source_db_display or ""
        if (
            source_db_display
            and source_db_display != "-- Select Datasource --"
            and source_db_display in datasource_names
        ):
            ds = DSRepo.get_by_name(source_db_display)
            if ds:
                source_db_actual = ds.get("dbname", source_db_display)

        tgt_db_display = st.session_state.get("mapper_tgt_db", target_db_input or "")
        tgt_db_actual = tgt_db_display or ""
        if (
            tgt_db_display
            and tgt_db_display != "-- Select Datasource --"
            and tgt_db_display in datasource_names
        ):
            ds = DSRepo.get_by_name(tgt_db_display)
            if ds:
                tgt_db_actual = ds.get("dbname", tgt_db_display)

        src_ds = None
        if (
            source_db_display
            and source_db_display != "-- Select Datasource --"
            and source_db_display in datasource_names
        ):
            src_ds = DSRepo.get_by_name(source_db_display)

        tgt_ds = None
        if (
            tgt_db_display
            and tgt_db_display != "-- Select Datasource --"
            and tgt_db_display in datasource_names
        ):
            tgt_ds = DSRepo.get_by_name(tgt_db_display)

        config_name = st.session_state.get(
            "mapper_config_name", f"{active_table}_config"
        )
        module = loaded_config.get("module", "patient") if loaded_config else "patient"

        params = {
            "config_name": config_name,
            "table_name": active_table,
            "module": module,
            "source_db": source_db_actual,
            "target_db": tgt_db_actual,
            "target_table": st.session_state.get(
                "mapper_tgt_tbl", target_table_input or ""
            ),
            "source_datasource_id": str(src_ds["id"])
            if src_ds and src_ds.get("id")
            else None,
            "source_datasource_name": source_db_display if src_ds else "",
            "target_datasource_id": str(tgt_ds["id"])
            if tgt_ds and tgt_ds.get("id")
            else None,
            "target_datasource_name": tgt_db_display if tgt_ds else "",
        }

        df_mapping = st.session_state.get(f"df_{active_table}")
        if df_mapping is None:
            return "-- No mapping data"
        batch_size = st.session_state.get("mapper_batch_size", 1000)
        json_data = generate_json_config(params, df_mapping)
        json_data["condition"] = st.session_state.get("mapper_condition", "")
        json_data["lookup"] = st.session_state.get("mapper_lookup", "")
        src_db_type = src_ds.get("db_type", "") if src_ds else ""
        return build_preview_sql(json_data, limit=batch_size, db_type=src_db_type)

    # --- Generate / Regenerate button ---
    if st.button("👁️ Generate SQL", use_container_width=True, type="secondary"):
        st.session_state.pop("mapper_sql_editor", None)
        st.session_state["mapper_generate_sql_text"] = _generate_sql()
        st.rerun()

    # --- SQL editor + Execute (always visible once SQL exists) ---
    current_sql = st.session_state.get("mapper_generate_sql_text", "")
    if not current_sql:
        return

    edited_sql = st.text_area(
        "SQL (editable)",
        value=current_sql,
        height=200,
        key="mapper_sql_editor",
        help="Edit the generated SQL before executing or saving.",
    )
    st.session_state["mapper_generate_sql_text"] = edited_sql

    c_run, c_reset = st.columns([1, 1])
    with c_run:
        source_db_display = st.session_state.get("mapper_source_db")
        can_execute = (
            source_db_display
            and source_db_display != "-- Select Datasource --"
            and source_db_display in datasource_names
        )
        if st.button("▶ Execute Query", use_container_width=True, type="primary"):
            if not can_execute:
                st.warning("Connect a source datasource to execute this query.")
            else:
                with st.spinner("Executing preview query..."):
                    ok, err_msg, result_df = execute_preview_sql(
                        source_db_display, edited_sql
                    )
                if ok and result_df is not None:
                    st.success(f"Query OK — {len(result_df)} rows returned")
                    st.dataframe(result_df, use_container_width=True)

                    new_cols = result_df.columns.tolist()
                    existing_cols = set(
                        st.session_state[f"df_{active_table}"]["Source Column"]
                        .dropna()
                        .tolist()
                    )
                    missing = [c for c in new_cols if c not in existing_cols]
                    if missing:
                        import utils.helpers as helpers

                        df = st.session_state[f"df_{active_table}"]
                        new_rows = []
                        for col_name in missing:
                            new_rows.append(
                                {
                                    "Status": "",
                                    "Source Column": col_name,
                                    "Type": str(result_df[col_name].dtype)
                                    if hasattr(result_df[col_name], "dtype")
                                    else "",
                                    "Target Column": helpers.to_snake_case(col_name),
                                    "Transformers": "",
                                    "Validators": "",
                                    "Default Value": "",
                                    "Required": False,
                                    "Ignore": False,
                                }
                            )
                        if new_rows:
                            df = pd.concat(
                                [df, pd.DataFrame(new_rows)], ignore_index=True
                            )
                            st.session_state[f"df_{active_table}"] = df
                            st.session_state.mapper_editor_ver = time.time()
                            st.toast(
                                f"+{len(new_rows)} column(s) added to Field Mapping",
                                icon="📋",
                            )
                else:
                    st.error(f"SQL Error: {err_msg}")
    with c_reset:
        if st.button("🔄 Regenerate SQL", use_container_width=True):
            st.session_state.pop("mapper_sql_editor", None)
            st.session_state["mapper_generate_sql_text"] = _generate_sql()
            st.rerun()
