"""
Schema Mapper View — pure rendering layer (MVC pattern).

This view delegates rendering to component functions while receiving
pre-fetched data and callbacks from the controller.
"""
import streamlit as st
from typing import Any, Callable, Dict, List
import pandas as pd

from views.components.schema_mapper.source_selector import render_source_selector
from views.components.schema_mapper.metadata_editor import render_target_selector, render_config_metadata
from views.components.schema_mapper.mapping_editor import init_editor_state, render_mapping_editor
from views.components.schema_mapper.history_viewer import render_history_panel, render_compare_panel
from views.components.schema_mapper.config_actions import render_bottom_controls, render_unmapped_required_check


def render_schema_mapper_page(
    datasources_df: pd.DataFrame,
    configs_df: pd.DataFrame,
    datasource_names: List[str],
    form_state: Dict[str, Any],
    callbacks: Dict[str, Callable],
) -> None:
    """
    Render the Schema Mapper page.

    Args:
        datasources_df: DataFrame of datasources
        configs_df: DataFrame of saved configs
        datasource_names: List of datasource names (with placeholder at start)
        form_state: Dict of current form state from controller
        callbacks: Dict of callback functions from controller
    """
    # --- Header ---
    c_title, c_mode = st.columns([3, 1])
    with c_title:
        st.markdown("## 🗂️ Schema Mapper (AI Powered 🧠)")

    with c_mode:
        btn_text = "🔍 Enter Focus Mode" if not form_state["mapper_focus_mode"] else "🔙 Exit Focus Mode"
        btn_type = "secondary" if not form_state["mapper_focus_mode"] else "primary"
        if st.button(btn_text, type=btn_type, use_container_width=True, key="toggle_focus"):
            st.session_state.mapper_focus_mode = not st.session_state.mapper_focus_mode
            st.rerun()

    # === 1. Source Selection ===
    render_source_selector(datasources_df, datasource_names)

    # === 2. Mapping Logic (only when source is selected) ===
    active_table = st.session_state.get("mapper_active_table")
    active_df_raw = st.session_state.get("mapper_df_raw")
    loaded_config = st.session_state.get("mapper_loaded_config")

    if not active_table or active_df_raw is None:
        return

    source_mode = st.session_state.get("source_mode", "Run ID")
    saved_config_mode = source_mode in ["Saved Config", "Upload File"] and loaded_config is not None
    source_db_input = st.session_state.get("mapper_source_db")
    source_table_name = st.session_state.get("mapper_source_tbl")

    # === 3. Target Selector ===
    target_db_input, target_table_input, real_target_columns = render_target_selector(
        datasource_names, active_table, saved_config_mode,
    )

    # === 4. Build maps for Required auto-check and defaults ===
    col_nullable_map = {}
    col_defaults_map = {}
    if real_target_columns and isinstance(real_target_columns[0], dict) if real_target_columns else False:
        for col_info in real_target_columns:
            col_nullable_map[col_info.get("name")] = col_info.get("is_nullable", True)
            col_defaults_map[col_info.get("name")] = bool(col_info.get("column_default"))

    # === 5. Init mapping DataFrame ===
    init_editor_state(active_df_raw, active_table, loaded_config, real_target_columns, col_nullable_map)

    # === 6. Config Metadata ===
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
        current_config_name = loaded_config.get("name", default_cfg_name) if loaded_config else default_cfg_name
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

    # === 8. Pre-save validation ===
    if not st.session_state.mapper_focus_mode:
        render_unmapped_required_check(
            active_table,
            col_nullable_map,
            col_defaults_map,
        )

    # === 9. Bottom Controls ===
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
