"""
Metadata Editor — Config name, source/target DB/table display, batch size,
and the Target Table Configuration expander (non-saved-config mode).

Returns:
    (current_config_name: str, is_edit_existing: bool)
"""
import streamlit as st
from services.datasource_repository import DatasourceRepository as DSRepo


def render_target_selector(
    datasource_names: list,
    active_table: str,
    saved_config_mode: bool,
) -> tuple[str | None, str | None, list]:
    """
    Renders the Target Table Configuration expander (non-saved-config mode only).
    In saved-config mode, reads existing values from session_state.

    Returns:
        (target_db_input, target_table_input, real_target_columns)
    """
    if saved_config_mode:
        return (
            st.session_state.get("mapper_tgt_db", ""),
            st.session_state.get("mapper_tgt_tbl", ""),
            st.session_state.get("mapper_real_tgt_cols", []),
        )

    if st.session_state.mapper_focus_mode:
        target_table_input = st.session_state.get("mapper_tgt_tbl")
        st.info(f"🔎 Focus Mode: `{active_table}` → `{target_table_input}`")
        return (
            st.session_state.get("mapper_tgt_db"),
            target_table_input,
            st.session_state.get("mapper_real_tgt_cols", []),
        )

    st.markdown("---")
    with st.expander("📤 Target Table Configuration", expanded=True):
        c_tgt_1, c_tgt_2 = st.columns(2)

        default_tgt_db = st.session_state.get("mapper_tgt_db")
        tgt_idx = datasource_names.index(default_tgt_db) if default_tgt_db in datasource_names else 0
        target_db_input = c_tgt_1.selectbox("Target Datasource", datasource_names,
                                             index=tgt_idx, key="tgt_ds")

        target_table_input = None
        real_target_columns = []
        default_tgt_tbl = st.session_state.get("mapper_tgt_tbl")

        if target_db_input and target_db_input != "-- Select Datasource --":
            ok, tables = DSRepo.get_tables(target_db_input)
            if ok:
                def_idx = (
                    tables.index(default_tgt_tbl) if (default_tgt_tbl and default_tgt_tbl in tables)
                    else (tables.index(active_table) if active_table in tables else 0)
                )
                target_table_input = c_tgt_2.selectbox("Target Table", tables,
                                                       index=def_idx, key="tgt_tbl_cfg_sel")
            else:
                target_table_input = c_tgt_2.text_input(
                    "Target Table",
                    value=default_tgt_tbl or active_table,
                    key="tgt_tbl_cfg_txt",
                )

            if target_table_input:
                ok_c, cols = DSRepo.get_columns(target_db_input, target_table_input)
                if ok_c:
                    real_target_columns = [c["name"] for c in cols]
        else:
            target_table_input = c_tgt_2.text_input(
                "Target Table", value="", placeholder="Please select datasource first",
                disabled=True, key="tgt_tbl_cfg_disabled",
            )

    st.session_state.mapper_tgt_db = target_db_input
    st.session_state.mapper_tgt_tbl = target_table_input
    st.session_state.mapper_real_tgt_cols = real_target_columns
    return target_db_input, target_table_input, real_target_columns


def render_config_metadata(
    active_table: str,
    datasource_names: list,
    loaded_config,
    source_db_input: str | None,
    source_table_name: str | None,
    saved_config_mode: bool,
    target_db_input: str | None,
    target_table_input: str | None,
) -> tuple[str, bool]:
    """
    Renders Config Name, History/Compare toggles, Source/Target readonly fields, Batch Size.

    Returns:
        (current_config_name, is_edit_existing)
    """
    default_config_name = f"{active_table}_config"
    is_edit_existing = False

    if loaded_config and loaded_config.get("name"):
        default_config_name = loaded_config.get("name")
        is_edit_existing = st.session_state.source_mode == "Saved Config"

    st.markdown("---")
    st.markdown("### 📝 Config Metadata")

    # Row 1: Config name + toggles
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        key = "config_name_edit" if is_edit_existing else "config_name_input"
        current_config_name = st.text_input("Config Name", value=default_config_name, key=key)
        st.session_state.mapper_config_name = current_config_name
    with c2:
        st.write("")
        st.write("")
        if st.button("📜 Show History", use_container_width=True):
            st.session_state.mapper_show_history = not st.session_state.mapper_show_history
    with c3:
        st.write("")
        st.write("")
        if st.button("🔄 Compare Versions", use_container_width=True):
            st.session_state.mapper_show_compare = not st.session_state.mapper_show_compare

    # Row 2: Source DB & Table (readonly)
    src_cols = st.columns(2)
    with src_cols[0]:
        src_db = loaded_config.get("source", {}).get("database", "") if loaded_config else source_db_input
        st.text_input("Source Database", value=src_db, disabled=True, key="metadata_src_db")
    with src_cols[1]:
        src_tbl = loaded_config.get("source", {}).get("table", "") if loaded_config else source_table_name
        st.text_input("Source Table", value=src_tbl, disabled=True, key="metadata_src_tbl")

    # Row 3: Target DB & Table
    tgt_cols = st.columns(2)
    with tgt_cols[0]:
        if saved_config_mode:
            st.text_input("Target Database", value=st.session_state.get("mapper_tgt_db", ""),
                          disabled=True, key="metadata_tgt_db_ro",
                          help="แก้ไขได้ใน Config Details ด้านบน")
            selected_tgt_db = st.session_state.get("mapper_tgt_db", "")
        else:
            cur = st.session_state.get("mapper_tgt_db", target_db_input or "")
            selected_tgt_db = st.selectbox(
                "Target Database", datasource_names,
                index=datasource_names.index(cur) if cur in datasource_names else 0,
                key="config_tgt_db_meta",
            )
            st.session_state["mapper_tgt_db"] = selected_tgt_db

    with tgt_cols[1]:
        if saved_config_mode:
            st.text_input("Target Table", value=st.session_state.get("mapper_tgt_tbl", ""),
                          disabled=True, key="metadata_tgt_tbl_ro",
                          help="แก้ไขได้ใน Config Details ด้านบน")
        else:
            cur_tbl = st.session_state.get("mapper_tgt_tbl", target_table_input or "")
            tgt_tables = []
            if selected_tgt_db and selected_tgt_db != "-- Select Datasource --":
                ok, tables = DSRepo.get_tables(selected_tgt_db)
                if ok:
                    tgt_tables = tables
            if tgt_tables:
                sel_tbl = st.selectbox(
                    "Target Table", tgt_tables,
                    index=tgt_tables.index(cur_tbl) if cur_tbl in tgt_tables else 0,
                    key="config_tgt_tbl_meta",
                )
                st.session_state["mapper_tgt_tbl"] = sel_tbl
            else:
                st.text_input("Target Table", value=cur_tbl, disabled=True,
                              help="Select a Target Database first", key="metadata_tgt_tbl_disabled")

    # Row 4: Batch Size
    batch_size = st.number_input(
        "Batch Size (records per batch)",
        min_value=10, max_value=10000, value=1000, step=10,
        help="Number of records to process in each batch during migration",
    )
    st.session_state.mapper_batch_size = batch_size

    return current_config_name, is_edit_existing
