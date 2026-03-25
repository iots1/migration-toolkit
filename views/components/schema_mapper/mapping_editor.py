"""
Mapping Editor — AgGrid column mapping table + Quick Edit panel.

Responsibilities:
  - Render the AgGrid table with source→target column mappings
  - Render the Quick Edit panel for per-row transformer/validator config
  - init_editor_state()     — initialize the mapping DataFrame in session_state
  - validate_mapping_in_table() — mark columns ✅/❌ vs real target columns
"""
import time
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

from config import TRANSFORMER_OPTIONS, VALIDATOR_OPTIONS
from services.ml_mapper import ml_mapper
import utils.helpers as helpers


# ---------------------------------------------------------------------------
# State Initialisation
# ---------------------------------------------------------------------------

def init_editor_state(df: pd.DataFrame, table_name: str, config_json: dict | None = None) -> None:
    """
    Populate session_state[f"df_{table_name}"] from raw profile df + optional config.
    No-op if the state key already exists (prevents overwrite on rerun).
    """
    state_key = f"df_{table_name}"
    if state_key in st.session_state:
        return

    mapping_dict: dict = {}
    if config_json:
        for m in config_json.get("mappings", []):
            mapping_dict[m["source"]] = m

    rows = []
    for _, row in df.iterrows():
        src_col = row.get("Column", "")
        dtype = row.get("DataType", "")

        target_col = helpers.to_snake_case(src_col)
        transformers, validators = [], []
        ignore = False
        default_value = ""

        if src_col in mapping_dict:
            rule = mapping_dict[src_col]
            target_col = rule.get("target", target_col)
            ignore = rule.get("ignore", False)
            default_value = rule.get("default_value", "")
            transformers = rule.get("transformers", [])
            validators = rule.get("validators", [])
        elif not config_json and "date" in str(dtype).lower():
            transformers.append("BUDDHIST_TO_ISO")
            validators.append("VALID_DATE")

        rows.append({
            "Status": "",
            "Source Column": src_col,
            "Type": dtype,
            "Target Column": target_col,
            "Transformers": ", ".join(transformers),
            "Validators": ", ".join(validators),
            "Default Value": default_value,
            "Required": False,
            "Ignore": ignore,
        })

    st.session_state[state_key] = pd.DataFrame(rows)


def validate_mapping_in_table(df_mapping: pd.DataFrame, real_columns: list) -> pd.DataFrame:
    """Mark each row Status ✅/❌/⚠️/⚪ vs the real target column list."""
    if not real_columns:
        return df_mapping

    df_mapping["Status"] = df_mapping["Status"].astype(str)
    valid_count = invalid_count = 0

    for idx, row in df_mapping.iterrows():
        tgt = row["Target Column"]
        if row.get("Ignore", False):
            df_mapping.at[idx, "Status"] = "⚪ Skip"
            continue
        if not tgt:
            df_mapping.at[idx, "Status"] = "⚠️ Empty"
            continue
        if tgt in real_columns:
            df_mapping.at[idx, "Status"] = "✅ OK"
            valid_count += 1
        else:
            df_mapping.at[idx, "Status"] = "❌ Invalid"
            invalid_count += 1

    if invalid_count > 0:
        st.toast(f"Validation Finished: {invalid_count} errors found.", icon="❌")
    else:
        st.toast(f"Validation Finished: All {valid_count} columns valid.", icon="✅")

    return df_mapping


# ---------------------------------------------------------------------------
# AgGrid Table
# ---------------------------------------------------------------------------

def render_mapping_editor(
    active_table: str,
    real_target_columns: list,
    active_df_raw: pd.DataFrame,
) -> None:
    """Renders the AgGrid column mapping table + Quick Edit panel."""
    if not st.session_state.mapper_focus_mode:
        _render_table_header(active_table, real_target_columns)

    if st.session_state.pop("_mapper_needs_rerun", False):
        st.rerun()

    df_to_edit = st.session_state[f"df_{active_table}"].copy()
    grid_response = _build_aggrid(df_to_edit, active_table, real_target_columns)

    if grid_response["data"] is not None:
        updated_df = pd.DataFrame(grid_response["data"])
        if not updated_df.equals(st.session_state[f"df_{active_table}"]):
            for idx, row in updated_df.iterrows():
                if row.get("Ignore", False):
                    updated_df.at[idx, "Required"] = False
            st.session_state[f"df_{active_table}"] = updated_df

    _render_quick_edit(active_table, real_target_columns, active_df_raw, grid_response)


# ---------------------------------------------------------------------------
# Private — AgGrid
# ---------------------------------------------------------------------------

def _render_table_header(active_table: str, real_target_columns: list) -> None:
    c_head, c_ai, c_ignore = st.columns([1.5, 1, 1.5])
    with c_head:
        st.markdown("### 📋 Field Mapping")
        st.caption("Select a row to edit details below.")

    with c_ignore:
        col_check, col_uncheck = st.columns(2)
        with col_check:
            if st.button("✓ Check All Ignore", use_container_width=True):
                df = st.session_state[f"df_{active_table}"]
                df["Ignore"] = True
                df["Required"] = False
                st.session_state[f"df_{active_table}"] = df
                st.session_state.mapper_editor_ver = time.time()
                st.session_state["_mapper_needs_rerun"] = True
        with col_uncheck:
            if st.button("✗ Uncheck All", use_container_width=True):
                df = st.session_state[f"df_{active_table}"]
                df["Ignore"] = False
                st.session_state[f"df_{active_table}"] = df
                st.session_state.mapper_editor_ver = time.time()
                st.session_state["_mapper_needs_rerun"] = True

    with c_ai:
        if real_target_columns:
            if st.button("🤖 AI Auto-Map", type="primary", use_container_width=True):
                with st.spinner("🤖 AI is analyzing column meanings..."):
                    source_cols = st.session_state[f"df_{active_table}"]["Source Column"].tolist()
                    suggestions = ml_mapper.suggest_mapping(source_cols, real_target_columns)
                    df = st.session_state[f"df_{active_table}"]
                    count = 0
                    for idx, row in df.iterrows():
                        src = row["Source Column"]
                        if src in suggestions and suggestions[src]:
                            df.at[idx, "Target Column"] = suggestions[src]
                            count += 1
                    st.session_state[f"df_{active_table}"] = df
                    st.session_state.mapper_editor_ver = time.time()
                    st.toast(f"AI matched {count} columns!", icon="🤖")
                    st.session_state["_mapper_needs_rerun"] = True


def _build_aggrid(df_to_edit: pd.DataFrame, active_table: str, real_target_columns: list):
    gb = GridOptionsBuilder.from_dataframe(df_to_edit)
    gb.configure_column("Status", editable=False, width=90, cellStyle={"textAlign": "center"})
    gb.configure_column("Source Column", editable=False, width=200)
    gb.configure_column("Type", editable=False, width=120)
    if real_target_columns:
        gb.configure_column("Target Column", editable=True, width=250,
                            cellEditor="agSelectCellEditor",
                            cellEditorParams={"values": real_target_columns})
    else:
        gb.configure_column("Target Column", editable=True, width=250)
    gb.configure_column("Transformers", editable=False, width=200)
    gb.configure_column("Validators", editable=False, width=200)
    gb.configure_column("Ignore", editable=True,
                        cellRenderer="agCheckboxCellRenderer",
                        cellEditor="agCheckboxCellEditor", width=80)
    gb.configure_selection("single")
    gb.configure_grid_options(suppressColumnVirtualisation=True)

    grid_height = 500 if st.session_state.mapper_focus_mode else 400
    editor_ver = st.session_state.get("mapper_editor_ver", "v1")
    source_ctx = st.session_state.get("mapper_source_db", "unknown")
    unique_key = f"aggrid_{source_ctx}_{active_table}_{editor_ver}"

    return AgGrid(
        df_to_edit, gridOptions=gb.build(), height=grid_height, width="100%",
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=False, allow_unsafe_jscode=True, key=unique_key,
    )


# ---------------------------------------------------------------------------
# Private — Quick Edit Panel
# ---------------------------------------------------------------------------

def _render_quick_edit(
    active_table: str,
    real_target_columns: list,
    active_df_raw: pd.DataFrame,
    grid_response,
) -> None:
    selected_rows = grid_response["selected_rows"]
    if selected_rows is None or len(selected_rows) == 0:
        return

    sel_row = selected_rows.iloc[0].to_dict() if isinstance(selected_rows, pd.DataFrame) else selected_rows[0]
    src_col = sel_row.get("Source Column")
    df_state = st.session_state[f"df_{active_table}"]
    row_idx_list = df_state.index[df_state["Source Column"] == src_col].tolist()
    if not row_idx_list:
        return

    idx = row_idx_list[0]
    with st.container(border=True):
        st.markdown(f"#### ✏️ Edit: `{src_col}`")
        c1, c2, c3 = st.columns(3)

        current_tgt = sel_row.get("Target Column", "")
        target_opts = list(dict.fromkeys(
            [current_tgt] + [c for c in real_target_columns if c != current_tgt]
        )) if real_target_columns else [current_tgt]

        with c1:
            new_target = st.selectbox("Target Column", target_opts, index=0, key=f"sb_tgt_{src_col}")

        current_trans = sel_row.get("Transformers", "")
        def_trans = [t.strip() for t in str(current_trans).split(",") if t.strip() and t.strip() in TRANSFORMER_OPTIONS]
        with c2:
            new_trans = st.multiselect("Transformers", TRANSFORMER_OPTIONS, default=def_trans, key=f"ms_tf_{src_col}")

        current_val = sel_row.get("Validators", "")
        def_vals = [v.strip() for v in str(current_val).split(",") if v.strip() and v.strip() in VALIDATOR_OPTIONS]
        with c3:
            new_vals = st.multiselect("Validators", VALIDATOR_OPTIONS, default=def_vals, key=f"ms_vd_{src_col}")

        is_ignored = sel_row.get("Ignore", False)

        # Default Value
        dv_key = f"default_value_{src_col}"
        if dv_key not in st.session_state:
            st.session_state[dv_key] = str(sel_row.get("Default Value", "") or "")
        st.text_input(
            "Default Value (ใส่ค่าสำรองเมื่อ transform แล้วได้ null เช่น `1900-01-01`)",
            key=dv_key,
            placeholder="ว่าง = ไม่ใช้ default",
        )

        # GENERATE_HN options
        if "GENERATE_HN" in new_trans:
            _render_generate_hn(src_col)

        # VALUE_MAP rules editor
        if "VALUE_MAP" in new_trans:
            _render_value_map(src_col, sel_row, active_df_raw)

        if st.button("✅ Update Row", type="primary"):
            df_state.at[idx, "Target Column"] = new_target
            df_state.at[idx, "Transformers"] = ", ".join(new_trans)
            df_state.at[idx, "Validators"] = ", ".join(new_vals)
            df_state.at[idx, "Default Value"] = st.session_state.get(dv_key, "")
            if is_ignored:
                df_state.at[idx, "Required"] = False
            st.session_state[f"df_{active_table}"] = df_state
            st.session_state.mapper_editor_ver = time.time()
            st.rerun()


def _render_generate_hn(src_col: str) -> None:
    st.markdown("**GENERATE_HN Options** — ตั้งค่า HN Counter")
    ghn_key = f"ghn_auto_detect_{src_col}"
    ghn_start_key = f"ghn_start_from_{src_col}"
    auto_detect = st.checkbox(
        "Auto-detect Max HN from Target DB (แนะนำ)",
        value=st.session_state.get(ghn_key, True),
        key=ghn_key,
    )
    if not auto_detect:
        st.number_input(
            "Start From (ตั้งค่า HN counter เริ่มต้น)",
            min_value=0,
            value=int(st.session_state.get(ghn_start_key, 0)),
            step=1,
            key=ghn_start_key,
        )


def _render_value_map(src_col: str, sel_row: dict, active_df_raw: pd.DataFrame) -> None:
    st.markdown("**VALUE_MAP Rules** — ค่าไหน → เปลี่ยนเป็นอะไร")
    vmap_key = f"vmap_rules_{src_col}"
    vmap_default_key = f"vmap_default_{src_col}"

    if vmap_key not in st.session_state:
        existing_rules = sel_row.get("transformer_params", {}).get("VALUE_MAP", {}).get("rules", [])
        if existing_rules:
            rows = []
            for rule in existing_rules:
                for col, val in rule.get("when", {}).items():
                    rows.append({"condition_column": col, "condition_value": str(val), "output": str(rule.get("then", ""))})
            st.session_state[vmap_key] = pd.DataFrame(rows)
        else:
            st.session_state[vmap_key] = pd.DataFrame(columns=["condition_column", "condition_value", "output"])

    available_cols = list(active_df_raw["Column"]) if active_df_raw is not None else [src_col]
    rules_df = st.session_state.get(vmap_key, pd.DataFrame(columns=["condition_column", "condition_value", "output"]))

    edited = st.data_editor(
        rules_df,
        num_rows="dynamic",
        column_config={
            "condition_column": st.column_config.SelectboxColumn("Column", options=available_cols, required=True),
            "condition_value": st.column_config.TextColumn("Value (=)", width="medium"),
            "output": st.column_config.TextColumn("→ Output", width="medium"),
        },
        key=f"de_vmap_{src_col}",
        use_container_width=True,
        hide_index=False,
    )
    st.session_state[vmap_key] = edited

    st.text_input(
        "Default (ไม่ match ใช้ค่านี้ หรือว่างไว้ = คง original)",
        value=st.session_state.get(vmap_default_key, ""),
        key=vmap_default_key,
    )
