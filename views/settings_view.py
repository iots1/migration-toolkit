"""
Settings View — pure Streamlit rendering for the Settings page.

Receives all data and callbacks from controllers/settings_controller.py.

Rules:
    - MUST NOT import database, services, or models.
    - MUST NOT access st.session_state directly (widget key= params are allowed).
    - MUST NOT contain business logic or validation beyond "is the field non-empty?".
    - All data mutations are delegated to callbacks provided by the controller.
"""

import streamlit as st
from config import DB_TYPES
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

_CHARSET_PRESETS = [
    ("", "Default (driver default)"),
    ("utf8mb4", "utf8mb4 — Modern UTF-8, MySQL 5.5.3+"),
    ("utf8", "utf8 — Standard UTF-8"),
    ("tis620", "tis620 — Thai TIS-620 (MySQL)"),
    ("cp874", "cp874 — Windows Thai (MSSQL/MySQL)"),
    ("TIS-620", "TIS-620 — Thai legacy (MSSQL pymssql)"),
    ("latin1", "latin1 — Latin-1 / ISO-8859-1"),
]
_CHARSET_VALUES = [v for v, _ in _CHARSET_PRESETS]
_CHARSET_LABELS = [lbl for _, lbl in _CHARSET_PRESETS]

from views.components.shared.styles import inject_global_css
from views.components.shared.dialogs import (
    generic_confirm_dialog,
    preview_config_dialog,
)


def render_settings_page(
    datasources_df,
    configs_df,
    form_state: dict,
    callbacks: dict,
) -> None:
    inject_global_css()
    st.subheader("🛠️ Project Settings (PostgreSQL)")

    tab_ds, tab_conf = st.tabs(["🔌 Datasources", "📄 Saved Configs"])
    with tab_ds:
        _render_datasource_tab(datasources_df, form_state, callbacks)
    with tab_conf:
        _render_configs_tab(configs_df, callbacks)


# ---------------------------------------------------------------------------
# Tab: Datasources
# ---------------------------------------------------------------------------


def _render_datasource_tab(datasources_df, form_state: dict, callbacks: dict) -> None:
    is_edit_mode = form_state["is_edit_mode"]
    edit_ds_id = form_state["edit_ds_id"]
    grid_key = f"ds_grid_{form_state['ds_grid_key']}"

    # Render form above the grid using a placeholder so it appears at the top
    form_slot = st.empty()

    st.markdown("#### Existing Datasources")
    st.caption("Click a row to edit details above.")

    # --- Grid ---
    with st.container():
        if not datasources_df.empty:
            display_df = datasources_df.copy()
            if "charset" in display_df.columns:
                display_df["charset"] = display_df["charset"].fillna("").astype(str)

            gb = GridOptionsBuilder.from_dataframe(display_df)
            gb.configure_selection("single", use_checkbox=False)
            if "id" in display_df.columns:
                gb.configure_column("id", hide=True)
            gb.configure_column(
                "name", header_name="Name", flex=1, filter=True, sortable=True
            )
            gb.configure_column("db_type", header_name="Type", width=110)
            gb.configure_column("host", header_name="Host", width=150)
            gb.configure_column("dbname", header_name="Database", width=130)
            gb.configure_column("username", header_name="User", width=110)
            gb.configure_column(
                "charset",
                header_name="Charset",
                width=110,
                cellStyle={"color": "#6c757d", "fontStyle": "italic"},
            )

            grid_response = AgGrid(
                display_df,
                gridOptions=gb.build(),
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                fit_columns_on_grid_load=True,
                height=300,
                width="100%",
                key=grid_key,
            )

            selected = grid_response["selected_rows"]
            if selected is not None and len(selected) > 0:
                sel_row = (
                    selected[0] if isinstance(selected, list) else selected.iloc[0]
                )
                sel_id = sel_row.get("id")
                # Guard: only notify controller if the selection actually changed
                if sel_id != edit_ds_id:
                    callbacks["on_row_select"](sel_id)
        else:
            st.info("No datasources defined.")

    # --- Form (rendered into the placeholder above the grid) ---
    with form_slot.container():
        form_title = "✏️ Edit Datasource" if is_edit_mode else "➕ Add New Datasource"
        st.markdown(f"#### {form_title}")

        with st.container(border=True):
            c1, c2 = st.columns(2)
            ds_name = c1.text_input("Profile Name (Unique)", key="new_ds_name")
            ds_type = c2.selectbox(
                "Type",
                DB_TYPES,
                index=form_state["ds_form_type_index"],
                key="new_ds_type",
            )

            c3, c4, c5 = st.columns([3, 2, 2])
            ds_host = c3.text_input("Host", key="new_ds_host")
            ds_port = c4.text_input("Port (Optional)", key="new_ds_port")
            _saved_charset = form_state.get("new_ds_charset", "")
            _preset_idx = _CHARSET_VALUES.index(_saved_charset) if _saved_charset in _CHARSET_VALUES else 0
            _sel_label = c5.selectbox(
                "Charset",
                options=_CHARSET_LABELS,
                index=_preset_idx,
                help="Character encoding for the DB connection. Use 'tis620' for Thai legacy HIS databases.",
                key="_charset_selectbox",
            )
            ds_charset = _CHARSET_VALUES[_CHARSET_LABELS.index(_sel_label)]

            c6, c7, c8 = st.columns(3)
            ds_db = c6.text_input("DB Name", key="new_ds_db")
            ds_user = c7.text_input("Username", key="new_ds_user")
            ds_pass = c8.text_input("Password", type="password", key="new_ds_pass")

            st.divider()

            if is_edit_mode:
                b1, b2, b3 = st.columns([1, 1, 1])

                if b1.button(
                    "💾 Save Changes", type="primary", use_container_width=True
                ):
                    if ds_name and ds_host:
                        ok, msg = callbacks["on_update"](
                            edit_ds_id,
                            ds_name,
                            ds_type,
                            ds_host,
                            ds_port,
                            ds_db,
                            ds_user,
                            ds_pass,
                            ds_charset,
                        )
                        if not ok:
                            st.error(msg)
                    else:
                        st.error("Name and Host are required.")

                b2.button(
                    "🚫 Cancel",
                    use_container_width=True,
                    on_click=callbacks["on_cancel"],
                )

                if b3.button("🗑️ Delete Datasource", use_container_width=True):
                    generic_confirm_dialog(
                        title=f"Delete profile: {ds_name}?",
                        message="This will permanently delete this datasource configuration.",
                        confirm_label="Delete Datasource",
                        on_confirm_func=callbacks["on_delete_ds"],
                        ds_id=edit_ds_id,
                    )
            else:
                if st.button(
                    "✨ Save New Datasource", type="primary", use_container_width=True
                ):
                    if ds_name and ds_host:
                        ok, msg = callbacks["on_save_new"](
                            ds_name,
                            ds_type,
                            ds_host,
                            ds_port,
                            ds_db,
                            ds_user,
                            ds_pass,
                            ds_charset,
                        )
                        if not ok:
                            st.error(msg)
                    else:
                        st.error("Name and Host are required.")


# ---------------------------------------------------------------------------
# Tab: Saved Configs
# ---------------------------------------------------------------------------


def _render_configs_tab(configs_df, callbacks: dict) -> None:
    st.markdown("#### Existing Saved Configs")
    st.caption("Select a row to preview JSON or delete.")

    if configs_df.empty:
        st.info("No configurations saved yet.")
        return

    gb = GridOptionsBuilder.from_dataframe(configs_df)
    gb.configure_selection("single", use_checkbox=True)
    gb.configure_column(
        "config_name", header_name="Config Name", flex=1, filter=True, sortable=True
    )
    gb.configure_column(
        "source_table", header_name="Source Table", width=150, filter=True
    )
    gb.configure_column(
        "destination_table", header_name="Destination Table", width=150, filter=True
    )
    gb.configure_column(
        "updated_at", header_name="Last Updated", width=180, sortable=True
    )
    gb.configure_grid_options(domLayout="autoHeight")

    grid_response = AgGrid(
        configs_df,
        gridOptions=gb.build(),
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        fit_columns_on_grid_load=True,
        height=400,
        width="100%",
        key="configs_grid",
    )

    selected = grid_response["selected_rows"]
    if selected is None or len(selected) == 0:
        return

    sel_row = selected[0] if isinstance(selected, list) else selected.iloc[0]
    config_name = sel_row.get("config_name")

    st.divider()
    st.markdown(f"**Selected Config:** `{config_name}`")

    c_view, c_del = st.columns([1, 1])
    with c_view:
        if st.button("👁️ Preview JSON", use_container_width=True, type="secondary"):
            # Controller fetches content; view only renders it
            content = callbacks["on_get_config_content"](config_name)
            preview_config_dialog(config_name, content)

    with c_del:
        if st.button("🗑️ Delete Config", use_container_width=True, type="secondary"):
            generic_confirm_dialog(
                title=f"Delete config: {config_name}?",
                message="This action cannot be undone.",
                confirm_label="Delete Config",
                on_confirm_func=callbacks["on_delete_config"],
                conf_name=config_name,
            )
