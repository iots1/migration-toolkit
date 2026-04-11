"""
Settings Controller — owns state, data fetching, and action callbacks.

Responsibilities:
    - Initialise all session state keys for the Settings page
    - Load data from the database (datasources, configs)
    - Define every action callback (CRUD operations)
    - Assemble form_state and callbacks dicts
    - Delegate rendering entirely to views/settings_view.py

Must NOT contain any st.* rendering calls (no st.button, st.text_input, etc.).
"""

from __future__ import annotations  # Enable modern type hints

import streamlit as st

import database as db
from config import DB_TYPES
from utils.state_manager import PageState
from views.settings_view import render_settings_page

_DEFAULTS: dict = {
    "new_ds_name": "",
    "new_ds_host": "",
    "new_ds_port": "",
    "new_ds_db": "",
    "new_ds_user": "",
    "new_ds_pass": "",
    "ds_form_type_index": 0,
    "is_edit_mode": False,
    "edit_ds_id": None,
    "trigger_ds_reset": False,
    "ds_grid_key": 0,
}


def run() -> None:
    """Entry point called by app.py. Orchestrates the Settings page."""
    PageState.init(_DEFAULTS)

    # Post-action reset: triggered by CRUD callbacks after successful operations
    if PageState.get("trigger_ds_reset"):
        _reset_to_new_mode()
        PageState.set("trigger_ds_reset", False)

    # --- Load data ---
    datasources_df = db.get_datasources()
    configs_df = db.get_configs_list()

    # --- Snapshot of form state for the view ---
    form_state = {
        "is_edit_mode": PageState.get("is_edit_mode"),
        "edit_ds_id": PageState.get("edit_ds_id"),
        "ds_form_type_index": PageState.get("ds_form_type_index"),
        "ds_grid_key": PageState.get("ds_grid_key"),
    }

    callbacks = {
        "on_row_select": _on_row_select,
        "on_save_new": _on_save_new,
        "on_update": _on_update,
        "on_delete_ds": _on_delete_ds,
        "on_cancel": _reset_to_new_mode,
        "on_delete_config": _on_delete_config,
        "on_get_config_content": _on_get_config_content,
    }

    render_settings_page(datasources_df, configs_df, form_state, callbacks)


# ---------------------------------------------------------------------------
# Private action callbacks
# ---------------------------------------------------------------------------


def _reset_to_new_mode() -> None:
    """Clear the datasource form and return to Add-New mode."""
    PageState.set("new_ds_name", "")
    PageState.set("new_ds_host", "")
    PageState.set("new_ds_port", "")
    PageState.set("new_ds_db", "")
    PageState.set("new_ds_user", "")
    PageState.set("new_ds_pass", "")
    PageState.set("ds_form_type_index", 0)
    PageState.set("is_edit_mode", False)
    PageState.set("edit_ds_id", None)
    # Increment grid key to force AgGrid to deselect the highlighted row
    PageState.set("ds_grid_key", PageState.get("ds_grid_key", 0) + 1)


def _on_row_select(ds_id) -> None:
    """Load a datasource into the form for editing and trigger a rerun."""
    full_data = db.get_datasource_by_id(ds_id)
    if not full_data:
        return
    try:
        type_index = DB_TYPES.index(full_data["db_type"])
    except ValueError:
        type_index = 0
    PageState.set("new_ds_name", full_data["name"])
    PageState.set("ds_form_type_index", type_index)
    PageState.set("new_ds_host", full_data["host"])
    PageState.set("new_ds_port", full_data["port"])
    PageState.set("new_ds_db", full_data["dbname"])
    PageState.set("new_ds_user", full_data["username"])
    PageState.set("new_ds_pass", full_data["password"])
    PageState.set("is_edit_mode", True)
    PageState.set("edit_ds_id", ds_id)
    st.rerun()


def _on_save_new(
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    """Create a new datasource. Reruns on success; returns (False, msg) on failure."""
    ok, msg = db.save_datasource(name, db_type, host, port, dbname, username, password)
    if ok:
        PageState.set("trigger_ds_reset", True)
        st.rerun()
    return ok, msg


def _on_update(
    ds_id,
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    """Update an existing datasource. Reruns on success; returns (False, msg) on failure."""
    ok, msg = db.update_datasource(
        ds_id, name, db_type, host, port, dbname, username, password
    )
    if ok:
        PageState.set("trigger_ds_reset", True)
        st.rerun()
    return ok, msg


def _on_delete_ds(ds_id) -> None:
    """Delete a datasource and trigger a full form reset."""
    db.delete_datasource(ds_id)
    PageState.set("trigger_ds_reset", True)
    st.rerun()


def _on_delete_config(
    conf_name: str = None, config_name: str = None, **kwargs
) -> tuple[bool, str]:
    """Delete a saved migration config. Reruns on success."""
    # Use the actual parameter name passed (either conf_name or config_name)
    param_name = conf_name if conf_name is not None else config_name
    if param_name is None:
        return False, "No config name provided"

    success, msg = db.delete_config(param_name)
    if success:
        st.rerun()
    return success, msg


def _on_get_config_content(config_name: str) -> dict | None:
    """Fetch the JSON content of a config. Called by the view before opening the preview dialog."""
    return db.get_config_content(config_name)
