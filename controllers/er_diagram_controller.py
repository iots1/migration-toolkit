"""ER Diagram Controller - Manages ER diagram page state and logic."""
from __future__ import annotations  # Enable modern type hints

import os
from utils.state_manager import PageState
from views.er_diagram_view import render_er_diagram_page
import database as db
from services.db_connector import get_tables_from_datasource, get_columns_from_table, get_foreign_keys

_DEFAULTS: dict = {
    "er_nodes": [],
    "er_edges": [],
    "er_edit_target": None,
    "er_loaded": False,
}

def run() -> None:
    """Run ER diagram page with full MVC separation."""
    PageState.init(_DEFAULTS)

    # Fetch datasources
    datasources_df = db.get_datasources()

    # Define callbacks
    callbacks = {
        "on_build_graph": _on_build_graph,
    }

    # Prepare initial state for view
    form_state = {
        "er_edit_target": PageState.get("er_edit_target"),
        "er_loaded": PageState.get("er_loaded"),
    }

    # Call view
    render_er_diagram_page(datasources_df, form_state, callbacks)


def _on_build_graph(datasource_name: str, schema: str | None) -> tuple[bool, str, list, list]:
    """
    Build graph state from datasource.

    Returns:
        tuple[bool, str, nodes, edges]: (success, message, nodes, edges)
    """
    ds = db.get_datasource_by_name(datasource_name)
    if not ds:
        return False, f"Datasource '{datasource_name}' not found", [], []

    try:
        tables = get_tables_from_datasource(ds)
        nodes = []
        edges = []

        for table in tables:
            # Get columns for this table
            columns = get_columns_from_table(ds, table, schema)
            col_list = [col["name"] for col in columns]

            # Create node
            nodes.append({
                "id": table,
                "label": f"{table}\\n({len(col_list)} cols)",
                "title": f"<b>{table}</b><br/>" + "<br/>".join(col_list[:10]),  # Show first 10 columns
                "size": 30 + len(col_list) * 2
            })

        # Get foreign keys for edges
        for table in tables:
            fks = get_foreign_keys(ds, table, schema)
            for fk in fks:
                edges.append({
                    "source": fk["from_table"],
                    "target": fk["to_table"],
                    "label": fk["from_column"] + " → " + fk["to_column"]
                })

        PageState.set("er_nodes", nodes)
        PageState.set("er_edges", edges)
        PageState.set("er_loaded", True)

        return True, f"Loaded {len(nodes)} tables and {len(edges)} relationships", nodes, edges

    except Exception as e:
        return False, f"Error building graph: {str(e)}", [], []


def render_editor_panel(datasource, schema):
    """Render editor panel (called by view, not exported)."""
    import streamlit as st

    st.markdown(f"### 📝 Editing: {PageState.get('er_edit_target')}")

    # This would show column details, allow editing, etc.
    # For now, just show a placeholder
    st.info("Editor panel - feature coming in Phase 7D")
