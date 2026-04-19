"""
Schema Mapper Controller — MVC wrapper for existing schema_mapper view.

This controller provides a clean interface for app.py while delegating
to the existing schema_mapper orchestrator. Future refactoring will
gradually extract business logic from components into this controller.
"""
from __future__ import annotations  # Enable modern type hints
from typing import Optional

from utils.state_manager import PageState

# Import the existing orchestrator
from views.schema_mapper import render_schema_mapper_page as legacy_render_schema_mapper_page


_DEFAULTS: dict = {
    "mapper_focus_mode": False,
    "source_mode": "Run ID",
    "mapper_active_table": None,
    "mapper_df_raw": None,
    "mapper_source_db": None,
    "mapper_source_tbl": None,
    "mapper_loaded_config": None,
    "mapper_editor_ver": 0,
    "mapper_config_name": "",
    "mapper_tgt_db": None,
    "mapper_tgt_tbl": None,
    "mapper_show_history": False,
    "mapper_show_compare": False,
    "mapper_compare_v1": 1,
    "mapper_compare_v2": 2,
    "conn_status_cache": {},
    "sm_sel_table_idx": 0,
}


def run() -> None:
    """
    Entry point called by app.py.

    Initializes state and delegates to the existing schema_mapper view.
    Future refactoring will extract business logic from components into this controller.
    """
    PageState.init(_DEFAULTS)

    # Delegate to existing orchestrator (still handles business logic)
    # TODO: Gradually extract logic from components into this controller
    legacy_render_schema_mapper_page()


# ---------------------------------------------------------------------------
# Future: These methods will replace business logic in components
# ---------------------------------------------------------------------------

# TODO: Extract from source_selector.py
# def _on_select_run_id(run_id: str, table_name: str) -> tuple[bool, str, pd.DataFrame | None]:
#     """Handle Run ID source selection."""
#     pass

# TODO: Extract from source_selector.py
# def _on_select_datasource(datasource_name: str, table_name: str) -> tuple[bool, str, pd.DataFrame | None]:
#     """Handle Datasource source selection."""
#     pass

# TODO: Extract from mapping_editor.py
# def _on_ai_suggest(source_columns: list, target_columns: list, threshold: float = 0.4) -> dict:
#     """Generate AI-powered column mapping suggestions."""
#     pass

# TODO: Extract from config_actions.py
# def _on_save_config(config_name: str, table_name: str, json_data: dict) -> tuple[bool, str]:
#     """Save or update a configuration."""
#     pass

# TODO: Extract from config_actions.py
# def _on_validate_targets(active_table: str, target_datasource: str, target_table: str,
#                          mappings_df: pd.DataFrame) -> tuple[bool, str, list]:
#     """Validate that mapped target columns exist in the actual target table."""
#     pass
