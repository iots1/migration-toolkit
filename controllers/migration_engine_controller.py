"""
Migration Engine Controller — MVC wrapper for existing migration_engine view.

This controller provides a clean interface for app.py while delegating
to the existing migration_engine orchestrator. Future refactoring will
gradually extract business logic from components into this controller.
"""
from __future__ import annotations  # Enable modern type hints

from utils.state_manager import PageState

# Import the existing orchestrator
from views.migration_engine import render_migration_engine_page as legacy_render_migration_engine_page


_DEFAULTS: dict = {
    # Wizard state
    "migration_step": 1,
    "migration_mode": None,  # "load_db" | "upload_file"
    "migration_config": None,

    # Connection profiles
    "migration_src_profile": None,
    "migration_tgt_profile": None,
    "migration_src_ok": False,
    "migration_tgt_ok": False,
    "src_charset": None,
    "src_sel": "Select Profile...",
    "tgt_sel": "Select Profile...",

    # Execution options
    "migration_test_sample": False,
    "truncate_target": False,
    "batch_size": 1000,
    "checkpoint_batch": 0,

    # Execution state
    "migration_running": False,
    "migration_completed": False,
    "migration_log_file": None,
    "last_migration_info": None,

    # Checkpoint state
    "resume_from_checkpoint": False,
    "checkpoint_available": False,
}


def run() -> None:
    """
    Entry point called by app.py.

    Initializes state and delegates to the existing migration_engine view.
    Future refactoring will extract business logic from components into this controller.
    """
    PageState.init(_DEFAULTS)

    # Delegate to existing orchestrator (still handles business logic)
    # TODO: Gradually extract logic from components into this controller
    legacy_render_migration_engine_page()


# ---------------------------------------------------------------------------
# Future: These methods will replace business logic in components
# ---------------------------------------------------------------------------

# TODO: Extract from step_config.py
# def _on_load_config(config_name: str) -> tuple[bool, str, dict | None]:
#     """Load a config from the database."""
#     pass

# TODO: Extract from step_connections.py
# def _on_test_source_connection(profile_name: str) -> tuple[bool, str]:
#     """Test connection to the source datasource."""
#     pass

# TODO: Extract from step_connections.py
# def _on_test_target_connection(profile_name: str) -> tuple[bool, str]:
#     """Test connection to the target datasource."""
#     pass

# TODO: Extract from step_execution.py
# def _on_start_migration(config: dict, src_profile: str, tgt_profile: str,
#                         options: dict) -> tuple[bool, str, dict]:
#     """Execute the migration."""
#     pass
