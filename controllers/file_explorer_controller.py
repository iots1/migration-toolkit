"""File Explorer Controller - Minimal controller for file explorer page."""
from __future__ import annotations  # Enable modern type hints

import os
from utils.state_manager import PageState
from views.file_explorer_view import render_file_explorer_page

_DEFAULTS: dict = {
    "file_explorer_init": False,
}

def run() -> None:
    """
    Run file explorer page.

    This is a minimal controller - the page is so simple that
    there's no real state management or business logic.
    """
    PageState.init(_DEFAULTS)

    # Fetch data (just directory paths)
    from config import ANALYSIS_DIR, BASE_DIR
    mini_his_dir = os.path.join(BASE_DIR, "mini_his")

    # Prepare data for view
    view_data = {
        "analysis_dir": ANALYSIS_DIR,
        "mini_his_dir": mini_his_dir,
        "has_analysis_dir": os.path.exists(ANALYSIS_DIR),
        "has_mini_his": os.path.exists(mini_his_dir),
    }

    # No callbacks needed for this simple page
    callbacks = {}

    # Call view
    render_file_explorer_page(view_data, callbacks)
