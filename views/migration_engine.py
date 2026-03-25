"""
Migration Engine Page — orchestrator only.

Initialises session state defaults, then delegates each wizard step
to its dedicated component.

Steps:
    1  → views/components/migration/step_config.py
    2  → views/components/migration/step_connections.py
    3  → views/components/migration/step_review.py
    4  → views/components/migration/step_execution.py
"""
import streamlit as st

from views.components.migration.step_config import render_step_config
from views.components.migration.step_connections import render_step_connections
from views.components.migration.step_review import render_step_review
from views.components.migration.step_execution import render_step_execution

_DEFAULTS: dict = {
    "migration_step": 1,
    "migration_config": None,
    "migration_src_profile": None,
    "migration_tgt_profile": None,
    "migration_src_ok": False,
    "migration_tgt_ok": False,
    "migration_test_sample": False,
    "truncate_target": False,
    "migration_running": False,
    "migration_completed": False,
    "resume_from_checkpoint": False,
    "checkpoint_batch": 0,
}


def render_migration_engine_page() -> None:
    st.subheader("🚀 Data Migration Execution Engine")

    for key, default in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = default

    step = st.session_state.migration_step
    if step == 1:
        render_step_config()
    elif step == 2:
        render_step_connections()
    elif step == 3:
        render_step_review()
    elif step == 4:
        render_step_execution()
