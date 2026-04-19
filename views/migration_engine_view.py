"""
Migration Engine View — pure rendering layer (MVC pattern).

This view delegates rendering to step components while receiving
pre-fetched data and callbacks from the controller.
"""
import streamlit as st
from typing import Any, Callable, Dict, List
import pandas as pd

from views.components.migration.step_config import render_step_config
from views.components.migration.step_connections import render_step_connections
from views.components.migration.step_review import render_step_review
from views.components.migration.step_execution import render_step_execution


def render_migration_engine_page(
    datasources_df: pd.DataFrame,
    configs_df: pd.DataFrame,
    ds_options: List[str],
    config_options: List[str],
    charset_map: Dict[str, str],
    wizard_state: Dict[str, Any],
    callbacks: Dict[str, Callable],
) -> None:
    """
    Render the Migration Engine page.

    Args:
        datasources_df: DataFrame of datasources
        configs_df: DataFrame of saved configs
        ds_options: List of datasource options
        config_options: List of config options
        charset_map: Dict of charset display names to values
        wizard_state: Dict of current wizard state from controller
        callbacks: Dict of callback functions from controller
    """
    st.subheader("🚀 Data Migration Execution Engine")

    step = wizard_state["step"]

    # Delegate to each step component
    if step == 1:
        render_step_config()
    elif step == 2:
        render_step_connections()
    elif step == 3:
        render_step_review()
    elif step == 4:
        render_step_execution()
