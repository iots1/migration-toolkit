"""
Pipeline Controller — owns state, data, and action callbacks for the
Data Pipeline page.

Follows the same MVC pattern as controllers/settings_controller.py:
    - Owns all session_state via PageState
    - Loads data from DB
    - Defines every action callback (no st.* rendering)
    - Delegates rendering entirely to views/pipeline_view.py
"""

from __future__ import annotations
from datetime import datetime, timedelta
import json
import uuid

import streamlit as st

import database as db
from models.pipeline_config import PipelineConfig, PipelineStep
from services.datasource_repository import DatasourceRepository as DSRepo
from services.pipeline_service import PipelineExecutor
from services.checkpoint_manager import load_pipeline_checkpoint
from utils.state_manager import PageState
from views.pipeline_view import render_pipeline_page

# Import repository adapters for DI injection (Phase 8)
from services.pipeline_service import (
    ConfigRepositoryAdapter,
    PipelineRunRepositoryAdapter,
)

_DEFAULTS: dict = {
    "pipeline_wizard_step": 1,
    "pipeline_mode": "new",  # "new" | "edit"
    "pipeline_selected": None,  # name of currently loaded pipeline
    "pipeline_current_id": None,  # UUID of the saved PipelineConfig
    "pipeline_form_name": "",
    "pipeline_form_desc": "",
    "pipeline_form_steps": [],  # list[dict] matching PipelineStep.to_dict()
    "pipeline_form_error_strategy": "fail_fast",
    "pipeline_form_batch_size": 1000,
    "pipeline_form_truncate": False,
    "pipeline_src_ds_name": None,
    "pipeline_tgt_ds_name": None,
    "pipeline_src_ok": False,
    "pipeline_tgt_ok": False,
    "pipeline_src_charset": None,
    "pipeline_running": False,
    "pipeline_completed": False,
    "pipeline_run_id": None,
    "pipeline_run_result": None,  # latest polled run snapshot dict
    "pipeline_zombie_warning": False,
}


def run() -> None:
    """Entry point called by app.py."""
    PageState.init(_DEFAULTS)

    # Detect zombie runs (running > 24 h — daemon thread died on server restart)
    PageState.set("pipeline_zombie_warning", _check_zombie_runs())

    # Load data for the view
    pipelines_df = db.get_pipelines()  # Fixed: was get_pipelines_list()
    configs_df = db.get_configs_list()
    datasources_df = db.get_datasources()

    form_state = {
        "wizard_step": PageState.get("pipeline_wizard_step"),
        "mode": PageState.get("pipeline_mode"),
        "form_name": PageState.get("pipeline_form_name"),
        "form_desc": PageState.get("pipeline_form_desc"),
        "form_steps": PageState.get("pipeline_form_steps"),
        "form_error_strategy": PageState.get("pipeline_form_error_strategy"),
        "form_batch_size": PageState.get("pipeline_form_batch_size"),
        "form_truncate": PageState.get("pipeline_form_truncate"),
        "src_ds_name": PageState.get("pipeline_src_ds_name"),
        "tgt_ds_name": PageState.get("pipeline_tgt_ds_name"),
        "src_ok": PageState.get("pipeline_src_ok"),
        "tgt_ok": PageState.get("pipeline_tgt_ok"),
        "running": PageState.get("pipeline_running"),
        "completed": PageState.get("pipeline_completed"),
        "run_id": PageState.get("pipeline_run_id"),
        "run_result": PageState.get("pipeline_run_result"),
        "zombie_warning": PageState.get("pipeline_zombie_warning"),
        "has_checkpoint": _has_checkpoint(),
    }

    callbacks = {
        "on_add_step": _on_add_step,
        "on_remove_step": _on_remove_step,
        "on_move_step_up": _on_move_step_up,
        "on_move_step_down": _on_move_step_down,
        "on_toggle_step": _on_toggle_step,
        "on_set_depends": _on_set_depends,
        "on_save_pipeline": _on_save_pipeline,
        "on_load_pipeline": _on_load_pipeline,
        "on_delete_pipeline": _on_delete_pipeline,
        "on_new_pipeline": _on_new_pipeline,
        "on_test_source": _on_test_source,
        "on_test_target": _on_test_target,
        "on_start_pipeline": _on_start_pipeline,
        "on_poll_status": _on_poll_status,
        "on_force_cancel": _on_force_cancel,
        "on_next_step": _on_next_step,
        "on_prev_step": _on_prev_step,
        "on_reset": _on_reset,
    }

    render_pipeline_page(
        pipelines_df, configs_df, datasources_df, form_state, callbacks
    )


# ---------------------------------------------------------------------------
# Step management callbacks
# ---------------------------------------------------------------------------


def _on_add_step(config_name: str) -> None:
    steps: list[dict] = list(PageState.get("pipeline_form_steps"))
    if any(s["config_name"] == config_name for s in steps):
        return  # already in pipeline
    steps.append(
        {
            "order": len(steps) + 1,
            "config_name": config_name,
            "depends_on": [],
            "enabled": True,
        }
    )
    _save_steps(steps)
    st.rerun()


def _on_remove_step(config_name: str) -> None:
    steps = [
        s
        for s in PageState.get("pipeline_form_steps")
        if s["config_name"] != config_name
    ]
    # Remove any depends_on references to this step
    for s in steps:
        s["depends_on"] = [d for d in s["depends_on"] if d != config_name]
    _save_steps(steps)
    st.rerun()


def _on_move_step_up(config_name: str) -> None:
    steps = list(PageState.get("pipeline_form_steps"))
    idx = next(
        (i for i, s in enumerate(steps) if s["config_name"] == config_name), None
    )
    if idx is None or idx == 0:
        return
    steps[idx - 1], steps[idx] = steps[idx], steps[idx - 1]
    _save_steps(steps)
    st.rerun()


def _on_move_step_down(config_name: str) -> None:
    steps = list(PageState.get("pipeline_form_steps"))
    idx = next(
        (i for i, s in enumerate(steps) if s["config_name"] == config_name), None
    )
    if idx is None or idx == len(steps) - 1:
        return
    steps[idx], steps[idx + 1] = steps[idx + 1], steps[idx]
    _save_steps(steps)
    st.rerun()


def _on_toggle_step(config_name: str) -> None:
    steps = list(PageState.get("pipeline_form_steps"))
    for s in steps:
        if s["config_name"] == config_name:
            s["enabled"] = not s["enabled"]
            break
    _save_steps(steps)
    st.rerun()


def _on_set_depends(config_name: str, depends_on: list[str]) -> None:
    steps = list(PageState.get("pipeline_form_steps"))
    for s in steps:
        if s["config_name"] == config_name:
            s["depends_on"] = depends_on
            break
    _save_steps(steps)


# ---------------------------------------------------------------------------
# Pipeline CRUD callbacks
# ---------------------------------------------------------------------------


def _on_save_pipeline(
    name: str, desc: str, error_strategy: str, batch_size: int, truncate: bool
) -> tuple[bool, str]:
    if not name.strip():
        return False, "Pipeline name is required."

    steps: list[dict] = PageState.get("pipeline_form_steps")
    if not steps:
        return False, "Add at least one step before saving."

    pc = (
        PipelineConfig.new(name)
        if not PageState.get("pipeline_current_id")
        else PipelineConfig.from_dict(
            {
                "id": PageState.get("pipeline_current_id"),
                "name": name,
            }
        )
    )
    pc.name = name
    pc.description = desc
    pc.error_strategy = error_strategy
    pc.batch_size = batch_size
    pc.truncate_targets = truncate
    pc.source_datasource_id = _ds_id(PageState.get("pipeline_src_ds_name"))
    pc.target_datasource_id = _ds_id(PageState.get("pipeline_tgt_ds_name"))
    pc.steps = [PipelineStep.from_dict(s) for s in steps]

    ok, msg = db.save_pipeline(
        pc.name,
        pc.description,
        json.dumps(pc.to_dict()),
        pc.source_datasource_id,
        pc.target_datasource_id,
        pc.error_strategy,
    )
    if ok:
        saved = db.get_pipeline_by_name(name)
        PageState.set("pipeline_current_id", saved["id"] if saved else pc.id)
        PageState.set("pipeline_selected", name)
        PageState.set("pipeline_form_name", name)
        PageState.set("pipeline_mode", "edit")
        st.rerun()
    return ok, msg


def _on_load_pipeline(name: str) -> None:
    row = db.get_pipeline_by_name(name)
    if not row:
        return
    pc = PipelineConfig.from_dict(json.loads(row["json_data"]))
    PageState.set("pipeline_mode", "edit")
    PageState.set("pipeline_selected", name)
    PageState.set("pipeline_current_id", row["id"])
    PageState.set("pipeline_form_name", pc.name)
    PageState.set("pipeline_form_desc", pc.description)
    PageState.set("pipeline_form_steps", [s.to_dict() for s in pc.steps])
    PageState.set("pipeline_form_error_strategy", pc.error_strategy)
    PageState.set("pipeline_form_batch_size", pc.batch_size)
    PageState.set("pipeline_form_truncate", pc.truncate_targets)
    # Restore datasource selectors if IDs are embedded
    if pc.source_datasource_id:
        ds = db.get_datasource_by_id(pc.source_datasource_id)
        PageState.set("pipeline_src_ds_name", ds["name"] if ds else None)
    if pc.target_datasource_id:
        ds = db.get_datasource_by_id(pc.target_datasource_id)
        PageState.set("pipeline_tgt_ds_name", ds["name"] if ds else None)
    PageState.set("pipeline_src_ok", False)
    PageState.set("pipeline_tgt_ok", False)
    PageState.set("pipeline_wizard_step", 1)
    st.rerun()


def _on_delete_pipeline(name: str) -> None:
    db.delete_pipeline(name)
    _on_new_pipeline()


def _on_new_pipeline() -> None:
    for key, val in _DEFAULTS.items():
        PageState.set(key, val)
    st.rerun()


# ---------------------------------------------------------------------------
# Connection callbacks
# ---------------------------------------------------------------------------


def _on_test_source(ds_name: str, charset: str | None) -> tuple[bool, str]:
    PageState.set("pipeline_src_ds_name", ds_name)
    PageState.set("pipeline_src_charset", charset)
    ok, msg = DSRepo.test_connection(ds_name)
    PageState.set("pipeline_src_ok", ok)
    return ok, msg


def _on_test_target(ds_name: str) -> tuple[bool, str]:
    PageState.set("pipeline_tgt_ds_name", ds_name)
    ok, msg = DSRepo.test_connection(ds_name)
    PageState.set("pipeline_tgt_ok", ok)
    return ok, msg


# ---------------------------------------------------------------------------
# Execution callbacks
# ---------------------------------------------------------------------------


def _on_start_pipeline() -> tuple[bool, str]:
    """Build PipelineConfig + conn configs, launch background thread."""
    pipeline_id = PageState.get("pipeline_current_id")
    if not pipeline_id:
        return False, "Save the pipeline before running it."

    # Guard: prevent concurrent runs
    latest = db.get_latest_run(pipeline_id)  # Fixed: was get_latest_pipeline_run()
    if latest and latest["status"] == "running":
        return False, "A run is already in progress for this pipeline."

    src_name = PageState.get("pipeline_src_ds_name")
    tgt_name = PageState.get("pipeline_tgt_ds_name")
    if not src_name or not tgt_name:
        return False, "Source and target datasources are required."

    try:
        src_conn = _ds_to_conn_config(src_name, PageState.get("pipeline_src_charset"))
        tgt_conn = _ds_to_conn_config(tgt_name)
    except ValueError as e:
        return False, str(e)

    row = db.get_pipeline_by_name(PageState.get("pipeline_form_name"))
    if not row:
        return False, "Pipeline not found in DB — save it first."

    pc = PipelineConfig.from_dict(json.loads(row["json_data"]))
    pc.id = row["id"]

    # Phase 8: DI injection - create repository adapter instances
    config_repo = ConfigRepositoryAdapter()  # Adapter over repository functions
    run_repo = PipelineRunRepositoryAdapter()  # Adapter over repository functions

    executor = PipelineExecutor(
        pipeline=pc,
        source_conn_config=src_conn,
        target_conn_config=tgt_conn,
        config_repo=config_repo,  # Injected (DIP)
        run_repo=run_repo,  # Injected (DIP)
    )
    run_id = executor.start_background()

    PageState.set("pipeline_run_id", run_id)
    PageState.set("pipeline_running", True)
    PageState.set("pipeline_completed", False)
    PageState.set("pipeline_run_result", None)
    PageState.set("pipeline_wizard_step", 4)
    st.rerun()
    return True, run_id


def _on_poll_status() -> None:
    """Fetch latest run snapshot from DB and update session state."""
    pipeline_id = PageState.get("pipeline_current_id")
    if not pipeline_id:
        st.warning("No pipeline loaded — save and start the pipeline first.")
        return
    latest = db.get_latest_run(pipeline_id)  # Fixed: was get_latest_pipeline_run()
    PageState.set("pipeline_run_result", latest)
    if latest and latest["status"] not in ("running", "pending"):
        PageState.set("pipeline_running", False)
        PageState.set("pipeline_completed", True)


def _on_force_cancel(run_id: str) -> None:
    """Mark a stuck/zombie run as failed in the DB."""
    # Import the update function directly from pipeline_run_repo
    from repositories.pipeline_run_repo import update as _update_run

    _update_run(
        run_id,
        "failed",
        "{}",
        error_message="Manually cancelled — suspected zombie run",
    )
    PageState.set("pipeline_running", False)
    PageState.set("pipeline_zombie_warning", False)
    st.rerun()


# ---------------------------------------------------------------------------
# Wizard navigation callbacks
# ---------------------------------------------------------------------------


def _on_next_step() -> None:
    current = PageState.get("pipeline_wizard_step")
    PageState.set("pipeline_wizard_step", min(current + 1, 4))
    st.rerun()


def _on_prev_step() -> None:
    current = PageState.get("pipeline_wizard_step")
    PageState.set("pipeline_wizard_step", max(current - 1, 1))
    st.rerun()


def _on_reset() -> None:
    PageState.set("pipeline_wizard_step", 1)
    PageState.set("pipeline_running", False)
    PageState.set("pipeline_completed", False)
    PageState.set("pipeline_run_id", None)
    PageState.set("pipeline_run_result", None)
    st.rerun()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _save_steps(steps: list[dict]) -> None:
    """Re-number orders and save steps list to session state."""
    for i, s in enumerate(steps):
        s["order"] = i + 1
    PageState.set("pipeline_form_steps", steps)


def _ds_to_conn_config(ds_name: str, charset: str | None = None) -> dict:
    ds = DSRepo.get_by_name(ds_name)
    if not ds:
        raise ValueError(f"Datasource '{ds_name}' not found.")
    return {
        "db_type": ds["db_type"],
        "host": ds["host"],
        "port": ds["port"],
        "db_name": ds["dbname"],
        "user": ds["username"],
        "password": ds["password"],
        "charset": charset,
    }


def _ds_id(ds_name: str | None) -> uuid.UUID | None:
    if not ds_name:
        return None
    ds = DSRepo.get_by_name(ds_name)
    return ds["id"] if ds else None


def _has_checkpoint() -> bool:
    name = PageState.get("pipeline_form_name")
    if not name:
        return False
    return load_pipeline_checkpoint(name) is not None


def _check_zombie_runs() -> bool:
    """Return True if the current pipeline has a run stuck in 'running' > 24 h."""
    pipeline_id = PageState.get("pipeline_current_id")
    if not pipeline_id:
        return False
    latest = db.get_latest_run(pipeline_id)  # Fixed: was get_latest_pipeline_run()
    if not latest or latest["status"] != "running":
        return False
    started_at = latest.get("started_at")
    if started_at:
        try:
            started = datetime.fromisoformat(str(started_at))
            return datetime.now() - started > timedelta(hours=24)
        except Exception:
            pass
    return False
