"""
Checkpoint Manager — saves/loads/clears migration resume state.

Responsibility (SRP): filesystem persistence for migration checkpoints only.
"""
from __future__ import annotations  # Enable modern type hints
from typing import Optional

import json
import os
from datetime import datetime

CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_checkpoints")


def _safe_name(config_name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in config_name)


def _checkpoint_path(config_name: str) -> str:
    return os.path.join(CHECKPOINT_DIR, f"checkpoint_{_safe_name(config_name)}.json")


def save_checkpoint(config_name: str, batch_num: int, rows_processed: int) -> None:
    """Persist checkpoint so migration can resume after interruption."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    data = {
        "config_name": config_name,
        "last_batch": batch_num,
        "rows_processed": rows_processed,
        "timestamp": datetime.now().isoformat(),
    }
    with open(_checkpoint_path(config_name), "w") as f:
        json.dump(data, f)


def load_checkpoint(config_name: str) -> Optional[dict]:
    """Return checkpoint dict if one exists, else None."""
    path = _checkpoint_path(config_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None


def clear_checkpoint(config_name: str) -> None:
    """Remove checkpoint file after successful migration."""
    path = _checkpoint_path(config_name)
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------------------
# 2D Pipeline Checkpoints (Challenge 2)
#
# Stored as: migration_checkpoints/pipeline_<safe_name>.json
#
# Schema:
# {
#   "pipeline_name": "my_pipeline",
#   "steps": {
#     "config_A": {"status": "completed", "last_batch": -1,  "rows_processed": 5000},
#     "config_B": {"status": "running",   "last_batch": 3,   "rows_processed": 1500},
#     "config_C": {"status": "pending",   "last_batch": 0,   "rows_processed": 0}
#   },
#   "timestamp": "2026-03-25T14:30:00"
# }
# ---------------------------------------------------------------------------

def _pipeline_checkpoint_path(pipeline_name: str) -> str:
    return os.path.join(CHECKPOINT_DIR, f"pipeline_{_safe_name(pipeline_name)}.json")


def save_pipeline_checkpoint(pipeline_name: str, steps_state: dict) -> None:
    """Persist the full step-state map for a pipeline run."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    data = {
        "pipeline_name": pipeline_name,
        "steps": steps_state,
        "timestamp": datetime.now().isoformat(),
    }
    with open(_pipeline_checkpoint_path(pipeline_name), "w") as f:
        json.dump(data, f, indent=2)


def load_pipeline_checkpoint(pipeline_name: str) -> Optional[dict]:
    """Return the pipeline checkpoint dict if one exists, else None."""
    path = _pipeline_checkpoint_path(pipeline_name)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None


def clear_pipeline_checkpoint(pipeline_name: str) -> None:
    """Remove pipeline checkpoint file after successful or cancelled run."""
    path = _pipeline_checkpoint_path(pipeline_name)
    if os.path.exists(path):
        os.remove(path)
