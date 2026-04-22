"""
Checkpoint Manager — saves/loads/clears migration resume state.

Responsibility (SRP): filesystem persistence for migration checkpoints only.
All writes are atomic via os.replace() (POSIX rename syscall) to prevent
corrupted JSON files during OS-level hard crashes.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_checkpoints")

_FSYNC_INTERVAL = 50


def _safe_name(config_name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in config_name)


def _checkpoint_path(config_name: str) -> str:
    return os.path.join(CHECKPOINT_DIR, f"checkpoint_{_safe_name(config_name)}.json")


def _atomic_write_json(path: str, data: dict, fsync: bool = False) -> None:
    """Write JSON to a temp file, then atomically replace the target via os.replace().

    os.replace() is atomic on POSIX — it swaps the directory entry in a single
    kernel operation.  If the process crashes *after* os.replace() the file is
    guaranteed to contain valid JSON.  If the process crashes *before*, the old
    file (if any) remains intact.

    ``fsync=True`` flushes OS page cache to disk, guarding against power loss
    at the cost of ~10ms per call.  Called sparingly (every N batches).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            if fsync:
                os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise


def save_checkpoint(
    config_name: str,
    batch_num: int,
    rows_processed: int,
    last_seen_pk: tuple | list | None = None,
) -> None:
    """Persist checkpoint so migration can resume after interruption.

    The write is atomic.  ``last_seen_pk`` is stored for cursor-based
    pagination resume (Phase 2).  ``fsync`` fires every ``_FSYNC_INTERVAL``
    batches as a compromise between durability and I/O overhead.
    """
    data = {
        "config_name": config_name,
        "last_batch": batch_num,
        "last_seen_pk": list(last_seen_pk) if last_seen_pk is not None else None,
        "rows_processed": rows_processed,
        "timestamp": datetime.now().isoformat(),
    }
    _atomic_write_json(
        _checkpoint_path(config_name),
        data,
        fsync=(batch_num % _FSYNC_INTERVAL == 0),
    )


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
    """Persist the full step-state map for a pipeline run (atomic write)."""
    data = {
        "pipeline_name": pipeline_name,
        "steps": steps_state,
        "timestamp": datetime.now().isoformat(),
    }
    _atomic_write_json(_pipeline_checkpoint_path(pipeline_name), data)


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
