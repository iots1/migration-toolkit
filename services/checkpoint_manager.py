"""
Checkpoint Manager — saves/loads/clears migration resume state.

Responsibility (SRP): filesystem persistence for migration checkpoints only.
"""
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


def load_checkpoint(config_name: str) -> dict | None:
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
