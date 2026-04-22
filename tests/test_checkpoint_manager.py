import json
import os
import pytest
from unittest.mock import patch


def test_save_and_load_checkpoint(tmp_dir):
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        save_checkpoint("my_config", batch_num=5, rows_processed=500)
        data = load_checkpoint("my_config")
        assert data["config_name"] == "my_config"
        assert data["last_batch"] == 5
        assert data["rows_processed"] == 500
        assert "timestamp" in data


def test_load_checkpoint_returns_none_when_missing(tmp_dir):
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import load_checkpoint
        result = load_checkpoint("nonexistent_config")
        assert result is None


def test_clear_checkpoint(tmp_dir):
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint, clear_checkpoint
        save_checkpoint("test", 1, 100)
        assert load_checkpoint("test") is not None
        clear_checkpoint("test")
        assert load_checkpoint("test") is None


def test_safe_name_in_filename(tmp_dir):
    """Config names with special chars should be sanitised in filenames."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        save_checkpoint("my config/v2", 1, 10)
        data = load_checkpoint("my config/v2")
        assert data is not None
        files = os.listdir(tmp_dir)
        assert any("checkpoint_" in f for f in files)


def test_atomic_write_replaces_file(tmp_dir):
    """Verify that save_checkpoint uses atomic write (no .tmp file left behind)."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint
        save_checkpoint("atomic_test", 1, 100)
        files = os.listdir(tmp_dir)
        assert not any(f.endswith(".tmp") for f in files), "Temp file should be cleaned up"


def test_last_seen_pk_saved_and_loaded(tmp_dir):
    """Verify last_seen_pk is persisted for cursor-based resume."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        pk = ("HN0001234",)
        save_checkpoint("pk_test", batch_num=42, rows_processed=42000, last_seen_pk=pk)
        data = load_checkpoint("pk_test")
        assert data["last_seen_pk"] == ["HN0001234"]
        assert data["last_batch"] == 42
        assert data["rows_processed"] == 42000


def test_last_seen_pk_none_not_stored(tmp_dir):
    """When last_seen_pk is None, it should not be stored or should be None."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        save_checkpoint("pk_none_test", 1, 100, last_seen_pk=None)
        data = load_checkpoint("pk_none_test")
        assert data["last_seen_pk"] is None


def test_composite_pk_saved(tmp_dir):
    """Composite PKs (multi-column) should be stored as a list."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        pk = ("col_a_val", "col_b_val")
        save_checkpoint("composite_pk_test", 10, 5000, last_seen_pk=pk)
        data = load_checkpoint("composite_pk_test")
        assert data["last_seen_pk"] == ["col_a_val", "col_b_val"]


def test_pipeline_checkpoint_atomic(tmp_dir):
    """Pipeline checkpoint should also be written atomically."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_pipeline_checkpoint, load_pipeline_checkpoint
        steps = {"config_A": {"status": "running", "last_batch": 5, "rows_processed": 5000}}
        save_pipeline_checkpoint("my_pipeline", steps)
        files = os.listdir(tmp_dir)
        assert not any(f.endswith(".tmp") for f in files), "Temp file should be cleaned up"
        data = load_pipeline_checkpoint("my_pipeline")
        assert data["steps"]["config_A"]["last_batch"] == 5


def test_overwrite_checkpoint(tmp_dir):
    """Second save should overwrite the first (no duplicates)."""
    with patch("services.checkpoint_manager.CHECKPOINT_DIR", tmp_dir):
        from services.checkpoint_manager import save_checkpoint, load_checkpoint
        save_checkpoint("overwrite_test", 1, 100)
        save_checkpoint("overwrite_test", 2, 200)
        data = load_checkpoint("overwrite_test")
        assert data["last_batch"] == 2
        assert data["rows_processed"] == 200
