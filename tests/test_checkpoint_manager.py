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
        # File should exist (special chars replaced with _)
        files = os.listdir(tmp_dir)
        assert any("checkpoint_" in f for f in files)
