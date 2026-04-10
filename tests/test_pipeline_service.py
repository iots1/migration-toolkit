"""
Tests for services/pipeline_service.py — PipelineExecutor.

Covers:
- Kahn's topological sort (linear, diamond, circular, unknown dep)
- _should_skip for all three error strategies
- Transitive skip propagation (skip_dependents)
- 2D checkpoint helpers (update + complete)
- Overall status logic (completed / partial / failed)
- Background thread + DB polling (integration with in-memory DB)
"""
import json
import os
import tempfile
import time
import pytest

# Point at a temp DB before importing anything that touches database.py
_tmp_db = tempfile.mktemp(suffix=".db")
os.environ["DB_FILE"] = _tmp_db

import database as db
from models.pipeline_config import PipelineConfig, PipelineStep
from services.pipeline_service import PipelineExecutor, StepResult, PipelineResult
from services.checkpoint_manager import (
    load_pipeline_checkpoint,
    clear_pipeline_checkpoint,
)


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """Each test gets its own isolated SQLite file."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr("database.DB_FILE", db_path)
    monkeypatch.setattr("config.DB_FILE", db_path)
    db.init_db()
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pipeline(*steps_cfg, error_strategy="fail_fast") -> PipelineConfig:
    """Build a PipelineConfig from (order, name, depends_on) tuples."""
    pc = PipelineConfig.new("test_pipe", error_strategy=error_strategy)
    pc.steps = [
        PipelineStep(order=o, config_name=n, depends_on=d)
        for o, n, d in steps_cfg
    ]
    return pc


def _executor(pipeline, **kwargs) -> PipelineExecutor:
    return PipelineExecutor(pipeline, {}, {}, **kwargs)


# ---------------------------------------------------------------------------
# Topological sort — _resolve_execution_order
# ---------------------------------------------------------------------------

class TestResolveExecutionOrder:
    def test_linear_chain(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), (3, "C", ["B"]))
        order = _executor(pc)._resolve_execution_order()
        assert [s.config_name for s in order] == ["A", "B", "C"]

    def test_independent_steps_sorted_by_order(self):
        pc = _pipeline((2, "B", []), (1, "A", []), (3, "C", []))
        order = _executor(pc)._resolve_execution_order()
        assert [s.config_name for s in order] == ["A", "B", "C"]

    def test_diamond_dependency(self):
        pc = _pipeline(
            (1, "A", []),
            (2, "B", ["A"]),
            (3, "C", ["A"]),
            (4, "D", ["B", "C"]),
        )
        order = _executor(pc)._resolve_execution_order()
        names = [s.config_name for s in order]
        assert names[0] == "A"
        assert names[-1] == "D"
        assert set(names[1:3]) == {"B", "C"}

    def test_circular_dependency_raises(self):
        pc = _pipeline((1, "A", ["C"]), (2, "B", ["A"]), (3, "C", ["B"]))
        with pytest.raises(ValueError, match="Circular dependency"):
            _executor(pc)._resolve_execution_order()

    def test_unknown_dependency_raises(self):
        pc = _pipeline((1, "A", ["GHOST"]))
        with pytest.raises(ValueError, match="GHOST"):
            _executor(pc)._resolve_execution_order()

    def test_single_step_no_deps(self):
        pc = _pipeline((1, "A", []))
        order = _executor(pc)._resolve_execution_order()
        assert len(order) == 1
        assert order[0].config_name == "A"


# ---------------------------------------------------------------------------
# _should_skip
# ---------------------------------------------------------------------------

class TestShouldSkip:
    def _results(self, **statuses) -> dict[str, StepResult]:
        return {name: StepResult(status=s, config_name=name) for name, s in statuses.items()}

    def test_fail_fast_no_skip_on_success(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), error_strategy="fail_fast")
        ex = _executor(pc)
        results = self._results(A="success")
        skip, _ = ex._should_skip(pc.steps[1], results)
        assert skip is False

    def test_skip_dependents_skips_on_failed_parent(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), error_strategy="skip_dependents")
        ex = _executor(pc)
        results = self._results(A="failed")
        skip, reason = ex._should_skip(pc.steps[1], results)
        assert skip is True
        assert "'A'" in reason

    def test_skip_dependents_transitive(self):
        """C depends on B; B was skipped because A failed — C must also be skipped."""
        pc = _pipeline(
            (1, "A", []),
            (2, "B", ["A"]),
            (3, "C", ["B"]),
            error_strategy="skip_dependents",
        )
        ex = _executor(pc)
        results = self._results(A="failed", B="skipped_dependency")
        skip, reason = ex._should_skip(pc.steps[2], results)
        assert skip is True
        assert "'B'" in reason

    def test_continue_on_error_never_skips(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), error_strategy="continue_on_error")
        ex = _executor(pc)
        results = self._results(A="failed")
        skip, _ = ex._should_skip(pc.steps[1], results)
        assert skip is False

    def test_no_deps_never_skipped(self):
        pc = _pipeline((1, "A", []), error_strategy="skip_dependents")
        ex = _executor(pc)
        skip, _ = ex._should_skip(pc.steps[0], {})
        assert skip is False


# ---------------------------------------------------------------------------
# 2D Checkpoint helpers
# ---------------------------------------------------------------------------

class TestCheckpointHelpers:
    def test_update_step_checkpoint_marks_running(self, tmp_path):
        pc = PipelineConfig.new("cp_test")
        ex = PipelineExecutor(pc, {}, {})
        ex._update_step_checkpoint("cfg_a", batch_num=5, rows=2500)
        loaded = load_pipeline_checkpoint("cp_test")
        assert loaded["steps"]["cfg_a"]["status"] == "running"
        assert loaded["steps"]["cfg_a"]["last_batch"] == 5
        assert loaded["steps"]["cfg_a"]["rows_processed"] == 2500
        clear_pipeline_checkpoint("cp_test")

    def test_complete_step_checkpoint_marks_completed(self, tmp_path):
        pc = PipelineConfig.new("cp_test2")
        ex = PipelineExecutor(pc, {}, {})
        ex._complete_step_checkpoint("cfg_b", rows=8000)
        loaded = load_pipeline_checkpoint("cp_test2")
        assert loaded["steps"]["cfg_b"]["status"] == "completed"
        assert loaded["steps"]["cfg_b"]["last_batch"] == -1
        clear_pipeline_checkpoint("cp_test2")

    def test_checkpoint_accumulates_multiple_steps(self):
        pc = PipelineConfig.new("cp_multi")
        ex = PipelineExecutor(pc, {}, {})
        ex._update_step_checkpoint("cfg_a", 3, 1500)
        ex._complete_step_checkpoint("cfg_b", 5000)
        loaded = load_pipeline_checkpoint("cp_multi")
        assert "cfg_a" in loaded["steps"]
        assert "cfg_b" in loaded["steps"]
        clear_pipeline_checkpoint("cp_multi")


# ---------------------------------------------------------------------------
# Database CRUD — pipelines + pipeline_runs
# ---------------------------------------------------------------------------

class TestPipelineCRUD:
    def test_save_and_get_pipeline(self):
        pc = PipelineConfig.new("alpha", description="first pipeline")
        ok, msg = db.save_pipeline(pc.name, pc.description, pc.to_dict(), None, None, pc.error_strategy)
        assert ok, msg

        row = db.get_pipeline_by_name("alpha")
        assert row is not None
        assert row["id"] == pc.id
        assert row["description"] == "first pipeline"
        assert row["json_data"]["name"] == "alpha"

    def test_save_pipeline_overwrites_preserves_id(self):
        pc = PipelineConfig.new("beta")
        db.save_pipeline(pc.name, pc.description, pc.to_dict(), None, None, "fail_fast")
        # Save again with a different description
        pc2 = PipelineConfig.from_dict({**pc.to_dict(), "description": "updated"})
        db.save_pipeline(pc2.name, pc2.description, pc2.to_dict(), None, None, "fail_fast")
        row = db.get_pipeline_by_name("beta")
        assert row["id"] == pc.id   # id preserved
        assert row["description"] == "updated"

    def test_delete_pipeline(self):
        pc = PipelineConfig.new("gamma")
        db.save_pipeline(pc.name, pc.description, pc.to_dict(), None, None, "fail_fast")
        ok, _ = db.delete_pipeline("gamma")
        assert ok
        assert db.get_pipeline_by_name("gamma") is None

    def test_get_pipelines_list(self):
        for name in ("p1", "p2", "p3"):
            pc = PipelineConfig.new(name)
            db.save_pipeline(pc.name, "", pc.to_dict(), None, None, "fail_fast")
        df = db.get_pipelines()  # Fixed: was get_pipelines_list()
        assert len(df) == 3
        assert set(df["name"].tolist()) == {"p1", "p2", "p3"}


class TestPipelineRunsCRUD:
    def _saved_pipeline(self) -> str:
        pc = PipelineConfig.new("run_test_pipe")
        db.save_pipeline(pc.name, "", pc.to_dict(), None, None, "fail_fast")
        return pc.id

    def test_save_and_get_latest_run(self):
        pid = self._saved_pipeline()
        run_id = db.save_pipeline_run(pid, "running", "{}")
        assert len(run_id) == 36

        latest = db.get_latest_run(pid)  # Fixed: was get_latest_pipeline_run()
        assert latest["id"] == run_id
        assert latest["status"] == "running"
        assert latest["steps"] == {}

    def test_update_run_sets_steps_json(self):
        pid = self._saved_pipeline()
        run_id = db.save_pipeline_run(pid, "running", "{}")
        steps = {"cfg_a": {"status": "success", "rows_processed": 100}}
        db.update_pipeline_run(run_id, "running", json.dumps(steps))

        latest = db.get_latest_run(pid)  # Fixed: was get_latest_pipeline_run()
        assert latest["steps"]["cfg_a"]["status"] == "success"

    def test_update_run_terminal_sets_completed_at(self):
        pid = self._saved_pipeline()
        run_id = db.save_pipeline_run(pid, "running", "{}")
        db.update_pipeline_run(run_id, "completed", "{}")

        latest = db.get_latest_run(pid)  # Fixed: was get_latest_pipeline_run()
        assert latest["status"] == "completed"
        assert latest["completed_at"] is not None

    def test_get_pipeline_runs_returns_all(self):
        pid = self._saved_pipeline()
        for _ in range(3):
            db.save_pipeline_run(pid, "completed", "{}")
        df = db.get_pipeline_runs(pid)
        assert len(df) == 3

    def test_update_run_error_message(self):
        pid = self._saved_pipeline()
        run_id = db.save_pipeline_run(pid, "running", "{}")
        db.update_pipeline_run(run_id, "failed", "{}", error_message="boom")
        latest = db.get_latest_run(pid)  # Fixed: was get_latest_pipeline_run()
        assert latest["error_message"] == "boom"


# ---------------------------------------------------------------------------
# Background thread (integration smoke test)
# ---------------------------------------------------------------------------

class TestBackgroundThread:
    def test_start_background_returns_run_id_and_updates_db(self, monkeypatch):
        """Executor with no real steps completes immediately in background thread."""
        pc = PipelineConfig.new("bg_test")
        pc.steps = []   # no steps — execute() returns instantly
        db.save_pipeline(pc.name, "", pc.to_dict(), None, None, "fail_fast")

        ex = PipelineExecutor(pc, {}, {})
        run_id = ex.start_background()
        assert len(run_id) == 36

        # Wait briefly for the daemon thread to finish
        deadline = time.time() + 5.0
        status = "running"
        while time.time() < deadline and status == "running":
            time.sleep(0.05)
            latest = db.get_latest_run(pc.id)  # Fixed: was get_latest_pipeline_run()
            if latest:
                status = latest["status"]

        assert status == "completed", f"Expected 'completed', got '{status}'"
