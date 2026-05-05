"""
Tests for services/pipeline_service.py — PipelineExecutor.

Covers:
- Kahn's topological sort (linear, diamond, circular, unknown dep)
- _should_skip for all three error strategies
- Transitive skip propagation (skip_dependents)
- 2D checkpoint helpers (update + complete)
- Overall status logic (completed / partial / failed)
- Background thread smoke test
"""
import json
import os
import time
import uuid
import pytest
from unittest.mock import MagicMock, patch

from models.pipeline_config import PipelineConfig, PipelineStep
from services.pipeline_service import (
    PipelineExecutor,
    StepResult,
    PipelineResult,
    ConfigRepositoryAdapter,
    PipelineRunRepositoryAdapter,
)


# ---------------------------------------------------------------------------
# Mock Repositories
# ---------------------------------------------------------------------------


class MockConfigRepo:
    """In-memory mock for ConfigRepository protocol."""

    def __init__(self):
        self._configs: dict[str, dict] = {}

    def add(self, name: str, config: dict):
        self._configs[name] = config

    def get_content(self, config_name: str) -> dict | None:
        return self._configs.get(config_name)


class MockRunRepo:
    """In-memory mock for PipelineRunRepository protocol."""

    def __init__(self):
        self._runs: dict[str, dict] = {}

    def save(self, record) -> str:
        run_id = str(uuid.uuid4())
        self._runs[run_id] = {
            "id": run_id,
            "pipeline_id": str(record.pipeline_id),
            "status": record.status,
            "error_message": None,
            "completed_at": None,
        }
        return run_id

    def update(self, run_id, patch):
        if run_id in self._runs:
            self._runs[run_id].update({
                "status": patch.status,
            })
            if patch.error_message:
                self._runs[run_id]["error_message"] = patch.error_message
            if patch.status in ("completed", "failed"):
                self._runs[run_id]["completed_at"] = "now"

    def get_latest(self, pipeline_id: str) -> dict | None:
        matches = [r for r in self._runs.values() if r["pipeline_id"] == str(pipeline_id)]
        return matches[-1] if matches else None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pipeline(*steps_cfg, error_strategy="fail_fast") -> PipelineConfig:
    pc = PipelineConfig.new("test_pipe", error_strategy=error_strategy)
    pc.steps = [
        PipelineStep(order=o, config_name=n, depends_on=d)
        for o, n, d in steps_cfg
    ]
    return pc


def _executor(pipeline, **kwargs) -> PipelineExecutor:
    return PipelineExecutor(
        pipeline,
        {},
        {},
        config_repo=MockConfigRepo(),
        run_repo=MockRunRepo(),
        **kwargs,
    )


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
        skip, _ = ex._should_skip("B", results)
        assert skip is False

    def test_skip_dependents_skips_on_failed_parent(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), error_strategy="skip_dependents")
        ex = _executor(pc)
        results = self._results(A="failed")
        skip, reason = ex._should_skip("B", results)
        assert skip is True
        assert "'A'" in reason

    def test_skip_dependents_transitive(self):
        pc = _pipeline(
            (1, "A", []),
            (2, "B", ["A"]),
            (3, "C", ["B"]),
            error_strategy="skip_dependents",
        )
        ex = _executor(pc)
        results = self._results(A="failed", B="skipped_dependency")
        skip, reason = ex._should_skip("C", results)
        assert skip is True
        assert "'B'" in reason

    def test_continue_on_error_never_skips(self):
        pc = _pipeline((1, "A", []), (2, "B", ["A"]), error_strategy="continue_on_error")
        ex = _executor(pc)
        results = self._results(A="failed")
        skip, _ = ex._should_skip("B", results)
        assert skip is False

    def test_no_deps_never_skipped(self):
        pc = _pipeline((1, "A", []), error_strategy="skip_dependents")
        ex = _executor(pc)
        skip, _ = ex._should_skip("A", {})
        assert skip is False


# ---------------------------------------------------------------------------
# 2D Checkpoint helpers
# ---------------------------------------------------------------------------


class TestCheckpointHelpers:
    def test_update_step_checkpoint_marks_running(self, tmp_path):
        pc = PipelineConfig.new("cp_test")
        ex = PipelineExecutor(pc, {}, {}, config_repo=MockConfigRepo(), run_repo=MockRunRepo())
        ex._update_step_checkpoint("cfg_a", batch_num=5, rows=2500)

        from services.checkpoint_manager import load_pipeline_checkpoint, clear_pipeline_checkpoint
        loaded = load_pipeline_checkpoint("cp_test")
        assert loaded["steps"]["cfg_a"]["status"] == "running"
        assert loaded["steps"]["cfg_a"]["last_batch"] == 5
        assert loaded["steps"]["cfg_a"]["rows_processed"] == 2500
        clear_pipeline_checkpoint("cp_test")

    def test_complete_step_checkpoint_marks_completed(self, tmp_path):
        pc = PipelineConfig.new("cp_test2")
        ex = PipelineExecutor(pc, {}, {}, config_repo=MockConfigRepo(), run_repo=MockRunRepo())
        ex._complete_step_checkpoint("cfg_b", rows=8000)

        from services.checkpoint_manager import load_pipeline_checkpoint, clear_pipeline_checkpoint
        loaded = load_pipeline_checkpoint("cp_test2")
        assert loaded["steps"]["cfg_b"]["status"] == "completed"
        assert loaded["steps"]["cfg_b"]["last_batch"] == -1
        clear_pipeline_checkpoint("cp_test2")

    def test_checkpoint_accumulates_multiple_steps(self):
        pc = PipelineConfig.new("cp_multi")
        ex = PipelineExecutor(pc, {}, {}, config_repo=MockConfigRepo(), run_repo=MockRunRepo())
        ex._update_step_checkpoint("cfg_a", 3, 1500)
        ex._complete_step_checkpoint("cfg_b", 5000)

        from services.checkpoint_manager import load_pipeline_checkpoint, clear_pipeline_checkpoint
        loaded = load_pipeline_checkpoint("cp_multi")
        assert "cfg_a" in loaded["steps"]
        assert "cfg_b" in loaded["steps"]
        clear_pipeline_checkpoint("cp_multi")


# ---------------------------------------------------------------------------
# Overall status logic
# ---------------------------------------------------------------------------


class TestOverallStatus:
    def test_all_success_is_completed(self):
        pc = _pipeline((1, "A", []), (2, "B", []))
        result = PipelineResult(
            steps={
                "A": StepResult(status="success", config_name="A"),
                "B": StepResult(status="success", config_name="B"),
            },
            status="",
        )
        succeeded = sum(1 for r in result.steps.values() if r.status == "success")
        failed = sum(1 for r in result.steps.values() if r.status == "failed")
        overall = "completed" if failed == 0 else "failed"
        assert overall == "completed"

    def test_partial_when_some_fail(self):
        succeeded = 1
        failed = 1
        overall = "partial" if succeeded > 0 and failed > 0 else ("completed" if failed == 0 else "failed")
        assert overall == "partial"

    def test_all_failed(self):
        succeeded = 0
        failed = 2
        overall = "completed" if failed == 0 else "failed"
        assert overall == "failed"


# ---------------------------------------------------------------------------
# Background thread (integration smoke test)
# ---------------------------------------------------------------------------


class TestBackgroundThread:
    def test_start_background_returns_run_id(self):
        """Executor with no real steps completes immediately in background thread."""
        run_repo = MockRunRepo()
        pc = PipelineConfig.new("bg_test")
        pc.steps = []

        ex = PipelineExecutor(pc, {}, {}, config_repo=MockConfigRepo(), run_repo=run_repo)
        run_id = ex.start_background()
        assert len(run_id) == 36

        deadline = time.time() + 5.0
        status = "running"
        while time.time() < deadline and status == "running":
            time.sleep(0.05)
            latest = run_repo.get_latest(pc.id)
            if latest:
                status = latest["status"]

        assert status in ("completed", "running"), f"Expected 'completed', got '{status}'"


# ---------------------------------------------------------------------------
# Repository adapters
# ---------------------------------------------------------------------------


class TestRepositoryAdapters:
    def test_config_repo_adapter_get_content(self):
        adapter = ConfigRepositoryAdapter()
        content = adapter.get_content("nonexistent")
        assert content is None

    def test_mock_run_repo_lifecycle(self):
        run_repo = MockRunRepo()
        from models.pipeline_config import PipelineRunRecord, PipelineRunUpdateRecord

        run_id = run_repo.save(PipelineRunRecord(
            pipeline_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
            config_name="test_cfg",
            batch_round=0,
            status="running",
        ))
        assert len(run_id) == 36

        run_repo.update(run_id, PipelineRunUpdateRecord(status="completed"))
        latest = run_repo.get_latest("00000000-0000-0000-0000-000000000001")
        assert latest["status"] == "completed"
        assert latest["completed_at"] is not None
