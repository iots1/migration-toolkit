"""
pipeline_service.py — Pipeline execution service.

Solves the three core challenges:

    Challenge 1 — Connection Timeout
        JIT engines: each step calls run_single_migration() with conn *config
        dicts* (not engine objects). run_single_migration creates fresh engines
        with pool_pre_ping=True / pool_recycle=3600 and disposes them in
        finally, so no engine is held open across the gap between steps.

    Challenge 2 — 2D Checkpoint
        save_pipeline_checkpoint() writes a per-step status map to disk after
        every batch and on step completion. execute() reads this map on startup
        and skips steps already marked "completed", resuming partially-run steps
        from their last_batch offset.

    Challenge 3 — UI Timeout
        start_background() launches a daemon thread and returns a run_id.
        The background thread writes to pipeline_runs via update_pipeline_run()
        after each step. The UI polls get_latest_pipeline_run(run_id) with a
        "Refresh Status" button — no autorefresh library required.

DIP Compliance (Phase 8)
    Dependencies are now injected via constructor following Dependency Inversion Principle.
    Uses protocol interfaces for config_repo and run_repo instead of concrete database module.

Thread-safety note
    update_pipeline_run() and save_pipeline_run() each open their own PostgreSQL
    connection internally via repositories with thread-safe connection managers.
"""
from __future__ import annotations
import json
import threading
import time as _time
from collections import deque
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from models.pipeline_config import PipelineConfig, PipelineStep
from services.migration_executor import run_single_migration
from services.checkpoint_manager import (
    load_pipeline_checkpoint,
    save_pipeline_checkpoint,
    clear_pipeline_checkpoint,
)


# ---------------------------------------------------------------------------
# Direct Repository Imports (Phase 8 - DI)
# ---------------------------------------------------------------------------

# Import repository functions directly (concrete implementations)
from repositories.config_repo import get_content as config_get_content
from repositories.pipeline_run_repo import save as run_save, update as run_update, get_latest as run_get_latest
from models.pipeline_config import PipelineRunRecord, PipelineRunUpdateRecord


# ---------------------------------------------------------------------------
# Repository Adapter Classes (Phase 8 - DI)
# ---------------------------------------------------------------------------

class ConfigRepositoryAdapter:
    """Adapter class that implements ConfigRepository protocol using repository functions."""

    def get_content(self, config_name: str) -> dict | None:
        """Get config content by name."""
        return config_get_content(config_name)


class PipelineRunRepositoryAdapter:
    """Adapter class that implements PipelineRunRepository protocol using repository functions."""

    def save(self, record: PipelineRunRecord) -> str:
        """Save a new pipeline run. Returns generated run_id."""
        return run_save(record)

    def update(self, run_id, patch: PipelineRunUpdateRecord) -> None:
        """Update pipeline run status."""
        run_update(run_id, patch)

    def get_latest(self, pipeline_id: str) -> dict | None:
        """Get latest pipeline run for a pipeline."""
        return run_get_latest(pipeline_id)




# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class StepResult:
    status: str          # "success" | "failed" | "skipped" | "skipped_dependency"
    config_name: str
    rows_processed: int = 0
    duration_seconds: float = 0.0
    error_message: str = ""


@dataclass
class PipelineResult:
    steps: dict[str, StepResult]
    status: str          # "completed" | "partial" | "failed"
    total_rows: int = 0
    total_duration: float = 0.0


# ---------------------------------------------------------------------------
# PipelineExecutor
# ---------------------------------------------------------------------------

class PipelineExecutor:
    """Orchestrates a multi-step migration pipeline.

    Usage (foreground):
        executor = PipelineExecutor(pipeline, src_cfg, tgt_cfg)
        result = executor.execute()

    Usage (background thread + UI polling):
        executor = PipelineExecutor(pipeline, src_cfg, tgt_cfg)
        run_id = executor.start_background()
        # UI calls db.get_latest_pipeline_run(run_id) to poll progress
    """

    def __init__(
        self,
        pipeline: PipelineConfig,
        source_conn_config: dict,
        target_conn_config: dict,
        config_repo: ConfigRepository,
        run_repo: PipelineRunRepository,
        log_callback=None,
        progress_callback=None,
        run_id: str | None = None,
    ) -> None:
        """
        Args:
            pipeline:            Fully populated PipelineConfig model.
            source_conn_config:  Dict with keys db_type, host, port, db_name,
                                 user, password, charset (optional).
            target_conn_config:  Same shape as source_conn_config.
            config_repo:         Config repository (DIP injection).
            run_repo:            Pipeline run repository (DIP injection).
            log_callback:        fn(message: str, icon: str) — optional.
            progress_callback:   fn(batch_num, rows_processed, rows_in_batch) — optional.
            run_id:              Pre-existing run_id; set automatically by
                                 start_background() if not provided.
        """
        self._pipeline = pipeline
        self._source_conn_config = source_conn_config
        self._target_conn_config = target_conn_config
        self._config_repo = config_repo  # Injected (DIP)
        self._run_repo = run_repo  # Injected (DIP)
        self._log_callback = log_callback
        self._progress_callback = progress_callback
        self._run_id = run_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self) -> PipelineResult:
        """Run all enabled steps in dependency-safe order. Blocking."""
        ordered = self._resolve_execution_order()

        checkpoint = load_pipeline_checkpoint(self._pipeline.name)
        steps_state: dict = checkpoint.get("steps", {}) if checkpoint else {}

        results: dict[str, StepResult] = {}
        total_start = _time.time()

        for step in ordered:
            # --- Disabled step ---
            if not step.enabled:
                results[step.config_name] = StepResult(
                    status="skipped", config_name=step.config_name
                )
                continue

            # --- Already completed in a previous run (2D checkpoint resume) ---
            if steps_state.get(step.config_name, {}).get("status") == "completed":
                results[step.config_name] = StepResult(
                    status="success",
                    config_name=step.config_name,
                    rows_processed=steps_state[step.config_name].get("rows_processed", 0),
                )
                self._log(f"[{step.config_name}] Skipped — already completed in previous run", "✅")
                continue

            # --- Dependency gate ---
            should_skip, reason = self._should_skip(step, results)
            if should_skip:
                results[step.config_name] = StepResult(
                    status="skipped_dependency",
                    config_name=step.config_name,
                    error_message=reason,
                )
                self._log(f"[{step.config_name}] Skipped — {reason}", "⏭️")
                self._flush_run_state(results)
                continue

            # --- Load migration config from DB ---
            config = self._config_repo.get_content(step.config_name)
            if config is None:
                err = f"Config '{step.config_name}' not found in database"
                results[step.config_name] = StepResult(
                    status="failed", config_name=step.config_name, error_message=err
                )
                self._log(f"[{step.config_name}] {err}", "❌")
                self._flush_run_state(results)
                if self._pipeline.error_strategy == "fail_fast":
                    break
                continue

            # --- Resume offset from 2D checkpoint ---
            skip_batches = steps_state.get(step.config_name, {}).get("last_batch", 0)
            self._log(
                f"[{step.config_name}] Starting"
                + (f" (resuming from batch {skip_batches})" if skip_batches else ""),
                "🚀",
            )

            # --- JIT migration (Challenge 1) ---
            # run_single_migration creates fresh engines and disposes them in
            # finally, so no engine object is held between steps.
            mig_result = run_single_migration(
                config=config,
                source_conn_config=self._source_conn_config,
                target_conn_config=self._target_conn_config,
                batch_size=self._pipeline.batch_size,
                truncate_target=self._pipeline.truncate_targets,
                skip_batches=skip_batches,
                log_callback=self._log_callback,
                progress_callback=self._progress_callback,
                checkpoint_callback=self._update_step_checkpoint,
            )

            results[step.config_name] = StepResult(
                status=mig_result.status,
                config_name=step.config_name,
                rows_processed=mig_result.rows_processed,
                duration_seconds=mig_result.duration_seconds,
                error_message=mig_result.error_message,
            )

            if mig_result.status == "success":
                self._complete_step_checkpoint(step.config_name, mig_result.rows_processed)
                self._log(
                    f"[{step.config_name}] Completed — "
                    f"{mig_result.rows_processed:,} rows in {mig_result.duration_seconds:.1f}s",
                    "✅",
                )
            else:
                self._log(
                    f"[{step.config_name}] Failed — {mig_result.error_message}", "❌"
                )

            # Persist step snapshot for UI polling (Challenge 3)
            self._flush_run_state(results)

            # --- Error strategy ---
            if mig_result.status == "failed" and self._pipeline.error_strategy == "fail_fast":
                break

        # --- Overall status ---
        succeeded = sum(1 for r in results.values() if r.status == "success")
        failed = sum(1 for r in results.values() if r.status == "failed")

        if failed == 0:
            overall = "completed"
        elif succeeded > 0:
            overall = "partial"
        else:
            overall = "failed"

        return PipelineResult(
            steps=results,
            status=overall,
            total_rows=sum(r.rows_processed for r in results.values()),
            total_duration=_time.time() - total_start,
        )

    def start_background(self) -> str:
        """Challenge 3: Launch execute() in a daemon thread.

        Returns run_id immediately so the caller can store it and poll
        self._run_repo.get_latest(run_id) for progress.
        """
        self._run_id = self._run_repo.save(PipelineRunRecord(
            pipeline_id=self._pipeline.id, status="running", steps_json="{}",
        ))
        thread = threading.Thread(
            target=self._background_run, daemon=True, name=f"pipeline-{self._pipeline.name}"
        )
        thread.start()
        return self._run_id

    # ------------------------------------------------------------------
    # Private — background thread target
    # ------------------------------------------------------------------

    def _background_run(self) -> None:
        """Thread entry point — wraps execute() with DB bookkeeping.

        PostgreSQL safety: all repo calls here use thread-safe connection managers
        internally, so no connection object crosses a thread boundary.
        """
        try:
            result = self.execute()
            steps_json = json.dumps(self._steps_to_json(result.steps))
            self._run_repo.update(self._run_id, PipelineRunUpdateRecord(
                status=result.status, steps_json=steps_json,
            ))
        except Exception as e:
            self._run_repo.update(self._run_id, PipelineRunUpdateRecord(
                status="failed", steps_json="{}", error_message=str(e),
            ))
        finally:
            # Completed (or crashed) — remove pipeline checkpoint so a fresh
            # start isn't accidentally resumed from stale state.
            clear_pipeline_checkpoint(self._pipeline.name)

    # ------------------------------------------------------------------
    # Private — Kahn's topological sort
    # ------------------------------------------------------------------

    def _resolve_execution_order(self) -> list[PipelineStep]:
        """Return steps in a valid execution order using Kahn's BFS algorithm.

        Properties:
        - Steps at the same dependency level are ordered by PipelineStep.order.
        - Raises ValueError if a depends_on target is not in the pipeline, or
          if a circular dependency is detected.
        """
        steps_by_name: dict[str, PipelineStep] = {
            s.config_name: s for s in self._pipeline.steps
        }

        # Validate: all depends_on references must exist within this pipeline
        for step in self._pipeline.steps:
            for dep in step.depends_on:
                if dep not in steps_by_name:
                    raise ValueError(
                        f"Step '{step.config_name}' depends on '{dep}' "
                        f"which is not part of this pipeline."
                    )

        # Build in-degree map and forward-adjacency list
        in_degree: dict[str, int] = {name: 0 for name in steps_by_name}
        adjacency: dict[str, list[str]] = {name: [] for name in steps_by_name}

        for step in self._pipeline.steps:
            for dep in step.depends_on:
                adjacency[dep].append(step.config_name)
                in_degree[step.config_name] += 1

        # Seed the queue with zero-in-degree nodes, sorted by .order
        queue: deque[str] = deque(
            sorted(
                (name for name, deg in in_degree.items() if deg == 0),
                key=lambda n: steps_by_name[n].order,
            )
        )

        ordered: list[PipelineStep] = []
        while queue:
            name = queue.popleft()
            ordered.append(steps_by_name[name])
            # Decrement in-degree of dependents; enqueue those that become ready
            for neighbor in sorted(adjacency[name], key=lambda n: steps_by_name[n].order):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If not all nodes were processed, there is a cycle
        if len(ordered) != len(steps_by_name):
            involved = [n for n, d in in_degree.items() if d > 0]
            raise ValueError(
                f"Circular dependency detected in pipeline '{self._pipeline.name}'. "
                f"Steps involved: {involved}"
            )

        return ordered

    # ------------------------------------------------------------------
    # Private — dependency skip logic
    # ------------------------------------------------------------------

    def _should_skip(
        self, step: PipelineStep, results: dict[str, StepResult]
    ) -> tuple[bool, str]:
        """Return (should_skip, reason) for the given step.

        Strategies:
            fail_fast         — never reaches here (loop breaks on first failure).
            continue_on_error — never skip; always attempt regardless of failures.
            skip_dependents   — skip if any direct dependency is in a failed or
                                skipped_dependency state.

        Transitivity is implicit: because we process in topological order, if
        step B (depends on A) is marked skipped_dependency, and C depends on B,
        then when we evaluate C we find B already in results with status
        'skipped_dependency' — which is in our target set. No DFS needed.
        """
        if self._pipeline.error_strategy == "continue_on_error":
            return False, ""

        failed_or_skipped = {
            name
            for name, r in results.items()
            if r.status in ("failed", "skipped_dependency")
        }

        for dep in step.depends_on:
            if dep in failed_or_skipped:
                return True, f"Dependency '{dep}' failed or was skipped"

        return False, ""

    # ------------------------------------------------------------------
    # Private — 2D checkpoint helpers
    # ------------------------------------------------------------------

    def _update_step_checkpoint(self, config_name: str, batch_num: int, rows: int) -> None:
        """Per-batch callback from run_single_migration.

        Marks the step as 'running' with the latest batch offset so the
        pipeline can resume mid-step after an interruption.
        """
        checkpoint = load_pipeline_checkpoint(self._pipeline.name) or {
            "pipeline_name": self._pipeline.name,
            "steps": {},
        }
        checkpoint["steps"][config_name] = {
            "status": "running",
            "last_batch": batch_num,
            "rows_processed": rows,
        }
        save_pipeline_checkpoint(self._pipeline.name, checkpoint["steps"])

    def _complete_step_checkpoint(self, config_name: str, rows: int) -> None:
        """Mark a step as 'completed' after run_single_migration succeeds.

        On the next execute() call (resume), this step will be skipped
        entirely rather than re-migrated.
        """
        checkpoint = load_pipeline_checkpoint(self._pipeline.name) or {
            "pipeline_name": self._pipeline.name,
            "steps": {},
        }
        checkpoint["steps"][config_name] = {
            "status": "completed",
            "last_batch": -1,
            "rows_processed": rows,
        }
        save_pipeline_checkpoint(self._pipeline.name, checkpoint["steps"])

    # ------------------------------------------------------------------
    # Private — misc helpers
    # ------------------------------------------------------------------

    def _log(self, msg: str, icon: str = "ℹ️") -> None:
        if self._log_callback:
            self._log_callback(msg, icon)

    def _flush_run_state(self, results: dict[str, StepResult]) -> None:
        """Write current results snapshot to pipeline_runs for UI polling.

        No-op when run_id is not set (foreground / unit-test execution).
        """
        if not self._run_id:
            return
        self._run_repo.update(self._run_id, PipelineRunUpdateRecord(
            status="running",
            steps_json=json.dumps(self._steps_to_json(results)),
        ))

    @staticmethod
    def _steps_to_json(results: dict[str, StepResult]) -> dict:
        return {
            name: {
                "status": r.status,
                "rows_processed": r.rows_processed,
                "duration_seconds": r.duration_seconds,
                "error_message": r.error_message,
            }
            for name, r in results.items()
        }
