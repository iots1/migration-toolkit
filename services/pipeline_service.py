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
import uuid
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

from repositories.config_repo import get_content as config_get_content
from repositories.pipeline_run_repo import (
    save as run_save,
    update as run_update,
    get_latest as run_get_latest,
)
from repositories.datasource_repo import get_by_id as ds_get_by_id
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
    """Adapter class that implements PipelineRunRepository protocol using repository functions.

    Args:
        job_id: Optional UUID to link the pipeline_run to a jobs record.
                Injected by the jobs router when triggering via API.
    """

    def __init__(self, job_id=None) -> None:
        self._job_id = job_id

    def save(self, record: PipelineRunRecord) -> str:
        """Save a new pipeline run. Injects job_id if set. Returns generated run_id."""
        if self._job_id is not None and record.job_id is None:
            # Inject job_id into the record
            record.job_id = self._job_id
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
    status: str  # "success" | "failed" | "skipped" | "skipped_dependency"
    config_name: str
    rows_processed: int = 0
    duration_seconds: float = 0.0
    error_message: str = ""


@dataclass
class PipelineResult:
    steps: dict[str, StepResult]
    status: str  # "completed" | "partial" | "failed"
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
        batch_event_callback=None,
        completion_callback=None,
    ) -> None:
        """
        Args:
            pipeline:              Fully populated PipelineConfig model.
            source_conn_config:    Dict with keys db_type, host, port, db_name,
                                   user, password, charset (optional).
            target_conn_config:    Same shape as source_conn_config.
            config_repo:           Config repository (DIP injection).
            run_repo:              Pipeline run repository (DIP injection).
            log_callback:          fn(message: str, icon: str) — optional.
            progress_callback:     fn(batch_num, rows_processed, rows_in_batch) — optional.
            run_id:                Pre-existing run_id; set automatically by
                                   start_background() if not provided.
            batch_event_callback:  fn(run_id, step_name, batch_num, rows, error|None)
                                   Called after every batch — used for socket.io + DB update.
            completion_callback:   fn(run_id, status, total_rows)
                                   Called once when the whole pipeline finishes.
        """
        self._pipeline = pipeline
        self._source_conn_config = source_conn_config
        self._target_conn_config = target_conn_config
        self._config_repo = config_repo  # Injected (DIP)
        self._run_repo = run_repo  # Injected (DIP)
        self._log_callback = log_callback
        self._progress_callback = progress_callback
        self._run_id = run_id
        self._batch_event_callback = batch_event_callback
        self._completion_callback = completion_callback

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self) -> PipelineResult:
        """Run all steps in dependency-safe order. Blocking.

        Uses pipeline_edges for dependency graph (topological sort) when
        available, otherwise falls back to PipelineStep.depends_on.
        Resolves source/target connection configs per-step from datasource UUIDs.
        """
        ordered = self._resolve_execution_order()

        checkpoint = load_pipeline_checkpoint(self._pipeline.name)
        steps_state: dict = checkpoint.get("steps", {}) if checkpoint else {}

        results: dict[str, StepResult] = {}
        total_start = _time.time()

        for item in ordered:
            use_edges = isinstance(item, dict)
            config_name = item["config_name"] if use_edges else item.config_name

            if use_edges:
                if steps_state.get(config_name, {}).get("status") == "completed":
                    results[config_name] = StepResult(
                        status="success",
                        config_name=config_name,
                        rows_processed=steps_state[config_name].get(
                            "rows_processed", 0
                        ),
                    )
                    self._log(
                        f"[{config_name}] Skipped — already completed in previous run",
                        "✅",
                    )
                    continue
            else:
                step = item
                if not step.enabled:
                    results[step.config_name] = StepResult(
                        status="skipped", config_name=step.config_name
                    )
                    continue
                if steps_state.get(step.config_name, {}).get("status") == "completed":
                    results[step.config_name] = StepResult(
                        status="success",
                        config_name=step.config_name,
                        rows_processed=steps_state[step.config_name].get(
                            "rows_processed", 0
                        ),
                    )
                    self._log(
                        f"[{step.config_name}] Skipped — already completed in previous run",
                        "✅",
                    )
                    continue

            should_skip, reason = self._should_skip(config_name, results)
            if should_skip:
                results[config_name] = StepResult(
                    status="skipped_dependency",
                    config_name=config_name,
                    error_message=reason,
                )
                self._log(f"[{config_name}] Skipped — {reason}", "⏭️")
                self._flush_run_state(results)
                continue

            config = self._config_repo.get_content(config_name)
            if config is None:
                err = f"Config '{config_name}' not found in database"
                results[config_name] = StepResult(
                    status="failed", config_name=config_name, error_message=err
                )
                self._log(f"[{config_name}] {err}", "❌")
                print(f"[JOB ERROR] [{config_name}] {err}")
                self._flush_run_state(results)
                if self._pipeline.error_strategy == "fail_fast":
                    break
                continue

            step_conn_configs = self._resolve_conn_configs_for_step(config)
            if isinstance(step_conn_configs, str):
                err = step_conn_configs
                results[config_name] = StepResult(
                    status="failed", config_name=config_name, error_message=err
                )
                self._log(f"[{config_name}] {err}", "❌")
                print(f"[JOB ERROR] [{config_name}] {err}")
                self._flush_run_state(results)
                if self._pipeline.error_strategy == "fail_fast":
                    break
                continue

            src_conn_cfg, tgt_conn_cfg = step_conn_configs

            skip_batches = steps_state.get(config_name, {}).get("last_batch", 0)
            self._log(
                f"[{config_name}] Starting"
                + (f" (resuming from batch {skip_batches})" if skip_batches else ""),
                "🚀",
            )
            print(f"[JOB] [{config_name}] Starting migration...")

            try:
                mig_result = run_single_migration(
                    config=config,
                    source_conn_config=src_conn_cfg,
                    target_conn_config=tgt_conn_cfg,
                    batch_size=self._pipeline.batch_size,
                    truncate_target=self._pipeline.truncate_targets,
                    skip_batches=skip_batches,
                    log_callback=self._log_callback,
                    progress_callback=self._progress_callback,
                    checkpoint_callback=self._update_step_checkpoint,
                    batch_insert_callback=self._save_batch_record,
                )
            except Exception as exc:
                err_msg = str(exc)
                results[config_name] = StepResult(
                    status="failed", config_name=config_name, error_message=err_msg
                )
                self._log(f"[{config_name}] Failed — {err_msg}", "❌")
                print(f"[JOB ERROR] [{config_name}] {err_msg}")
                self._flush_run_state(results)
                if self._pipeline.error_strategy == "fail_fast":
                    break
                continue

            results[config_name] = StepResult(
                status=mig_result.status,
                config_name=config_name,
                rows_processed=mig_result.rows_processed,
                duration_seconds=mig_result.duration_seconds,
                error_message=mig_result.error_message,
            )

            if mig_result.status == "success":
                self._complete_step_checkpoint(config_name, mig_result.rows_processed)
                self._log(
                    f"[{config_name}] Completed — "
                    f"{mig_result.rows_processed:,} rows in {mig_result.duration_seconds:.1f}s",
                    "✅",
                )
                print(
                    f"[JOB] [{config_name}] Completed — {mig_result.rows_processed:,} rows"
                )
            else:
                self._log(f"[{config_name}] Failed — {mig_result.error_message}", "❌")
                print(f"[JOB ERROR] [{config_name}] {mig_result.error_message}")
                if self._batch_event_callback and self._run_id:
                    try:
                        self._batch_event_callback(
                            str(self._run_id),
                            config_name,
                            -1,
                            mig_result.rows_processed,
                            mig_result.error_message or "unknown error",
                        )
                    except Exception:
                        pass

            self._flush_run_state(results)

            if (
                mig_result.status == "failed"
                and self._pipeline.error_strategy == "fail_fast"
            ):
                break

        succeeded = sum(1 for r in results.values() if r.status == "success")
        failed = sum(1 for r in results.values() if r.status == "failed")

        if failed == 0:
            overall = "completed"
        elif succeeded > 0:
            overall = "partial"
        else:
            overall = "failed"

        print(f"[JOB] Pipeline finished — status={overall}")
        return PipelineResult(
            steps=results,
            status=overall,
            total_rows=sum(r.rows_processed for r in results.values()),
            total_duration=_time.time() - total_start,
        )

    def start_background(self) -> str:
        """Challenge 3: Launch execute() in a daemon thread.

        Returns run_id immediately so the caller can store it and poll
        progress via batch records (each batch creates a pipeline_runs record).

        Note: With batch-level records, we don't create an initial record here.
        Batch records are created by _save_batch_record() after each batch.
        This run_id is just a UUID for correlation with job_id.
        """
        self._run_id = str(uuid.uuid4())
        thread = threading.Thread(
            target=self._background_run,
            daemon=True,
            name=f"pipeline-{self._pipeline.name}",
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
            self._run_repo.update(
                self._run_id,
                PipelineRunUpdateRecord(
                    status=result.status,
                    steps_json=steps_json,
                ),
            )
            if self._completion_callback:
                self._completion_callback(
                    self._run_id, result.status, result.total_rows
                )
        except Exception as e:
            print(f"[JOB ERROR] Pipeline '{self._pipeline.name}' crashed: {e}")
            import traceback as _tb

            _tb.print_exc()
            try:
                self._run_repo.update(
                    self._run_id,
                    PipelineRunUpdateRecord(
                        status="failed",
                        steps_json="{}",
                        error_message=str(e),
                    ),
                )
            except Exception as repo_err:
                print(f"[JOB ERROR] Failed to update run status: {repo_err}")
            if self._completion_callback:
                self._completion_callback(self._run_id, "failed", 0)
        finally:
            # Completed (or crashed) — remove pipeline checkpoint so a fresh
            # start isn't accidentally resumed from stale state.
            clear_pipeline_checkpoint(self._pipeline.name)

    # ------------------------------------------------------------------
    # Private — Kahn's topological sort
    # ------------------------------------------------------------------

    def _resolve_execution_order(self) -> list:
        """Return steps in a valid execution order using Kahn's BFS algorithm.

        When pipeline edges are available, builds the dependency graph from
        edges (source_config_name → target_config_name means target depends
        on source) and nodes (for ordering tiebreak via order_sort).
        Otherwise falls back to PipelineStep.depends_on.
        """
        if self._pipeline.edges:
            return self._resolve_order_from_edges()
        return self._resolve_order_from_steps()

    def _resolve_order_from_edges(self) -> list[dict]:
        """Topological sort from pipeline_edges + pipeline_nodes."""
        nodes_by_name: dict[str, dict] = {
            n["config_name"]: n for n in self._pipeline.nodes
        }

        in_degree: dict[str, int] = {name: 0 for name in nodes_by_name}
        adjacency: dict[str, list[str]] = {name: [] for name in nodes_by_name}

        for edge in self._pipeline.edges:
            src = edge.get("source_config_name", "")
            tgt = edge.get("target_config_name", "")
            if src in nodes_by_name and tgt in nodes_by_name:
                adjacency[src].append(tgt)
                in_degree[tgt] += 1

        queue: deque[str] = deque(
            sorted(
                (name for name, deg in in_degree.items() if deg == 0),
                key=lambda n: nodes_by_name[n].get("order_sort", 0),
            )
        )

        ordered: list[dict] = []
        while queue:
            name = queue.popleft()
            ordered.append(nodes_by_name[name])
            for neighbor in sorted(
                adjacency[name], key=lambda n: nodes_by_name[n].get("order_sort", 0)
            ):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(nodes_by_name):
            involved = [n for n, d in in_degree.items() if d > 0]
            raise ValueError(
                f"Circular dependency detected in pipeline '{self._pipeline.name}'. "
                f"Nodes involved: {involved}"
            )

        return ordered

    def _resolve_order_from_steps(self) -> list[PipelineStep]:
        """Legacy topological sort from PipelineStep.depends_on."""
        steps_by_name: dict[str, PipelineStep] = {
            s.config_name: s for s in self._pipeline.steps
        }

        for step in self._pipeline.steps:
            for dep in step.depends_on:
                if dep not in steps_by_name:
                    raise ValueError(
                        f"Step '{step.config_name}' depends on '{dep}' "
                        f"which is not part of this pipeline."
                    )

        in_degree: dict[str, int] = {name: 0 for name in steps_by_name}
        adjacency: dict[str, list[str]] = {name: [] for name in steps_by_name}

        for step in self._pipeline.steps:
            for dep in step.depends_on:
                adjacency[dep].append(step.config_name)
                in_degree[step.config_name] += 1

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
            for neighbor in sorted(
                adjacency[name], key=lambda n: steps_by_name[n].order
            ):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

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
        self, config_name: str, results: dict[str, StepResult]
    ) -> tuple[bool, str]:
        """Return (should_skip, reason) for the given config_name.

        When edges are available, finds upstream dependencies from edges.
        Otherwise uses PipelineStep.depends_on.

        Strategies:
            fail_fast         — never reaches here (loop breaks on first failure).
            continue_on_error — never skip; always attempt regardless of failures.
            skip_dependents   — skip if any direct dependency is in a failed or
                                skipped_dependency state.
        """
        if self._pipeline.error_strategy == "continue_on_error":
            return False, ""

        failed_or_skipped = {
            name
            for name, r in results.items()
            if r.status in ("failed", "skipped_dependency")
        }

        if self._pipeline.edges:
            for edge in self._pipeline.edges:
                if edge.get("target_config_name") == config_name:
                    dep = edge.get("source_config_name", "")
                    if dep in failed_or_skipped:
                        return True, f"Dependency '{dep}' failed or was skipped"
        else:
            step_map = {s.config_name: s for s in self._pipeline.steps}
            step = step_map.get(config_name)
            if step:
                for dep in step.depends_on:
                    if dep in failed_or_skipped:
                        return True, f"Dependency '{dep}' failed or was skipped"

        return False, ""

    def _resolve_conn_configs_for_step(self, config: dict) -> tuple[dict, dict] | str:
        """Resolve source/target connection configs from config's datasource UUIDs.

        Each config can point to different datasources, so we resolve per-step.
        Returns (src_conn_config, tgt_conn_config) or an error message string.
        """
        src_ds_id = config.get("_datasource_source_id")
        tgt_ds_id = config.get("_datasource_target_id")
        if not src_ds_id:
            return f"Config '{config.get('config_name', '')}' missing source datasource"
        if not tgt_ds_id:
            return f"Config '{config.get('config_name', '')}' missing target datasource"

        src_ds = ds_get_by_id(src_ds_id)
        if not src_ds:
            return f"Source datasource (id={src_ds_id}) not found"
        tgt_ds = ds_get_by_id(tgt_ds_id)
        if not tgt_ds:
            return f"Target datasource (id={tgt_ds_id}) not found"

        charset = config.get("source", {}).get("charset")
        if src_ds["db_type"] == "PostgreSQL" and charset == "tis620":
            charset = "WIN874"

        src_conn = {
            "db_type": src_ds["db_type"],
            "host": src_ds["host"],
            "port": src_ds["port"],
            "db_name": src_ds["dbname"],
            "user": src_ds["username"],
            "password": src_ds["password"],
            "charset": charset,
        }
        tgt_conn = {
            "db_type": tgt_ds["db_type"],
            "host": tgt_ds["host"],
            "port": tgt_ds["port"],
            "db_name": tgt_ds["dbname"],
            "user": tgt_ds["username"],
            "password": tgt_ds["password"],
        }
        return src_conn, tgt_conn

        failed_or_skipped = {
            name
            for name, r in results.items()
            if r.status in ("failed", "skipped_dependency")
        }

        if self._pipeline.edges:
            for edge in self._pipeline.edges:
                if edge.get("target_config_name") == config_name:
                    dep = edge.get("source_config_name", "")
                    if dep in failed_or_skipped:
                        return True, f"Dependency '{dep}' failed or was skipped"
        else:
            step_map = {s.config_name: s for s in self._pipeline.steps}
            step = step_map.get(config_name)
            if step:
                for dep in step.depends_on:
                    if dep in failed_or_skipped:
                        return True, f"Dependency '{dep}' failed or was skipped"

        return False, ""

    # ------------------------------------------------------------------
    # Private — 2D checkpoint helpers
    # ------------------------------------------------------------------

    def _update_step_checkpoint(
        self, config_name: str, batch_num: int, rows: int
    ) -> None:
        """Per-batch callback from run_single_migration.

        Marks the step as 'running' with the latest batch offset so the
        pipeline can resume mid-step after an interruption.
        Also fires batch_event_callback for socket.io + DB update (if set).
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

        if self._batch_event_callback and self._run_id:
            try:
                self._batch_event_callback(
                    str(self._run_id), config_name, batch_num, rows, None
                )
            except Exception:
                pass  # Never let callback failure break migration

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

    def _save_batch_record(
        self,
        config_name: str,
        batch_round: int,
        rows_in_batch: int,
        rows_cumulative: int,
        batch_size: int,
        total_records_in_config: int,
        status: str,
        error_message: str | None = None,
        transformation_warnings: str | None = None,
    ) -> None:
        """
        Save a batch-level record to pipeline_runs table.

        Called after each batch completes (success or failure).
        This is the new callback from migration_executor.py for batch-level tracking.

        Args:
            config_name: Name of the config being migrated
            batch_round: Batch number (0-indexed)
            rows_in_batch: Rows inserted in THIS batch (0 if failed)
            rows_cumulative: Total rows from batch 0 to this batch
            batch_size: Configured batch size
            total_records_in_config: Total records in this config
            status: 'success' or 'failed'
            error_message: Error text if status='failed'
            transformation_warnings: JSON string of warnings
        """
        try:
            from models.pipeline_config import PipelineRunRecord

            # Get job_id if available (from run_repo._job_id if using adapter)
            job_id = None
            if hasattr(self._run_repo, "_job_id"):
                job_id = self._run_repo._job_id

            # Get pipeline_id
            pipeline_id = uuid.UUID(self._pipeline.id)

            # Create batch record
            record = PipelineRunRecord(
                pipeline_id=pipeline_id,
                config_name=config_name,
                batch_round=batch_round,
                rows_in_batch=rows_in_batch,
                rows_cumulative=rows_cumulative,
                batch_size=batch_size,
                total_records_in_config=total_records_in_config,
                status=status,
                job_id=job_id,
                error_message=error_message,
                transformation_warnings=transformation_warnings,
            )

            # Save to database
            self._run_repo.save(record)

            # Also fire batch_event_callback if set (for socket.io, etc.)
            if self._batch_event_callback and self._run_id:
                try:
                    self._batch_event_callback(
                        str(self._run_id),
                        config_name,
                        batch_round,
                        rows_in_batch,
                        error_message if status == "failed" else None,
                    )
                except Exception:
                    pass  # Never let callback failure break migration

        except Exception as e:
            # Log but don't fail the migration
            self._log(f"Warning: Failed to save batch record for {config_name}: {e}", "⚠️")

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
        self._run_repo.update(
            self._run_id,
            PipelineRunUpdateRecord(
                status="running",
                steps_json=json.dumps(self._steps_to_json(results)),
            ),
        )

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
