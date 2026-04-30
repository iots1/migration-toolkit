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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Protocol, runtime_checkable

from models.db_type import DbType
from models.pipeline_config import PipelineConfig, PipelineStep
from services.migration_executor import (
    run_single_migration,
    MigrationInterrupted,
    _write_heartbeat,
    _read_heartbeat,
    _clean_heartbeat,
)
from services.migration_logger import MigrationLogger
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
        config_repo: PipelineRepository,
        run_repo: PipelineRunRepository,
        log_callback=None,
        progress_callback=None,
        run_id: str | None = None,
        batch_event_callback=None,
        completion_callback=None,
        run_event_callback=None,
        shutdown_event: threading.Event | None = None,
        job_id: str | None = None,
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
            run_event_callback:    fn(event_name: str, data: dict)
                                   Called for pipeline_run lifecycle events (batch/completed/failed).
                                   Injected by API layer to emit Socket.IO events (DIP).
            shutdown_event:        threading.Event to signal graceful shutdown between batches.
            job_id:                UUID string of the associated job record.
        """
        self._pipeline = pipeline
        self._source_conn_config = source_conn_config
        self._target_conn_config = target_conn_config
        self._config_repo = config_repo
        self._run_repo = run_repo
        self._log_callback = log_callback
        self._progress_callback = progress_callback
        self._run_id = run_id
        self._batch_event_callback = batch_event_callback
        self._completion_callback = completion_callback
        self._run_event_callback = run_event_callback
        self._shutdown_event = shutdown_event
        self._job_id = job_id
        self._migration_logger: MigrationLogger | None = None
        self._batch_buffer: list[PipelineRunRecord] = []
        self._batch_buffer_lock = threading.Lock()
        self._batch_buffer_limit = 5

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute(self) -> PipelineResult:
        """Run all steps in dependency-safe order. Blocking.

        When ``parallel_enabled`` is True and the pipeline has independent
        steps at the same dependency level, those steps run concurrently
        using a thread pool.  Otherwise steps run sequentially.
        """
        if self._pipeline.parallel_enabled:
            return self._execute_parallel()

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

            sr = self._execute_step(config_name, steps_state)
            results[config_name] = sr
            self._flush_run_state(results)

            if sr.status == "failed" and self._pipeline.error_strategy == "fail_fast":
                break

        return self._finalize_result(results, total_start)

    def _execute_parallel(self) -> PipelineResult:
        """Run steps grouped by dependency level with parallel execution per level."""
        levels = self._resolve_execution_levels()

        checkpoint = load_pipeline_checkpoint(self._pipeline.name)
        steps_state: dict = checkpoint.get("steps", {}) if checkpoint else {}

        results: dict[str, StepResult] = {}
        total_start = _time.time()

        for level_idx, level_steps in enumerate(levels):
            eligible: list[tuple[str, dict]] = []
            for step_info in level_steps:
                config_name = step_info["config_name"] if isinstance(step_info, dict) else step_info.config_name
                state = steps_state.get(config_name, {})
                if state.get("status") == "completed":
                    results[config_name] = StepResult(
                        status="success",
                        config_name=config_name,
                        rows_processed=state.get("rows_processed", 0),
                    )
                    self._log(f"[{config_name}] Skipped — already completed", "✅")
                    continue
                should_skip, reason = self._should_skip(config_name, results)
                if should_skip:
                    results[config_name] = StepResult(
                        status="skipped_dependency",
                        config_name=config_name,
                        error_message=reason,
                    )
                    self._log(f"[{config_name}] Skipped — {reason}", "⏭️")
                    continue
                eligible.append((config_name, step_info))

            if not eligible:
                continue

            if len(eligible) == 1:
                config_name, _ = eligible[0]
                sr = self._execute_step(config_name, steps_state)
                results[config_name] = sr
            else:
                self._log(
                    f"Level {level_idx}: running {len(eligible)} steps in parallel",
                    "🚀",
                )
                level_results = self._execute_level_parallel(eligible, steps_state)
                results.update(level_results)

            self._flush_run_state(results)

            if self._pipeline.error_strategy == "fail_fast":
                if any(r.status == "failed" for r in results.values()):
                    break

        return self._finalize_result(results, total_start)

    def _execute_level_parallel(
        self, steps: list[tuple[str, dict]], steps_state: dict
    ) -> dict[str, StepResult]:
        """Run multiple independent steps concurrently via thread pool."""
        level_results: dict[str, StepResult] = {}
        max_workers = min(self._pipeline.max_parallel_steps, len(steps))

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._execute_step, cn, steps_state): cn
                for cn, _ in steps
            }
            for future in as_completed(futures):
                config_name = futures[future]
                try:
                    level_results[config_name] = future.result()
                except Exception as e:
                    level_results[config_name] = StepResult(
                        status="failed",
                        config_name=config_name,
                        error_message=str(e),
                    )
                    self._persist_step_error(config_name, str(e))

        return level_results

    def _execute_step(
        self, config_name: str, steps_state: dict
    ) -> StepResult:
        """Execute a single migration step. Returns StepResult."""
        config = self._config_repo.get_content(config_name)
        if config is None:
            err = f"Config '{config_name}' not found in database"
            self._log(f"[{config_name}] {err}", "❌")
            print(f"[JOB ERROR] [{config_name}] {err}")
            self._persist_step_error(config_name, err)
            return StepResult(status="failed", config_name=config_name, error_message=err)

        step_conn_configs = self._resolve_conn_configs_for_step(config)
        if isinstance(step_conn_configs, str):
            err = step_conn_configs
            self._log(f"[{config_name}] {err}", "❌")
            print(f"[JOB ERROR] [{config_name}] {err}")
            self._persist_step_error(config_name, err)
            return StepResult(status="failed", config_name=config_name, error_message=err)

        src_conn_cfg, tgt_conn_cfg = step_conn_configs

        step_state = steps_state.get(config_name, {})
        is_resuming = step_state.get("status") == "running"
        skip_batches = step_state.get("last_batch", 0)
        should_truncate = self._pipeline.truncate_targets and not is_resuming

        self._log(
            f"[{config_name}] {'Resuming' if is_resuming else 'Starting'}"
            + (f" from batch {skip_batches}" if skip_batches else ""),
            "🚀",
        )
        print(f"[JOB] [{config_name}] {'Resuming' if is_resuming else 'Starting'} migration...")

        try:
            mig_result = run_single_migration(
                config=config,
                source_conn_config=src_conn_cfg,
                target_conn_config=tgt_conn_cfg,
                batch_size=self._pipeline.batch_size,
                truncate_target=should_truncate,
                skip_batches=skip_batches,
                log_callback=self._log_callback,
                progress_callback=self._progress_callback,
                checkpoint_callback=self._update_step_checkpoint,
                batch_insert_callback=self._save_batch_record,
                shutdown_event=self._shutdown_event,
                job_id=self._job_id,
                migration_logger=self._migration_logger,
            )
        except MigrationInterrupted:
            self._log(f"[{config_name}] Interrupted — shutdown requested", "🛑")
            print(f"[JOB] [{config_name}] Interrupted — shutdown requested")
            self._persist_step_error(config_name, "Migration interrupted by shutdown signal", "interrupted")
            return StepResult(
                status="interrupted",
                config_name=config_name,
                error_message="Migration interrupted by shutdown signal",
            )
        except Exception as exc:
            err_msg = str(exc)
            self._log(f"[{config_name}] Failed — {err_msg}", "❌")
            print(f"[JOB ERROR] [{config_name}] {err_msg}")
            self._persist_step_error(config_name, err_msg)
            return StepResult(
                status="failed", config_name=config_name, error_message=err_msg
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
            self._persist_step_error(config_name, mig_result.error_message or "unknown error", mig_result.status)
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

        return StepResult(
            status=mig_result.status,
            config_name=config_name,
            rows_processed=mig_result.rows_processed,
            duration_seconds=mig_result.duration_seconds,
            error_message=mig_result.error_message,
        )

    def _finalize_result(
        self, results: dict[str, StepResult], total_start: float
    ) -> PipelineResult:
        """Compute overall status and build PipelineResult."""
        with self._batch_buffer_lock:
            self._flush_batch_buffer()

        succeeded = sum(1 for r in results.values() if r.status == "success")
        failed = sum(1 for r in results.values() if r.status == "failed")
        interrupted = sum(1 for r in results.values() if r.status == "interrupted")

        if failed == 0 and interrupted == 0:
            overall = "completed"
        elif succeeded > 0 or interrupted > 0:
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
        if self._job_id:
            self._migration_logger = MigrationLogger(self._job_id)
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
        """Thread entry point — wraps execute() with DB bookkeeping."""
        result = None
        try:
            _heartbeat_stop = threading.Event()

            def _heartbeat_flusher():
                if not self._job_id:
                    return
                while not _heartbeat_stop.wait(30):
                    try:
                        hb = _read_heartbeat(self._job_id)
                        if hb and _time.time() - hb["timestamp"] < 60:
                            from repositories import job_repo as _job_repo
                            from models.job import JobUpdateRecord as _JobUpdate
                            from datetime import datetime, timezone
                            _job_repo.update(
                                uuid.UUID(self._job_id),
                                _JobUpdate(
                                    status="running",
                                    last_heartbeat=datetime.now(timezone.utc).isoformat(),
                                ),
                            )
                    except Exception:
                        pass

            hb_thread = threading.Thread(
                target=_heartbeat_flusher, daemon=True, name="heartbeat-flusher"
            )
            hb_thread.start()

            result = self.execute()
            _heartbeat_stop.set()
            self._run_repo.update(
                self._run_id,
                PipelineRunUpdateRecord(status=result.status),
            )
            if self._migration_logger and self._job_id:
                try:
                    from repositories import job_repo as _job_repo
                    from models.job import JobUpdateRecord as _JobUpdate
                    summary = self._migration_logger.build_summary(
                        result.total_rows, result.status
                    )
                    _job_repo.update(
                        uuid.UUID(self._job_id),
                        _JobUpdate(status=result.status, summary=summary),
                    )
                except Exception:
                    pass
            if self._completion_callback:
                self._completion_callback(
                    self._run_id, result.status, result.total_rows
                )
            if self._run_event_callback:
                try:
                    self._run_event_callback(
                        "pipeline_run:completed",
                        {
                            "run_id": self._run_id,
                            "job_id": self._job_id,
                            "pipeline_id": self._pipeline.id,
                            "status": result.status,
                            "total_rows": result.total_rows,
                            "total_duration": result.total_duration,
                            "steps": self._steps_to_json(result.steps),
                        },
                    )
                except Exception:
                    pass
        except Exception as e:
            print(f"[JOB ERROR] Pipeline '{self._pipeline.name}' crashed: {e}")
            import traceback as _tb

            _tb.print_exc()
            try:
                self._run_repo.update(
                    self._run_id,
                    PipelineRunUpdateRecord(status="failed", error_message=str(e)),
                )
            except Exception as repo_err:
                print(f"[JOB ERROR] Failed to update run status: {repo_err}")
            if self._completion_callback:
                self._completion_callback(self._run_id, "failed", 0)
            if self._run_event_callback:
                try:
                    self._run_event_callback(
                        "pipeline_run:failed",
                        {
                            "run_id": self._run_id,
                            "job_id": self._job_id,
                            "pipeline_id": self._pipeline.id,
                            "status": "failed",
                            "error_message": str(e),
                        },
                    )
                except Exception:
                    pass
        finally:
            final_status = result.status if result else "failed"
            final_rows = result.total_rows if result else 0

            if final_status == "completed":
                clear_pipeline_checkpoint(self._pipeline.name)
            else:
                cp = load_pipeline_checkpoint(self._pipeline.name)
                if cp:
                    print(
                        f"[JOB] Pipeline '{self._pipeline.name}' "
                        f"did not complete — checkpoint preserved for resume"
                        f" ({len(cp.get('steps', {}))} step(s) tracked)"
                    )
                else:
                    print(
                        f"[JOB] Pipeline '{self._pipeline.name}' "
                        f"did not complete (status={final_status})"
                    )

            if self._job_id:
                _clean_heartbeat(self._job_id)
            if self._migration_logger:
                try:
                    summary = self._migration_logger.build_summary(
                        final_rows,
                        final_status,
                    )
                    self._migration_logger.log(
                        event="pipeline_finished",
                        status=final_status,
                        summary=summary,
                    )
                except Exception:
                    pass
                self._migration_logger.close()

    # ------------------------------------------------------------------
    # Private — Kahn's topological sort
    # ------------------------------------------------------------------

    def _resolve_execution_levels(self) -> list[list]:
        """Return steps grouped by dependency level for parallel execution.

        Each inner list contains steps that can run concurrently.
        Falls back to single-level if edges/steps are not available.
        """
        if self._pipeline.edges and self._pipeline.nodes:
            return self._resolve_levels_from_edges()
        if self._pipeline.nodes:
            return [sorted(self._pipeline.nodes, key=lambda n: n.get("order_sort", 0))]
        if self._pipeline.steps:
            levels = self._resolve_levels_from_steps()
            return [[s] for s in levels]
        return []

    def _resolve_levels_from_edges(self) -> list[list[dict]]:
        """Level-aware topological sort from pipeline_edges + pipeline_nodes."""
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

        current_level = sorted(
            (name for name, deg in in_degree.items() if deg == 0),
            key=lambda n: nodes_by_name[n].get("order_sort", 0),
        )

        levels: list[list[dict]] = []
        while current_level:
            levels.append([nodes_by_name[name] for name in current_level])
            next_level = []
            for name in current_level:
                for neighbor in sorted(
                    adjacency[name], key=lambda n: nodes_by_name[n].get("order_sort", 0)
                ):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.append(neighbor)
            current_level = sorted(next_level, key=lambda n: nodes_by_name[n].get("order_sort", 0))

        if sum(len(lvl) for lvl in levels) != len(nodes_by_name):
            involved = [n for n, d in in_degree.items() if d > 0]
            raise ValueError(
                f"Circular dependency detected in pipeline '{self._pipeline.name}'. "
                f"Nodes involved: {involved}"
            )

        return levels

    def _resolve_levels_from_steps(self) -> list[PipelineStep]:
        """Legacy: single-level topological sort from PipelineStep.depends_on."""
        return self._resolve_order_from_steps()

    def _resolve_execution_order(self) -> list:
        """Return steps in a valid execution order using Kahn's BFS algorithm.

        Priority:
        1. Edges present  → topological sort via _resolve_order_from_edges()
        2. Nodes present, no edges → single/isolated nodes run in order_sort order
           (handles single-node pipelines or pipelines not yet wired with edges)
        3. Neither → legacy PipelineStep.depends_on fallback
        """
        if self._pipeline.edges:
            return self._resolve_order_from_edges()
        if self._pipeline.nodes:
            return sorted(
                self._pipeline.nodes,
                key=lambda n: n.get("order_sort", 0),
            )
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
        is_custom = config.get("config_type") == "custom"

        src_ds_id = config.get("_datasource_source_id")
        tgt_ds_id = config.get("_datasource_target_id")

        if not src_ds_id and not is_custom:
            return f"Config '{config.get('config_name', '')}' missing source datasource"
        if not tgt_ds_id:
            return f"Config '{config.get('config_name', '')}' missing target datasource"

        tgt_ds = ds_get_by_id(tgt_ds_id)
        if not tgt_ds:
            return f"Target datasource (id={tgt_ds_id}) not found"

        tgt_charset = tgt_ds.get("charset") or None
        tgt_conn = {
            "db_type": tgt_ds["db_type"],
            "host": tgt_ds["host"],
            "port": tgt_ds["port"],
            "db_name": tgt_ds["dbname"],
            "user": tgt_ds["username"],
            "password": tgt_ds["password"],
            "charset": tgt_charset,
        }

        # Custom scripts skip source entirely — return empty dict as placeholder.
        if is_custom:
            return {}, tgt_conn

        src_ds = ds_get_by_id(src_ds_id)
        if not src_ds:
            return f"Source datasource (id={src_ds_id}) not found"

        src_charset = src_ds.get("charset") or config.get("source", {}).get("charset") or None
        if src_ds["db_type"] == DbType.POSTGRESQL and src_charset == "tis620":
            src_charset = "WIN874"

        src_conn = {
            "db_type": src_ds["db_type"],
            "host": src_ds["host"],
            "port": src_ds["port"],
            "db_name": src_ds["dbname"],
            "user": src_ds["username"],
            "password": src_ds["password"],
            "charset": src_charset,
        }
        return src_conn, tgt_conn

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
        try:
            job_id = None
            if hasattr(self._run_repo, "_job_id"):
                job_id = self._run_repo._job_id

            pipeline_id = uuid.UUID(self._pipeline.id)

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

            should_flush = status == "failed" or error_message

            with self._batch_buffer_lock:
                self._batch_buffer.append(record)
                if len(self._batch_buffer) >= self._batch_buffer_limit or should_flush:
                    self._flush_batch_buffer()

            if self._run_event_callback:
                try:
                    self._run_event_callback(
                        "pipeline_run:batch",
                        {
                            "pipeline_id": str(pipeline_id),
                            "job_id": str(job_id) if job_id else None,
                            "config_name": config_name,
                            "batch_round": batch_round,
                            "rows_in_batch": rows_in_batch,
                            "rows_cumulative": rows_cumulative,
                            "batch_size": batch_size,
                            "total_records_in_config": total_records_in_config,
                            "status": status,
                            "error_message": error_message,
                            "transformation_warnings": transformation_warnings,
                        },
                    )
                except Exception:
                    pass

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
                    pass

        except Exception as e:
            self._log(f"Warning: Failed to save batch record for {config_name}: {e}", "⚠️")

    def _flush_batch_buffer(self) -> None:
        """Flush buffered batch records to DB. Caller must hold _batch_buffer_lock."""
        if not self._batch_buffer:
            return
        from repositories import pipeline_run_repo as _run_repo

        try:
            _run_repo.save_batch(self._batch_buffer)
        except Exception:
            for rec in self._batch_buffer:
                try:
                    self._run_repo.save(rec)
                except Exception:
                    pass
        self._batch_buffer.clear()

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
            PipelineRunUpdateRecord(status="running"),
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

    def _persist_step_error(
        self,
        config_name: str,
        error_message: str,
        step_status: str = "failed",
    ) -> None:
        """Insert a failed pipeline_runs record AND update job error_message.

        Called at every failure path to ensure errors are persisted to the
        database regardless of how the step failed.
        """
        try:
            record = PipelineRunRecord(
                pipeline_id=uuid.UUID(self._pipeline.id),
                job_id=uuid.UUID(self._job_id) if self._job_id else None,
                config_name=config_name,
                batch_round=-1,
                status=step_status,
                error_message=(error_message or "")[:500],
            )
            self._run_repo.save(record)
        except Exception:
            pass

        if self._job_id:
            try:
                from repositories import job_repo as _jr
                from models.job import JobUpdateRecord as _JobUpdate

                _jr.update(
                    uuid.UUID(self._job_id),
                    _JobUpdate(
                        status="running",
                        error_message=error_message[:300] if error_message else None,
                    ),
                )
            except Exception:
                pass

        if self._run_event_callback:
            try:
                self._run_event_callback(
                    "pipeline_run:failed",
                    {
                        "job_id": self._job_id,
                        "pipeline_id": self._pipeline.id,
                        "config_name": config_name,
                        "error_message": error_message,
                    },
                )
            except Exception:
                pass
