"""Jobs service — CRUD + pipeline trigger logic."""

from __future__ import annotations

import os
import threading
import time
import uuid

from fastapi import HTTPException

from api.base.service import BaseService
from api.base.query_params import QueryParams
from api.socket_manager import emit_from_thread
from models.job import JobRecord, JobUpdateRecord
from models.pipeline_config import PipelineConfig, PipelineRunRecord
from repositories import pipeline_repo, job_repo
from repositories import pipeline_run_repo
from repositories.datasource_repo import get_by_id as ds_get_by_id
from services.pipeline_service import (
    PipelineExecutor,
    ConfigRepositoryAdapter,
    PipelineRunRepositoryAdapter,
)
from services.migration_executor import _read_heartbeat, _clean_heartbeat

_STALE_THRESHOLD_SECONDS = 300


class JobsService(BaseService):
    resource_type = "jobs"
    allowed_fields = [
        "id",
        "pipeline_id",
        "status",
        "completed_at",
        "error_message",
        "total_config",
        "created_at",
    ]

    # ------------------------------------------------------------------
    # Standard CRUD
    # ------------------------------------------------------------------

    def find_all(self, params: QueryParams) -> dict:
        total_records = self.execute_db_operation(lambda: job_repo.count_all())
        data = self.execute_db_operation(lambda: job_repo.get_all(limit=10_000))
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)
        return {
            "data": page_data,
            "total": total,
            "total_records": total_records,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str) -> dict:
        try:
            job_id = uuid.UUID(id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {id}")
        result = self.execute_db_operation(lambda: job_repo.get_by_id(job_id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def find_pipeline_runs(self, job_id: str) -> list[dict]:
        """Get all pipeline_run records for a given job."""
        try:
            jid = uuid.UUID(job_id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {job_id}")
        rows = self.execute_db_operation(
            lambda: pipeline_run_repo.get_by_job(jid)
        )
        return rows

    def update(self, id: str, data: dict) -> dict:
        raise HTTPException(status_code=405, detail="Jobs cannot be updated")

    def delete(self, id: str) -> None:
        raise HTTPException(status_code=405, detail="Jobs cannot be deleted")

    def _is_job_stale(self, running_job_id: uuid.UUID) -> bool:
        """Three-tier stale detection: heartbeat file, DB last_heartbeat, created_at age."""
        running_job = self.execute_db_operation(
            lambda: job_repo.get_by_id(running_job_id)
        )
        if not running_job:
            return True

        hb = _read_heartbeat(str(running_job_id))
        if hb:
            return (time.time() - hb["timestamp"]) > _STALE_THRESHOLD_SECONDS

        last_hb = running_job.get("last_heartbeat")
        if last_hb:
            from datetime import datetime, timezone
            if isinstance(last_hb, str):
                try:
                    last_hb = datetime.fromisoformat(last_hb)
                except Exception:
                    last_hb = None
            if last_hb and hasattr(last_hb, "tzinfo"):
                if last_hb.tzinfo is None:
                    from datetime import timezone as _tz
                    last_hb = last_hb.replace(tzinfo=_tz.utc)
                elapsed = (datetime.now(timezone.utc) - last_hb).total_seconds()
                return elapsed > _STALE_THRESHOLD_SECONDS

        created_at = running_job.get("created_at")
        if created_at:
            from datetime import datetime, timezone
            if isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at)
                except Exception:
                    created_at = None
            if created_at and hasattr(created_at, "tzinfo"):
                if created_at.tzinfo is None:
                    from datetime import timezone as _tz
                    created_at = created_at.replace(tzinfo=_tz.utc)
                elapsed = (datetime.now(timezone.utc) - created_at).total_seconds()
                return elapsed > 600

        return False

    # ------------------------------------------------------------------
    # Pipeline trigger (called by POST /api/v1/jobs)
    # ------------------------------------------------------------------

    def create(self, data: dict) -> dict:
        """Trigger a pipeline migration job.

        Creates a job record, starts a background executor thread, and returns
        immediately with job_id + run_id so the frontend can subscribe to
        Socket.IO events.

        When ``resume=True``, stale 'running' jobs are marked 'failed' and the
        executor reuses any existing checkpoint to skip completed steps and
        resume partially-run steps from their last batch.
        """
        pipeline_id_str = data.get("pipeline_id", "")
        resume_mode = data.get("resume", False)

        pipeline_row = self.execute_db_operation(
            lambda: pipeline_repo.get_by_id(pipeline_id_str)
        )
        self._assert_found(pipeline_row, pipeline_id_str)

        try:
            pc = PipelineConfig.from_dict(pipeline_row.get("json_data", {}) or {})
            pc.id = pipeline_row["id"]
            pc.nodes = pipeline_row.get("nodes", [])
            pc.edges = pipeline_row.get("edges", [])
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Invalid pipeline data: {exc}")

        # Guard: reject if a job is already running for this pipeline
        recent = self.execute_db_operation(
            lambda: job_repo.get_by_pipeline(uuid.UUID(pc.id), limit=1)
        )
        if recent and recent[0]["status"] == "running":
            running_job_id = uuid.UUID(recent[0]["id"])
            if self._is_job_stale(running_job_id):
                self.execute_db_operation(
                    lambda: job_repo.update(
                        running_job_id,
                        JobUpdateRecord(
                            status="failed",
                            error_message="Process died — no heartbeat for 5 minutes",
                        ),
                    )
                )
                _clean_heartbeat(str(running_job_id))
            elif resume_mode:
                self.execute_db_operation(
                    lambda: job_repo.update(
                        running_job_id,
                        JobUpdateRecord(
                            status="failed",
                            error_message="Marked stale — resume requested",
                        ),
                    )
                )
            else:
                raise HTTPException(
                    status_code=409,
                    detail="A job is already running for this pipeline. "
                    "Use resume=true to force resume.",
                )

        if resume_mode:
            from services.checkpoint_manager import load_pipeline_checkpoint

            checkpoint = load_pipeline_checkpoint(pc.name)
            if not checkpoint:
                raise HTTPException(
                    status_code=404,
                    detail=f"No checkpoint found for pipeline '{pc.name}'. "
                    "Cannot resume — start a fresh run instead.",
                )

        total_config = len(pc.nodes) if pc.nodes else 0
        job_record = JobRecord(
            pipeline_id=uuid.UUID(pc.id),
            status="running",
            total_config=total_config,
        )
        job_id: uuid.UUID = self.execute_db_operation(lambda: job_repo.save(job_record))
        job_id_str = str(job_id)

        def batch_event_callback(
            run_id: str,
            step_name: str,
            batch_num: int,
            rows_processed: int,
            error: str | None = None,
        ) -> None:
            if error:
                try:
                    job_repo.update(job_id, JobUpdateRecord(status="running", error_message=error))
                except Exception:
                    pass
                emit_from_thread(
                    "job:error",
                    {
                        "run_id": run_id,
                        "job_id": job_id_str,
                        "pipeline_id": pipeline_id_str,
                        "step": step_name,
                        "batch_num": batch_num,
                        "error_message": error,
                    },
                )
            else:
                emit_from_thread(
                    "job:batch",
                    {
                        "run_id": run_id,
                        "job_id": job_id_str,
                        "pipeline_id": pipeline_id_str,
                        "step": step_name,
                        "batch_num": batch_num,
                        "rows_processed": rows_processed,
                    },
                )

        def completion_callback(run_id: str, status: str, total_rows: int) -> None:
            try:
                job_repo.update(job_id, JobUpdateRecord(status=status))
            except Exception:
                pass
            emit_from_thread(
                "job:completed",
                {
                    "run_id": run_id,
                    "job_id": job_id_str,
                    "pipeline_id": pipeline_id_str,
                    "status": status,
                    "total_rows": total_rows,
                },
            )

        config_repo = ConfigRepositoryAdapter()
        run_repo = PipelineRunRepositoryAdapter(job_id=job_id)
        shutdown_event = threading.Event()

        def run_event_callback(event_name: str, data: dict) -> None:
            emit_from_thread(event_name, data)

        executor = PipelineExecutor(
            pipeline=pc,
            source_conn_config={},
            target_conn_config={},
            config_repo=config_repo,
            run_repo=run_repo,
            batch_event_callback=batch_event_callback,
            completion_callback=completion_callback,
            run_event_callback=run_event_callback,
            shutdown_event=shutdown_event,
            job_id=job_id_str,
        )
        run_id = executor.start_background()

        return {
            "job_id": job_id_str,
            "run_id": str(run_id),
            "pipeline_id": pipeline_id_str,
            "status": "running",
        }
