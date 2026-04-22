"""Jobs service — CRUD + pipeline trigger logic."""

from __future__ import annotations

import threading
import uuid

from fastapi import HTTPException

from api.base.service import BaseService
from api.base.query_params import QueryParams
from api.jobs.stale_detector import is_job_stale
from models.job import JobRecord, JobUpdateRecord
from models.pipeline_config import PipelineConfig, PipelineRunRecord
from repositories import pipeline_repo, job_repo
from repositories import pipeline_run_repo
from services.pipeline_service import (
    PipelineExecutor,
    ConfigRepositoryAdapter,
    PipelineRunRepositoryAdapter,
)
from services.migration_executor import _clean_heartbeat


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

    def __init__(self, emit_fn=None):
        self._emit_fn = emit_fn

    def _count_all(self) -> int:
        return job_repo.count_all()

    def _list_all(self) -> list[dict]:
        return job_repo.get_all(limit=10_000)

    def find_by_id(self, id: str) -> dict:
        job_id = self._parse_uuid(id)
        result = self.execute_db_operation(lambda: job_repo.get_by_id(job_id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def find_pipeline_runs(self, job_id: str) -> list[dict]:
        """Get all pipeline_run records for a given job."""
        jid = self._parse_uuid(job_id)
        return self.execute_db_operation(
            lambda: pipeline_run_repo.get_by_job(jid)
        )

    def update(self, id: str, data: dict) -> dict:
        raise HTTPException(status_code=405, detail="Jobs cannot be updated")

    def delete(self, id: str) -> None:
        raise HTTPException(status_code=405, detail="Jobs cannot be deleted")

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

        recent = self.execute_db_operation(
            lambda: job_repo.get_by_pipeline(uuid.UUID(pc.id), limit=1)
        )
        if recent and recent[0]["status"] == "running":
            running_job = recent[0]
            running_job_id = uuid.UUID(running_job["id"])
            if is_job_stale(running_job):
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

        emit = self._emit_fn or _noop_emit

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
                emit(
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
                emit(
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
            emit(
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
            emit(event_name, data)

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


def _noop_emit(event: str, data: dict) -> None:
    pass
