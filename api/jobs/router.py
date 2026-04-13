"""
POST /api/v1/jobs — Create a migration job and trigger the pipeline.

Flow:
    1. Validate pipeline_id exists
    2. Build PipelineConfig from pipeline's json_data
    3. Resolve datasource connection configs from first step's migration config
    4. Create a jobs record (source of truth for this job request)
    5. Start PipelineExecutor in background thread — returns run_id
    6. Return job_id + run_id immediately (202 Accepted)

Socket.IO events (frontend connects to /ws/socket.io/):
    "job:batch"     — after each successful batch
                      { run_id, job_id, pipeline_id, step, batch_num, rows_processed }
    "job:error"     — on batch failure or step failure
                      { run_id, job_id, pipeline_id, step, batch_num, error_message }
    "job:completed" — when the whole pipeline finishes
                      { run_id, job_id, pipeline_id, status, total_rows }
"""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, HTTPException

from api.jobs.schemas import CreateJobSchema, JobCreatedResponse
from api.socket_manager import emit_from_thread
from models.job import JobRecord, JobUpdateRecord
from models.pipeline_config import (
    PipelineConfig,
    PipelineRunRecord,
    PipelineRunUpdateRecord,
)
from repositories import pipeline_repo, pipeline_run_repo, job_repo
from repositories.datasource_repo import get_by_id as ds_get_by_id
from services.pipeline_service import (
    PipelineExecutor,
    ConfigRepositoryAdapter,
    PipelineRunRepositoryAdapter,
)

router = APIRouter(prefix="/api/v1/jobs", tags=["Jobs"])


def _build_conn_config(ds_row: dict, charset: str | None = None) -> dict:
    return {
        "db_type": ds_row["db_type"],
        "host": ds_row["host"],
        "port": ds_row["port"],
        "db_name": ds_row["dbname"],
        "user": ds_row["username"],
        "password": ds_row["password"],
        "charset": charset,
    }


def _resolve_conn_configs(pc: PipelineConfig, config_repo: ConfigRepositoryAdapter):
    """Resolve source/target connection configs from the pipeline's first node config.

    Uses datasource_source_id / datasource_target_id (UUID FK) from configs table.
    """
    nodes = pc.nodes or []
    if not nodes:
        raise HTTPException(status_code=422, detail="Pipeline has no nodes")

    first_node = nodes[0]
    config_name = (
        first_node["config_name"]
        if isinstance(first_node, dict)
        else first_node.config_name
    )
    config = config_repo.get_content(config_name)
    if config is None:
        raise HTTPException(status_code=404, detail=f"Config '{config_name}' not found")

    src_ds_id = config.get("_datasource_source_id")
    tgt_ds_id = config.get("_datasource_target_id")
    if not src_ds_id:
        raise HTTPException(
            status_code=422,
            detail=f"Config '{config_name}' missing source datasource. "
            "Please edit the config and select a source datasource.",
        )
    if not tgt_ds_id:
        raise HTTPException(
            status_code=422,
            detail=f"Config '{config_name}' missing target datasource. "
            "Please edit the config and select a target datasource.",
        )

    src_ds = ds_get_by_id(src_ds_id)
    if not src_ds:
        raise HTTPException(
            status_code=404, detail=f"Source datasource (id={src_ds_id}) not found"
        )
    tgt_ds = ds_get_by_id(tgt_ds_id)
    if not tgt_ds:
        raise HTTPException(
            status_code=404, detail=f"Target datasource (id={tgt_ds_id}) not found"
        )

    charset = config.get("source", {}).get("charset")
    if src_ds["db_type"] == "PostgreSQL" and charset == "tis620":
        charset = "WIN874"

    source_conn_config = _build_conn_config(src_ds, charset)
    target_conn_config = _build_conn_config(tgt_ds)
    return source_conn_config, target_conn_config


@router.post("", response_model=JobCreatedResponse, status_code=202)
def create_job(body: CreateJobSchema):
    """
    Trigger a pipeline migration job.

    Creates a job record, starts a background migration thread, and returns
    immediately with job_id + run_id so the frontend can subscribe to socket events.
    """
    pipeline_row = pipeline_repo.get_by_id(body.pipeline_id)
    if not pipeline_row:
        raise HTTPException(
            status_code=404, detail=f"Pipeline '{body.pipeline_id}' not found"
        )

    try:
        pc = PipelineConfig.from_dict(pipeline_row.get("json_data", {}) or {})
        pc.id = pipeline_row["id"]
        pc.nodes = pipeline_row.get("nodes", [])
        pc.edges = pipeline_row.get("edges", [])
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid pipeline data: {exc}")

    config_repo = ConfigRepositoryAdapter()

    latest = pipeline_run_repo.get_latest(uuid.UUID(pc.id))
    if latest and latest["status"] == "running":
        raise HTTPException(
            status_code=409,
            detail="A run is already in progress for this pipeline",
        )

    job_record = JobRecord(
        pipeline_id=uuid.UUID(pc.id),
        status="running",
    )
    job_id: uuid.UUID = job_repo.save(job_record)
    job_id_str = str(job_id)
    pipeline_id_str = pc.id

    def batch_event_callback(
        run_id: str,
        step_name: str,
        batch_num: int,
        rows_processed: int,
        error: str | None = None,
    ) -> None:
        if error:
            try:
                job_repo.update(
                    job_id, JobUpdateRecord(status="running", error_message=error)
                )
            except Exception:
                pass
            try:
                pipeline_run_repo.update(
                    uuid.UUID(run_id),
                    PipelineRunUpdateRecord(
                        status="running",
                        steps_json=json.dumps(
                            {
                                step_name: {
                                    "status": "failed",
                                    "batch_num": batch_num,
                                    "rows_processed": rows_processed,
                                    "error_message": error,
                                }
                            }
                        ),
                        error_message=error,
                    ),
                )
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
            try:
                pipeline_run_repo.update(
                    uuid.UUID(run_id),
                    PipelineRunUpdateRecord(
                        status="running",
                        steps_json=json.dumps(
                            {
                                step_name: {
                                    "status": "running",
                                    "batch_num": batch_num,
                                    "rows_processed": rows_processed,
                                }
                            }
                        ),
                    ),
                )
            except Exception:
                pass
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

    run_repo = PipelineRunRepositoryAdapter(job_id=job_id)

    executor = PipelineExecutor(
        pipeline=pc,
        source_conn_config={},
        target_conn_config={},
        config_repo=config_repo,
        run_repo=run_repo,
        batch_event_callback=batch_event_callback,
        completion_callback=completion_callback,
    )
    run_id = executor.start_background()

    return JobCreatedResponse(
        job_id=job_id_str,
        run_id=str(run_id),
        pipeline_id=pipeline_id_str,
        status="running",
    )


def get_jobs_router() -> APIRouter:
    return router
