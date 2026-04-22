"""Data Explorers API router — POST /api/v1/db-explorers"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.data_explorers.schemas import ExecuteQueryRequest
from api.base.json_api import create_success_response
from services.query_executor import QueryExecutor
from services.sql_validator import SqlValidationError

router = APIRouter(prefix="/api/v1/db-explorers", tags=["Data Explorers"])


def _get_executor() -> QueryExecutor:
    return QueryExecutor()


@router.post("", status_code=200)
def execute_sql(body: ExecuteQueryRequest, executor: QueryExecutor = Depends(_get_executor)):
    try:
        result = executor.execute(str(body.datasource_id), body.cmd)
    except SqlValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return create_success_response(
        resource_type="db_explorers",
        data=result,
        base_url="/api/v1/db-explorers",
    )


def get_data_explorers_router() -> APIRouter:
    return router
