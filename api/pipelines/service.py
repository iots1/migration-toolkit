"""Pipelines service — handles CRUD for pipelines table."""

from __future__ import annotations

import uuid

from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import pipeline_repo
from repositories import pipeline_node_repo
from repositories import pipeline_edge_repo
from models.pipeline_config import (
    PipelineRecord,
    PipelineNodeRecord,
    PipelineEdgeRecord,
)


class PipelinesService(BaseService):
    resource_type = "pipelines"
    allowed_fields = [
        "id",
        "name",
        "description",
        "created_at",
        "updated_at",
    ]

    def find_all(self, params: QueryParams) -> dict:
        data = self.execute_db_operation(lambda: pipeline_repo.get_all_list())
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        data = self._attach_children(data)
        page_data, total, total_pages = self._paginate(data, params)

        return {
            "data": page_data,
            "total": total,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str) -> dict:
        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(id))
        self._assert_found(result, id)
        result = self._sanitize_response(result)
        return self._attach_children_single(result)

    def create(self, data: dict) -> dict:
        nodes_raw = data.pop("nodes", None)
        edges_raw = data.pop("edges", None)

        record = self._to_record(data)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.save(record))
        self._assert_success(ok, msg)

        result = self.execute_db_operation(
            lambda: pipeline_repo.get_by_name(record.name)
        )
        pipeline_id = uuid.UUID(result["id"])

        if nodes_raw is not None:
            self._sync_nodes(pipeline_id, nodes_raw)
        if edges_raw is not None:
            self._sync_edges(pipeline_id, edges_raw)

        result = self._sanitize_response(result)
        return self._attach_children_single(result)

    def update(self, id: str, data: dict) -> dict:
        nodes_raw = data.pop("nodes", None)
        edges_raw = data.pop("edges", None)

        existing = self.find_by_id(id)
        record = self._to_record(data, existing=existing, name_override=id)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.save(record))
        self._assert_success(ok, msg)

        result = self.execute_db_operation(
            lambda: pipeline_repo.get_by_name(record.name)
        )
        pipeline_id = uuid.UUID(result["id"])

        if nodes_raw is not None:
            self._sync_nodes(pipeline_id, nodes_raw)
        if edges_raw is not None:
            self._sync_edges(pipeline_id, edges_raw)

        result = self._sanitize_response(result)
        return self._attach_children_single(result)

    def delete(self, id: str) -> None:
        self.find_by_id(id)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.delete(id))
        self._assert_success(ok, msg)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_record(
        self,
        data: dict,
        *,
        existing: dict | None = None,
        name_override: str | None = None,
    ) -> PipelineRecord:
        ex = existing or {}
        return PipelineRecord(
            name=name_override or data.get("name", ""),
            description=data.get("description") or ex.get("description", ""),
        )

    def _sync_nodes(self, pipeline_id: uuid.UUID, nodes: list[dict]) -> None:
        self.execute_db_operation(
            lambda: pipeline_node_repo.delete_by_pipeline(pipeline_id)
        )
        if not nodes:
            return
        records = [
            PipelineNodeRecord(
                pipeline_id=pipeline_id,
                config_id=uuid.UUID(n["config_id"]),
                position_x=n.get("position_x", 0),
                position_y=n.get("position_y", 0),
                order_sort=n.get("order_sort", 0),
            )
            for n in nodes
        ]
        self.execute_db_operation(lambda: pipeline_node_repo.bulk_insert(records))

    def _sync_edges(self, pipeline_id: uuid.UUID, edges: list[dict]) -> None:
        self.execute_db_operation(
            lambda: pipeline_edge_repo.delete_by_pipeline(pipeline_id)
        )
        if not edges:
            return
        records = [
            PipelineEdgeRecord(
                pipeline_id=pipeline_id,
                source_config_uuid=uuid.UUID(e["source_config_uuid"]),
                target_config_uuid=uuid.UUID(e["target_config_uuid"]),
            )
            for e in edges
        ]
        self.execute_db_operation(lambda: pipeline_edge_repo.bulk_insert(records))

    def _attach_children_single(self, pipeline: dict) -> dict:
        pipeline_id = uuid.UUID(pipeline["id"])
        nodes = self.execute_db_operation(
            lambda: pipeline_node_repo.get_by_pipeline(pipeline_id)
        )
        edges = self.execute_db_operation(
            lambda: pipeline_edge_repo.get_by_pipeline(pipeline_id)
        )
        pipeline["nodes"] = nodes
        pipeline["edges"] = edges
        return pipeline

    def _attach_children(self, pipelines: list[dict]) -> list[dict]:
        for p in pipelines:
            try:
                pipeline_id = uuid.UUID(p["id"])
                nodes = self.execute_db_operation(
                    lambda pid=pipeline_id: pipeline_node_repo.get_by_pipeline(pid)
                )
                edges = self.execute_db_operation(
                    lambda pid=pipeline_id: pipeline_edge_repo.get_by_pipeline(pid)
                )
                p["nodes"] = nodes
                p["edges"] = edges
            except Exception:
                p["nodes"] = []
                p["edges"] = []
        return pipelines
