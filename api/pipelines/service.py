"""Pipelines service — handles CRUD for pipelines table."""

from __future__ import annotations

import uuid

from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import pipeline_repo
from repositories import pipeline_node_repo
from repositories import pipeline_edge_repo
from repositories import job_repo
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

    def _count_all(self) -> int:
        return pipeline_repo.count_all()

    def _list_all(self) -> list[dict]:
        return pipeline_repo.get_all_list()

    def _post_process_page(self, page_data: list[dict]) -> list[dict]:
        return self._attach_children(page_data)

    def find_by_id(self, id: str) -> dict:
        result = self.execute_db_operation(lambda: pipeline_repo.get_by_id(id))
        self._assert_found(result, id)
        result = self._sanitize_response(result)
        return self._attach_children_single(result)

    def create(self, data: dict) -> dict:
        nodes_raw = data.pop("nodes", None)
        edges_raw = data.pop("edges", None)

        record = self._to_record(data)
        new_id = self.execute_db_operation(lambda: pipeline_repo.save(record))
        pipeline_id = uuid.UUID(str(new_id))

        if nodes_raw is not None:
            self._sync_nodes(pipeline_id, nodes_raw)
        if edges_raw is not None:
            self._sync_edges(pipeline_id, edges_raw)

        result = self.execute_db_operation(lambda: pipeline_repo.get_by_id(str(new_id)))
        result = self._sanitize_response(result)
        return self._attach_children_single(result)

    def update(self, id: str, data: dict) -> dict:
        nodes_raw = data.pop("nodes", None)
        edges_raw = data.pop("edges", None)

        existing = self.find_by_id(id)
        record = self._to_record(data, existing=existing)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.update_by_id(id, record))
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: pipeline_repo.get_by_id(id))
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

    def find_jobs(self, pipeline_id: str) -> list[dict]:
        """Get all jobs for a given pipeline."""
        try:
            pid = uuid.UUID(pipeline_id)
        except ValueError:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {pipeline_id}")
        return self.execute_db_operation(lambda: job_repo.get_by_pipeline(pid))

    def duplicate(self, id: str) -> dict:
        existing = self.find_by_id(id)
        new_name = f"{existing['name']} copy"
        nodes = [
            {
                "config_id": n["config_id"],
                "position_x": n.get("position_x", 0),
                "position_y": n.get("position_y", 0),
                "order_sort": n.get("order_sort", 0),
            }
            for n in existing.get("nodes", [])
        ]
        edges = [
            {
                "source_config_uuid": e["source_config_uuid"],
                "target_config_uuid": e["target_config_uuid"],
            }
            for e in existing.get("edges", [])
        ]
        return self.create({
            "name": new_name,
            "description": existing.get("description", ""),
            "nodes": nodes,
            "edges": edges,
        })

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
            name=name_override or data.get("name") or ex.get("name", ""),
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
        if "nodes" not in pipeline or "edges" not in pipeline:
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
        if not pipelines:
            return pipelines
        pipeline_ids = [p["id"] for p in pipelines]
        nodes_map = self.execute_db_operation(
            lambda: pipeline_node_repo.get_nodes_by_pipeline_ids(pipeline_ids)
        )
        edges_map = self.execute_db_operation(
            lambda: pipeline_edge_repo.get_edges_by_pipeline_ids(pipeline_ids)
        )
        for p in pipelines:
            p["nodes"] = nodes_map.get(p["id"], [])
            p["edges"] = edges_map.get(p["id"], [])
        return pipelines
