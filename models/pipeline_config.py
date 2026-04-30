"""
Pipeline domain models.

PipelineStep / PipelineConfig: domain model for pipeline execution logic.
PipelineRecord:                write model for the pipelines table.
PipelineRunRecord:             write model for INSERT into pipeline_runs.
PipelineRunUpdateRecord:       write model for patching pipeline_runs status.

PipelineRecord / PipelineRunRecord / PipelineRunUpdateRecord are the single
source of truth for their table columns — pass them to the repo instead of
flat kwargs. Adding a new column = add the field here only.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass
class PipelineStep:
    order: int
    config_name: str
    depends_on: list[str] = field(default_factory=list)
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineStep":
        return cls(
            order=d.get("order", 0),
            config_name=d.get("config_name", ""),
            depends_on=d.get("depends_on", []),
            enabled=d.get("enabled", True),
        )

    def to_dict(self) -> dict:
        return {
            "order": self.order,
            "config_name": self.config_name,
            "depends_on": self.depends_on,
            "enabled": self.enabled,
        }


@dataclass
class PipelineConfig:
    id: str
    name: str
    description: str = ""
    steps: list[PipelineStep] = field(default_factory=list)
    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)
    error_strategy: str = "fail_fast"  # fail_fast | continue_on_error | skip_dependents
    batch_size: int = 1000
    truncate_targets: bool = False
    parallel_enabled: bool = False
    max_parallel_steps: int = 4
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def new(cls, name: str, **kwargs) -> "PipelineConfig":
        """Convenience factory: auto-generates UUID and ISO timestamps."""
        now = datetime.now().isoformat()
        return cls(
            id=str(uuid.uuid4()), name=name, created_at=now, updated_at=now, **kwargs
        )

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineConfig":
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            name=d.get("name", ""),
            description=d.get("description", ""),
            steps=[PipelineStep.from_dict(s) for s in d.get("steps", [])],
            nodes=d.get("nodes", []),
            edges=d.get("edges", []),
            error_strategy=d.get("error_strategy", "fail_fast"),
            batch_size=d.get("batch_size", 1000),
            truncate_targets=d.get("truncate_targets", False),
            parallel_enabled=d.get("parallel_enabled", False),
            max_parallel_steps=d.get("max_parallel_steps", 4),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "steps": [s.to_dict() for s in self.steps],
            "nodes": self.nodes,
            "edges": self.edges,
            "error_strategy": self.error_strategy,
            "batch_size": self.batch_size,
            "truncate_targets": self.truncate_targets,
            "parallel_enabled": self.parallel_enabled,
            "max_parallel_steps": self.max_parallel_steps,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class PipelineRecord:
    """
    Single source of truth for writable pipelines table columns.

    Pass to pipeline_repo.save() instead of 6 flat kwargs.
    Adding a new column = add the field here and update col_params in save() once.
    """

    name: str
    description: str = ""
    json_data: str | dict = field(default_factory=dict)
    error_strategy: str = "fail_fast"


@dataclass
class PipelineNodeRecord:
    """Write model for pipeline_nodes table."""

    pipeline_id: uuid.UUID
    config_id: uuid.UUID
    position_x: int = 0
    position_y: int = 0
    order_sort: int = 0


@dataclass
class PipelineEdgeRecord:
    """Write model for pipeline_edges table."""

    pipeline_id: uuid.UUID
    source_config_uuid: uuid.UUID
    target_config_uuid: uuid.UUID


@dataclass
class PipelineRunRecord:
    """
    Write model for INSERT into pipeline_runs (1 record per batch).

    Each batch execution creates a new record with flat columns.
    Example: 2 configs × 5 batches each = 10 records in pipeline_runs.

    Pass to pipeline_run_repo.save() instead of flat params.
    """

    pipeline_id: uuid.UUID
    config_name: str
    batch_round: int
    rows_in_batch: int = 0
    rows_cumulative: int = 0
    batch_size: int = 1000
    total_records_in_config: int = 0
    status: str = "success"  # success | failed
    job_id: uuid.UUID | None = None
    error_message: str | None = None
    transformation_warnings: str | None = None  # JSON string or semicolon-delimited


@dataclass
class PipelineRunUpdateRecord:
    """
    Write model for patching pipeline_runs (rare, mainly for status changes).

    Since each batch is a separate INSERT record, patches are minimal.
    Primarily used to change status after batch completes.
    """

    status: str
    error_message: str | None = None
