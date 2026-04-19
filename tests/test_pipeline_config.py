"""Tests for models/pipeline_config.py — PipelineStep and PipelineConfig."""
import pytest
from models.pipeline_config import PipelineConfig, PipelineStep


# ---------------------------------------------------------------------------
# PipelineStep
# ---------------------------------------------------------------------------

def test_pipeline_step_defaults():
    step = PipelineStep.from_dict({"order": 1, "config_name": "cfg_a"})
    assert step.order == 1
    assert step.config_name == "cfg_a"
    assert step.depends_on == []
    assert step.enabled is True


def test_pipeline_step_roundtrip():
    d = {"order": 2, "config_name": "cfg_b", "depends_on": ["cfg_a"], "enabled": False}
    step = PipelineStep.from_dict(d)
    assert step.to_dict() == d


# ---------------------------------------------------------------------------
# PipelineConfig
# ---------------------------------------------------------------------------

def _make_pipeline(**kwargs) -> PipelineConfig:
    pc = PipelineConfig.new("test_pipeline", **kwargs)
    pc.steps = [
        PipelineStep(order=1, config_name="cfg_patient"),
        PipelineStep(order=2, config_name="cfg_visit", depends_on=["cfg_patient"]),
    ]
    return pc


def test_pipeline_config_new_generates_uuid():
    pc = PipelineConfig.new("my_pipe")
    assert len(pc.id) == 36  # UUID4 format
    assert pc.name == "my_pipe"
    assert pc.created_at != ""
    assert pc.updated_at != ""


def test_pipeline_config_roundtrip():
    pc = _make_pipeline(description="desc", error_strategy="skip_dependents", batch_size=500)
    d = pc.to_dict()
    pc2 = PipelineConfig.from_dict(d)

    assert pc2.id == pc.id
    assert pc2.name == pc.name
    assert pc2.description == "desc"
    assert pc2.error_strategy == "skip_dependents"
    assert pc2.batch_size == 500
    assert len(pc2.steps) == 2
    assert pc2.steps[1].depends_on == ["cfg_patient"]


def test_pipeline_config_from_dict_defaults():
    """from_dict must tolerate missing optional fields (backward compat)."""
    pc = PipelineConfig.from_dict({"id": "abc", "name": "x"})
    assert pc.error_strategy == "fail_fast"
    assert pc.batch_size == 1000
    assert pc.truncate_targets is False
    assert pc.steps == []

def test_pipeline_config_step_order_preserved():
    pc = _make_pipeline()
    d = pc.to_dict()
    steps = d["steps"]
    assert steps[0]["config_name"] == "cfg_patient"
    assert steps[1]["config_name"] == "cfg_visit"
    assert steps[1]["depends_on"] == ["cfg_patient"]
