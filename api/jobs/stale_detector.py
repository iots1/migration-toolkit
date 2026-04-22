"""Stale job detection — checks heartbeat and age of running jobs."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from services.migration_executor import _read_heartbeat

_STALE_THRESHOLD_SECONDS = 300
_AGE_THRESHOLD_SECONDS = 600


def is_job_stale(running_job: dict) -> bool:
    """Three-tier stale detection: heartbeat file, DB last_heartbeat, created_at age."""
    hb = _read_heartbeat(str(running_job["id"]))
    if hb:
        return (time.time() - hb["timestamp"]) > _STALE_THRESHOLD_SECONDS

    last_hb = running_job.get("last_heartbeat")
    if last_hb:
        dt = _parse_datetime(last_hb)
        if dt:
            return (datetime.now(timezone.utc) - dt).total_seconds() > _STALE_THRESHOLD_SECONDS

    created_at = running_job.get("created_at")
    if created_at:
        dt = _parse_datetime(created_at)
        if dt:
            return (datetime.now(timezone.utc) - dt).total_seconds() > _AGE_THRESHOLD_SECONDS

    return False


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except Exception:
            return None
    if value and hasattr(value, "tzinfo"):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value
    return None
