"""
Migration Logger — structured JSONL logging per job + ETA estimation.

Responsibility (SRP): file-based structured logging for migration jobs.
Each job gets its own JSONL file: logs/job_{job_id}.jsonl

Features:
    - Structured JSONL format (one JSON object per line)
    - Batch timing + ETA estimation (rolling average of last 10 batches)
    - Log rotation at 50MB per file
    - Safe newline handling (no broken JSONL from values containing \\n)
"""

from __future__ import annotations

import json
import os
from collections import deque
from datetime import datetime, timezone


LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
MAX_LOG_SIZE = 50 * 1024 * 1024  # 50MB
MAX_BATCH_TIMES = 100  # keep last N batch times for ETA


class MigrationLogger:
    """Per-job structured logger writing JSONL to logs/job_{job_id}.jsonl."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        os.makedirs(LOG_DIR, exist_ok=True)
        self._path = os.path.join(LOG_DIR, f"job_{job_id}.jsonl")
        self._file = open(self._path, "a", encoding="utf-8")
        self._step_stats: dict[str, dict] = {}
        self._peak_memory_pct: int = 0
        self._total_retries: int = 0
        self._total_quarantined: dict[str, int] = {}
        self._start_time = datetime.now(timezone.utc)
        self._rotate_if_needed()

    def log(
        self,
        step: str = "",
        batch: int = 0,
        event: str = "",
        **extra,
    ) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "job_id": self.job_id,
            "step": step,
            "batch": batch,
            "event": event,
            **extra,
        }
        line = json.dumps(entry, ensure_ascii=False, default=str)
        line = line.replace("\n", "\\n").replace("\r", "\\r")
        try:
            self._file.write(line + "\n")
            self._file.flush()
        except Exception:
            pass
        self._rotate_if_needed()

    def record_batch_time(self, step: str, duration_seconds: float) -> None:
        if step not in self._step_stats:
            self._step_stats[step] = {
                "batch_times": deque(maxlen=MAX_BATCH_TIMES),
                "rows": 0,
                "batches": 0,
            }
        self._step_stats[step]["batch_times"].append(duration_seconds)
        self._step_stats[step]["batches"] += 1

    def record_rows(self, step: str, rows: int) -> None:
        if step not in self._step_stats:
            self._step_stats[step] = {
                "batch_times": deque(maxlen=MAX_BATCH_TIMES),
                "rows": 0,
                "batches": 0,
            }
        self._step_stats[step]["rows"] = rows

    def record_memory(self, pct: int) -> None:
        if pct > self._peak_memory_pct:
            self._peak_memory_pct = pct

    def record_retry(self) -> None:
        self._total_retries += 1

    def record_quarantined(self, step: str, count: int) -> None:
        self._total_quarantined[step] = self._total_quarantined.get(step, 0) + count

    def estimate_eta(
        self,
        step: str,
        batch_num: int,
        total_rows: int,
        rows_processed: int,
    ) -> str | None:
        stats = self._step_stats.get(step)
        if not stats or not stats["batch_times"]:
            return None

        recent = list(stats["batch_times"])[-10:]
        avg_seconds = sum(recent) / len(recent)

        if batch_num > 0:
            avg_rows_per_batch = rows_processed / batch_num
        else:
            avg_rows_per_batch = 1

        remaining_rows = max(0, total_rows - rows_processed)
        remaining_batches = remaining_rows / max(avg_rows_per_batch, 1)
        eta_seconds = remaining_batches * avg_seconds

        if eta_seconds < 60:
            return f"~{int(eta_seconds)}s"
        elif eta_seconds < 3600:
            return f"~{int(eta_seconds / 60)}m"
        else:
            hours = int(eta_seconds // 3600)
            mins = int((eta_seconds % 3600) // 60)
            return f"~{hours}h {mins}m"

    def build_summary(self, total_rows: int, status: str) -> dict:
        steps_summary = {}
        for step_name, stats in self._step_stats.items():
            steps_summary[step_name] = {
                "rows": stats["rows"],
                "batches": stats["batches"],
                "avg_batch_time_s": round(
                    sum(stats["batch_times"]) / max(len(stats["batch_times"]), 1), 2
                ) if stats["batch_times"] else 0,
            }

        duration_s = (datetime.now(timezone.utc) - self._start_time).total_seconds()
        summary = {
            "total_rows": total_rows,
            "total_duration_s": round(duration_s, 1),
            "steps": steps_summary,
            "peak_memory_pct": self._peak_memory_pct,
            "retries": self._total_retries,
        }
        if self._total_quarantined:
            summary["quarantined_rows"] = self._total_quarantined
        return summary

    def _rotate_if_needed(self) -> None:
        try:
            if os.path.exists(self._path) and os.path.getsize(self._path) > MAX_LOG_SIZE:
                self._file.close()
                old_path = self._path + ".old"
                if os.path.exists(old_path):
                    os.remove(old_path)
                os.rename(self._path, old_path)
                self._file = open(self._path, "a", encoding="utf-8")
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


def _safe_name(config_name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in config_name)


def create_log_file(config_name: str) -> str | None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"migration_{_safe_name(config_name)}_{timestamp}.log")
        return path
    except Exception:
        return None


def write_log(log_file: str, message: str) -> None:
    if not log_file:
        return
    try:
        with open(log_file, "a", encoding="utf-8", errors="replace") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass


def read_log_file(log_file: str) -> str | None:
    if not log_file or not os.path.exists(log_file):
        return None
    for encoding in ["utf-8", "cp874", "tis-620", "latin-1"]:
        try:
            with open(log_file, "r", encoding=encoding, errors="replace") as f:
                return f.read()
        except Exception:
            continue
    return None
