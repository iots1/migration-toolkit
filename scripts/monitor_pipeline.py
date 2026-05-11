#!/usr/bin/env python3
"""
Pipeline Monitor — CLI tool to monitor running pipelines (custom + std).

Usage:
    python scripts/monitor_pipeline.py                  # show all running
    python scripts/monitor_pipeline.py <pipeline_id>    # monitor specific pipeline
    python scripts/monitor_pipeline.py --watch          # auto-refresh every 5s
    python scripts/monitor_pipeline.py --watch 10       # auto-refresh every 10s
"""
from __future__ import annotations

import os
import sys
import time
import urllib.request
import json
from datetime import datetime, timezone

API_BASE = os.getenv("API_BASE", "http://localhost:8000/api/v1")
HEARTBEAT_DIR = os.path.join(os.path.expanduser("~"), ".his_analyzer", "heartbeats")
STALE_THRESHOLD = 300

_BOLD = "\033[1m"
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_DIM = "\033[2m"
_RESET = "\033[0m"


def _api(path: str) -> dict | list:
    url = f"{API_BASE}/{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            return data
    except Exception as e:
        return {"error": str(e)}


def _read_heartbeat(job_id: str) -> dict | None:
    path = os.path.join(HEARTBEAT_DIR, f"{job_id}.heartbeat")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            content = f.read().strip()
        step, batch, ts = content.split("|")
        return {"step": step, "batch": int(batch), "timestamp": float(ts)}
    except Exception:
        return None


def _time_ago(ts_str: str) -> str:
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        diff = (datetime.now(timezone.utc) - dt).total_seconds()
        if diff < 60:
            return f"{diff:.0f}s ago"
        if diff < 3600:
            return f"{diff / 60:.1f}m ago"
        return f"{diff / 3600:.1f}h ago"
    except Exception:
        return ts_str


def _heartbeat_age(job_id: str) -> tuple[str, str]:
    hb = _read_heartbeat(job_id)
    if not hb:
        return "NO FILE", _DIM + "-" + _RESET
    age = time.time() - hb["timestamp"]
    if age < STALE_THRESHOLD:
        age_str = f"{age:.0f}s"
        status = _GREEN + "ALIVE" + _RESET
    else:
        age_str = f"{age:.0f}s"
        status = _RED + "STALE" + _RESET
    return status, f"{age_str} (step={hb['step']} batch={hb['batch']})"


def _clear():
    os.system("cls" if os.name == "nt" else "clear")


def show_overview():
    jobs = _api("jobs/?limit=20")
    if isinstance(jobs, dict) and "error" in jobs:
        print(f"{_RED}Cannot connect to API: {jobs['error']}{_RESET}")
        return

    running = [j for j in jobs if j["attributes"]["status"] == "running"]
    recent = [j for j in jobs if j["attributes"]["status"] != "running"][:5]

    print(f"\n{_BOLD}{'=' * 72}")
    print(f"  PIPELINE MONITOR  {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'=' * 72}{_RESET}\n")

    if running:
        print(f"{_BOLD}RUNNING JOBS:{_RESET}")
        for j in running:
            a = j["attributes"]
            jid = j["id"]
            hb_status, hb_detail = _heartbeat_age(jid)
            print(f"  Job: {jid[:8]}... | Pipeline: {a['pipeline_id'][:8]}...")
            print(f"    Status: {_CYAN}{a['status']}{_RESET} | "
                  f"Configs: {a['total_config']} | "
                  f"Started: {_time_ago(a['created_at'])}")
            print(f"    Heartbeat: {hb_status} | {hb_detail}")
            _show_job_runs(jid)
            print()
    else:
        print(f"  {_DIM}No running jobs{_RESET}\n")

    if recent:
        print(f"{_BOLD}RECENT JOBS:{_RESET}")
        for j in recent:
            a = j["attributes"]
            status_color = _GREEN if a["status"] == "completed" else _RED
            err = ""
            if a.get("error_message"):
                err = f" | {_DIM}{a['error_message'][:50]}{_RESET}"
            print(f"  {j['id'][:8]}... | {status_color}{a['status']}{_RESET}"
                  f" | {_time_ago(a['created_at'])}{err}")


def _show_job_runs(job_id: str):
    runs = _api(f"jobs/{job_id}/pipeline-runs")
    if isinstance(runs, dict) and "error" in runs:
        return
    if not runs:
        print(f"    {_DIM}No batch records yet{_RESET}")
        return

    by_step: dict[str, dict] = {}
    for r in runs:
        a = r["attributes"]
        name = a["config_name"]
        if name not in by_step or a["batch_round"] > by_step[name].get("batch_round", 0):
            by_step[name] = a

    print(f"    {_BOLD}Step Progress:{_RESET}")
    for name, a in by_step.items():
        total = a.get("total_records_in_config", 0)
        cumul = a.get("rows_cumulative", 0)
        batch = a.get("batch_round", 0)
        status = a.get("status", "")
        pct = (cumul / total * 100) if total > 0 else 0

        bar_w = 20
        filled = int(pct / 100 * bar_w)
        bar = _GREEN + "█" * filled + _DIM + "░" * (bar_w - filled) + _RESET

        status_icon = {"success": "✓", "failed": "✗", "running": "→"}.get(status, "?")
        print(f"      {status_icon} {name:<30s} {bar} {pct:5.1f}% "
              f"({cumul:,}/{total:,}) batch={batch}")


def show_pipeline(pipeline_id: str):
    jobs = _api(f"jobs/?limit=50")
    if isinstance(jobs, dict) and "error" in jobs:
        print(f"{_RED}Cannot connect to API: {jobs['error']}{_RESET}")
        return

    pipeline_jobs = [
        j for j in jobs
        if j["attributes"]["pipeline_id"] == pipeline_id
    ]

    if not pipeline_jobs:
        print(f"No jobs found for pipeline {pipeline_id}")
        return

    print(f"\n{_BOLD}{'=' * 72}")
    print(f"  PIPELINE: {pipeline_id}")
    print(f"{'=' * 72}{_RESET}\n")

    for j in pipeline_jobs[:5]:
        a = j["attributes"]
        jid = j["id"]
        status_color = {
            "running": _CYAN,
            "completed": _GREEN,
            "failed": _RED,
            "partial": _YELLOW,
        }.get(a["status"], "")

        hb_status, hb_detail = _heartbeat_age(jid)
        print(f"{_BOLD}Job {jid[:8]}...{_RESET}  {status_color}{a['status']}{_RESET}"
              f"  |  {_time_ago(a['created_at'])}")
        if a.get("error_message"):
            print(f"  Error: {_RED}{a['error_message'][:100]}{_RESET}")
        if a["status"] == "running":
            print(f"  Heartbeat: {hb_status} | {hb_detail}")
        _show_job_runs(jid)
        print()


def main():
    args = sys.argv[1:]
    watch = False
    interval = 5
    target_pipeline = None

    for arg in args:
        if arg == "--watch":
            watch = True
        elif arg.startswith("--"):
            pass
        elif arg.isdigit() and watch:
            interval = int(arg)
        else:
            target_pipeline = arg

    try:
        while True:
            _clear()
            if target_pipeline:
                show_pipeline(target_pipeline)
            else:
                show_overview()
            if not watch:
                break
            print(f"\n{_DIM}Refreshing every {interval}s... (Ctrl+C to stop){_RESET}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n{_DIM}Stopped.{_RESET}")


if __name__ == "__main__":
    main()
