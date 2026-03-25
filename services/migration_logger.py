"""
Migration Logger — creates and writes per-run log files.

Responsibility (SRP): file-based logging for migration execution only.
"""
import os
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_logs")


def _safe_name(config_name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in config_name)


def create_log_file(config_name: str) -> str | None:
    """Create a timestamped log file for this migration run. Returns path or None."""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(LOG_DIR, f"migration_{_safe_name(config_name)}_{timestamp}.log")
        return path
    except Exception:
        return None


def write_log(log_file: str, message: str) -> None:
    """Append a timestamped line to the log file."""
    if not log_file:
        return
    try:
        with open(log_file, "a", encoding="utf-8", errors="replace") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        print(f"Error writing to log: {e}")


def read_log_file(log_file: str) -> str | None:
    """Read log content, trying multiple encodings for Thai legacy files."""
    if not log_file or not os.path.exists(log_file):
        return None
    for encoding in ["utf-8", "cp874", "tis-620", "latin-1"]:
        try:
            with open(log_file, "r", encoding=encoding, errors="replace") as f:
                return f.read()
        except Exception:
            continue
    return None
