"""
Migration: Add resilience columns to jobs table.

Adds:
    - last_heartbeat TIMESTAMPTZ — for stale job detection
    - summary JSONB — for post-migration summary data

Usage:
    python3.11 -m scripts.migrate_add_resilience_columns
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from repositories.connection import get_engine
from sqlalchemy import text


def migrate() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE jobs
            ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMPTZ
        """))
        conn.execute(text("""
            ALTER TABLE jobs
            ADD COLUMN IF NOT EXISTS summary JSONB
        """))
    print("Migration complete: added last_heartbeat and summary to jobs table")


if __name__ == "__main__":
    migrate()
