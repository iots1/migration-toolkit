"""
Migration: Add new columns to configs table.

New columns:
  - datasource_source_id UUID FK → datasources(id)
  - datasource_target_id UUID FK → datasources(id)
  - config_type VARCHAR(20) DEFAULT 'std'
  - script TEXT
  - generate_sql TEXT
  - condition TEXT
  - lookup TEXT

Usage:
    python3.11 scripts/migrate_configs_add_columns.py
    # or with custom DATABASE_URL:
    DATABASE_URL=postgresql://user:pass@localhost:5432/his_analyzer \
        python3.11 scripts/migrate_configs_add_columns.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from repositories.connection import get_engine

NEW_COLUMNS = [
    ("datasource_source_id", "UUID REFERENCES datasources(id) ON DELETE SET NULL"),
    ("datasource_target_id", "UUID REFERENCES datasources(id) ON DELETE SET NULL"),
    ("config_type", "VARCHAR(20) DEFAULT 'std'"),
    ("script", "TEXT"),
    ("generate_sql", "TEXT"),
    ("condition", "TEXT"),
    ("lookup", "TEXT"),
]


def migrate():
    engine = get_engine()

    with engine.begin() as conn:
        existing = conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'configs'"
            )
        )
        existing_cols = {row[0] for row in existing.fetchall()}

        added = []
        for col_name, col_def in NEW_COLUMNS:
            if col_name not in existing_cols:
                conn.execute(
                    text(f"ALTER TABLE configs ADD COLUMN {col_name} {col_def}")
                )
                added.append(col_name)
                print(f"  + Added column: {col_name} ({col_def})")
            else:
                print(f"  . Column already exists: {col_name}")

        if not added:
            print("\nAll columns already present — nothing to do.")
        else:
            print(f"\nDone. Added {len(added)} column(s): {', '.join(added)}")


if __name__ == "__main__":
    print("Migrating configs table — adding new columns...")
    migrate()
