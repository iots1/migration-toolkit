"""
Migration script: create jobs table + add job_id to pipeline_runs + drop legacy columns.

Run once on existing databases:
    python3.11 scripts/migrate_add_jobs_table.py
"""

from __future__ import annotations

from sqlalchemy import text
from repositories.connection import get_engine


LEGACY_COLUMNS = [
    "source_datasource",
    "target_datasource",
    "source_charset",
    "batch_size",
    "started_at",
]


def migrate() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS jobs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
                status VARCHAR(50) DEFAULT 'running',
                completed_at TIMESTAMP WITH TIME ZONE,
                error_message TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                created_by UUID,
                updated_by UUID,
                is_deleted BOOLEAN NOT NULL DEFAULT false,
                deleted_at TIMESTAMP WITH TIME ZONE,
                deleted_by UUID,
                deleted_reason TEXT
            )
        """)
        )
        print("jobs table created (or already exists)")

        conn.execute(
            text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'pipeline_runs' AND column_name = 'job_id'
                ) THEN
                    ALTER TABLE pipeline_runs
                        ADD COLUMN job_id UUID REFERENCES jobs(id) ON DELETE SET NULL;
                END IF;
            END;
            $$
        """)
        )
        print("pipeline_runs.job_id column added (or already exists)")

        for col in LEGACY_COLUMNS:
            conn.execute(
                text(f"""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'jobs' AND column_name = '{col}'
                    ) THEN
                        ALTER TABLE jobs DROP COLUMN {col};
                    END IF;
                END;
                $$
            """)
            )
        print(f"legacy columns dropped from jobs: {LEGACY_COLUMNS}")

    print("\nMigration complete.")


if __name__ == "__main__":
    migrate()
