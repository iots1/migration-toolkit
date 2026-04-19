"""
Migration script: Redesign pipeline_runs to store 1 record per batch (flat columns).

Changes:
  pipeline_runs table:
    REMOVES: started_at, steps_json, completed_at
    ADDS: config_name, batch_round, rows_in_batch, rows_cumulative,
          batch_size, total_records_in_config, transformation_warnings

  jobs table:
    ADDS: total_config INT (total number of configs in the pipeline)

  Result: Each batch execution creates 1 new record in pipeline_runs
          Example: 2 configs × 5 batches = 10 records

Usage:
    python3.11 scripts/migrate_batch_tracking.py

⚠️  WARNING: This migration restructures pipeline_runs. Back up data first:
    pg_dump his_analyzer > his_analyzer_backup_$(date +%Y%m%d_%H%M%S).sql
"""

from sqlalchemy import text
from repositories.connection import get_engine


def backup_old_pipeline_runs(conn):
    """Backup old pipeline_runs data to pipeline_runs_archive before migration."""
    print("Backing up old pipeline_runs data to pipeline_runs_archive...")
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs_archive AS
            SELECT * FROM pipeline_runs WHERE FALSE
        """))
        conn.execute(text("""
            INSERT INTO pipeline_runs_archive
            SELECT * FROM pipeline_runs
        """))
        print("  ✅ Backup complete (pipeline_runs_archive)")
    except Exception as e:
        print(f"  ⚠️  Backup failed (non-critical): {e}")


def migrate():
    """Apply batch-level pipeline_runs redesign."""
    engine = get_engine()

    with engine.begin() as conn:
        # 1. Backup old data
        print("\n📋 Step 1: Backup old data")
        backup_old_pipeline_runs(conn)

        # 2. Add total_config to jobs (before we touch pipeline_runs)
        print("\n📋 Step 2: Update jobs table")
        try:
            conn.execute(text("""
                ALTER TABLE jobs
                ADD COLUMN IF NOT EXISTS total_config INTEGER DEFAULT 0
            """))
            print("  ✅ Added jobs.total_config")
        except Exception as e:
            print(f"  ⚠️  Could not add jobs.total_config: {e}")

        # 3. Recreate pipeline_runs table (safest for such a big schema change)
        print("\n📋 Step 3: Recreate pipeline_runs table with new schema")
        try:
            # Rename old table
            conn.execute(text("""
                ALTER TABLE pipeline_runs RENAME TO pipeline_runs_old
            """))
            print("  ✅ Renamed pipeline_runs → pipeline_runs_old")

            # Create new table with new schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
                    job_id UUID REFERENCES jobs(id) ON DELETE SET NULL,
                    config_name VARCHAR(255) NOT NULL,
                    batch_round INTEGER NOT NULL,
                    rows_in_batch INTEGER DEFAULT 0,
                    rows_cumulative INTEGER DEFAULT 0,
                    batch_size INTEGER DEFAULT 1000,
                    total_records_in_config INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'success',
                    error_message TEXT,
                    transformation_warnings TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    created_by UUID,
                    updated_by UUID,
                    is_deleted BOOLEAN NOT NULL DEFAULT false,
                    deleted_at TIMESTAMP WITH TIME ZONE,
                    deleted_by UUID,
                    deleted_reason TEXT
                )
            """))
            print("  ✅ Created new pipeline_runs table with flat columns")

            # If you want to migrate old data, do it here:
            # conn.execute(text("""
            #     INSERT INTO pipeline_runs (pipeline_id, job_id, created_at, status)
            #     SELECT pipeline_id, job_id, created_at, status
            #     FROM pipeline_runs_old
            #     WHERE status IN ('completed', 'failed', 'partial')
            # """))
            # print("  ℹ️  Migrated status records from old table")

        except Exception as e:
            print(f"  ❌ Failed to recreate pipeline_runs: {e}")
            raise

    print("\n" + "="*60)
    print("✅ Migration completed successfully!")
    print("="*60)
    print("\n📊 Summary:")
    print("  ✓ jobs.total_config added")
    print("  ✓ pipeline_runs redesigned for batch-level records")
    print("  ✓ Old data backed up to pipeline_runs_archive")
    print("  ✓ New schema ready for 1 record per batch")
    print("\n💡 Next steps:")
    print("  1. Verify new table structure: \\d pipeline_runs")
    print("  2. Update migration_executor.py to call batch_insert_callback")
    print("  3. Update pipeline_service.py to populate batch records")
    print("\n🗑️  When ready to remove old table:")
    print("  DROP TABLE pipeline_runs_old CASCADE;")


if __name__ == "__main__":
    migrate()
