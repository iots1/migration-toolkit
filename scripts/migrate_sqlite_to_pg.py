#!/usr/bin/env python3
"""
One-time migration: SQLite → PostgreSQL with validation

Usage:
    DATABASE_URL=postgresql://user:pass@localhost:5432/migration_toolkit \\
        python scripts/migrate_sqlite_to_pg.py

This script:
1. Checks if migration_tool.db exists
2. Reads all data from SQLite
3. Migrates to PostgreSQL with proper UUID handling
4. Validates data integrity
5. Reports results
"""
import os
import sys
import sqlite3
import uuid
import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repositories.connection import get_transaction
from repositories.base import init_db


def get_row_counts(conn, table: str) -> int:
    """Get row count for a table."""
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
    return result.scalar()


def migrate_table(source_conn, table_name: str, columns: list[str], id_column: str = "id"):
    """Migrate a single table from SQLite to PostgreSQL with UUID remapping."""
    # Read from SQLite
    df = pd.read_sql_query(f"SELECT {', '.join(columns)} FROM {table_name}", source_conn)

    # Build UUID remapping table (SQLite UUID → PostgreSQL UUID)
    uuid_map = {}  # Maps old UUID string → new UUID object

    # For configs table, we need to track old → new UUID mapping
    if table_name == "configs":
        # Insert configs first, get their new UUIDs
        with get_transaction() as conn:
            for _, row in df.iterrows():
                # Insert configs with auto-generated UUID
                conn.execute(text("""
                    INSERT INTO configs (config_name, table_name, json_data, updated_at)
                    VALUES (:config_name, :table_name, :json_data, CURRENT_TIMESTAMP)
                    ON CONFLICT (config_name) DO UPDATE SET
                        table_name = EXCLUDED.table_name,
                        json_data = EXCLUDED.json_data,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING id, config_name
                """), {
                    "config_name": row["config_name"],
                    "table_name": row["table_name"],
                    "json_data": row["json_data"]
                })
                result = conn.execute(text("SELECT id, config_name FROM configs WHERE config_name = :name"),
                    {"name": row["config_name"]})
                new_id, config_name = result.fetchone()
                # Store mapping
                old_id = row["id"]
                uuid_map[old_id] = new_id

        # Return early - no actual data inserted yet (just configs)
        return len(df)

    # For other tables, convert UUID strings to UUID objects
    if table_name in ["config_histories", "pipelines", "pipeline_runs"]:
        for col in ["id", "config_id", "pipeline_id"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: uuid.UUID(x) if pd.notna(x) and x else None)

    # Write to PostgreSQL
    with get_transaction() as conn:
        for _, row in df.iterrows():
            # Build column names and placeholders
            cols = row.index.tolist()
            placeholders = [f":{col}" for col in cols]

            # Build INSERT query
            query = f"""
                INSERT INTO {table_name} ({', '.join(cols)})
                VALUES ({', '.join(placeholders)})
            """

            # Convert row to dict, handling NaN/None
            row_dict = {col: (None if pd.isna(val) else val) for col, val in row.items()}
            conn.execute(text(query), row_dict)

    return len(df)


def clear_tables() -> None:
    """Clear all tables before migration (for clean migration)."""
    print("🗑️  Clearing existing data...")
    tables_reversed = ["pipeline_runs", "pipelines", "config_histories", "configs", "datasources"]

    with get_transaction() as conn:
        for table in tables_reversed:
            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    print("   ✅ Tables cleared")


def migrate():
    """Execute migration from SQLite to PostgreSQL."""
    print("🚀 Starting SQLite → PostgreSQL migration...")
    load_dotenv()

    # Connect to source
    sqlite_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'migration_tool.db')
    if not os.path.exists(sqlite_path):
        print("⚠️  No migration_tool.db found — nothing to migrate")
        print("   This appears to be a fresh PostgreSQL installation.")
        print("✅ Skipping migration - ready for fresh start!")
        return

    print(f"📂 Found SQLite database: {sqlite_path}")

    source = sqlite3.connect(sqlite_path)
    source.row_factory = sqlite3.Row  # Access columns by name

    # Record source row counts
    print("\n📊 Recording source row counts...")
    tables = ["datasources", "configs", "config_histories", "pipelines", "pipeline_runs"]
    sqlite_counts = {}

    for table in tables:
        try:
            cursor = source.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            sqlite_counts[table] = count
            print(f"   {table}: {count} rows")
        except Exception as e:
            print(f"   ⚠️  {table}: Error counting - {e}")
            sqlite_counts[table] = 0

    # Clear existing data (for clean migration)
    clear_tables()

    # Initialize PostgreSQL schema
    print("\n🔧 Initializing PostgreSQL schema...")
    init_db()

    # Migrate each table (order matters for foreign keys!)
    print("\n📦 Migrating data...")
    migrated_counts = {}

    try:
        # datasources (no dependencies)
        print("   Migrating datasources...")
        count = migrate_table(
            source,
            "datasources",
            ["id", "name", "db_type", "host", "port", "dbname", "username", "password"],
            "name"
        )
        migrated_counts["datasources"] = count

        # configs (referenced by config_histories)
        print("   Migrating configs...")
        count = migrate_table(
            source,
            "configs",
            ["id", "config_name", "table_name", "json_data", "updated_at"],
            "config_name"
        )
        migrated_counts["configs"] = count

        # config_histories (depends on configs)
        print("   Migrating config_histories...")
        count = migrate_table(
            source,
            "config_histories",
            ["id", "config_id", "version", "json_data", "created_at"],
            "id"
        )
        migrated_counts["config_histories"] = count

        # pipelines (referenced by pipeline_runs)
        print("   Migrating pipelines...")
        count = migrate_table(
            source,
            "pipelines",
            ["id", "name", "description", "json_data", "error_strategy", "created_at", "updated_at"],
            "name"
        )
        migrated_counts["pipelines"] = count

        # pipeline_runs (depends on pipelines)
        print("   Migrating pipeline_runs...")
        count = migrate_table(
            source,
            "pipeline_runs",
            ["id", "pipeline_id", "status", "started_at", "completed_at", "steps_json", "error_message"],
            "id"
        )
        migrated_counts["pipeline_runs"] = count

    except Exception as e:
        print(f"\n❌ Migration failed: {str(e)}")
        source.close()
        sys.exit(1)

    source.close()

    # Validate migration
    print("\n✅ Validating migration...")
    pg_counts = {}
    with get_transaction() as conn:
        for table in tables:
            pg_counts[table] = get_row_counts(conn, table)

    all_valid = True
    for table in tables:
        sqlite_count = sqlite_counts[table]
        pg_count = pg_counts[table]
        if sqlite_count == pg_count:
            print(f"   ✅ {table}: {pg_count} rows (matches)")
        else:
            print(f"   ❌ {table}: SQLite={sqlite_count}, PG={pg_count} (MISMATCH!)")
            all_valid = False

    if all_valid:
        print("\n🎉 Migration completed successfully!")
        print(f"   Total rows migrated: {sum(migrated_counts.values())}")
        print("\n📝 Next steps:")
        print("   1. Test the application: streamlit run app.py")
        print("   2. Verify all pages work correctly")
        print("   3. Once verified, you can archive/remove migration_tool.db")
    else:
        print("\n❌ Migration validation failed! Please review errors above.")
        sys.exit(1)


if __name__ == "__main__":
    migrate()
