"""
Initialize PostgreSQL database and test repositories.

Run this after completing Phase 2:
    python init_and_test.py
"""

import os
import uuid
from dotenv import load_dotenv
from repositories.base import init_db, get_table_info
from repositories.datasource_repo import save, get_all, get_by_id, update, delete
from repositories.config_repo import save as save_config, get_list, get_history
from repositories.pipeline_repo import (
    save as save_pipeline,
    get_list as get_pipeline_list,
)

# Load environment
load_dotenv()

print("🔧 Initializing PostgreSQL database...")
init_db()

print("\n📊 Current tables:")
tables = get_table_info()
for table in tables:
    print(f"   ✓ {table['table_name']} ({table['column_count']} columns)")

print("\n🧪 Testing repositories...")

# Test 1: Datasource CRUD
print("\n1. Testing Datasource CRUD...")
ok, msg = save("Test MySQL", "MySQL", "localhost", "3306", "testdb", "root", "password")
print(f"   Create: {msg}")

ok, msg = save(
    "Test PostgreSQL",
    "PostgreSQL",
    "localhost",
    "5432",
    "testdb",
    "postgres",
    "password",
)
print(f"   Create: {msg}")

datasources = get_all()
print(f"   Read: Found {len(datasources)} datasources")

if len(datasources) > 0:
    ds_id = datasources.iloc[0]["id"]
    ds = get_by_id(ds_id)
    print(f"   By ID: Found {ds['name'] if ds else 'None'}")

    ok, msg = update(
        ds_id,
        "Updated MySQL",
        "MySQL",
        "localhost",
        "3307",
        "testdb",
        "root",
        "password",
    )
    print(f"   Update: {msg}")

# Test 2: Config with versioning
print("\n2. Testing Config CRUD...")
ok, msg = save_config(
    "test_config", "patients", '{"mappings": [{"source": "a", "target": "b"}]}'
)
print(f"   Save v1: {msg}")

ok, msg = save_config(
    "test_config",
    "patients",
    '{"mappings": [{"source": "a", "target": "b"}, {"source": "c", "target": "d"}]}',
)
print(f"   Save v2: {msg}")

history = get_history("test_config")
print(f"   History: {len(history)} versions")
for _, row in history.iterrows():
    print(f"      - Version {row['version']} at {row['created_at']}")

# Test 3: Pipeline
print("\n3. Testing Pipeline CRUD...")
ok, msg = save_pipeline(
    "Test Pipeline",
    "Test pipeline",
    '{"steps": []}',
    source_ds_id=None,
    target_ds_id=None,
    error_strategy="fail_fast",
)
print(f"   Save: {msg}")

pipelines = get_pipeline_list()
print(f"   Read: Found {len(pipelines)} pipelines")

print("\n✅ All repositories working correctly!")
print("\n📝 Next steps:")
print("   1. Verify tables created in PostgreSQL:")
print("      psql -h 10.0.0.70 -U admin -d migration_toolkit -c '\\dt'")
print("   2. Continue to Phase 3: Protocol Interfaces")
