import pytest
from models.migration_config import MigrationConfig, MappingItem
from models.datasource import Datasource

# --- MappingItem ---

def test_mapping_item_from_dict_defaults():
    item = MappingItem.from_dict({"source": "col_a", "target": "col_b"})
    assert item.source == "col_a"
    assert item.target == "col_b"
    assert item.ignore is False
    assert item.transformers == []
    assert item.validators == []

def test_mapping_item_roundtrip():
    original = {
        "source": "col_a",
        "target": "col_b",
        "transformers": ["TRIM"],
        "validators": ["REQUIRED"],
        "ignore": False,
        "transformer_params": {},
        "default_value": "N/A",
    }
    item = MappingItem.from_dict(original)
    assert item.to_dict() == original

# --- MigrationConfig ---

def test_migration_config_from_dict():
    raw = {
        "config_name": "patient_map",
        "source": {"database": "src_db", "table": "patients"},
        "target": {"database": "tgt_db", "table": "his_patients"},
        "mappings": [
            {"source": "hn", "target": "hospital_number",
             "transformers": [], "validators": [], "ignore": False,
             "transformer_params": {}, "default_value": ""}
        ],
    }
    cfg = MigrationConfig.from_dict(raw)
    assert cfg.config_name == "patient_map"
    assert cfg.source_table == "patients"
    assert cfg.target_table == "his_patients"
    assert len(cfg.mappings) == 1
    assert cfg.mappings[0].source == "hn"

def test_migration_config_roundtrip():
    raw = {
        "config_name": "test",
        "source": {"database": "s", "table": "t1"},
        "target": {"database": "d", "table": "t2"},
        "mappings": [],
        "batch_size": 500,
    }
    cfg = MigrationConfig.from_dict(raw)
    result = cfg.to_dict()
    assert result["config_name"] == "test"
    assert result["source"]["table"] == "t1"
    assert result["batch_size"] == 500

# --- Datasource ---

def test_datasource_from_dict():
    ds = Datasource.from_dict({
        "id": 1, "name": "MyDB", "db_type": "PostgreSQL",
        "host": "localhost", "port": "5432",
        "dbname": "mydb", "username": "user", "password": "pass",
    })
    assert ds.id == 1
    assert ds.db_type == "PostgreSQL"
    assert ds.port == 5432
