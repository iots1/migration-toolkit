# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Setup
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Run the App
```bash
streamlit run app.py
# With hot-reload:
python3.11 -m streamlit run app.py --server.runOnSave true
```

### Database Analysis (Bash)
```bash
cd analysis_report/
# Configure config.json with DB credentials, then:
bash unified_db_analyzer.sh
# Output: migration_report/YYYYMMDD_HHMM/{ddl_schema/, data_profile/, process.log}
```

### Tests
```bash
python test_analysis_simple.py   # AI pattern detection tests
python test_column_analysis.py   # Column analysis tests
```

## Architecture

This is a Streamlit-based HIS (Hospital Information System) database migration toolkit. The pipeline has three phases:

1. **Analysis** — `analysis_report/unified_db_analyzer.sh` profiles a source DB and outputs CSV/DDL reports
2. **Mapping** — Streamlit UI (`views/schema_mapper.py`) for mapping source→target columns, optionally using AI-powered suggestions from `services/ml_mapper.py`
3. **Migration** — Streamlit UI (`views/migration_engine.py`) executes the ETL: reads source in batches, applies transformers, inserts to target

### Key Files

| File | Role |
|------|------|
| `app.py` | Streamlit routing and sidebar navigation |
| `config.py` | Constants: `TRANSFORMER_OPTIONS` (20 types), `VALIDATOR_OPTIONS` (10 types), `DB_TYPES` |
| `database.py` | SQLite CRUD for `datasources`, `configs`, and `config_histories` tables |
| `views/schema_mapper.py` | Column mapping UI + AI auto-mapping |
| `views/migration_engine.py` | ETL execution with batch processing, checkpoints, streaming logs |
| `views/settings.py` | Datasource management + config version history/rollback |
| `services/db_connector.py` | SQLAlchemy engine factory for MySQL, PostgreSQL, MSSQL |
| `services/ml_mapper.py` | `SmartMapper` class: HIS dictionary + sentence-transformer semantic matching |
| `services/transformers.py` | `DataTransformer` class: vectorized Pandas transformations |

### Data Flow

- **Mapping configs** are saved as JSON blobs in SQLite (`migration_tool.db`), versioned in `config_histories`
- **Migration** reads the saved config, streams source rows in batches through `DataTransformer`, then bulk-inserts to target
- **Checkpoints** in `migration_checkpoints/` allow resuming interrupted migrations
- **Logs** written to `migration_logs/migration_NAME_TIMESTAMP.log`

### Config JSON Structure

The core data structure passed between Schema Mapper and Migration Engine:
```json
{
  "source": {"database": "<datasource_id or run_id_XXX>", "table": "<table>"},
  "target": {"database": "<datasource_id>", "table": "<table>"},
  "mappings": [
    {"source": "col_a", "target": "col_b", "transformers": ["TRIM"], "validators": [], "ignore": false}
  ]
}
```

### Healthcare Domain Notes

- `ml_mapper.py` contains a Thai HIS dictionary with domain acronyms: `HN` (hospital number), `VN` (visit number), `CID` (citizen ID), etc.
- Transformer `BUDDHIST_TO_ISO` converts Thai Buddhist calendar years (BE = CE + 543)
- `mini_his/full_his_mockup.sql` is an 884KB PostgreSQL schema with mock patient/visit data for testing
