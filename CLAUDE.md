# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Setup
```bash
python3.12 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Database Setup
```bash
# Create PostgreSQL database
createdb his_analyzer

# Set DATABASE_URL in .env file
echo "DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer" > .env

# Initialize schema (done automatically on first run, or manually:)
python3.12 -c "from repositories.base import init_db; init_db()"
```

### Run the App
```bash
streamlit run app.py
# With hot-reload:
python3.12 -m streamlit run app.py --server.runOnSave true
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
python3.12 -m pytest tests/ -v
python test_analysis_simple.py   # AI pattern detection tests
python test_column_analysis.py   # Column analysis tests
```

### Migration (SQLite → PostgreSQL)
```bash
# If migrating from legacy SQLite database
DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer \
    python3.12 scripts/migrate_sqlite_to_pg.py
```

## Architecture Overview

This is a Streamlit-based HIS (Hospital Information System) database migration toolkit with Clean Architecture + SOLID principles:

**Stack**: PostgreSQL + SQLAlchemy + Streamlit + Python 3.12

**Architecture**: Clean Architecture with MVC pattern, Repository pattern, and SOLID principles

**Key Achievements**:
- ✅ Migrated from SQLite to PostgreSQL (Phase 1-10 complete)
- ✅ SOLID principles fully implemented
- ✅ Repository pattern for data access
- ✅ Protocol interfaces for DI (Dependency Inversion)
- ✅ Registry patterns for transformers/validators/dialects (Open/Closed)
- ✅ Strict MVC separation for all pages
- ✅ Thread-safe connection pooling

### Directory Structure (PostgreSQL + SOLID)

```
├── app.py                          # Router: delegates to controllers
├── config.py                       # Environment configuration
├── database.py                     # Legacy facade (deprecated, being removed)
├── .env.example                    # Environment variables template
│
├── models/                         # Domain models (dataclasses, pure Python)
│   ├── datasource.py               # Datasource connection profile
│   ├── migration_config.py         # MigrationConfig & MappingItem
│   └── pipeline_config.py          # PipelineConfig & PipelineStep
│
├── protocols/                      # Protocol interfaces (DIP - Dependency Inversion)
│   ├── __init__.py
│   ├── repository.py               # Repository protocol interfaces
│   ├── database_dialect.py         # Database dialect protocol
│   └── transformer.py              # Transformer protocol
│
├── repositories/                   # Data access layer (PostgreSQL)
│   ├── __init__.py
│   ├── connection.py               # SQLAlchemy engine singleton
│   ├── base.py                     # DDL + init_db()
│   ├── datasource_repo.py          # Datasource CRUD
│   ├── config_repo.py              # Config CRUD + versioning
│   ├── pipeline_repo.py            # Pipeline CRUD
│   └── pipeline_run_repo.py        # Pipeline Run CRUD
│
├── dialects/                       # Database dialects (OCP - Open/Closed)
│   ├── __init__.py
│   ├── registry.py                 # Dialect registry
│   ├── base.py                     # BaseDialect ABC
│   ├── mysql.py                    # MySQL dialect
│   ├── postgresql.py               # PostgreSQL dialect
│   └── mssql.py                    # MSSQL dialect
│
├── data_transformers/              # Data transformations (OCP - pluggable)
│   ├── __init__.py
│   ├── registry.py                 # @register_transformer decorator
│   ├── base.py                     # DataTransformer class
│   ├── text.py                     # Text transformers (TRIM, UPPER, etc.)
│   ├── dates.py                    # Date transformers
│   ├── healthcare.py               # Healthcare-specific transformers
│   ├── names.py                    # Name transformers
│   ├── data_type.py                # Data type transformers
│   └── lookup.py                   # Lookup transformers
│
├── validators/                     # Data validators (OCP - pluggable)
│   ├── __init__.py
│   ├── registry.py                 # @register_validator decorator
│   ├── not_null.py                 # NOT_NULL validator
│   ├── unique.py                   # UNIQUE_CHECK validator
│   └── range_check.py              # RANGE_CHECK validator
│
├── services/                       # Business logic (pure Python)
│   ├── db_connector.py             # SQLAlchemy engine factory (slim)
│   ├── connection_pool.py          # Raw DBAPI connection pool
│   ├── connection_tester.py        # Connection testing
│   ├── schema_inspector.py         # Schema inspection & sampling
│   ├── ml_mapper.py                # AI semantic column mapping (no Streamlit)
│   ├── migration_executor.py       # ETL execution engine
│   ├── pipeline_service.py         # Pipeline orchestration (DI-compliant)
│   ├── checkpoint_manager.py       # Migration resumability
│   ├── encoding_helper.py          # Character encoding detection
│   ├── migration_logger.py         # Logging service
│   ├── query_builder.py            # SQL query builder
│   └── datasource_repository.py    # Datasource query helper
│
├── controllers/                    # MVC Controllers (6/6 complete)
│   ├── __init__.py
│   ├── settings_controller.py      # ✅ Settings page
│   ├── pipeline_controller.py      # ✅ Data Pipeline page
│   ├── file_explorer_controller.py # ✅ File Explorer page
│   ├── er_diagram_controller.py    # ✅ ER Diagram page
│   ├── schema_mapper_controller.py # ✅ Schema Mapper page
│   └── migration_engine_controller.py # ✅ Migration Engine page
│
├── views/                          # MVC Views (pure rendering)
│   ├── settings_view.py            # ✅ Settings page rendering
│   ├── pipeline_view.py            # ✅ Data Pipeline rendering
│   ├── file_explorer.py            # ✅ File Explorer rendering
│   ├── er_diagram.py               # ✅ ER Diagram rendering
│   ├── schema_mapper.py            # ✅ Schema Mapper rendering
│   ├── migration_engine.py         # ✅ Migration Engine rendering
│   └── components/                 # Reusable UI components
│       ├── shared/                 # Shared components
│       │   ├── dialogs.py          # Generic dialogs
│       │   └── styles.py           # Global CSS utilities
│       ├── schema_mapper/          # Schema mapper components
│       └── migration/              # Migration engine components
│
├── scripts/                        # Utility scripts
│   └── migrate_sqlite_to_pg.py     # One-time SQLite → PostgreSQL migration
│
└── tests/                          # Test suite
    ├── test_pipeline_service.py
    └── ...
```

### Key Files & Their Status

| File | Role | Status | Notes |
|------|------|--------|-------|
| `app.py` | Router | ✅ Complete | Routes to all 6 controllers |
| `database.py` | Legacy facade | 🚧 Deprecated | Being removed, use repositories directly |
| `repositories/*.py` | Data access layer | ✅ Complete | PostgreSQL CRUD operations |
| `protocols/*.py` | Protocol interfaces | ✅ Complete | DIP compliance |
| `dialects/*.py` | DB dialects | ✅ Complete | OCP compliance |
| `data_transformers/*.py` | Transformers | ✅ Complete | OCP compliance |
| `validators/*.py` | Validators | ✅ Complete | OCP compliance |
| `controllers/*.py` | MVC Controllers | ✅ 6/6 Complete | All pages refactored |
| `services/pipeline_service.py` | Pipeline orchestration | ✅ Complete | DI-compliant (Phase 8) |

### Data Flow

**PostgreSQL Architecture**:
- **Connection pooling**: Thread-safe via SQLAlchemy Engine singleton
- **Repositories**: Handle all PostgreSQL operations via `repositories/connection.py`
- **Transactions**: Use `get_transaction()` context manager for auto-commit
- **Thread safety**: Each thread gets its own connection from the pool

**Config Storage**:
- **Mapping configs** → Saved as JSON in PostgreSQL `configs` table
- **Version history** → Stored in `config_histories` table
- **Pipelines** → Stored in `pipelines` table with UUID primary keys
- **Pipeline runs** → Stored in `pipeline_runs` table with status tracking

**Migration Engine**:
- **Checkpoints** → Stored in `migration_checkpoints/` (versioned format v2)
- **Logs** → Written to `migration_logs/migration_NAME_TIMESTAMP.log`

### Config JSON Structure

Core data structure passed between Schema Mapper and Migration Engine:
```json
{
  "source": {"database": "<datasource_id>", "table": "<table>"},
  "target": {"database": "<datasource_id>", "table": "<table>"},
  "mappings": [
    {
      "source": "col_a",
      "target": "col_b",
      "transformers": ["TRIM"],
      "validators": [],
      "ignore": false
    }
  ]
}
```

### Healthcare Domain Notes

- `ml_mapper.py` — Thai HIS dictionary with acronyms: `HN` (hospital number), `VN` (visit number), `CID` (citizen ID), etc.
- Transformer `BUDDHIST_TO_ISO` — converts Thai Buddhist years (BE = CE + 543)
- `mini_his/full_his_mockup.sql` — 884KB PostgreSQL schema with mock patient/visit data

### PostgreSQL Schema

**Tables** (all in PostgreSQL):
- `datasources` — Datasource connection profiles (SERIAL PK)
- `configs` — Migration configurations (UUID PK)
- `config_histories` — Config version history (UUID PK)
- `pipelines` — Pipeline definitions (UUID PK)
- `pipeline_runs` — Pipeline execution runs (UUID PK)

**Connection String**:
```bash
DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer
```

## SOLID Principles Implementation

### ✅ Single Responsibility Principle (SRP)
- Each repository handles ONE domain (datasource, config, pipeline, pipeline_run)
- Services split into focused modules (db_connector, connection_pool, schema_inspector, connection_tester)
- Controllers own ONE page's logic

### ✅ Open/Closed Principle (OCP)
- Transformers: Add new transformers via `@register_transformer` decorator
- Validators: Add new validators via `@register_validator` decorator
- Dialects: Add new databases via `dialects/registry.py`

### ✅ Liskov Substitution Principle (LSP)
- Protocol interfaces ensure implementations are interchangeable
- All repository implementations follow the same protocol

### ✅ Interface Segregation Principle (ISP)
- Focused protocol interfaces (DatasourceRepository, ConfigRepository, etc.)
- No fat interfaces

### ✅ Dependency Inversion Principle (DIP)
- Controllers depend on protocol interfaces, not concrete implementations
- `PipelineExecutor` receives repositories via constructor injection
- `ml_mapper` has no Streamlit dependencies (moved caching to controller)

## Migration Status

**All 10 phases complete** ✅

| Phase | Description | Status |
|-------|-------------|--------|
| Phase 1 | PostgreSQL connection setup | ✅ Complete |
| Phase 2 | Repository pattern | ✅ Complete |
| Phase 3 | Protocol interfaces (DIP) | ✅ Complete |
| Phase 4 | Database dialect registry (OCP) | ✅ Complete |
| Phase 5 | Transformer/Validator registries (OCP) | ✅ Complete |
| Phase 6 | ML Mapper refactored (DIP) | ✅ Complete |
| Phase 7 | MVC refactoring (6/6 pages) | ✅ Complete |
| Phase 8 | Pipeline Service DI injection | ✅ Complete |
| Phase 9 | DB Connector split (SRP) | ✅ Complete |
| Phase 10 | Migration script + cleanup | ✅ Complete |

See `plan/migrate-sqlite-to-postgresql.md` for detailed implementation notes.

## Development Guidelines

### Adding New Transformers
```python
# In data_transformers/text.py (or new file)
from data_transformers.registry import register

@register("MY_TRANSFORMER", "My Transformer", "Description")
def my_transformer(series, params=None):
    return series.astype(str).str.upper()
```

### Adding New Validators
```python
# In validators/new_validator.py
from validators.registry import register_validator

@register_validator("MY_VALIDATOR", "My Validator")
def my_validator(series, params=None):
    # Validation logic
    pass
```

### Creating New Controllers
Follow `CODE_OF_CONTRACT.md` for strict MVC conventions:
1. Controller owns all session state
2. Controller fetches all data
3. Controller defines all callbacks
4. View does pure rendering only

### Testing
```bash
# Unit tests
python3.12 -m pytest tests/test_pipeline_service.py -v

# Integration tests
python3.12 -m pytest tests/integration/ -v

# Coverage
python3.12 -m pytest --cov=repositories --cov=services
```

## Important Notes

⚠️ **Database.py Facade Being Removed**
- The `database.py` file is a legacy re-export facade
- Import from repositories directly instead:
  - ❌ `import database as db; db.get_datasources()`
  - ✅ `from repositories.datasource_repo import get_all`

⚠️ **Thread Safety**
- SQLAlchemy Engine is thread-safe
- Connection objects are NOT thread-safe
- Use `get_transaction()` context manager for thread-safe operations

⚠️ **UUID Handling**
- PostgreSQL uses native UUID type
- Pass `uuid.UUID` objects, not strings
- Let PostgreSQL generate UUIDs via `gen_random_uuid()` default

---

**Last Updated**: 2026-04-10
**Python Version**: 3.12
**Database**: PostgreSQL 18+
**Architecture**: Clean Architecture + SOLID + MVC
**Status**: ✅ Production Ready
