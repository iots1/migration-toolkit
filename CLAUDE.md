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

## Architecture Overview

This is a Streamlit-based HIS (Hospital Information System) database migration toolkit with a three-phase pipeline:

1. **Analysis** — `analysis_report/unified_db_analyzer.sh` profiles source DB and outputs CSV/DDL reports
2. **Mapping** — Streamlit UI for mapping source→target columns with optional AI suggestions (`services/ml_mapper.py`)
3. **Migration** — Streamlit UI executes ETL: reads source in batches, applies transformers, inserts to target

### Directory Structure & MVC Pattern

The codebase follows **strict MVC (Model-View-Controller)** architecture. See `CODE_OF_CONDUCT.md` for detailed conventions.

```
├── app.py                          # Router: delegates to controllers/views based on navigation
├── config.py                       # Constants (transformers, validators, DB types)
├── database.py                     # Data access layer (SQLite CRUD)
├── models/                         # Domain models (dataclasses, NO streamlit imports)
│   ├── datasource.py               # Datasource connection profile
│   ├── migration_config.py         # MigrationConfig & MappingItem
│   └── pipeline_config.py          # PipelineConfig & PipelineStep
├── services/                       # Business logic services (pure Python, NO streamlit imports)
│   ├── db_connector.py             # SQLAlchemy engine factory (MySQL, PostgreSQL, SQLite)
│   ├── ml_mapper.py                # AI semantic column mapping
│   ├── transformers.py             # Vectorized data transformations
│   ├── checkpoint_manager.py       # Migration resumability
│   ├── datasource_repository.py    # Query helper for datasources
│   ├── encoding_helper.py          # Character encoding detection
│   ├── migration_logger.py         # Logging service
│   ├── query_builder.py            # SQL query builder
│   ├── migration_executor.py       # ETL execution engine
│   └── pipeline_service.py         # Pipeline orchestration service
├── utils/                          # Utilities (state management, helpers)
│   ├── state_manager.py            # PageState class for session_state
│   └── ui_components.py            # Legacy CSS/dialogs (being migrated)
├── controllers/                    # MVC Controllers (orchestrate state & logic)
│   ├── settings_controller.py      # ✅ Settings page: owns state, fetches data, calls view
│   └── pipeline_controller.py      # ✅ Data Pipeline: multi-step wizard with execution
├── views/                          # Streamlit rendering (DUMB — only receive data + callbacks)
│   ├── settings_view.py            # ✅ Pure rendering, delegates all logic to controller
│   ├── pipeline_view.py            # ✅ Pure rendering for data pipeline
│   ├── schema_mapper.py            # 🚧 Legacy (to refactor)
│   ├── migration_engine.py         # 🚧 Legacy (to refactor)
│   ├── er_diagram.py               # 🚧 Legacy (simple, to refactor)
│   └── file_explorer.py            # 🚧 Legacy (simple, to refactor)
└── views/components/               # Reusable UI components
    ├── shared/                     # Shared across features
    │   ├── dialogs.py              # Generic dialogs (confirm, preview)
    │   └── styles.py               # Global CSS utilities
    ├── schema_mapper/              # Schema mapper components
    │   ├── source_selector.py      # Source table selection
    │   ├── mapping_editor.py       # Column mapping grid
    │   ├── metadata_editor.py      # Config metadata editor
    │   ├── config_actions.py       # Save/load/delete actions
    │   └── history_viewer.py       # Config history viewer
    └── migration/                  # Migration engine components
        ├── step_connections.py     # Source/target connection step
        ├── step_config.py          # Config selection step
        ├── step_review.py          # Pre-execution review step
        └── step_execution.py       # Execution monitoring step
```

### Key Files

| File | Role | Status |
|------|------|--------|
| `app.py` | Router: dispatches to controllers/views | ✅ MVC-ready |
| `database.py` | SQLite CRUD ops | ✅ Pure data layer |
| `config.py` | Constants | ✅ No changes needed |
| `controllers/settings_controller.py` | [PoC] Manages state, fetches data, defines callbacks | ✅ MVC pattern |
| `views/settings_view.py` | [PoC] Pure Streamlit rendering | ✅ MVC pattern |
| `views/schema_mapper.py` | Schema mapping UI | 🚧 Legacy (to refactor) |
| `views/migration_engine.py` | Migration execution UI | 🚧 Legacy (to refactor) |
| `services/ml_mapper.py` | AI semantic mapping | ✅ Pure Python |
| `services/transformers.py` | Data transformation | ✅ Pure Python |

### Data Flow

- **Mapping configs** → saved as JSON blobs in SQLite (`migration_tool.db`), versioned in `config_histories`
- **Migration engine** → reads config, streams source rows in batches through `DataTransformer`, bulk-inserts to target
- **Checkpoints** → stored in `migration_checkpoints/` for resuming interrupted migrations
- **Logs** → written to `migration_logs/migration_NAME_TIMESTAMP.log`

### Config JSON Structure

Core data structure passed between Schema Mapper and Migration Engine:
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

- `ml_mapper.py` — Thai HIS dictionary with acronyms: `HN` (hospital number), `VN` (visit number), `CID` (citizen ID), etc.
- Transformer `BUDDHIST_TO_ISO` — converts Thai Buddhist years (BE = CE + 543)
- `mini_his/full_his_mockup.sql` — 884KB PostgreSQL schema with mock patient/visit data for testing

## Refactoring Roadmap (MVC Transition)

The project is transitioning from mixed-logic views to strict MVC. Order of refactoring:

1. ✅ **Settings** — `controllers/settings_controller.py` + `views/settings_view.py` (PoC complete)
2. 🔜 **ER Diagram** — simple, no state
3. 🔜 **File Explorer** — simple, minimal state
4. 🔜 **Schema Mapper** — complex state management
5. 🔜 **Migration Engine** — most complex (multi-step wizard)

See `CODE_OF_CONDUCT.md` for strict MVC conventions and how to structure each new controller/view pair.
