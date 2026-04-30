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

### Database Setup

```bash
# Create PostgreSQL database
createdb his_analyzer

# Set DATABASE_URL in .env file
echo "DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer" > .env

# Initialize schema (done automatically on first run, or manually:)
python3.11 -c "from repositories.base import init_db; init_db()"
```

### Run the App

```bash
# Streamlit dashboard (MVC frontend)
streamlit run app.py
python3.11 -m streamlit run app.py --server.runOnSave true

# FastAPI backend (REST API + Socket.IO)
python3.11 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
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
python3.11 -m pytest tests/ -v
python test_analysis_simple.py   # AI pattern detection tests
python test_column_analysis.py   # Column analysis tests
```

### Migration (SQLite → PostgreSQL)

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/his_analyzer \
    python3.11 scripts/migrate_sqlite_to_pg.py
```

## Architecture Overview

Dual-interface HIS (Hospital Information System) database migration toolkit with Clean Architecture + SOLID principles.

**Stack**: PostgreSQL + SQLAlchemy + FastAPI + Streamlit + Socket.IO + Python 3.11

**Architecture**: Clean Architecture with MVC pattern, Repository pattern, REST API, and SOLID principles

**Key Achievements**:

- ✅ PostgreSQL backend with full CRUD API (FastAPI)
- ✅ Streamlit MVC dashboard (6 pages)
- ✅ Pipeline workflow with visual node/edge graph (React Flow frontend)
- ✅ Background job execution with Socket.IO real-time events
- ✅ Per-step datasource resolution (mixed PostgreSQL/MSSQL pipelines)
- ✅ SOLID principles fully implemented
- ✅ Repository pattern for data access
- ✅ Protocol interfaces for DI (Dependency Inversion)
- ✅ Registry patterns for transformers/validators/dialects (Open/Closed)

**Last Updated**: 2026-04-22
**Python Version**: 3.11
**Database**: PostgreSQL 18+
**Architecture**: Clean Architecture + SOLID + MVC + REST API
**Status**: ✅ Production Ready

### Directory Structure

```
├── app.py                          # Streamlit router: delegates to controllers
├── config.py                       # Environment configuration
├── database.py                     # Legacy facade (deprecated, being removed)
├── .env.example                    # Environment variables template
│
├── models/                         # Domain models (dataclasses, pure Python)
│   ├── datasource.py               # Datasource connection profile
│   ├── migration_config.py         # ConfigRecord, MigrationConfig, MappingItem
│   ├── job.py                      # JobRecord, JobUpdateRecord
│   └── pipeline_config.py          # PipelineConfig, PipelineStep, PipelineNodeRecord,
│                                   #   PipelineEdgeRecord, PipelineRunRecord
│
├── protocols/                      # Protocol interfaces (DIP - Dependency Inversion)
│   ├── __init__.py
│   └── repository.py               # DatasourceRepository, ConfigRepository,
│                                   #   PipelineRepository, PipelineRunRepository, JobRepository
│
├── repositories/                   # Data access layer (PostgreSQL)
│   ├── __init__.py
│   ├── connection.py               # SQLAlchemy engine singleton
│   ├── base.py                     # DDL + init_db()
│   ├── datasource_repo.py          # Datasource CRUD
│   ├── config_repo.py              # Config CRUD + versioning
│   ├── pipeline_repo.py            # Pipeline CRUD + get_by_id (JOIN nodes/edges/configs)
│   ├── pipeline_node_repo.py       # Pipeline node CRUD
│   ├── pipeline_edge_repo.py       # Pipeline edge CRUD
│   ├── pipeline_run_repo.py        # Pipeline Run CRUD
│   └── job_repo.py                 # Job CRUD
│
├── api/                            # FastAPI REST API + Socket.IO
│   ├── main.py                     # App setup, CORS, router registration, /ws mount
│   ├── socket_manager.py           # Async Socket.IO server + emit_from_thread()
│   ├── base/                       # Shared API infrastructure
│   │   ├── controller.py           # BaseController (generic CRUD)
│   │   ├── service.py              # BaseService with pagination/sanitize
│   │   ├── exceptions.py           # JSON API error handlers
│   │   ├── auth.py                 # API key verification
│   │   ├── query_params.py         # Pagination params
│   │   └── json_api.py             # JSON:API response builder
│   ├── datasources/                # /api/v1/datasources
│   ├── configs/                    # /api/v1/configs (+ /histories, /{id}/versions/{version})
│   ├── pipelines/                  # /api/v1/pipelines (with nodes/edges sub-resources)
│   ├── pipeline_runs/              # /api/v1/pipeline-runs
│   ├── jobs/                       # /api/v1/jobs (POST → trigger background pipeline)
│   │   ├── router.py               # Job routes + emit_fn injection
│   │   ├── service.py              # JobsService (DIP-compliant, no api/ imports)
│   │   ├── schemas.py              # Pydantic schemas
│   │   └── stale_detector.py       # Stale job detection (SRP extraction)
│   ├── data_explorers/             # /api/v1/db-explorers
│   ├── transformers/               # /api/v1/transformers
│   └── validators/                 # /api/v1/validators
│
├── dialects/                       # Database dialects (OCP - Open/Closed)
│   ├── registry.py                 # Dialect registry
│   ├── base.py                     # BaseDialect ABC
│   ├── mysql.py                    # MySQL dialect
│   ├── postgresql.py               # PostgreSQL dialect
│   └── mssql.py                    # MSSQL dialect
│
├── data_transformers/              # Data transformations (OCP - pluggable)
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
│   ├── registry.py                 # @register_validator decorator
│   ├── not_null.py                 # NOT_NULL validator
│   ├── unique.py                   # UNIQUE_CHECK validator
│   └── range_check.py              # RANGE_CHECK validator
│
├── services/                       # Business logic (pure Python, no Streamlit)
│   ├── db_connector.py             # SQLAlchemy engine factory (MySQL, PG, MSSQL)
│   ├── connection_pool.py          # Raw DBAPI connection pool
│   ├── connection_tester.py        # Connection testing
│   ├── schema_inspector.py         # Schema inspection & sampling
│   ├── ml_mapper.py                # AI semantic column mapping
│   ├── migration_executor.py       # ETL execution (single-table)
│   ├── pipeline_service.py         # Pipeline orchestration with per-step datasource resolution
│   ├── checkpoint_manager.py       # Migration resumability
│   ├── encoding_helper.py          # Character encoding detection
│   ├── migration_logger.py         # Logging service
│   ├── query_builder.py            # SQL query builder + batch transform
│   └── datasource_repository.py    # Datasource query helper (legacy facade)
│
├── controllers/                    # MVC Controllers (6/6 complete)
│   ├── settings_controller.py      # Settings page
│   ├── pipeline_controller.py      # Data Pipeline page
│   ├── file_explorer_controller.py # File Explorer page
│   ├── er_diagram_controller.py    # ER Diagram page
│   ├── schema_mapper_controller.py # Schema Mapper page
│   └── migration_engine_controller.py # Migration Engine page
│
├── views/                          # MVC Views (pure rendering)
│   ├── settings_view.py, pipeline_view.py, file_explorer.py
│   ├── er_diagram.py, schema_mapper.py, migration_engine.py
│   └── components/                 # Reusable UI components
│       ├── shared/                 # dialogs, styles
│       ├── schema_mapper/          # source_selector, mapping_editor, config_actions, ...
│       └── migration/              # step_config, step_connections, step_execution, ...
│
├── scripts/                        # Utility scripts
│   ├── migrate_sqlite_to_pg.py     # One-time SQLite → PostgreSQL migration
│   └── migrate_add_jobs_table.py   # Create jobs table + add job_id to pipeline_runs
│
└── tests/                          # Test suite
    └── test_pipeline_service.py
```

### Key Files & Their Status

| File                           | Role                        | Status        |
| ------------------------------ | --------------------------- | ------------- |
| `app.py`                       | Streamlit router            | ✅ Complete   |
| `api/main.py`                  | FastAPI app + Socket.IO      | ✅ Complete   |
| `api/jobs/router.py`           | POST /jobs → trigger pipeline | ✅ Complete   |
| `api/jobs/service.py`          | JobsService (DIP-compliant)   | ✅ Complete   |
| `api/jobs/stale_detector.py`   | Stale job detection (SRP)     | ✅ Complete   |
| `api/socket_manager.py`        | Socket.IO emit from thread  | ✅ Complete   |
| `database.py`                  | Legacy facade               | 🚧 Deprecated |
| `repositories/pipeline_repo.py`| Pipeline CRUD + nodes/edges  | ✅ Complete   |
| `repositories/job_repo.py`     | Job CRUD                    | ✅ Complete   |
| `repositories/pipeline_node_repo.py` | Pipeline node CRUD     | ✅ Complete   |
| `repositories/pipeline_edge_repo.py` | Pipeline edge CRUD     | ✅ Complete   |
| `services/pipeline_service.py` | Pipeline orchestration     | ✅ Complete   |
| `services/migration_executor.py` | Single-table ETL engine   | ✅ Complete   |

### Data Flow

**Dual Interface**:

- **Streamlit** (`app.py` → controllers → views) — Dashboard for config, mapping, pipeline design
- **FastAPI** (`api/main.py` → routers) — REST API for frontend + job triggering

**Pipeline Execution Flow**:

```
Frontend (React Flow) → POST /api/v1/jobs {pipeline_id}
    → pipeline_repo.get_by_id() — loads pipeline + nodes (JOIN configs) + edges (JOIN configs)
    → PipelineExecutor.execute()
        → _resolve_order_from_edges() — topological sort from edges
        → For each node:
            → config_repo.get_content(config_name)
            → _resolve_conn_configs_for_step(config) — resolve datasource UUIDs → db_type → engine
            → run_single_migration(config, src_conn, tgt_conn)
                → generate_sql (priority) or build_select_query (fallback)
                → pd.read_sql() → transform_batch() → batch_insert()
        → Socket.IO events: job:batch, job:error, job:completed
```

**Per-Step Datasource Resolution**:

Each pipeline node points to a config via `pipeline_nodes.config_id`. Each config has
`datasource_source_id` and `datasource_target_id` (UUID FK → datasources). The executor
resolves these per-step, so a single pipeline can mix PostgreSQL and MSSQL datasources.

### Config JSON Structure

```json
{
  "source": { "database": "<display_name>", "table": "<table>" },
  "target": { "database": "<display_name>", "table": "<table>" },
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

**generate_sql** (optional): If set, used as SELECT query instead of auto-generated one.
Transformers still apply after `pd.read_sql(generate_sql)`.

### PostgreSQL Schema

**Tables** (all in PostgreSQL):

| Table              | PK           | Description                                    |
| ------------------ | ------------ | ---------------------------------------------- |
| `datasources`      | SERIAL       | Connection profiles (name, db_type, host, port) |
| `configs`          | UUID         | Migration configs (json_data, versioning)      |
| `config_histories` | UUID         | Config version snapshots                       |
| `pipelines`        | UUID         | Pipeline definitions                            |
| `pipeline_nodes`   | UUID         | Nodes — each links to a config (config_id FK)   |
| `pipeline_edges`   | UUID         | Edges — source_config_uuid → target_config_uuid |
| `pipeline_runs`    | UUID         | Run tracking (status, steps_json, job_id FK)   |
| `jobs`             | UUID         | Job requests (pipeline_id FK, status)          |

**Key Relationships**:

- `pipeline_nodes.config_id` → `configs.id` (each node = one migration config)
- `pipeline_edges.source_config_uuid` → `configs.id` (dependency graph)
- `pipeline_edges.target_config_uuid` → `configs.id`
- `pipeline_runs.job_id` → `jobs.id` ON DELETE SET NULL
- `jobs.pipeline_id` → `pipelines.id` ON DELETE CASCADE
- `configs.datasource_source_id` → `datasources.id` (source DB for config)
- `configs.datasource_target_id` → `datasources.id` (target DB for config)

### Healthcare Domain Notes

- `ml_mapper.py` — Thai HIS dictionary with acronyms: `HN` (hospital number), `VN` (visit number), `CID` (citizen ID), etc.
- Transformer `BUDDHIST_TO_ISO` — converts Thai Buddhist years (BE = CE + 543)
- `mini_his/full_his_mockup.sql` — 884KB PostgreSQL schema with mock patient/visit data

## SOLID Principles Implementation

### ✅ Single Responsibility Principle (SRP)

- Each repository handles ONE domain (datasource, config, pipeline, pipeline_run, job)
- Services split into focused modules (db_connector, connection_pool, migration_executor, pipeline_service)
- Controllers own ONE page's logic; API routers own ONE resource
- **Data Ownership**: Each repo owns exactly ONE database table. A repo MUST NOT query another repo's table. Cross-repo lookups go through the service layer.

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
- `ml_mapper` has no Streamlit dependencies
- **Layer dependency flow is strictly one-directional**:

```
api/ → services/ → repositories/ → models/
 ✓   Dependencies flow inward only
 ✗   NO reverse imports (e.g. services/ MUST NOT import from api/)
```

- When `services/` needs to notify the API layer (e.g. Socket.IO emit), use **callback injection** — the API layer passes a callback function, the service layer calls it without knowing about the API.

```python
# ✅ CORRECT — callback injection (DIP)
class PipelineExecutor:
    def __init__(self, ..., run_event_callback=None):
        self._run_event_callback = run_event_callback  # Injected by API layer

    def _save_batch_record(self, ...):
        if self._run_event_callback:
            self._run_event_callback("pipeline_run:batch", {...})

# ❌ WRONG — service imports from API layer
from api.socket_manager import emit_from_thread  # Breaks DIP
```

- **API services (in `api/*/service.py`) MUST NOT import from `api.socket_manager`**. Use constructor injection: the router passes `emit_fn` to the service.

```python
# ✅ CORRECT — router injects, service receives
# api/jobs/router.py
from api.socket_manager import emit_from_thread
service = JobsService(emit_fn=emit_from_thread)

# api/jobs/service.py
class JobsService(BaseService):
    def __init__(self, emit_fn=None):
        self._emit_fn = emit_fn  # No api/ imports needed
```

## API Service Layer Patterns

### Template Method Pattern (BaseService)

All API services extend `BaseService` which provides a concrete `find_all()` implementation via the **Template Method** pattern. Subclasses implement two hooks:

```python
# ✅ CORRECT — implement only the two hooks
class DatasourcesService(BaseService):
    def _count_all(self) -> int:
        return datasource_repo.count_all()

    def _list_all(self) -> list[dict]:
        return datasource_repo.get_all_list()

# For post-pagination transforms (e.g., attaching child records):
class PipelinesService(BaseService):
    def _count_all(self) -> int:
        return pipeline_repo.count_all()

    def _list_all(self) -> list[dict]:
        return pipeline_repo.get_all_list()

    def _post_process_page(self, page_data: list[dict]) -> list[dict]:
        return self._attach_children(page_data)

# ❌ WRONG — overriding find_all() directly (bypasses template method)
class MyService(BaseService):
    def find_all(self, params):  # Don't do this — use _count_all / _list_all
        ...
```

### UUID Validation Helper

Use `_parse_uuid()` from BaseService instead of manual try/except:

```python
# ✅ CORRECT
def find_by_id(self, id: str) -> dict:
    run_id = self._parse_uuid(id)
    ...

# ❌ WRONG — duplicate UUID validation
try:
    run_id = uuid.UUID(id)
except ValueError:
    raise HTTPException(status_code=400, detail=f"Invalid UUID: {id}")
```

### Field Merge Helper

Use `_merge_fields()` for patch-style updates where request data overrides existing values:

```python
# ✅ CORRECT — concise merge
def update(self, id, data):
    existing = self.find_by_id(id)
    merged = self._merge_fields(data, existing, ["name", "db_type", "host", ...])
    record = DatasourceRecord(**merged)

# ❌ WRONG — manual per-field merge
record = DatasourceRecord(
    name=data.get("name") if "name" in data else existing.get("name", ""),
    db_type=data.get("db_type") if "db_type" in data else existing.get("db_type", ""),
    ...
)
```

### Router → Service Boundary

Routers contain **only HTTP concerns** (response formatting, status codes). All business logic belongs in the service:

```python
# ✅ CORRECT — router delegates to service
@router.get("/{datasource_id}/tables")
def list_tables(datasource_id: str):
    data = service.get_tables(datasource_id)
    return create_collection_response("datasource_tables", data, ...)

# ❌ WRONG — business logic in router
@router.get("/{datasource_id}/tables")
def list_tables(datasource_id: str):
    ds = get_datasource(datasource_id)  # Repo call in router!
    kw = _datasource_kwargs(ds)          # Business logic in router!
    ok, result = get_tables(**kw)
    ...
```

### Dependency Injection for External Dependencies

Services that need external resources (Socket.IO, etc.) receive them via constructor injection from the router:

```python
# ✅ CORRECT
service = JobsService(emit_fn=emit_from_thread)  # Router injects

# ❌ WRONG — service imports from API layer
class JobsService(BaseService):
    from api.socket_manager import emit_from_thread  # Breaks DIP
```

## RESTful API Design

### Resource-Oriented URIs (OOP Style)

All endpoints follow **OOP style** (resource nouns) — **NOT RPC style** (verb-based actions).

```
✅ OOP Style (Resource-Oriented)          ❌ RPC Style (Action-Oriented)
GET    /api/v1/pipelines                   GET    /api/v1/pipelines/get-all
GET    /api/v1/pipelines/{id}              GET    /api/v1/pipelines/get-by-id
GET    /api/v1/pipelines/{id}/nodes        GET    /api/v1/pipelines/get-nodes
POST   /api/v1/jobs                        POST   /api/v1/jobs/trigger
```

### Query Params for Filtering and Search

Use a **single GET endpoint with query parameters** instead of creating multiple endpoints for variations.

```
✅ Single endpoint + query params                         ❌ Multiple endpoints
GET /api/v1/pipelines?filter=name||$eq||MyPipeline        GET /api/v1/pipelines/get-by-name
GET /api/v1/pipelines?filter=status||$eq||running         GET /api/v1/pipelines/get-running
GET /api/v1/jobs?filter=pipeline_id||$eq||{uuid}          GET /api/v1/pipelines/{id}/jobs
GET /api/v1/datasources?s=PostgreSQL                      GET /api/v1/datasources/search
GET /api/v1/pipeline-runs?filter=job_id||$eq||{uuid}      GET /api/v1/jobs/{id}/pipeline-runs
```

**Query param operators:**

| Param    | Format                              | Example                                      |
|----------|-------------------------------------|----------------------------------------------|
| `filter` | `{field}\|\|$op\|\|{value}`         | `filter=name\|\|$eq\|\|MyPipeline`            |
| `s`      | Full-text search (shorthand)        | `s=postgres`                                  |
| `sort`   | `{field}:{direction}`               | `sort=created_at:desc`                        |
| `page`   | Page number                         | `page=2`                                      |
| `limit`  | Items per page                      | `limit=25`                                    |

**Supported filter operators:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$like`, `$ilike`, `$in`, `$isnull`

### Sub-Resources (Parent-Child Relationships)

Sub-resources under a parent entity are acceptable for direct ownership relationships:

```
GET /api/v1/pipelines/{id}/nodes          ← pipeline_nodes belongs to pipeline
GET /api/v1/pipelines/{id}/edges          ← pipeline_edges belongs to pipeline
GET /api/v1/configs/{id}/histories        ← config_histories belongs to config
GET /api/v1/configs/{id}/versions/{ver}   ← specific version of a config
```

**BUT prefer query params when the relationship is a filter, not ownership:**

```
✅ Query param (filter by FK)
GET /api/v1/jobs?filter=pipeline_id||$eq||{uuid}

✅ Sub-resource (direct ownership)
GET /api/v1/pipelines/{id}/nodes
```

### PATCH for Partial Updates and State Transitions

`PATCH` on a sub-resource or specific attribute is acceptable for **partial updates** and **state transitions**:

```
✅ ACCEPTABLE — Partial update / state transition
PATCH /api/v1/jobs/{id}/status          { "status": "cancelled" }
PATCH /api/v1/pipeline-runs/{id}/status { "status": "failed" }
```

This is the **only exception** to OOP-style URIs. Use sparingly — only for operations that modify part of a resource or transition state, where a full `PUT` would be wasteful.

## Important Notes

⚠️ **Database.py Facade Being Removed**

- Import from repositories directly:
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

⚠️ **Datasource Resolution in Pipelines**

- Pipeline executor resolves datasource connections **per-step** from `config._datasource_source_id` / `_datasource_target_id`
- Do NOT pass a single `source_conn_config`/`target_conn_config` for the whole pipeline — each step may use different databases (e.g., PostgreSQL source + MSSQL source in same pipeline)
- Datasource lookup is always by UUID, never by display name

⚠️ **generate_sql Priority**

- If `config.generate_sql` is set and non-empty, it is used as the SELECT query
- Dynamic `build_select_query()` is only used as fallback when `generate_sql` is null
- Transformers still apply after `pd.read_sql()` regardless of which SELECT is used

---

**Last Updated**: 2026-04-22
**Python Version**: 3.11
**Database**: PostgreSQL 18+
**Architecture**: Clean Architecture + SOLID + MVC + REST API
**Status**: ✅ Production Ready

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **his-analyzer** (4365 symbols, 8239 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/his-analyzer/context` | Codebase overview, check index freshness |
| `gitnexus://repo/his-analyzer/clusters` | All functional areas |
| `gitnexus://repo/his-analyzer/processes` | All execution flows |
| `gitnexus://repo/his-analyzer/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
