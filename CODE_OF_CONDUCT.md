# CODE_OF_CONDUCT.md

This document establishes strict MVC (Model-View-Controller) architectural conventions for the HIS Migration Toolkit. **All code contributions must adhere to these rules.** This is not optional — it ensures consistency, testability, and maintainability.

**Architecture Status**: ✅ **Complete** — All 6 pages refactored to strict MVC pattern

---

## Quick Reference: The Three Layers

| Layer          | Location                                              | What It Does                                                                      | What It CANNOT Do                                                                    |
| -------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **Model**      | `models/`, `repositories/`, `services/`, `protocols/` | Pure Python: data structures, business logic, DB queries via repositories         | ❌ NO `import streamlit`                                                             |
| **View**       | `views/`, `views/components/`                         | Streamlit rendering ONLY: `st.button`, `st.text_input`, etc.                      | ❌ NO `import database`, `import services.*`, direct `st.session_state` manipulation |
| **Controller** | `controllers/`                                        | Orchestrate: init state, fetch data via repositories, define callbacks, call view | ✅ CAN import repositories, services, models, CAN manipulate `st.session_state`      |

---

## Architecture Overview (PostgreSQL + SOLID)

**Database**: PostgreSQL with UUID primary keys, timezone-aware timestamps
**Connection Pool**: Thread-safe SQLAlchemy Engine singleton
**Pattern**: Repository + Protocol interfaces (DIP), Registry patterns (OCP)
**MVC**: Strict separation for all 6 pages

```
User Request
    ↓
app.py (Router)
    ↓
Controller (Orchestration)
    ├─→ Repositories (Data Access)
    ├─→ Services (Business Logic)
    └─→ View (Pure Rendering)
         ↓
    Streamlit UI
```

---

## Layer 1: Models, Repositories & Services (Pure Python)

### Rule 1.1: NO Streamlit Imports

**Files in `models/`, `repositories/`, `services/`, `protocols/` MUST NEVER import `streamlit`.**

```python
# ❌ WRONG
import streamlit as st
from services.transformers import DataTransformer

# ✅ CORRECT
from services.transformers import DataTransformer
```

### Rule 1.1b: NO API Layer Imports in Services

**Files in `services/` MUST NEVER import from `api/`.** Layer dependencies flow strictly inward:

```
api/ → services/ → repositories/ → models/
 ✓   Dependencies flow inward only
 ✗   NO reverse imports (e.g. services/ MUST NOT import from api/)
```

When `services/` needs to notify the API layer (e.g. Socket.IO emit), use **callback injection** — the API layer passes a callback function, the service layer calls it without knowing about the API.

**This also applies to `api/*/service.py` files — they MUST NOT import from `api.socket_manager`.** Use constructor injection: the router passes `emit_fn` to the service.

```python
# ✅ CORRECT — router injects, service receives
# api/jobs/router.py
from api.socket_manager import emit_from_thread
service = JobsService(emit_fn=emit_from_thread)

# api/jobs/service.py
class JobsService(BaseService):
    def __init__(self, emit_fn=None):
        self._emit_fn = emit_fn  # No api/ imports needed

# ❌ WRONG — service imports from API layer
from api.socket_manager import emit_from_thread  # Breaks DIP
```

### Rule 1.2: Use Repositories for Data Access

**Controllers import from `repositories/`, NOT from `database.py` (legacy facade being removed).**

```python
# ✅ CORRECT - Direct repository imports
from repositories.datasource_repo import get_all, get_by_id, save
from repositories.config_repo import get_list, get_content, save as config_save
from repositories.pipeline_repo import get_list, get_by_name, save as pipeline_save

# ❌ WRONG - Legacy facade (being removed)
import database as db
datasources = db.get_datasources()
```

### Rule 1.2b: Data Ownership — One Repo, One Table

**Each repository owns exactly ONE database table. A repo MUST NOT query another repo's table.**

```python
# ✅ CORRECT — job_repo queries only jobs table
# repositories/job_repo.py
def get_by_pipeline(pipeline_id: uuid.UUID) -> list[dict]:
    """Get jobs for a pipeline — queries ONLY jobs table."""
    ...

# ✅ CORRECT — pipeline_run_repo queries only pipeline_runs table
# repositories/pipeline_run_repo.py
def get_by_job(job_id: uuid.UUID) -> list[dict]:
    """Get pipeline runs for a job — queries ONLY pipeline_runs table."""
    ...

# ❌ WRONG — job_repo queries pipeline_runs table (violates data ownership)
# repositories/job_repo.py
def get_pipeline_runs(job_id: uuid.UUID) -> list[dict]:
    """WRONG: job_repo has no business querying pipeline_runs table."""
    # SELECT ... FROM pipeline_runs WHERE job_id = :job_id
    ...
```

**Cross-repo lookups** (e.g. "get jobs for pipeline X" or "get pipeline_runs for job Y") go through the **service layer**:

```python
# ✅ CORRECT — service orchestrates across repos
class JobsService(BaseService):
    def find_pipeline_runs(self, job_id: str) -> list[dict]:
        return self.execute_db_operation(
            lambda: pipeline_run_repo.get_by_job(jid)  # Uses correct owner
        )
```

### Rule 1.3: Repository Functions Return Tuples for Mutations

**Save/update/delete functions return `(success: bool, message: str)`**

```python
# ✅ GOOD - Repository pattern
from repositories.datasource_repo import save

ok, msg = save("MyDB", "MySQL", "localhost", "3306", "mydb", "user", "pass")
if ok:
    print("Success!")
else:
    print(f"Error: {msg}")

# ❌ WRONG - Direct DB calls in service
def save_datasource(...):
    conn.execute(...)  # Services shouldn't touch DB directly
```

### Rule 1.4: Use Protocol Interfaces for DI (Dependency Inversion)

**Services accept protocol interfaces, not concrete implementations.**

```python
# ✅ GOOD - Protocol-based DI
from typing import Protocol

class ConfigRepository(Protocol):
    def get_content(self, config_name: str) -> dict | None: ...
    def save(self, config_name: str, table_name: str, json_data: str) -> tuple[bool, str]: ...

class MyService:
    def __init__(self, config_repo: ConfigRepository):
        self.config_repo = config_repo  # Can be any implementation
```

### Rule 1.5: Services Are Pure Functions & Classes

**Services handle business logic without side effects. All I/O is explicit.**

```python
# ✅ GOOD
class DataTransformer:
    def apply_trim(self, series: pd.Series) -> pd.Series:
        return series.str.strip()

# ❌ BAD
def fetch_and_display_results():
    results = repository.get_all()
    st.dataframe(results)  # Rendering code in service layer
```

### Rule 1.6: Use Registry Patterns for Extensibility (OCP)

**Add new transformers/validators/dialects via decorators, not by modifying existing code.**

```python
# ✅ GOOD - Open/Closed Principle
from data_transformers.registry import register

@register("MY_TRANSFORMER", "My Transformer", "Description")
def my_transformer(series, params=None):
    return series.astype(str).str.upper()

# ❌ WRONG - Modifying core code
# In transformers.py:
# TRANSFORMER_OPTIONS.append({"name": "MY_TRANSFORMER", ...})
```

---

## Layer 2: Views (Dumb Rendering)

### Rule 2.1: Views Are Pure Streamlit Renderers

**Views MUST ONLY contain `st.*` calls. No business logic, no repository imports.**

```python
# ✅ GOOD
def render_settings_page(datasources_df, configs_df, form_state: dict, callbacks: dict) -> None:
    st.subheader("Settings")
    if st.button("Save", type="primary"):
        callbacks["on_save"]()

# ❌ BAD (business logic in view)
def render_settings_page():
    from repositories.datasource_repo import get_all  # NO!
    datasources = get_all()
    if len(datasources) > 10:
        st.warning("Too many datasources")
```

### Rule 2.2: Views Receive All Data as Arguments

**Views must be 100% determined by their arguments. No global state, no assumptions.**

```python
# ✅ GOOD
def render_datasource_tab(datasources_df, form_state: dict, callbacks: dict) -> None:
    is_edit_mode = form_state["is_edit_mode"]
    if is_edit_mode:
        st.write("Editing mode")

# ❌ BAD (accessing global state)
def render_datasource_tab():
    if st.session_state.is_edit_mode:  # View shouldn't access session_state directly
        st.write("Editing mode")
```

### Rule 2.3: Views Accept Callbacks for All Actions

**Button clicks, form submissions, etc. delegate to callbacks provided by the controller.**

```python
# ✅ GOOD
if st.button("Save Changes", type="primary", use_container_width=True):
    if form_name and form_host:
        ok, msg = callbacks["on_update"](form_id, form_name, form_host, ...)
        if not ok:
            st.error(msg)

# ❌ BAD (view doing the repository call)
if st.button("Save Changes", type="primary"):
    from repositories.datasource_repo import save  # NO!
    save(...)  # Controller should do this
    st.success("Updated!")
```

### Rule 2.4: Views Are Private Functions (Prefix with `_`)

**Private render functions prevent accidental direct imports.**

```python
# views/settings_view.py

def render_settings_page(...) -> None:
    """Public entry point called by controller."""
    _render_datasource_tab(...)
    _render_config_tab(...)

def _render_datasource_tab(...) -> None:
    """Private helper, called only by render_settings_page."""
```

### Rule 2.5: Widget Keys Must Use Session State Keys from Controller

```python
# ✅ GOOD
ds_name = st.text_input("Name", key="new_ds_name")

# ❌ BAD (hardcoded keys outside session_state management)
ds_name = st.text_input("Name", key="some_random_key_12345")
```

---

## Layer 3: Controllers (Orchestration)

### Rule 3.1: Controllers Own All Session State for Their Feature

**Controllers initialize, read, and modify session state for their page.**

```python
# controllers/settings_controller.py

_DEFAULTS: dict = {
    "new_ds_name": "",
    "new_ds_host": "",
    "is_edit_mode": False,
    "edit_ds_id": None,
}

def run() -> None:
    PageState.init(_DEFAULTS)

    # Now session state is safe to use
    is_edit_mode = PageState.get("is_edit_mode")
```

### Rule 3.2: Controllers Fetch All Data via Repositories

**Controllers call `repositories/` to gather data before rendering.**

```python
# ✅ CORRECT - Use repositories directly
from repositories.datasource_repo import get_all
from repositories.config_repo import get_list

def run() -> None:
    PageState.init(_DEFAULTS)

    # Fetch data
    datasources_df = get_all()
    configs_df = get_list()

    # Assemble state snapshot
    form_state = {
        "is_edit_mode": PageState.get("is_edit_mode"),
        "edit_ds_id": PageState.get("edit_ds_id"),
    }

    # Pass to view
    render_settings_page(datasources_df, configs_df, form_state, callbacks)

# ❌ WRONG - Using legacy facade
import database as db  # Don't use this anymore
datasources_df = db.get_datasources()
```

### Rule 3.3: Controllers Define All Action Callbacks

**Every button click, form submission, or data mutation flows through a callback defined in the controller.**

```python
# ✅ GOOD - All callbacks in controller
callbacks = {
    "on_row_select": _on_row_select,
    "on_save_new": _on_save_new,
    "on_update": _on_update,
    "on_delete": _on_delete,
    "on_cancel": _reset_to_new_mode,
    "on_get_data": _on_get_config_content,
}

def _on_row_select(ds_id: int) -> None:
    from repositories.datasource_repo import get_by_id
    full_data = get_by_id(ds_id)
    PageState.set("is_edit_mode", True)
    PageState.set("edit_ds_id", ds_id)
    st.rerun()

def _on_save_new(name: str, host: str, ...) -> tuple[bool, str]:
    from repositories.datasource_repo import save
    ok, msg = save(name, "MySQL", host, "3306", ...)
    if ok:
        PageState.set("trigger_reset", True)
        st.rerun()
    return ok, msg
```

### Rule 3.4: Controllers Call the View's Public Render Function

**The view is called exactly once, at the end of the controller.**

```python
def run() -> None:
    PageState.init(_DEFAULTS)

    # Fetch data
    datasources_df = get_all()
    configs_df = get_list()

    # Define callbacks
    callbacks = {...}

    # Call view ONCE
    render_settings_page(datasources_df, configs_df, form_state, callbacks)  # ← ONLY HERE
```

### Rule 3.5: Controllers Manage State Mutations, Not Views

**Only controllers can call `PageState.set()`. Views never touch session state.**

```python
# ✅ CONTROLLER
def _on_update(...):
    PageState.set("trigger_reset", True)

# ❌ VIEW (NEVER)
def render_form(...):
    # Views NEVER manipulate state
    st.session_state.trigger_reset = True  # ❌ WRONG
```

---

## File Structure & Naming Conventions

### Naming Pattern (PostgreSQL Architecture)

```
feature/
  models/feature_model.py              (if needed, contains @dataclass)
  repositories/feature_repo.py         (PostgreSQL CRUD)
  services/feature_service.py          (if needed, contains logic)
  controllers/feature_controller.py    (owns state, fetches data via repos, calls view)
  views/feature_view.py                (pure rendering)
  views/components/feature/
    sub_component.py                   (reusable sub-components)
```

### Completed Controllers (6/6)

```
✅ controllers/settings_controller.py      + views/settings_view.py
✅ controllers/pipeline_controller.py      + views/pipeline_view.py
✅ controllers/file_explorer_controller.py + views/file_explorer.py
✅ controllers/er_diagram_controller.py    + views/er_diagram.py
✅ controllers/schema_mapper_controller.py + views/schema_mapper.py
✅ controllers/migration_engine_controller.py + views/migration_engine.py
```

---

## Shared Components (Views)

### Rule 4.1: Dialogs Live in `views/components/shared/dialogs.py`

**All `@st.dialog` components should be reusable and receive pre-fetched data.**

```python
# ✅ GOOD: Controller fetches, dialog renders
@st.dialog("Preview Configuration")
def preview_config_dialog(config_name: str, content: dict | None) -> None:
    if content:
        st.json(content, expanded=True)
    else:
        st.error("Could not load configuration.")

# In controller:
from repositories.config_repo import get_content
content = get_content(config_name)
preview_config_dialog(config_name, content)

# ❌ BAD: Dialog fetches data itself
@st.dialog("Preview Configuration")
def preview_config_dialog(config_name: str) -> None:
    from repositories.config_repo import get_content  # Dialog shouldn't fetch
    content = get_content(config_name)
    st.json(content)
```

### Rule 4.2: Shared CSS Goes in `views/components/shared/styles.py`

**Global CSS that affects multiple pages should be centralized.**

```python
# views/components/shared/styles.py
def inject_global_css() -> None:
    st.markdown("""<style>...</style>""", unsafe_allow_html=True)

# Called from any view
from views.components.shared.styles import inject_global_css
inject_global_css()
```

---

## API Service Layer Patterns

### Rule 7.1: Use Template Method for find_all() (BaseService)

**All API services extend `BaseService` which provides a concrete `find_all()`. Subclasses implement only `_count_all()` and `_list_all()`.**

```python
# ✅ CORRECT — implement only the two hooks
class DatasourcesService(BaseService):
    def _count_all(self) -> int:
        return datasource_repo.count_all()

    def _list_all(self) -> list[dict]:
        return datasource_repo.get_all_list()

# For post-pagination transforms (e.g., attaching child records):
class PipelinesService(BaseService):
    def _post_process_page(self, page_data: list[dict]) -> list[dict]:
        return self._attach_children(page_data)

# ❌ WRONG — overriding find_all() directly
class MyService(BaseService):
    def find_all(self, params):  # Bypasses template method — don't do this
        ...
```

### Rule 7.2: Use _parse_uuid() Instead of Manual try/except

**Use `self._parse_uuid(value)` from BaseService for UUID validation.**

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

### Rule 7.3: Use _merge_fields() for Patch-Style Updates

**Use `self._merge_fields(data, existing, fields)` from BaseService instead of manual per-field merge.**

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

### Rule 7.4: Routers Contain Only HTTP Concerns

**Routers MUST NOT contain business logic, repo calls, or service-level imports. All logic belongs in the service.**

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
```

### Rule 7.5: Inject External Dependencies via Constructor

**Services that need external resources (Socket.IO, etc.) receive them via constructor injection from the router.**

```python
# ✅ CORRECT — router injects
# api/jobs/router.py
service = JobsService(emit_fn=emit_from_thread)

# ❌ WRONG — service imports from API layer
class JobsService(BaseService):
    from api.socket_manager import emit_from_thread  # Breaks DIP
```

---

## RESTful API Design Rules

### Rule 6.1: OOP-Style Resource URIs (NOT RPC Style)

All endpoints use **resource nouns** — never verb-based action paths.

```
✅ OOP Style (Resource-Oriented)          ❌ RPC Style (Action-Oriented)
GET    /api/v1/pipelines                   GET    /api/v1/pipelines/get-all
GET    /api/v1/pipelines/{id}              GET    /api/v1/pipelines/get-by-id
GET    /api/v1/pipelines/{id}/nodes        GET    /api/v1/pipelines/get-nodes
POST   /api/v1/jobs                        POST   /api/v1/jobs/trigger
```

### Rule 6.2: Single GET Endpoint + Query Params

Use **one GET endpoint with query parameters** for filtering, searching, and sorting. Never create separate endpoints for variations.

```
✅ Single endpoint + query params                         ❌ Multiple endpoints
GET /api/v1/pipelines?filter=name||$eq||MyPipeline        GET /api/v1/pipelines/get-by-name
GET /api/v1/pipelines?filter=status||$eq||running         GET /api/v1/pipelines/get-running
GET /api/v1/jobs?filter=pipeline_id||$eq||{uuid}          GET /api/v1/pipelines/{id}/jobs
GET /api/v1/datasources?s=PostgreSQL                      GET /api/v1/datasources/search
GET /api/v1/pipeline-runs?filter=job_id||$eq||{uuid}      GET /api/v1/jobs/{id}/pipeline-runs
```

**Available query params:**

| Param    | Format                              | Example                                      |
|----------|-------------------------------------|----------------------------------------------|
| `filter` | `{field}\|\|$op\|\|{value}`         | `filter=name\|\|$eq\|\|MyPipeline`            |
| `s`      | Full-text search (shorthand)        | `s=postgres`                                  |
| `sort`   | `{field}:{direction}`               | `sort=created_at:desc`                        |
| `page`   | Page number                         | `page=2`                                      |
| `limit`  | Items per page                      | `limit=25`                                    |

**Supported filter operators:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$like`, `$ilike`, `$in`, `$isnull`

### Rule 6.3: Sub-Resources Only for Direct Ownership

Sub-resources (`/{parent_id}/{child}`) are acceptable **only when the child entity directly belongs to the parent** (1:N ownership, not just a FK filter).

```
✅ Sub-resource (direct ownership — child cannot exist without parent)
GET /api/v1/pipelines/{id}/nodes          ← pipeline_nodes belongs to pipeline
GET /api/v1/pipelines/{id}/edges          ← pipeline_edges belongs to pipeline
GET /api/v1/configs/{id}/histories        ← config_histories belongs to config
GET /api/v1/configs/{id}/versions/{ver}   ← specific version of a config

❌ Sub-resource when relationship is just a FK filter — use query params instead
GET /api/v1/pipelines/{id}/jobs          ← jobs have their own lifecycle, not owned by pipeline
GET /api/v1/jobs/{id}/pipeline-runs      ← pipeline_runs have their own lifecycle, not owned by job
```

```python
# ✅ CORRECT — use filter for FK lookups
GET /api/v1/jobs?filter=pipeline_id||$eq||{uuid}
GET /api/v1/pipeline-runs?filter=job_id||$eq||{uuid}

# ❌ WRONG — sub-resource for non-ownership FK relationship
GET /api/v1/pipelines/{id}/jobs
GET /api/v1/jobs/{id}/pipeline-runs
```

### Rule 6.4: PATCH Only for Partial Updates and State Transitions

`PATCH` on a sub-resource is acceptable **only** for partial updates or state transitions where a full `PUT` would be wasteful. This is the **only exception** to OOP-style URIs — use sparingly.

```
✅ ACCEPTABLE — Partial update / state transition
PATCH /api/v1/jobs/{id}/status          { "status": "cancelled" }
PATCH /api/v1/pipeline-runs/{id}/status { "status": "failed" }

❌ WRONG — RPC-style action disguised as PATCH
PATCH /api/v1/jobs/{id}/cancel
PATCH /api/v1/pipelines/{id}/run
```

### Rule 6.5: All Responses Follow JSON:API Format

Every API response MUST use the standard JSON:API envelope:

```json
{
  "data": { "type": "resource_type", "id": "uuid", "attributes": { ... } },
  "links": { "self": "/api/v1/resource/uuid" },
  "meta": { "timestamp": "..." },
  "status": { "code": 200000, "message": "Request Succeeded" }
}
```

Use `api/base/json_api.py` helpers: `create_success_response`, `create_collection_response`, `create_paginated_response`, `create_created_response`.

```python
# ✅ CORRECT — use json_api helpers
from api.base import json_api

def list_items(request: Request):
    return json_api.create_collection_response("items", data, str(request.url.path))

# ❌ WRONG — raw dict response
def list_items():
    return {"data": items}
```

### Rule 5.1: Use Thread-Safe Connection Managers

**For background threads (pipeline_service.py), use `get_transaction()` context manager.**

```python
# ✅ GOOD - Thread-safe
from repositories.connection import get_transaction

def my_background_task():
    with get_transaction() as conn:
        result = conn.execute(text("SELECT ..."))
    # Connection automatically closed after with block

# ❌ BAD - Not thread-safe
from repositories.connection import get_engine

engine = get_engine()
conn = engine.connect()  # Don't share connections across threads
```

### Rule 5.2: Handle UUID Types Correctly

**PostgreSQL uses native UUID type. Pass `uuid.UUID` objects, not strings.**

```python
# ✅ GOOD
import uuid
from repositories.pipeline_repo import save

pipeline_id = uuid.uuid4()  # UUID object
save("MyPipeline", "Desc", json_data, src_id, tgt_id, "fail_fast")

# ❌ BAD
pipeline_id = str(uuid.uuid4())  # Don't convert to string
```

### Rule 5.3: Use Repository Functions, Not Raw SQL

**Let repositories handle all SQL. Controllers shouldn't write queries.**

```python
# ✅ GOOD - Use repository
from repositories.config_repo import get_content
content = get_content("my_config")

# ❌ BAD - Raw SQL in controller
from repositories.connection import get_engine
engine = get_engine()
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM configs WHERE ..."))
```

---

## Testing & Validation

### Unit Test Pattern: Repositories

```python
# tests/test_datasource_repo.py
def test_save_and_get_datasource():
    from repositories.datasource_repo import save, get_all, get_by_name

    # Save
    ok, msg = save("test_ds", "MySQL", "localhost", "3306", "testdb", "user", "pass")
    assert ok

    # Get all
    df = get_all()
    assert "test_ds" in df["name"].values

    # Get by name
    ds = get_by_name("test_ds")
    assert ds["db_type"] == "MySQL"
```

### Unit Test Pattern: Services

```python
# tests/test_transformer.py
def test_data_transformer():
    from data_transformers.text import trim
    import pandas as pd

    df = pd.DataFrame({"name": [" Alice ", " Bob "]})
    result = trim(df)
    assert result["name"].tolist() == ["Alice", "Bob"]
```

### Integration Test Pattern: Controllers

```python
# Tests should verify:
# 1. Controller initializes state correctly
# 2. Controller fetches data via repositories correctly
# 3. Callbacks modify state and trigger rerun correctly
# 4. View renders with provided data and callbacks
```

---

## Code Review Checklist

Before merging any code, verify:

### Architecture

- [ ] **Model Layer**: No `import streamlit` in `models/`, `repositories/`, `services/`, `protocols/`
- [ ] **Layer Dependency**: No reverse imports — `services/` MUST NOT import from `api/` (use callback injection)
- [ ] **DIP**: `api/*/service.py` MUST NOT import from `api.socket_manager` — use constructor injection (`emit_fn`)
- [ ] **Data Ownership**: Each repo queries ONLY its own table — cross-repo lookups go through service layer
- [ ] **Repository Layer**: All DB access via `repositories/`, NOT `database.py` facade
- [ ] **View Layer**: No repository imports, no `PageState.set()`, pure rendering only
- [ ] **Controller Layer**: All state mutations happen here, all data fetches via repositories

### API Service Layer

- [ ] **Template Method**: Services use `_count_all()` / `_list_all()` — NOT overriding `find_all()` directly
- [ ] **UUID Validation**: Uses `_parse_uuid()` from BaseService — no manual try/except blocks
- [ ] **Field Merge**: Uses `_merge_fields()` for patch-style updates — no manual per-field merge
- [ ] **Router Boundary**: Routers contain only HTTP response formatting — no business logic or repo calls
- [ ] **DI**: External dependencies (Socket.IO, etc.) injected via constructor, not imported

### RESTful API Design

- [ ] **OOP URIs**: All endpoints use resource nouns, no verb-based paths (no `/get-all`, `/trigger`)
- [ ] **Query Params**: Filtering/searching uses `?filter=`, `?s=`, `?sort=` — no separate endpoints for variations
- [ ] **Sub-Resources**: `/{parent}/{child}` only for direct ownership, not FK filters
- [ ] **PATCH**: Only for partial updates/state transitions, not RPC-style actions
- [ ] **JSON:API Format**: All responses wrapped with `json_api.create_*_response()`

### MVC Pattern

- [ ] **Naming**: Functions prefixed with `_render`, `_on_*`, `run()`, etc.
- [ ] **Callbacks**: All callbacks defined in controller, not hardcoded in view
- [ ] **Session State**: Only controller initializes and mutates session state
- [ ] **App.py**: Routes to `controller.run()` not `view.render_*()`

### PostgreSQL

- [ ] **UUID Handling**: Using `uuid.UUID` objects, not strings
- [ ] **Thread Safety**: Background threads use `get_transaction()` context manager
- [ ] **Connection Pool**: Using SQLAlchemy Engine singleton, not creating raw connections

### Documentation

- [ ] **Docstrings**: Public functions have docstrings explaining their contract
- [ ] **Type Hints**: Using `from __future__ import annotations` for Python 3.11 compatibility

---

## Examples & Anti-Patterns

### ✅ Good: Controller Using Repositories

```python
# controllers/settings_controller.py
from repositories.datasource_repo import get_all, save, update, delete
from repositories.config_repo import get_list

def run() -> None:
    PageState.init(_DEFAULTS)

    # Fetch via repositories
    datasources_df = get_all()
    configs_df = get_list()

    # Define callbacks that use repositories
    def _on_save_new(...) -> tuple[bool, str]:
        ok, msg = save(name, db_type, host, port, ...)
        return ok, msg
```

### ❌ Bad: Controller Using Legacy Facade

```python
# ❌ WRONG: Using database.py facade
import database as db

def run():
    datasources = db.get_datasources()  # Don't use this
```

### ✅ Good: Dialog with Pre-Fetched Data

```python
# Controller
from repositories.config_repo import get_content

def _on_preview_config(config_name: str):
    content = get_content(config_name)
    preview_config_dialog(config_name, content)

# View
if st.button("Preview"):
    callbacks["on_preview_config"](config_name)

# Dialog
@st.dialog("Preview")
def preview_config_dialog(config_name: str, content: dict | None):
    st.json(content)
```

### ❌ Bad: Dialog Fetching Data

```python
# ❌ WRONG: Dialog fetching data
@st.dialog("Preview")
def preview_config_dialog(config_name: str):
    from repositories.config_repo import get_content  # NO!
    content = get_content(config_name)
    st.json(content)
```

---

## Summary

The three rules of MVC here:

1. **Models/Repositories/Services** — Pure Python, zero Streamlit, use repositories for DB access
2. **Views** — Only Streamlit rendering, receive all data + callbacks as arguments, NEVER fetch data
3. **Controllers** — Orchestrate everything: init state, fetch data via repositories, define callbacks, call view once

**When in doubt**: _"If it's data or business logic, it belongs in the controller. If it's a button or text input, it belongs in the view. If it's a pure function or DB access, it belongs in repositories/services."_

---

**Last Updated**: 2026-04-22
**Status**: ✅ Active — enforced on all new code
**Architecture**: PostgreSQL + Clean Architecture + SOLID + MVC + REST API
**Migration**: All 10 phases complete
