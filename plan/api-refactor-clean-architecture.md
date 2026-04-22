# Plan: API Layer Clean Architecture Refactor

**Date**: 2026-04-22
**Scope**: `api/` directory — SOLID, DRY, Clean Architecture enforcement
**Status**: ✅ Complete

---

## Problem Statement

Code review revealed 11 violations across the `api/` layer:

| # | Issue | Principle | Severity |
|---|-------|-----------|----------|
| 1 | `JobsService` imports `api.socket_manager` directly | DIP | 🔴 High |
| 2 | `find_all()` boilerplate duplicated in all 5 services | DRY / SRP | 🟡 Medium |
| 3 | `exec()` in BaseController for dynamic endpoints | Maintainability | 🟡 Medium (deferred) |
| 4 | Duplicate route registration `"/"` and `""` | Hack / DRY | 🟢 Low (deferred) |
| 5 | UUID validation repeated in every service | DRY | 🟡 Medium |
| 6 | Manual field merge pattern duplicated | DRY | 🟡 Medium |
| 7 | Business logic in `datasources/router.py` | SRP | 🟡 Medium |
| 8 | `JobsService` too large (294 lines, 5 responsibilities) | SRP | 🔴 High |
| 9 | Module-level singleton in `data_explorers/router.py` | DI / Testability | 🟢 Low |
| 10 | `SqlQueryBuilder` loads ALL data then filters in-memory | Performance | 🔴 High (deferred — requires repo layer changes) |
| 11 | Sanitization not auto-enforced | Security | 🟡 Medium (deferred) |

---

## Changes Made

### 1. Template Method Pattern in `BaseService` (Issues #2, #5, #6)

**File**: `api/base/service.py`

- Replaced abstract `find_all()` with **concrete template method** using two new abstract hooks:
  - `_count_all() -> int` — return total record count
  - `_list_all() -> list[dict]` — return all records
  - `_post_process_page(page_data) -> list[dict]` — optional hook for post-pagination transforms
- Added `_parse_uuid(value, field) -> uuid.UUID` — centralized UUID validation (eliminates 8+ duplicate try/except blocks)
- Added `_merge_fields(data, existing, fields, defaults) -> dict` — centralized field merge pattern

**Before** (repeated in 5 services):
```python
def find_all(self, params: QueryParams) -> dict:
    total_records = self.execute_db_operation(lambda: repo.count_all())
    data = self.execute_db_operation(lambda: repo.get_all_list())
    data = self._apply_query_params(data, params)
    data = self._sanitize_list(data)
    page_data, total, total_pages = self._paginate(data, params)
    return { "data": page_data, "total": total, ... }
```

**After** (once in BaseService, services implement 2-line hooks):
```python
class DatasourcesService(BaseService):
    def _count_all(self) -> int:
        return datasource_repo.count_all()
    def _list_all(self) -> list[dict]:
        return datasource_repo.get_all_list()
```

### 2. DIP Fix — Callback Injection for Socket.IO (Issue #1)

**Files**: `api/jobs/service.py`, `api/jobs/router.py`

- **Removed**: `from api.socket_manager import emit_from_thread` from `JobsService`
- **Added**: `emit_fn` constructor parameter injected by the router layer
- Router imports `emit_from_thread` and passes it: `JobsService(emit_fn=emit_from_thread)`
- Service uses `self._emit_fn` (or `_noop_emit` fallback for testing)

**Before**:
```python
# api/jobs/service.py (WRONG — service imports from API layer)
from api.socket_manager import emit_from_thread
```

**After**:
```python
# api/jobs/service.py (CORRECT — dependency injected)
class JobsService(BaseService):
    def __init__(self, emit_fn=None):
        self._emit_fn = emit_fn

# api/jobs/router.py (API layer injects the dependency)
from api.socket_manager import emit_from_thread
service = JobsService(emit_fn=emit_from_thread)
```

### 3. SRP — Extract Stale Job Detection (Issue #8)

**New file**: `api/jobs/stale_detector.py`

Extracted `_is_job_stale()` from `JobsService` into a standalone function `is_job_stale(running_job: dict)`. This:
- Removes ~40 lines of datetime parsing logic from the service
- Accepts the job dict directly (no redundant DB fetch)
- Reusable outside of `JobsService`

### 4. SRP — Move Business Logic from Router to Service (Issue #7)

**Files**: `api/datasources/service.py`, `api/datasources/router.py`

Moved from router to `DatasourcesService`:
- `_resolve_datasource()` → `service.resolve_datasource(datasource_id)`
- `_datasource_kwargs()` → `service._to_connection_kwargs(ds)` (static)
- Table/columns inspection → `service.get_tables()`, `service.get_columns()`

Router now contains only HTTP response formatting — zero business logic.

### 5. DI — Fix Module-Level Singleton (Issue #9)

**File**: `api/data_explorers/router.py`

Replaced `_executor = QueryExecutor()` with `Depends(_get_executor)` for testability.

### 6. DRY — Use `_merge_fields` in DatasourcesService (Issue #6)

**File**: `api/datasources/service.py`

Replaced 7-line manual field mapping in `create()` and `update()` with:
```python
merged = self._merge_fields(data, existing, ["name", "db_type", ...])
record = DatasourceRecord(**merged)
```

### 7. DRY — Use `_parse_uuid` in PipelineRunsService (Issue #5)

**File**: `api/pipeline_runs/service.py`

Replaced 3 duplicate UUID validation blocks with `self._parse_uuid(id)`.
Removed unused `import uuid`.

---

## Deferred Items

| # | Issue | Reason |
|---|-------|--------|
| 3 | `exec()` in BaseController | Works correctly; replacing requires FastAPI Annotated pattern refactor. Low risk, high effort. |
| 4 | Duplicate `"/"` and `""` routes | Cosmetic; `redirect_slashes=False` makes this necessary. |
| 10 | In-memory `SqlQueryBuilder` | Requires repo layer to accept query params and push filtering to SQL. Cross-layer change. |
| 11 | Auto-enforce sanitization | Requires response pipeline changes; current manual approach works. |

---

## Files Changed

| File | Change |
|------|--------|
| `api/base/service.py` | Template method `find_all()`, `_parse_uuid()`, `_merge_fields()` |
| `api/jobs/stale_detector.py` | **NEW** — extracted stale detection logic |
| `api/jobs/service.py` | DIP fix, SRP (use stale_detector), template hooks |
| `api/jobs/router.py` | Inject `emit_fn` into JobsService |
| `api/datasources/service.py` | Template hooks, `get_tables()`, `get_columns()`, `_merge_fields` |
| `api/datasources/router.py` | Removed business logic, delegates to service |
| `api/configs/service.py` | Template hooks (`_count_all`, `_list_all`) |
| `api/pipelines/service.py` | Template hooks + `_post_process_page` |
| `api/pipeline_runs/service.py` | Template hooks, `_parse_uuid` |
| `api/data_explorers/router.py` | `Depends()` for executor |

## Validation

- All changes are backward-compatible (same API endpoints, same response formats)
- No new dependencies introduced
- `JobsService(emit_fn=None)` falls back to no-op for testing
- `_parse_uuid` preserves the exact same error message format
