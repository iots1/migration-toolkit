---
name: add-entity-or-column
description: >
  Checklist and instructions for adding a new database column to an existing table,
  or implementing a brand-new entity (table + repo + API) in this HIS Analyzer project.
  Invoke this skill before starting any schema change work.
argument-hint: "[table] [column]  |  [new-entity-name]"
user-invocable: true
allowed-tools: Read Grep Glob
---

# HIS Analyzer — Schema Change Playbook

Use `$ARGUMENTS` to determine scope:
- One argument → new entity (e.g. `audit_logs`)
- Two arguments → new column on existing table (e.g. `configs priority`)
- No arguments → print both checklists below

---

## Part 1 — Adding a Column to an Existing Table

> Example: add `priority VARCHAR(20)` to the `configs` table

### How Many Places to Touch: **6–7**

The rule: **start at the Record model, everything else follows**.

---

### Step 1 — Record Model (Single Source of Truth)

Every writable table has one Record dataclass. This is the contract.
Adding a field here is always the first step.

| Table | Record class | File |
|---|---|---|
| `datasources` | `DatasourceRecord` | `models/datasource.py` |
| `configs` | `ConfigRecord` | `models/migration_config.py` |
| `pipelines` | `PipelineRecord` | `models/pipeline_config.py` |
| `pipeline_runs` INSERT | `PipelineRunRecord` | `models/pipeline_config.py` |
| `pipeline_runs` PATCH | `PipelineRunUpdateRecord` | `models/pipeline_config.py` |

```python
# Example: models/migration_config.py
@dataclass
class ConfigRecord:
    ...
    priority: str | None = None   # ← add here first
```

---

### Step 2 — DDL (fresh installs)

```python
# repositories/base.py  →  TABLES_DDL  →  target table block
"""CREATE TABLE IF NOT EXISTS configs (
    ...
    priority VARCHAR(20),   # ← add
    ...
)"""
```

> ⚠️ The type here **must match** the migration script (Step 6).
> The `generate_sql` column was once `BOOLEAN` in DDL but `TEXT` in the migration — that mismatch has been fixed. Don't repeat it.

---

### Step 3 — Repository `col_params`

Each repo's `save()` (or `update()`) has a `col_params` dict.
Adding the field here automatically covers both `INSERT` and `UPDATE`
because both use `**col_params` spread.

```python
# repositories/config_repo.py  →  def save(record: ConfigRecord)
col_params: dict = {
    ...
    "priority": record.priority or None,   # ← one line
}
```

**BUT** you still need to name the column in two SQL strings:
- `INSERT INTO configs (..., priority)`
- `UPDATE configs SET ..., priority = :priority`

> ⚠️ **`datasource_repo` is different** — `save()` and `update()` are separate functions,
> each with their own `col_params` dict. You must add the field in **both** functions.

---

### Step 4 — Protocol Interface

```python
# protocols/repository.py
# ✅ No change needed.
# All repo protocols use record objects (e.g. save(record: ConfigRecord)).
# The protocol signature does not list individual fields.
```

---

### Step 5 — API Schemas

```python
# api/configs/schemas.py
class CreateConfigSchema(BaseModel):
    ...
    priority: str | None = None   # ← add to Create

class UpdateConfigSchema(BaseModel):
    ...
    priority: str | None = None   # ← add to Update (always Optional)
```

---

### Step 6 — `allowed_fields` in API Service

```python
# api/configs/service.py  →  class ConfigsService
allowed_fields = [
    ...,
    "priority",   # ← if missing, _sanitize_response() silently strips it from every response
]
```

> ⚠️ This is the most common silent bug: the field saves to DB fine but never appears in API responses.

---

### Step 7 — Migration Script (existing databases)

```python
# Create: scripts/migrate_<table>_add_<column>.py
# Copy from: scripts/migrate_configs_add_columns.py

NEW_COLUMNS = [
    ("priority", "VARCHAR(20)"),   # type must match base.py DDL
]
```

---

### Checklist — Add Column

```
[ ] models/*          — add field to Record dataclass
[ ] repositories/base.py         — add to DDL
[ ] repositories/*_repo.py       — add to col_params + INSERT columns + UPDATE SET
[ ] api/*/schemas.py             — add to Create/Update schema
[ ] api/*/service.py             — add to allowed_fields
[ ] scripts/migrate_*_add_*.py   — migration script for existing DBs

Verify:
[ ] python3.11 -c "from repositories.base import init_db; init_db()"
[ ] python3.11 scripts/migrate_<table>_add_<column>.py
```

---

## Part 2 — Adding a Brand-New Entity

> Example: new table `audit_logs`

### How Many Places to Touch: **9**

Follow this order — each step depends on the previous.

---

### Step 1 — Domain Model + Record

```python
# Create: models/audit_log.py

@dataclass
class AuditLog:
    """Read model — populated from a DB row."""
    id: str
    action: str
    entity: str
    entity_id: str
    created_at: str

@dataclass
class AuditLogRecord:
    """Write model — single source of truth for audit_logs columns."""
    action: str
    entity: str
    entity_id: str
```

---

### Step 2 — DDL

```python
# repositories/base.py  →  append to TABLES_DDL

"""CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action VARCHAR(50) NOT NULL,
    entity VARCHAR(100) NOT NULL,
    entity_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by UUID,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMP WITH TIME ZONE,
    deleted_by UUID,
    deleted_reason TEXT
)""",
```

> ⚠️ **FK ordering**: parent table DDL must appear before child. Current order:
> `datasources → configs → config_histories → pipelines → pipeline_runs`
> Insert the new table in the correct position.

---

### Step 3 — Repository

```python
# Create: repositories/audit_log_repo.py

from models.audit_log import AuditLogRecord

def save(record: AuditLogRecord) -> None:
    col_params = {
        "action": record.action,
        "entity": record.entity,
        "entity_id": record.entity_id,
    }
    with get_transaction() as conn:
        conn.execute(text("INSERT INTO audit_logs (action, entity, entity_id) "
                          "VALUES (:action, :entity, :entity_id)"), col_params)

def get_all_list() -> list[dict]: ...
def get_by_id(id: str) -> dict | None: ...
```

---

### Step 4 — Protocol

```python
# protocols/repository.py  →  add import + new class

from models.audit_log import AuditLogRecord

@runtime_checkable
class AuditLogRepository(Protocol):
    def save(self, record: AuditLogRecord) -> None: ...
    def get_all_list(self) -> list[dict]: ...
    def get_by_id(self, id: str) -> dict | None: ...
```

---

### Step 5 — API Schemas

```python
# Create: api/audit_logs/schemas.py

class CreateAuditLogSchema(BaseModel):
    action: str = Field(..., min_length=1)
    entity: str = Field(..., min_length=1)
    entity_id: str

class UpdateAuditLogSchema(BaseModel):
    action: str | None = None
    entity: str | None = None

class AuditLogSchema(BaseModel):
    id: str
    action: str
    entity: str
    entity_id: str
    created_at: str
```

---

### Step 6 — API Service

```python
# Create: api/audit_logs/service.py

from models.audit_log import AuditLogRecord
from repositories import audit_log_repo

class AuditLogsService(BaseService):
    resource_type = "audit-logs"
    allowed_fields = ["id", "action", "entity", "entity_id", "created_at"]

    def create(self, data: dict) -> dict:
        record = AuditLogRecord(
            action=data.get("action", ""),
            entity=data.get("entity", ""),
            entity_id=data.get("entity_id", ""),
        )
        self.execute_db_operation(lambda: audit_log_repo.save(record))
        ...
```

---

### Step 7 — API Router

```python
# Create: api/audit_logs/router.py

from api.base.controller import BaseController
from api.audit_logs.service import AuditLogsService
from api.audit_logs.schemas import CreateAuditLogSchema, UpdateAuditLogSchema

def get_audit_logs_router():
    service = AuditLogsService()
    controller = BaseController(
        prefix="audit-logs",
        service=service,
        create_schema=CreateAuditLogSchema,
        update_schema=UpdateAuditLogSchema,
        tags=["Audit Logs"],
    )
    return controller.router
```

---

### Step 8 — Register Router

```python
# api/main.py

from api.audit_logs.router import get_audit_logs_router
app.include_router(get_audit_logs_router())
```

---

### Step 9 — `__init__.py`

```bash
touch api/audit_logs/__init__.py
```

---

### Checklist — New Entity

```
[ ] models/<entity>.py                  — Read model + Record (write model)
[ ] repositories/base.py                — DDL (correct FK order)
[ ] repositories/<entity>_repo.py       — save() + read functions
[ ] protocols/repository.py             — Protocol interface
[ ] api/<entity>s/__init__.py
[ ] api/<entity>s/schemas.py            — Create / Update / Response schemas
[ ] api/<entity>s/service.py            — BaseService subclass + allowed_fields
[ ] api/<entity>s/router.py             — BaseController router
[ ] api/main.py                         — include_router(...)
[ ] scripts/migrate_<entity>.py         — migration script (existing DBs)
```

---

## Part 3 — Gotchas Reference

### G1 — `allowed_fields` silently strips missing fields
If a field is saved to DB but missing from `allowed_fields`, it will never appear
in API responses. No error is raised. Always double-check after adding a column.

### G2 — `generate_sql` is TEXT, not BOOLEAN
The DDL once had `generate_sql BOOLEAN DEFAULT false` — this was wrong.
It stores SQL text. Current correct type: `generate_sql TEXT`.
Never revert it.

### G3 — UUID must be stringified before returning from repos
```python
data["id"] = str(data["id"])          # correct
data["pipeline_id"] = str(data["pipeline_id"])
```
All existing repos do this. New repos must too.

### G4 — `datasource_repo.update()` overwrites all fields
There is no `COALESCE` in the datasources UPDATE — every column is overwritten.
The service **must merge** with existing values before building the record:
```python
record = DatasourceRecord(
    name=data.get("name") or existing.get("name"),  # never pass None
    password=data.get("password") or existing.get("password"),
    ...
)
```

### G5 — `datasource_repo` has separate `save()` and `update()` functions
Unlike `config_repo` (which has a single upsert), `datasource_repo` has two separate
functions each with their own `col_params`. Adding a column requires updating both.

### G6 — DDL type must match migration script type
`repositories/base.py` (fresh install) and `scripts/migrate_*.py` (existing DB)
must declare the same SQL type for each column.

### G7 — `database.py` facade is deprecated
`database.py` re-exports repo functions for legacy Streamlit view compatibility.
New code should import from `repositories.*` directly.
New repo functions do **not** need to be added to `database.py` unless called
by a legacy Streamlit view that cannot be refactored immediately.

### G8 — `pipeline_run_repo.update()` uses COALESCE
`steps_json = COALESCE(:steps_json, steps_json)` means passing `None` preserves
the existing value. If you intentionally need to clear a field, the SQL must
be changed to remove the `COALESCE` guard for that column.

### G9 — DDL table ordering (FK constraints)
`init_db()` runs DDL statements in list order. A child table referencing a parent
via FK will fail if the parent DDL comes later. Current safe order:
```
datasources → configs → config_histories → pipelines → pipeline_runs
```

---

## Quick Reference

```
Add a column (6–7 places):
  1. models/*           ← Record dataclass  (start here)
  2. repositories/base  ← DDL
  3. repositories/*repo ← col_params + INSERT cols + UPDATE SET
  4. api/*/schemas      ← Create / Update schema
  5. api/*/service      ← allowed_fields
  6. scripts/migrate_*  ← migration script
 [7. protocols/repo     ← only if adding a new repo method signature]

Add a new entity (9 places):
  models → base (DDL) → repo → protocol → api/__init__ →
  api/schemas → api/service → api/router → api/main
```
