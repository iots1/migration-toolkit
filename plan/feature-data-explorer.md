# Plan: Data Explorer (SQL Executor) Feature

## 1. Context & Objective

A new **Data Explorer** feature that allows the Frontend to send custom SQL commands
to be executed against a connected Datasource and return the results.

**Payload**: `{ "cmd": "<SQL>", "datasource_id": "<UUID>" }`

**Flow**:
1. Frontend sends `POST /api/v1/db-explorers` with `{ cmd, datasource_id }`
2. API looks up `datasource_id` → finds connection credentials
3. API validates and sanitizes the SQL in `cmd`
4. API executes the query against the resolved datasource
5. API returns the result set in JSON:API format

## 2. Security & Stability Requirements

### 2.1 SQL Injection & Stacked Queries Prevention
- Block semicolons (`;`) to prevent stacked queries
  (e.g. `SELECT * FROM users; DROP TABLE users;`)
- Strip all SQL comments before validation:
  - Block comments: `/* ... */`
  - Line comments: `-- ...`

### 2.2 Forbidden Keywords (Blacklist)
- Reject queries containing DML, DDL, DCL keywords:
  `DROP`, `TRUNCATE`, `ALTER`, `DELETE`, `UPDATE`, `INSERT`,
  `GRANT`, `REVOKE`, `EXEC`, `EXECUTE`
- Reject `INTO OUTFILE`, `INTO DUMPFILE`, `LOAD_FILE`
- Use **word-boundary** matching (`\b...\b`) with case-insensitive flag
- Apply blacklist **after** stripping comments

### 2.3 SELECT-Only Enforcement
- The first significant keyword of the SQL must be `SELECT`
- Even if a query passes the blacklist, it must start with `SELECT`

### 2.4 Row Limit (Resource Protection)
- Wrap user query in a subquery to enforce `LIMIT 1000`:

```sql
SELECT * FROM (
    <user_query>
) AS _data_explorer_subq LIMIT 1000
```

- For MSSQL (uses `TOP` instead of `LIMIT`), apply dialect-specific wrapping
  via the existing `dialects/` registry pattern

### 2.5 Execution Timeout
- PostgreSQL: `SET statement_timeout = '30s'` before executing
- MySQL: `SET SESSION max_execution_time = 30000`
- On timeout: cancel the query, return 408 with generic error message

### 2.6 Connection Management
- Reuse existing `services/connection_pool.py` (`DatabaseConnectionPool` singleton)
- Always close connection immediately after fetch (in `finally` block)
- Never leave connections open on error paths

### 2.7 Data Privacy
- Never include datasource password in response, logs, or error messages
- Sanitize all database error messages before returning to client
  (return generic error, log actual error server-side)
- Do not include the executed query in the response

### 2.8 Audit Logging
- Log every query execution:
  - `datasource_id`, SQL (truncated to 200 chars), row count, elapsed time
- Use Python `logging` module at INFO level

---

## 3. Project Structure

Follow the **non-CRUD** pattern used by `api/jobs/` (standalone `APIRouter`,
no `BaseController`/`BaseService`):

```
api/
└── data_explorers/                # NEW module (plural)
    ├── __init__.py
    ├── router.py                  # POST /api/v1/db-explorers
    └── schemas.py                 # Pydantic request/response models

services/
└── query_executor.py              # NEW — SQL validation + execution logic
```

**Why not in `api/datasources/`?** Data Explorer is a distinct feature (SQL execution),
not a sub-resource of datasource CRUD. Keeping it separate follows SRP and matches
how `api/jobs/` is separate from `api/pipelines/`.

---

## 4. Implementation Steps

### Step 1: Create `api/data_explorers/schemas.py`

```python
"""Pydantic schemas for Data Explorers API."""

from __future__ import annotations

import uuid
from pydantic import BaseModel, Field


class ExecuteQueryRequest(BaseModel):
    cmd: str = Field(..., min_length=1, max_length=10000, description="SQL query to execute")
    datasource_id: uuid.UUID = Field(..., description="UUID of the target datasource")
```

### Step 2: Create `services/query_executor.py`

Business logic module with three responsibilities:
1. **SQL Validation** (`validate_sql`)
2. **SQL Execution** (`execute_query`)
3. **Error Sanitization**

```python
"""Query executor — validates and executes user SQL against a datasource."""

from __future__ import annotations

import logging
import re

from services.connection_pool import _connection_pool

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
QUERY_TIMEOUT_SECONDS = 30
MAX_SQL_LENGTH = 10000

_FORBIDDEN_PATTERN = re.compile(
    r'\b('
    r'DROP|TRUNCATE|ALTER|DELETE|UPDATE|INSERT|'
    r'GRANT|REVOKE|EXEC|EXECUTE|'
    r'INTO\s+(OUTFILE|DUMPFILE)|LOAD_FILE'
    r')\b',
    re.IGNORECASE,
)


def validate_sql(sql: str) -> str:
    stripped = _strip_comments(sql)

    if ';' in stripped.rstrip(';'):
        raise ValueError("Semicolons are not allowed (stacked queries blocked)")

    if _FORBIDDEN_PATTERN.search(stripped):
        raise ValueError("Query contains forbidden keywords or unsafe patterns")

    if not stripped.strip().upper().startswith('SELECT'):
        raise ValueError("Only SELECT queries are allowed")

    return stripped


def execute_query(datasource_id: str, cmd: str) -> dict:
    from repositories.datasource_repo import get_by_id as get_datasource

    sql = validate_sql(cmd)

    ds = get_datasource(datasource_id)
    if not ds:
        raise ValueError(f"Datasource '{datasource_id}' not found")

    conn = None
    cursor = None
    try:
        conn, cursor = _connection_pool.get_connection(
            db_type=ds["db_type"],
            host=ds["host"],
            port=ds["port"],
            db_name=ds["dbname"],
            user=ds["username"],
            password=ds["password"],
        )

        if ds["db_type"] == "PostgreSQL":
            cursor.execute("SET statement_timeout = '30000'")
        elif ds["db_type"] == "MySQL":
            cursor.execute("SET SESSION max_execution_time = 30000")

        wrapped_sql = _wrap_with_limit(sql, ds["db_type"])
        cursor.execute(wrapped_sql)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(MAX_ROWS + 1)
        truncated = len(rows) > MAX_ROWS
        rows = rows[:MAX_ROWS]

        row_dicts = [dict(zip(columns, row)) for row in rows]

        result = {
            "columns": columns,
            "rows": row_dicts,
            "row_count": len(row_dicts),
            "limit": MAX_ROWS,
            "truncated": truncated,
        }

        logger.info(
            "DataExplorer: datasource=%s rows=%d sql=%.200s",
            datasource_id, len(row_dicts), sql,
        )

        return result
    except ValueError:
        raise
    except Exception as exc:
        logger.error("DataExplorer error: datasource=%s error=%s", datasource_id, exc)
        raise ValueError("Query execution failed. Please check your SQL and try again.")
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            _connection_pool.close_connection(conn, db_type=ds["db_type"])


def _strip_comments(sql: str) -> str:
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    return sql


def _wrap_with_limit(sql: str, db_type: str) -> str:
    if db_type == "MSSQL":
        return f"SELECT TOP {MAX_ROWS} * FROM ({sql}) AS _data_explorer_subq"
    return f"SELECT * FROM ({sql}) AS _data_explorer_subq LIMIT {MAX_ROWS}"
```

### Step 3: Create `api/data_explorers/router.py`

```python
"""Data Explorers API router — POST /api/v1/db-explorers"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException

from api.data_explorers.schemas import ExecuteQueryRequest
from api.base.json_api import create_success_response
from services.query_executor import execute_query

router = APIRouter(prefix="/api/v1/db-explorers", tags=["Data Explorers"])


@router.post("", status_code=200)
def execute_sql(body: ExecuteQueryRequest):
    try:
        result = execute_query(str(body.datasource_id), body.cmd)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return create_success_response(
        resource_type="db_explorers",
        data=result,
        base_url="/api/v1/db-explorers",
    )


def get_data_explorers_router() -> APIRouter:
    return router
```

### Step 4: Register Router in `api/main.py`

Add the new router to the existing app:

```python
from api.data_explorers.router import get_data_explorers_router

# In the "Register routers" section:
app.include_router(get_data_explorers_router())
```

---

## 5. API Contract

### Endpoint

```
POST /api/v1/db-explorers
```

### Request

```json
{
  "cmd": "SELECT * FROM users ORDER BY created_at DESC",
  "datasource_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Response (200 — Success)

Follows the project's JSON:API standard via `create_success_response()`:

```json
{
  "data": {
    "type": "db_explorers",
    "id": "<generated>",
    "attributes": {
      "columns": ["id", "username", "email", "created_at"],
      "rows": [
        {"id": 1, "username": "admin", "email": "admin@test.com", "created_at": "2026-04-15"},
        {"id": 2, "username": "nurse", "email": "nurse@test.com", "created_at": "2026-04-14"}
      ],
      "row_count": 2,
      "limit": 1000,
      "truncated": false
    }
  },
  "links": {
    "self": "/api/v1/db-explorers/<id>"
  },
  "meta": {
    "timestamp": "2026-04-15T10:30:00.000000"
  },
  "status": {
    "code": 200000,
    "message": "Request Succeeded"
  }
}
```

### Response (400 — Security Check Failed)

Handled by the project's existing `http_exception_handler` in `api/base/exceptions.py`:

```json
{
  "data": null,
  "meta": {
    "timestamp": "2026-04-15T10:30:00.000000"
  },
  "errors": [
    {
      "detail": "Query contains forbidden keywords or unsafe patterns",
      "status": 400
    }
  ]
}
```

### Response (400 — Stacked Query Blocked)

```json
{
  "data": null,
  "meta": {
    "timestamp": "2026-04-15T10:30:00.000000"
  },
  "errors": [
    {
      "detail": "Semicolons are not allowed (stacked queries blocked)",
      "status": 400
    }
  ]
}
```

### Response (400 — Not SELECT)

```json
{
  "data": null,
  "meta": {
    "timestamp": "2026-04-15T10:30:00.000000"
  },
  "errors": [
    {
      "detail": "Only SELECT queries are allowed",
      "status": 400
    }
  ]
}
```

### Response (400 — Execution Error)

```json
{
  "data": null,
  "meta": {
    "timestamp": "2026-04-15T10:30:00.000000"
  },
  "errors": [
    {
      "detail": "Query execution failed. Please check your SQL and try again.",
      "status": 400
    }
  ]
}
```

---

## 6. Files to Create/Modify

| Action | File | Description |
|--------|------|-------------|
| CREATE | `api/data_explorers/__init__.py` | Empty init |
| CREATE | `api/data_explorers/schemas.py` | Request Pydantic model |
| CREATE | `api/data_explorers/router.py` | `POST /api/v1/db-explorers` endpoint |
| CREATE | `services/query_executor.py` | SQL validation + execution logic |
| MODIFY | `api/main.py` | Register new router |

---

## 7. Future Considerations

- **Rate Limiting**: Per-datasource or per-user query rate limits
- **Query History**: Store executed queries for audit trail in a new table
- **Read-Only DB User**: Connect using a database user with SELECT-only privileges
  (the ultimate defense against writes, independent of application-level checks)
- **Column-Level Access Control**: Restrict sensitive columns (e.g. password, CID)
