# Plan: FastAPI REST API Layer

## Context

เพิ่ม REST API layer ด้วย FastAPI เพื่อให้ Svelte frontend เรียกใช้แทน Streamlit views
Streamlit ยังคงทำงานได้บน port 8501 ตามปกติ FastAPI รันแยกบน port 8000

---

## ทำไมต้อง FastAPI

- **OpenAPI / Swagger docs** สร้างอัตโนมัติที่ `/docs`
- **Pydantic v2** สำหรับ request validation และ serialization
- **async support** รองรับ concurrent requests
- **Python native** ไม่ต้องเพิ่ม runtime ใหม่

---

## Architecture

ออกแบบตาม NestJS pattern — `BaseController` + `BaseService` inheritance ลด boilerplate:

```
api/
├── main.py                          # FastAPI app, CORS middleware, register routers
├── base/
│   ├── __init__.py
│   ├── controller.py                # BaseController: factory register CRUD routes ลง APIRouter
│   ├── service.py                   # BaseService: abstract CRUD interface + shared helpers
│   ├── query_params.py              # QueryParams Pydantic model (ครบทุก field)
│   ├── query_builder.py             # SQLQueryBuilder: แปลง QueryParams เป็น SQL fragments
│   ├── json_api.py                  # JSON:API response builder functions
│   └── exceptions.py                # Global exception handlers (AllExceptionsFilter equivalent)
├── datasources/
│   ├── __init__.py
│   ├── router.py                    # DatasourcesController(BaseController)
│   ├── service.py                   # DatasourcesService(BaseService)
│   └── schemas.py                   # Pydantic DTOs
├── configs/
│   ├── __init__.py
│   ├── router.py                    # ConfigsController + extra history endpoints
│   ├── service.py                   # ConfigsService(BaseService)
│   └── schemas.py
├── config_histories/
│   ├── __init__.py
│   ├── router.py                    # Read-only GET endpoints only
│   ├── service.py
│   └── schemas.py
├── pipelines/
│   ├── __init__.py
│   ├── router.py                    # PipelinesController(BaseController)
│   ├── service.py                   # PipelinesService(BaseService)
│   └── schemas.py
└── pipeline_runs/
    ├── __init__.py
    ├── router.py                    # PipelineRunsController
    ├── service.py                   # PipelineRunsService(BaseService)
    └── schemas.py
```

---

## Dependencies (เพิ่มใน requirements.txt)

```
fastapi>=0.115.0
uvicorn[standard]>=0.30.0
```

---

## 1. JSON:API Response Standard

เลียนแบบ `json-api.util.ts` + `transform-interceptor.util.ts`

### Single Resource (GET /{id}, POST 201, PUT)

```json
{
  "data": {
    "type": "datasources",
    "id": "1",
    "attributes": {
      "name": "HIS Source",
      "db_type": "mysql",
      "host": "localhost"
    }
  },
  "links": { "self": "http://localhost:8000/api/v1/datasources/1" },
  "meta": { "timestamp": "2026-04-11T10:00:00Z" },
  "status": { "code": 200000, "message": "Request Succeeded" }
}
```

### Collection with Pagination (GET /)

```json
{
  "data": [
    { "type": "datasources", "id": "1", "attributes": { ... } },
    { "type": "datasources", "id": "2", "attributes": { ... } }
  ],
  "meta": {
    "timestamp": "2026-04-11T10:00:00Z",
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 5,
      "total_records": 5,
      "total_pages": 1
    }
  },
  "links": {
    "self": "http://...?page=1&limit=20",
    "first": "http://...?page=1&limit=20",
    "last": "http://...?page=1&limit=20"
  },
  "status": { "code": 200000, "message": "Request Succeeded" }
}
```

### Created (POST 201)

```json
{
  "data": { "type": "configs", "id": "uuid", "attributes": { ... } },
  "links": { "self": "http://.../uuid" },
  "meta": { "timestamp": "..." },
  "status": { "code": 201000, "message": "Created successfully" }
}
```

### Delete (204 No Content)

```json
{ "data": null, "meta": { "timestamp": "..." } }
```

### Error (4xx / 5xx)

```json
{
  "status": {
    "code": 404,
    "message": "An error occurred"
  },
  "errors": [
    {
      "code": 404,
      "title": "An error occurred",
      "detail": "Not found: 99"
    }
  ],
  "meta": { "timestamp": "2026-04-11T10:00:00Z" }
}
```

---

### `api/base/json_api.py` — Response Builder

```python
def create_success_response(resource_type, data, request) -> dict
def create_collection_response(resource_type, data, request) -> dict
def create_paginated_response(resource_type, data, pagination, request) -> dict
def create_created_response(resource_type, data, request) -> dict
def create_no_content_response() -> dict

# Shared helper: extract id from data, put rest in attributes
def _build_resource_object(resource_type, item) -> dict
```

**TransformResponse** — dependency ที่ controller เรียก หรือ helper function ใน BaseController
ทำงานเหมือน NestJS interceptor: ตรวจ shape ของ output แล้ว wrap ด้วย JSON:API

---

## 2. QueryParams Schema

เลียนแบบ `QueryParamsDTO` ครบทุก field:

### `api/base/query_params.py`

```python
from pydantic import BaseModel, Field, field_validator

class QueryParams(BaseModel):
    # Pagination
    page: int = Field(default=1, ge=1, description="Current page number")
    offset: int | None = Field(default=None, ge=0, description="Raw offset (overrides page)")
    limit: int = Field(default=10, ge=1, le=1000, description="Records per page")

    # Sorting: "field:asc" or "field1:asc,field2:desc"
    sort: str | None = Field(default=None, description="Sort: field:direction[,field2:direction2]")

    # Search: JSON string e.g. '{"status":"active","age":{">":25}}'
    s: str | None = Field(default=None, description='JSON search: {"field": {"operator": value}}')

    # Filtering: ["field||$operator||value"]
    # Operators: $eq $ne $gt $lt $gte $lte $cont $starts $ends $in $notin $isnull $notnull $between
    filter: list[str] = Field(default=[], description='Filter: field||$operator||value')

    # OR conditions (same format as filter)
    or_: list[str] = Field(default=[], alias="or", description='OR filter: field||$operator||value')

    # Sparse fieldsets: "field1,field2"
    fields: str | None = Field(default=None, description="Comma-separated field names to return")

    # Timezone for datetime comparison
    timezone: str = Field(default="Asia/Bangkok", description="IANA timezone")

    # Exclude specific IDs: "id1,id2,id3"
    exclude_ids: str | None = Field(default=None, description="Comma-separated IDs to exclude")

    # Flags
    ignore_limit: bool = Field(default=False, description="Return all records without pagination")
    get_count_only: bool = Field(default=False, description="Return only total count")

    class Config:
        populate_by_name = True   # allow both "or" and "or_" aliases
```

---

## 3. SQLQueryBuilder

เลียนแบบ `typeorm-query-builder.util.ts` แต่ใช้ raw SQL แทน TypeORM
เพราะ repositories เดิมใช้ SQLAlchemy Core (text() + parameterized queries)

### `api/base/query_builder.py`

```python
class SQLQueryBuilder:
    """Translates QueryParams into SQL WHERE / ORDER BY / LIMIT / OFFSET fragments"""

    FILTER_OPERATORS = {
        "$eq": "= :{}",
        "$ne": "!= :{}",
        "$gt": "> :{}",
        "$lt": "< :{}",
        "$gte": ">= :{}",
        "$lte": "<= :{}",
        "$cont": "ILIKE :{}",       # %value%
        "$starts": "ILIKE :{}",     # value%
        "$ends": "ILIKE :{}",       # %value
        "$in": "IN :{}",
        "$notin": "NOT IN :{}",
        "$isnull": "IS NULL",
        "$notnull": "IS NOT NULL",
        "$between": "BETWEEN :{} AND :{}",
    }

    def __init__(self, params: QueryParams, allowed_fields: list[str]):
        self.params = params
        self.allowed_fields = allowed_fields
        self.bind_params: dict = {}
        self._counter = 0

    def build(self) -> dict:
        return {
            "where": self._build_where(),
            "order_by": self._build_order(),
            "limit": None if self.params.ignore_limit else self.params.limit,
            "offset": self._calc_offset(),
            "bind_params": self.bind_params,
            "fields": self._parse_fields(),
            "exclude_ids": self._parse_exclude_ids(),
        }

    def _build_where(self) -> str:
        # Parse params.filter (AND conditions)
        # Parse params.or_ (OR conditions)
        # Parse params.s (JSON search conditions)
        # Combine and return SQL fragment

    def _build_order(self) -> str:
        # Parse params.sort → "field ASC, field2 DESC"

    def _calc_offset(self) -> int:
        if self.params.offset is not None:
            return self.params.offset
        return (self.params.page - 1) * self.params.limit

    def _parse_fields(self) -> list[str] | None:
        if not self.params.fields:
            return None
        return [f.strip() for f in self.params.fields.split(",")]

    def _parse_exclude_ids(self) -> list[str]:
        if not self.params.exclude_ids:
            return []
        return [x.strip() for x in self.params.exclude_ids.split(",")]

    def _validate_field(self, field: str) -> None:
        if field not in self.allowed_fields:
            raise HTTPException(400, detail=f"Invalid filter field: '{field}'")
```

**Filter syntax** (เหมือน NestJS):

- `filter=status||$eq||active` → `WHERE status = 'active'`
- `filter=name||$cont||hospital` → `WHERE name ILIKE '%hospital%'`
- `filter=created_at||$between||2026-01-01,2026-12-31` → `WHERE created_at BETWEEN ...`
- `or=status||$eq||active&or=status||$eq||pending` → `WHERE (status='active' OR status='pending')`

**s (JSON search)**:

- `s={"name":{"like":"hospital"}}` → `WHERE name ILIKE '%hospital%'`
- `s={"db_type":"mysql"}` → `WHERE db_type = 'mysql'`

---

## 4. Exception Handling

เลียนแบบ `AllExceptionsFilter` — global handler ที่ format error response แบบสม่ำเสมอ

### Exception Types

| Type                        | HTTP Status | status.code | เมื่อไหร่                    |
| --------------------------- | ----------- | ----------- | ---------------------------- |
| `InvalidParameterException` | 400         | `400002`    | query param format ผิด       |
| `ValidationException`       | 422         | `400001`    | request body validation fail |
| `HTTPException`             | varies      | HTTP status | 404, 403, 409 etc.           |
| Unhandled (Exception)       | 500         | `500`       | unexpected crash             |

### Error Response Format

**InvalidParameterException** (query param format errors):

```json
{
  "status": { "code": 400002, "message": "Invalid Parameters" },
  "errors": [
    {
      "code": "INVALID_PARAMETER_FORMAT",
      "title": "Invalid Parameter Format",
      "detail": "Invalid filter field: 'unknown_field'",
      "source": { "parameter": "filter" }
    }
  ],
  "meta": { "timestamp": "2026-04-11T10:00:00Z" }
}
```

**ValidationException** (request body errors):

```json
{
  "status": { "code": 400001, "message": "Validation Failed" },
  "errors": [
    {
      "code": "VALIDATION_ERROR",
      "title": "Invalid Input",
      "detail": "field required",
      "source": { "pointer": "/data/attributes/name" }
    }
  ],
  "meta": { "timestamp": "2026-04-11T10:00:00Z" }
}
```

**HTTPException** (404, 403, 409 etc.):

```json
{
  "status": { "code": 404, "message": "Not found: 99" },
  "errors": [
    {
      "code": 404,
      "title": "An error occurred",
      "detail": "Not found: 99"
    }
  ],
  "meta": { "timestamp": "2026-04-11T10:00:00Z" }
}
```

**Unhandled Exception** (500):

```json
{
  "status": { "code": 500, "message": "Internal Server Error" },
  "errors": [
    {
      "code": "INTERNAL_SERVER_ERROR",
      "title": "An unexpected error occurred",
      "detail": "Please try again later or contact support."
    }
  ],
  "meta": { "timestamp": "2026-04-11T10:00:00Z" }
}
```

### `api/base/exceptions.py` — Custom Exception Classes + Handlers

```python
# Custom exception classes
class InvalidParameterException(HTTPException):
    """Query param format errors (code 400002)"""
    def __init__(self, errors: list[dict]):
        super().__init__(status_code=400)
        self.validation_errors = errors   # [{"field": ..., "message": ...}]

class ValidationException(HTTPException):
    """Request body validation errors (code 400001)"""
    def __init__(self, errors: list[dict]):
        super().__init__(status_code=422)
        self.validation_errors = errors   # [{"field": ..., "messages": [...]}]

# Exception handlers — registered in api/main.py
async def invalid_parameter_handler(request, exc: InvalidParameterException):
    return JSONResponse(status_code=400, content={
        "status": {"code": 400002, "message": "Invalid Parameters"},
        "errors": [{"code": "INVALID_PARAMETER_FORMAT",
                    "title": "Invalid Parameter Format",
                    "detail": e["message"],
                    "source": {"parameter": e["field"]}}
                   for e in exc.validation_errors],
        "meta": {"timestamp": datetime.utcnow().isoformat()}
    })

async def validation_handler(request, exc: ValidationException):
    # Pydantic RequestValidationError → 400001
    ...

async def http_exception_handler(request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={
        "status": {"code": exc.status_code, "message": exc.detail},
        "errors": [{"code": exc.status_code, "title": "An error occurred",
                    "detail": exc.detail}],
        "meta": {"timestamp": datetime.utcnow().isoformat()}
    })

async def unhandled_exception_handler(request, exc: Exception):
    # log exc
    return JSONResponse(status_code=500, content={
        "status": {"code": 500, "message": "Internal Server Error"},
        "errors": [{"code": "INTERNAL_SERVER_ERROR",
                    "title": "An unexpected error occurred",
                    "detail": "Please try again later or contact support."}],
        "meta": {"timestamp": datetime.utcnow().isoformat()}
    })
```

**Registration in `api/main.py`:**

```python
from fastapi.exceptions import RequestValidationError

app.add_exception_handler(InvalidParameterException, invalid_parameter_handler)
app.add_exception_handler(ValidationException, validation_handler)
app.add_exception_handler(RequestValidationError, validation_handler)  # Pydantic errors
app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)
```

---

## 5. Clean Architecture — Layer Rules

อ้างอิงจาก `clean-architecture.md` + `base-operations-architecture.md`

```
Client Request
     ↓
Controller          ← HTTP routing, DTO validation (thin — no business logic)
     ↓
Service             ← Business logic, DB error handling, query building
     ↓
Repository          ← Database access (raw SQL via SQLAlchemy Core)
     ↓
PostgreSQL
```

### Controller Layer Rules
- **DO**: Map HTTP routes, extract inputs (body, params, query), call service, return result
- **DO**: Rely on JSON:API response builder + exception handlers to format output
- **DO NOT**: Put business logic, catch exceptions, or access DB directly
- **DO NOT**: Transform data — that's the service's job

### Service Layer Rules
- **DO**: Own all business logic, validate rules, wrap DB calls in `execute_db_operation()`
- **DO**: Throw specific exceptions (`HTTPException 404`, `ConflictException 409`, etc.)
- **DO**: Use `get_transaction()` for multi-step writes
- **DO**: Define `allowed_fields` for query validation
- **DO NOT**: Know about HTTP, requests, or responses
- **DO NOT**: Catch exceptions silently (no `try/except` that returns `None`)

### Repository Layer Rules
- **DO**: Execute SQL only (SELECT, INSERT, UPDATE, DELETE)
- **DO NOT**: Contain business logic or HTTP concepts
- Existing repositories ใช้ได้ตรง ๆ ไม่ต้องแก้ไข

---

## 6. Base Classes

### `api/base/service.py` — Abstract Base Service

```python
from abc import ABC, abstractmethod
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

class BaseService(ABC):
    # ── Subclass must declare ───────────────────────────────────────────────
    resource_type: str                    # e.g. "datasources"
    allowed_fields: list[str] = []        # for query/filter field validation

    # ── Abstract CRUD methods ───────────────────────────────────────────────
    @abstractmethod
    def find_all(self, params: QueryParams) -> dict:
        """Return raw dict: {data: list, total: int, page: int, page_size: int, total_pages: int}"""

    @abstractmethod
    def find_by_id(self, id: str | int) -> dict:
        """Return single record dict or raise HTTPException 404"""

    @abstractmethod
    def create(self, data) -> dict:
        """Return created record dict"""

    @abstractmethod
    def update(self, id: str | int, data) -> dict:
        """Return updated record dict"""

    @abstractmethod
    def delete(self, id: str | int) -> None:
        """Raise HTTPException 404 if not found"""

    # ── execute_db_operation wrapper ────────────────────────────────────────
    # เลียนแบบ BaseServiceOperations.executeDbOperation()
    # จับ PostgreSQL error codes แล้ว map เป็น HTTP exceptions
    def execute_db_operation(self, operation: callable):
        """
        Wrap DB calls to catch PostgreSQL-specific errors
        and re-raise as consistent HTTP exceptions.
        """
        try:
            return operation()
        except IntegrityError as e:
            pg_code = getattr(e.orig, 'pgcode', None)
            detail = str(e.orig) if e.orig else str(e)

            error_map = {
                '23505': (409, f"Duplicate record in {self.resource_type}. {detail}"),
                '23503': (400, f"Invalid reference to another record. {detail}"),
                '23502': (400, f"A required field was left empty. {detail}"),
                '22P02': (400, f"Invalid format for a field. {detail}"),
            }

            if pg_code in error_map:
                status, msg = error_map[pg_code]
                raise HTTPException(status_code=status, detail=msg)

            raise HTTPException(status_code=500, detail=f"Database error: {detail}")
        except HTTPException:
            raise  # re-raise HTTP exceptions as-is
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    # ── PostgreSQL Error Code Reference ─────────────────────────────────────
    # | PG Code | Meaning          | HTTP Status | Exception          |
    # |---------|------------------|-------------|--------------------|
    # | 23505   | Unique violation | 409         | Conflict           |
    # | 23503   | FK violation     | 400         | Bad Request        |
    # | 23502   | Not-null         | 400         | Bad Request        |
    # | 22P02   | Invalid UUID     | 400         | Bad Request        |
    # | Other   | Unknown          | 500         | Internal Server    |

    # ── Shared helpers ──────────────────────────────────────────────────────
    def _df_to_list(self, df) -> list[dict]:
        """DataFrame → list[dict], fix UUID columns, replace NaN with None"""

    def _assert_found(self, result, id: str | int) -> None:
        if result is None:
            raise HTTPException(status_code=404, detail=f"Not found: {id}")

    def _assert_success(self, ok: bool, message: str) -> None:
        """Convert repo's (bool, str) tuple → HTTPException 400 if failed"""
        if not ok:
            raise HTTPException(status_code=400, detail=message)

    def _build_pagination_meta(self, total: int, params: QueryParams) -> dict:
        page_size = params.limit
        total_pages = max(1, math.ceil(total / page_size))
        return {
            "page": params.page,
            "page_size": page_size,
            "total": total,
            "total_records": total,
            "total_pages": total_pages,
        }
```

### `api/base/controller.py` — Base Controller

```python
class BaseController:
    def __init__(
        self,
        prefix: str,                      # e.g. "datasources"
        service: BaseService,
        create_schema: Type[BaseModel],
        update_schema: Type[BaseModel],
        tags: list[str],
    ):
        self.router = APIRouter(prefix=f"/api/v1/{prefix}", tags=tags)
        self.service = service
        self._prefix = prefix
        self._register_routes(create_schema, update_schema)

    def _register_routes(self, create_schema, update_schema):
        router = self.router
        svc = self.service

        @router.get("/")
        def find_all(request: Request, params: QueryParams = Depends()):
            result = svc.find_all(params)
            # result shape: {data: list, total: int, page: int, page_size: int, total_pages: int}
            return create_paginated_response(svc.resource_type, result, request)

        @router.get("/{id}")
        def find_by_id(id: str, request: Request):
            item = svc.find_by_id(id)
            return create_success_response(svc.resource_type, item, request)

        @router.post("/", status_code=201)
        def create(data: create_schema, request: Request):
            created = svc.create(data)
            return create_created_response(svc.resource_type, created, request)

        @router.put("/{id}")
        def update(id: str, data: update_schema, request: Request):
            updated = svc.update(id, data)
            return create_success_response(svc.resource_type, updated, request)

        @router.delete("/{id}", status_code=204)
        def delete(id: str, request: Request):
            svc.delete(id)
            return create_no_content_response()
```

---

## 7. Resource Endpoints

### `/api/v1/datasources` — SERIAL int PK

| Method | Path                       | Status    | Description                |
| ------ | -------------------------- | --------- | -------------------------- |
| GET    | `/api/v1/datasources`      | 200       | List all (password ไม่ส่ง) |
| GET    | `/api/v1/datasources/{id}` | 200 / 404 | Get by integer id          |
| POST   | `/api/v1/datasources`      | 201       | Create datasource          |
| PUT    | `/api/v1/datasources/{id}` | 200 / 404 | Update datasource          |
| DELETE | `/api/v1/datasources/{id}` | 204 / 404 | Delete datasource          |

`DatasourcesService` delegates to: `repositories/datasource_repo.py`

---

### `/api/v1/configs` — UUID PK, natural key = config_name

| Method | Path                                       | Status    | Description               |
| ------ | ------------------------------------------ | --------- | ------------------------- |
| GET    | `/api/v1/configs`                          | 200       | List all configs          |
| GET    | `/api/v1/configs/{name}`                   | 200 / 404 | Get by config_name        |
| POST   | `/api/v1/configs`                          | 201       | Create/upsert config      |
| PUT    | `/api/v1/configs/{name}`                   | 200 / 404 | Update config             |
| DELETE | `/api/v1/configs/{name}`                   | 204 / 404 | Delete config             |
| GET    | `/api/v1/configs/{name}/history`           | 200       | Version history list      |
| GET    | `/api/v1/configs/{name}/history/{version}` | 200 / 404 | Specific version snapshot |

`ConfigsService` delegates to: `repositories/config_repo.py`

---

### `/api/v1/config-histories` — UUID PK, read-only

| Method | Path                                                         | Status    | Description           |
| ------ | ------------------------------------------------------------ | --------- | --------------------- |
| GET    | `/api/v1/config-histories?filter=config_id\|\|$eq\|\|{uuid}` | 200       | List histories        |
| GET    | `/api/v1/config-histories/{id}`                              | 200 / 404 | Single history record |

---

### `/api/v1/pipelines` — UUID PK, natural key = name

| Method | Path                       | Status    | Description            |
| ------ | -------------------------- | --------- | ---------------------- |
| GET    | `/api/v1/pipelines`        | 200       | List all pipelines     |
| GET    | `/api/v1/pipelines/{name}` | 200 / 404 | Get by name            |
| POST   | `/api/v1/pipelines`        | 201       | Create/upsert pipeline |
| PUT    | `/api/v1/pipelines/{name}` | 200 / 404 | Update pipeline        |
| DELETE | `/api/v1/pipelines/{name}` | 204 / 404 | Delete pipeline        |

`PipelinesService` delegates to: `repositories/pipeline_repo.py`

---

### `/api/v1/pipeline-runs` — UUID PK

| Method | Path                                                        | Status    | Description                    |
| ------ | ----------------------------------------------------------- | --------- | ------------------------------ |
| GET    | `/api/v1/pipeline-runs?filter=pipeline_id\|\|$eq\|\|{uuid}` | 200       | List runs (filter by pipeline) |
| GET    | `/api/v1/pipeline-runs/running`                             | 200       | All currently running runs     |
| GET    | `/api/v1/pipeline-runs/{id}`                                | 200 / 404 | Get single run                 |
| POST   | `/api/v1/pipeline-runs`                                     | 201       | Create a new run               |
| PUT    | `/api/v1/pipeline-runs/{id}`                                | 200       | Update run status              |

`PipelineRunsService` delegates to: `repositories/pipeline_run_repo.py`

---

## 8. Existing Files — Reused Directly (ไม่แก้ไข)

- `repositories/datasource_repo.py`
- `repositories/config_repo.py`
- `repositories/pipeline_repo.py`
- `repositories/pipeline_run_repo.py`
- `repositories/connection.py`
- `models/*.py`
- `app.py` + ทุก controllers/views ของ Streamlit

---

## 9. Modified Files

- `requirements.txt` — เพิ่ม `fastapi>=0.115.0`, `uvicorn[standard]>=0.30.0`

---

## 10. CORS (สำหรับ Svelte dev server)

```python
# api/main.py
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],   # Svelte Vite default
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## 11. How to Run

```bash
# FastAPI (port 8000) — development with auto-reload
uvicorn api.main:app --reload --port 8000

# Streamlit (port 8501) — unchanged
streamlit run app.py
```

**Swagger UI:** `http://localhost:8000/docs`
**ReDoc:** `http://localhost:8000/redoc`

---

## 12. Limitations & Future Enhancements

### Relations (`relations` parameter)
เนื่องจาก repositories ปัจจุบันใช้ raw SQL ไม่ใช่ ORM models จึงยังไม่รองรับ:
- eager loading ผ่าน `?relations=addresses`
- `allowedRelations` whitelist per service
- nested relation validation (dot notation)

**Future**: เมื่อ migrate ไป SQLAlchemy ORM models สามารถเพิ่ม `relations` parameter + whitelist ได้

### Fields Validation
ปัจจุบัน `fields` parameter จะ return ทุก column แล้ว filter ที่ Python layer
**Future**: generate `SELECT col1, col2` ตรง SQL level เพื่อ performance

### Soft Delete
ยังไม่มี `is_deleted` / `deleted_at` columns ใน tables ปัจจุบัน
**Future**: เพิ่ม soft delete columns + auto-filter ใน BaseService เหมือน NestJS

---

## 13. Verification Steps

1. `pip install fastapi "uvicorn[standard]"`
2. `uvicorn api.main:app --reload --port 8000`
3. เปิด `http://localhost:8000/docs` → ตรวจว่า 5 resource routers ขึ้นถูกต้อง
4. Test ผ่าน Swagger UI:
   - `GET /api/v1/datasources` → 200, JSON:API collection response พร้อม pagination meta
   - `POST /api/v1/datasources` → 201, JSON:API single resource response พร้อม status.code 201000
   - `PUT /api/v1/datasources/{id}` → 200
   - `DELETE /api/v1/datasources/{id}` → 204
   - `GET /api/v1/datasources?filter=db_type||$eq||mysql` → filtered results
   - `GET /api/v1/datasources?sort=name:asc&limit=5&page=1` → sorted paginated
   - `GET /api/v1/configs/{name}/history` → version list
5. ตรวจว่า Streamlit (`streamlit run app.py`) ยังทำงานปกติ
6. `python3.11 -m pytest tests/ -v` → no regressions
