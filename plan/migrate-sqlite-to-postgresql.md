# Plan: SQLite → PostgreSQL Migration + SOLID Refactoring + Clean Architecture

## Overview

| Aspect | Current | Target |
|--------|---------|--------|
| Internal DB | `sqlite3` raw + `pd.read_sql_query()` | PostgreSQL via SQLAlchemy Core `text()` + Engine pooling |
| Credentials | Hardcoded path `migration_tool.db` | Environment variable `DATABASE_URL` |
| Architecture | `database.py` God Module (8 responsibilities, 22 functions) | Split repositories per domain + Protocol interfaces |
| SOLID | Multiple violations across layers | Protocol-based DI, registry patterns, split responsibilities |
| MVC | 2/6 pages refactored; 10/13 view files import services/database directly | All pages follow strict MVC |
| Table naming | Mixed | Plural nouns + snake_case |
| Directory structure | Flat `models/`, `services/`, `views/` | Incremental: add `repositories/`, `protocols/`, `registries/` |

### Scope Decisions

- **DB**: PostgreSQL only (drop SQLite entirely)
- **Connection method**: SQLAlchemy Core + `text()` (no ORM)
- **Clean Arch**: Incremental — keep existing folder structure, add new abstractions alongside
- **Legacy views**: Refactor all remaining pages to strict MVC
- **Single file**: All changes planned in this one document

---

## Phase 1: PostgreSQL Migration — Config & Connection

### 1A. `config.py` — Replace `DB_FILE` with `DATABASE_URL`

**Before:**
```python
DB_FILE = os.path.join(BASE_DIR, "migration_tool.db")
```

**After:**
```python
def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required.\n"
            "Example: DATABASE_URL=postgresql://user:pass@localhost:5432/his_analyzer"
        )
    return url
```

Keep all other constants (`TRANSFORMER_OPTIONS`, `VALIDATOR_OPTIONS`, `DB_TYPES`) for now — they migrate to registries in Phase 5.

### 1B. New file `repositories/connection.py` — Engine singleton

```python
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config import get_database_url

_engine: Engine | None = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_database_url(),
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _engine

def dispose_engine() -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
```

**Why this over SQLite pattern:**
- SQLAlchemy Engine is thread-safe (fixes `check_same_thread` concern)
- Connection pooling built-in (no more open/close per function)
- `pool_pre_ping=True` auto-detects stale connections

### 1C. `.env.example` + `python-dotenv`

```env
DATABASE_URL=postgresql://his_user:your_password@localhost:5432/his_analyzer
```

Add `python-dotenv` to `requirements.txt`.

In `app.py`, add before `db.init_db()`:
```python
from dotenv import load_dotenv
load_dotenv()
```

---

## Phase 2: Split `database.py` → Repositories (SRP)

### 2A. New directory `repositories/`

```
repositories/
├── __init__.py
├── connection.py          # get_engine(), dispose_engine() (from Phase 1B)
├── base.py                # DDL init, shared helpers
├── datasource_repo.py     # Datasource CRUD (5 functions)
├── config_repo.py         # Config CRUD + history (7 functions)
├── pipeline_repo.py       # Pipeline CRUD (4 functions)
└── pipeline_run_repo.py   # Pipeline Run CRUD (4 functions)
```

### 2B. `repositories/base.py` — DDL + init

```python
from sqlalchemy import text
from repositories.connection import get_engine

TABLES_DDL = [
    """CREATE TABLE IF NOT EXISTS datasources (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        db_type VARCHAR(50),
        host VARCHAR(255),
        port VARCHAR(10),
        dbname VARCHAR(255),
        username VARCHAR(255),
        password VARCHAR(255)
    )""",
    """CREATE TABLE IF NOT EXISTS configs (
        id VARCHAR(36) PRIMARY KEY,
        config_name VARCHAR(255) UNIQUE NOT NULL,
        table_name VARCHAR(255),
        json_data TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS config_histories (
        id VARCHAR(36) PRIMARY KEY,
        config_id VARCHAR(36) NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        version INTEGER NOT NULL,
        json_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS pipelines (
        id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        description TEXT DEFAULT '',
        json_data TEXT,
        source_datasource_id INTEGER,
        target_datasource_id INTEGER,
        error_strategy VARCHAR(50) DEFAULT 'fail_fast',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        id VARCHAR(36) PRIMARY KEY,
        pipeline_id VARCHAR(36) NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        status VARCHAR(50) DEFAULT 'pending',
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        steps_json TEXT,
        error_message TEXT
    )""",
]

def init_db() -> None:
    with get_engine().begin() as conn:
        for ddl in TABLES_DDL:
            conn.execute(text(ddl))
```

### 2C. `repositories/datasource_repo.py` — Example conversion

```python
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import pandas as pd
from repositories.connection import get_engine

def get_all() -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(
            "SELECT id, name, db_type, host, dbname, username FROM datasources",
            conn
        )

def get_by_id(id: int) -> dict | None:
    with get_engine().connect() as conn:
        result = conn.execute(
            text("SELECT * FROM datasources WHERE id = :id"),
            {"id": id}
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))

def get_by_name(name: str) -> dict | None:
    with get_engine().connect() as conn:
        result = conn.execute(
            text("SELECT * FROM datasources WHERE name = :name"),
            {"name": name}
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))

def save(name, db_type, host, port, dbname, username, password) -> tuple[bool, str]:
    try:
        with get_engine().begin() as conn:
            conn.execute(text("""
                INSERT INTO datasources (name, db_type, host, port, dbname, username, password)
                VALUES (:name, :db_type, :host, :port, :dbname, :username, :password)
            """), {"name": name, "db_type": db_type, "host": host,
                   "port": port, "dbname": dbname,
                   "username": username, "password": password})
        return True, f"✅ บันทึก '{name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{name}' มีอยู่แล้ว"

def update(id, name, db_type, host, port, dbname, username, password) -> tuple[bool, str]:
    try:
        with get_engine().begin() as conn:
            conn.execute(text("""
                UPDATE datasources SET name=:name, db_type=:db_type, host=:host,
                    port=:port, dbname=:dbname, username=:username, password=:password
                WHERE id=:id
            """), {"id": id, "name": name, "db_type": db_type, "host": host,
                   "port": port, "dbname": dbname,
                   "username": username, "password": password})
        return True, f"✅ อัปเดต '{name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{name}' มีอยู่แล้ว"

def delete(id) -> None:
    with get_engine().begin() as conn:
        conn.execute(text("DELETE FROM datasources WHERE id = :id"), {"id": id})
```

### 2D. SQLite → PostgreSQL dialect mapping (all repos must follow)

| SQLite | PostgreSQL / SQLAlchemy |
|--------|------------------------|
| `sqlite3.connect(DB_FILE)` | `get_engine().connect()` / `get_engine().begin()` |
| `cursor.execute("... WHERE id=?", (id,))` | `conn.execute(text("... WHERE id = :id"), {"id": id})` |
| `cursor.fetchone()` | `result.fetchone()` + `dict(zip(result.keys(), row))` |
| `INSERT OR REPLACE INTO` | `INSERT INTO ... ON CONFLICT (col) DO UPDATE SET ...` |
| `sqlite3.IntegrityError` | `sqlalchemy.exc.IntegrityError` |
| `pd.read_sql_query(sql, conn)` | `pd.read_sql(sql, conn)` |
| `conn.commit()` | `with engine.begin() as conn:` (auto-commit) |
| `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| `sqlite_master` / `PRAGMA` | `information_schema` |

### 2E. `repositories/config_repo.py` — Upsert pattern

```python
def save(config_name, table_name, json_data) -> tuple[bool, str]:
    config_id = str(uuid.uuid4())
    with get_engine().begin() as conn:
        result = conn.execute(
            text("SELECT id FROM configs WHERE config_name = :name"),
            {"name": config_name}
        )
        existing = result.fetchone()

        if existing:
            config_id = existing[0]
            conn.execute(text("""
                UPDATE configs SET table_name=:table_name, json_data=:json_data,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=:id
            """), {"id": config_id, "table_name": table_name, "json_data": json_data})
        else:
            conn.execute(text("""
                INSERT INTO configs (id, config_name, table_name, json_data, updated_at)
                VALUES (:id, :config_name, :table_name, :json_data, CURRENT_TIMESTAMP)
            """), {"id": config_id, "config_name": config_name,
                   "table_name": table_name, "json_data": json_data})

        # Version history
        ver_result = conn.execute(
            text("SELECT COALESCE(MAX(version), 0) FROM config_histories WHERE config_id = :cid"),
            {"cid": config_id}
        )
        next_version = ver_result.scalar() + 1
        conn.execute(text("""
            INSERT INTO config_histories (id, config_id, version, json_data, created_at)
            VALUES (:id, :config_id, :version, :json_data, CURRENT_TIMESTAMP)
        """), {"id": str(uuid.uuid4()), "config_id": config_id,
               "version": next_version, "json_data": json_data})

    return True, f"✅ บันทึก config '{config_name}' สำเร็จ (version {next_version})"
```

### 2F. `repositories/pipeline_repo.py` — Upsert with ON CONFLICT

```python
def save(name, description, json_data, source_ds_id, target_ds_id, error_strategy) -> tuple[bool, str]:
    pipeline_id = str(uuid.uuid4())
    try:
        with get_engine().begin() as conn:
            conn.execute(text("""
                INSERT INTO pipelines (id, name, description, json_data,
                    source_datasource_id, target_datasource_id, error_strategy, updated_at)
                VALUES (:id, :name, :description, :json_data,
                    :src_ds, :tgt_ds, :strategy, CURRENT_TIMESTAMP)
                ON CONFLICT (name) DO UPDATE SET
                    id = EXCLUDED.id,
                    description = EXCLUDED.description,
                    json_data = EXCLUDED.json_data,
                    source_datasource_id = EXCLUDED.source_datasource_id,
                    target_datasource_id = EXCLUDED.target_datasource_id,
                    error_strategy = EXCLUDED.error_strategy,
                    updated_at = CURRENT_TIMESTAMP
            """), {"id": pipeline_id, "name": name, "description": description,
                   "json_data": json_data, "src_ds": source_ds_id,
                   "tgt_ds": target_ds_id, "strategy": error_strategy})
        return True, f"✅ Pipeline '{name}' บันทึกสำเร็จ"
    except IntegrityError:
        return False, f"❌ Pipeline '{name}' มีอยู่แล้ว"
```

### 2G. `repositories/pipeline_run_repo.py` — Thread-safe writes

```python
def save(pipeline_id, status, steps_json) -> str:
    run_id = str(uuid.uuid4())
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_runs (id, pipeline_id, status, started_at, steps_json)
            VALUES (:id, :pipeline_id, :status, CURRENT_TIMESTAMP, :steps_json)
        """), {"id": run_id, "pipeline_id": pipeline_id,
               "status": status, "steps_json": steps_json})
    return run_id

def update(run_id, status, steps_json=None, error_message=None) -> None:
    with get_engine().begin() as conn:
        conn.execute(text("""
            UPDATE pipeline_runs
            SET status = :status,
                steps_json = COALESCE(:steps_json, steps_json),
                error_message = COALESCE(:error_message, error_message),
                completed_at = CASE WHEN :status IN ('completed','failed','partial')
                    THEN CURRENT_TIMESTAMP ELSE completed_at END
            WHERE id = :id
        """), {"id": run_id, "status": status,
               "steps_json": steps_json, "error_message": error_message})
```

**Key change:** PostgreSQL's `CASE WHEN` replaces SQLite's `CASE WHEN ? THEN ? ELSE` positional params with named `:status` — cleaner and less error-prone.

### 2H. Delete `database.py`

After all repos are verified, delete `database.py` entirely. All 15 importers will point to new repos (see Phase 7).

---

## Phase 3: Protocol Interfaces (DIP)

### 3A. New directory `protocols/`

```
protocols/
├── __init__.py
├── repository.py           # Repository protocols
├── database_dialect.py     # Database dialect protocol
└── transformer.py          # Transformer + Validator protocols
```

### 3B. `protocols/repository.py`

```python
from typing import Protocol, runtime_checkable
import pandas as pd

@runtime_checkable
class DatasourceRepository(Protocol):
    def get_all(self) -> pd.DataFrame: ...
    def get_by_id(self, id: int) -> dict | None: ...
    def get_by_name(self, name: str) -> dict | None: ...
    def save(self, name, db_type, host, port, dbname, username, password) -> tuple[bool, str]: ...
    def update(self, id, name, db_type, host, port, dbname, username, password) -> tuple[bool, str]: ...
    def delete(self, id) -> None: ...

@runtime_checkable
class ConfigRepository(Protocol):
    def save(self, config_name, table_name, json_data) -> tuple[bool, str]: ...
    def get_list(self) -> pd.DataFrame: ...
    def get_content(self, config_name: str) -> dict | None: ...
    def delete(self, config_name: str) -> tuple[bool, str]: ...
    def get_history(self, config_name: str) -> pd.DataFrame: ...
    def get_version(self, config_name: str, version: int) -> dict | None: ...
    def compare_versions(self, config_name: str, v1: int, v2: int) -> dict | None: ...

@runtime_checkable
class PipelineRepository(Protocol):
    def save(self, name, description, json_data, src_ds, tgt_ds, strategy) -> tuple[bool, str]: ...
    def get_list(self) -> pd.DataFrame: ...
    def get_by_name(self, name: str) -> dict | None: ...
    def delete(self, name: str) -> tuple[bool, str]: ...

@runtime_checkable
class PipelineRunRepository(Protocol):
    def save(self, pipeline_id, status, steps_json) -> str: ...
    def update(self, run_id, status, steps_json=None, error_message=None) -> None: ...
    def get_list(self, pipeline_id: str) -> pd.DataFrame: ...
    def get_latest(self, pipeline_id: str) -> dict | None: ...
```

### 3C. `protocols/database_dialect.py`

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class DatabaseDialect(Protocol):
    name: str
    default_port: str
    default_charset: str

    def build_url(self, host, port, dbname, username, password, charset) -> str: ...
    def get_schema_default(self) -> str: ...
    def quote_identifier(self, name: str) -> str: ...
```

### 3D. `protocols/transformer.py`

```python
from typing import Protocol, runtime_checkable, Any

@runtime_checkable
class Transformer(Protocol):
    name: str
    label: str
    description: str
    has_params: bool

    def transform(self, series: Any, params: dict | None = None) -> Any: ...
```

---

## Phase 4: Database Dialect Strategy (OCP — fix hardcoded `if db_type == "MySQL"`)

### 4A. New directory `dialects/`

```
dialects/
├── __init__.py
├── registry.py             # DialectRegistry singleton
├── base.py                 # BaseDialect ABC
├── mysql.py                # MySQLDialect
├── postgresql.py           # PostgreSQLDialect
└── mssql.py                # MSSQLDialect
```

### 4B. `dialects/base.py` — Abstract base

```python
from abc import ABC, abstractmethod

class BaseDialect(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def default_port(self) -> str: ...

    @property
    @abstractmethod
    def default_charset(self) -> str: ...

    @abstractmethod
    def build_url(self, host, port, dbname, username, password, charset) -> str: ...

    def get_schema_default(self) -> str:
        return "public"

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'
```

### 4C. `dialects/mysql.py` — Example

```python
from sqlalchemy import URL
from dialects.base import BaseDialect

class MySQLDialect(BaseDialect):
    name = "MySQL"
    default_port = "3306"
    default_charset = "utf8mb4"

    def build_url(self, host, port, dbname, username, password, charset):
        return URL.create(
            "mysql+pymysql",
            username=username, password=password,
            host=host, port=int(port), database=dbname,
            query={"charset": charset or self.default_charset}
        )

    def get_schema_default(self) -> str:
        return "dbo"

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"
```

### 4D. `dialects/registry.py`

```python
from dialects.base import BaseDialect

_dialects: dict[str, BaseDialect] = {}

def register(dialect: BaseDialect) -> None:
    _dialects[dialect.name] = dialect

def get(name: str) -> BaseDialect:
    if name not in _dialects:
        raise ValueError(f"Unknown database type: {name}")
    return _dialects[name]

def available_types() -> list[str]:
    return list(_dialects.keys())

# Auto-register built-in dialects
from dialects.mysql import MySQLDialect
from dialects.postgresql import PostgreSQLDialect
from dialects.mssql import MSSQLDialect

register(MySQLDialect())
register(PostgreSQLDialect())
register(MSSQLDialect())
```

### 4E. `config.py` — Replace hardcoded `DB_TYPES`

```python
# Before:
DB_TYPES = ["MySQL", "Microsoft SQL Server", "PostgreSQL"]

# After:
def get_db_types() -> list[str]:
    from dialects.registry import available_types
    return available_types()
```

### 4F. `services/db_connector.py` — Use dialect registry

Replace all `if db_type == "MySQL" ... elif` chains (20+ locations across 3 files):

```python
# Before (20+ if/elif locations):
if db_type == "MySQL":
    connection_url = URL.create("mysql+pymysql", ...)
elif db_type == "PostgreSQL":
    connection_url = URL.create("postgresql+psycopg2", ...)
elif db_type == "Microsoft SQL Server":
    connection_url = URL.create("mssql+pymssql", ...)

# After:
from dialects.registry import get as get_dialect

dialect = get_dialect(db_type)
connection_url = dialect.build_url(host, port, dbname, username, password, charset)
engine = create_engine(connection_url, **engine_kwargs)
```

Same pattern for `get_tables_from_datasource()`, `get_columns_from_table()`, `get_foreign_keys()` — replace schema/identifier quoting with `dialect.get_schema_default()` and `dialect.quote_identifier()`.

---

## Phase 5: Transformer & Validator Registry (OCP)

### 5A. `services/transformers.py` → Split into `transformers/` package

```
transformers/
├── __init__.py              # Re-exports DataTransformer + registry
├── registry.py              # @register_transformer decorator
├── base.py                  # DataTransformer (refactored)
├── text.py                  # TRIM, UPPER, LOWER, CLEAN_SPACES, CONCAT
├── dates.py                 # BUDDHIST_TO_ISO, ENG_DATE_TO_ISO
├── healthcare.py            # GENERATE_HN, MAP_GENDER, FORMAT_PHONE
├── names.py                 # REMOVE_PREFIX, SPLIT_THAI_NAME, SPLIT_ENG_NAME
├── data_type.py             # TO_NUMBER, REPLACE_EMPTY_WITH_NULL, BIT_CAST
└── lookup.py                # LOOKUP_REPLACE, VALUE_MAP
```

### 5B. `transformers/registry.py`

```python
_TRANSFORMERS: dict[str, callable] = {}
_LABELS: dict[str, str] = {}
_DESCRIPTIONS: dict[str, str] = {}
_HAS_PARAMS: dict[str, bool] = {}

def register(name: str, label: str, description: str = "", has_params: bool = False):
    def decorator(fn):
        _TRANSFORMERS[name] = fn
        _LABELS[name] = label
        _DESCRIPTIONS[name] = description
        _HAS_PARAMS[name] = has_params
        return fn
    return decorator

def get(name: str) -> callable:
    return _TRANSFORMERS[name]

def options() -> list[dict]:
    return [
        {"name": k, "label": _LABELS[k], "description": _DESCRIPTIONS[k], "has_params": _HAS_PARAMS[k]}
        for k in _TRANSFORMERS
    ]
```

### 5C. Example — `transformers/text.py`

```python
import pandas as pd
from transformers.registry import register

@register("TRIM", "Trim", "ลบช่องว่างหน้าหลัง")
def trim(series, params=None):
    return series.astype(str).str.strip()

@register("UPPER", "Upper", "แปลงเป็นตัวพิมพ์ใหญ่")
def upper(series, params=None):
    return series.astype(str).str.upper()

@register("CLEAN_SPACES", "Clean Spaces", "ลบช่องว่างซ้ำ")
def clean_spaces(series, params=None):
    return series.astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
```

### 5D. `config.py` — Replace static lists

```python
# Before:
TRANSFORMER_OPTIONS = [
    {"name": "TRIM", "label": "Trim", ...},
    ...
]

# After:
def get_transformer_options() -> list[dict]:
    from transformers.registry import options
    return options()
```

### 5E. Same pattern for validators

```
validators/
├── __init__.py
├── registry.py              # @register_validator decorator
├── not_null.py              # NOT_NULL
├── unique.py                # UNIQUE_CHECK
└── range_check.py           # RANGE_CHECK
```

---

## Phase 6: Fix `ml_mapper.py` Streamlit Dependency (DIP)

### Current problem
```python
# services/ml_mapper.py line 1, 35
import streamlit as st

@st.cache_resource
def load_model():
    ...
```

### Fix — Move caching to controller

```python
# services/ml_mapper.py — Remove streamlit import entirely
class MLMapper:
    def __init__(self):
        self._model = None

    def load_model(self):
        if self._model is None:
            self._model = SentenceTransformer(...)
        return self._model

    def suggest_mapping(self, source_columns, target_columns):
        ...

ml_mapper = MLMapper()  # module-level singleton (no st.cache needed)
```

```python
# controllers/schema_mapper_controller.py — Apply caching here
import streamlit as st

@st.cache_resource
def get_ml_mapper():
    from services.ml_mapper import MLMapper
    return MLMapper()
```

---

## Phase 7: Legacy View Refactoring → Strict MVC

### Current violations (10 view files import database/services directly)

| Legacy View | Business Logic to Extract | New Controller |
|-------------|--------------------------|----------------|
| `views/schema_mapper.py` + 5 components | DB calls, config generation, datasource resolution | `controllers/schema_mapper_controller.py` |
| `views/migration_engine.py` + 4 components | Datasource resolution, encoding, truncate, rollback | `controllers/migration_engine_controller.py` |
| `views/er_diagram.py` | DB calls, graph building, schema inspection | `controllers/er_diagram_controller.py` |
| `views/file_explorer.py` | Minimal (just reads config path) | Inline in `app.py` or trivial controller |
| `views/settings.py` | Already superseded — **delete** | (already migrated) |
| `views/components/shared/dialogs.py` | `db.get_config_version()` call | Move to controller callback |

### 7A. `controllers/schema_mapper_controller.py`

Extract from `views/schema_mapper.py` + components:

```python
class SchemaMapperController:
    def __init__(self, ds_repo: DatasourceRepository,
                 config_repo: ConfigRepository,
                 mapper: MLMapper):
        self.ds_repo = ds_repo
        self.config_repo = config_repo
        self.mapper = mapper

    def run(self):
        PageState.init(_DEFAULTS)
        datasources = self.ds_repo.get_all()
        configs = self.config_repo.get_list()
        callbacks = {
            "on_select_source": self._on_select_source,
            "on_test_connection": self._on_test_connection,
            "on_save_config": self._on_save_config,
            "on_load_config": self._on_load_config,
            "on_ai_suggest": self._on_ai_suggest,
            ...
        }
        render_schema_mapper(datasources, configs, callbacks)

    def _on_save_config(self, config):
        ...

    def _on_ai_suggest(self, source_cols, target_cols):
        return self.mapper.suggest_mapping(source_cols, target_cols)
```

### 7B. `controllers/migration_engine_controller.py`

Extract from `views/migration_engine.py` + components:

```python
class MigrationEngineController:
    def __init__(self, ds_repo, config_repo, run_repo):
        self.ds_repo = ds_repo
        self.config_repo = config_repo
        self.run_repo = run_repo

    def run(self):
        PageState.init(_DEFAULTS)
        datasources = self.ds_repo.get_all()
        configs = self.config_repo.get_list()
        callbacks = {
            "on_select_config": self._on_select_config,
            "on_test_connection": self._on_test_connection,
            "on_start_migration": self._on_start_migration,
            "on_emergency_truncate": self._on_emergency_truncate,
            "on_rollback": self._on_rollback,
            ...
        }
        render_migration_engine(datasources, configs, callbacks)

    def _resolve_connection_config(self, datasource_id: int, charset: str) -> dict:
        ds = self.ds_repo.get_by_id(datasource_id)
        return {
            "db_type": ds["db_type"],
            "host": ds["host"],
            "port": ds["port"],
            "dbname": ds["dbname"],
            "username": ds["username"],
            "password": ds["password"],
            "charset": charset,
        }
```

### 7C. `controllers/er_diagram_controller.py`

Extract from `views/er_diagram.py`:

```python
class ERDiagramController:
    def __init__(self, ds_repo, inspector):
        self.ds_repo = ds_repo
        self.inspector = inspector

    def run(self):
        datasources = self.ds_repo.get_all()
        callbacks = {
            "on_select_datasource": self._on_select_datasource,
            "on_build_graph": self._on_build_graph,
        }
        render_er_diagram(datasources, callbacks)

    def _on_select_datasource(self, ds_name):
        ds = self.ds_repo.get_by_name(ds_name)
        tables = self.inspector.get_tables(ds)
        return tables

    def _on_build_graph(self, ds_name, selected_tables):
        ds = self.ds_repo.get_by_name(ds_name)
        nodes, edges = [], []
        for table in selected_tables:
            columns = self.inspector.get_columns(ds, table)
            fks = self.inspector.get_foreign_keys(ds, table)
            nodes.append({"table": table, "columns": columns})
            edges.extend(fks)
        return {"nodes": nodes, "edges": edges}
```

### 7D. `views/components/shared/dialogs.py` — Fix `db.get_config_version()` violation

Move the DB call to a controller callback:

```python
# Before (dialog calls database directly):
def show_diff_dialog(config_name, v1, v2):
    data_v1 = db.get_config_version(config_name, v1)  # VIOLATION
    data_v2 = db.get_config_version(config_name, v2)  # VIOLATION
    ...

# After (dialog receives data via parameter):
def show_diff_dialog(diff_data: dict):
    # Pure rendering — no DB calls
    ...
```

### 7E. Delete legacy files

After refactoring is complete:
- Delete `views/settings.py` (superseded by `settings_view.py` + `settings_controller.py`)
- Keep `views/schema_mapper.py`, `views/migration_engine.py`, `views/er_diagram.py` as thin rendering shells (no business logic)

---

## Phase 8: `services/pipeline_service.py` — Fix DIP Violation

### Current problem
```python
import database as db  # Direct concrete dependency
```

### Fix — Inject repositories via constructor

```python
class PipelineExecutor:
    def __init__(self,
                 pipeline: PipelineConfig,
                 source_conn_config: dict,
                 target_conn_config: dict,
                 config_repo: ConfigRepository,       # injected
                 run_repo: PipelineRunRepository,      # injected
                 log_callback=None,
                 progress_callback=None,
                 run_id: str | None = None):
        self.config_repo = config_repo
        self.run_repo = run_repo
        ...

    def execute(self):
        ...
        config = self.config_repo.get_content(step.config_name)  # via interface
        self.run_repo.update(self.run_id, "running", ...)         # via interface
        ...
```

```python
# controllers/pipeline_controller.py — Wire dependencies
def _on_start_pipeline(self):
    executor = PipelineExecutor(
        pipeline=config,
        source_conn_config=src_conn,
        target_conn_config=tgt_conn,
        config_repo=config_repo,        # concrete PostgreSQL impl
        run_repo=run_repo,              # concrete PostgreSQL impl
        ...
    )
    executor.start_background()
```

---

## Phase 9: `services/db_connector.py` — Split Responsibilities (SRP)

### Current: 5 responsibilities in 1 file

### Target: Split into focused modules

| Module | Responsibility | Functions |
|--------|---------------|-----------|
| `services/db_connector.py` | Engine factory (thin wrapper over dialect registry) | `create_sqlalchemy_engine()` |
| `services/connection_pool.py` | Raw DBAPI connection pool | `DatabaseConnectionPool` |
| `services/schema_inspector.py` | Schema inspection & sampling | `get_tables()`, `get_columns()`, `get_foreign_keys()`, `get_sample_data()`, `get_column_samples()` |
| `services/connection_tester.py` | Connection testing | `test_connection()` |
| `dialects/` | Dialect-specific SQL generation | Per-dialect classes |

---

## Phase 10: Data Migration Script + Cleanup

### 10A. `scripts/migrate_sqlite_to_pg.py`

```python
"""One-time migration: SQLite → PostgreSQL

Usage:
    DATABASE_URL=postgresql://user:pass@localhost:5432/his_analyzer \
        python scripts/migrate_sqlite_to_pg.py
"""
import sqlite3
import os
import sys
from sqlalchemy import text

def migrate():
    sqlite_path = os.path.join(os.path.dirname(__file__), '..', 'migration_tool.db')
    if not os.path.exists(sqlite_path):
        print("No migration_tool.db found — nothing to migrate")
        return

    source = sqlite3.connect(sqlite_path)
    # ... read from SQLite, insert to PG via repositories ...

if __name__ == "__main__":
    migrate()
```

### 10B. `.gitignore` cleanup

Remove:
```
migration_tool.db
migration_tool.db.backup
```

### 10C. `services/db_connector.py` cleanup

Remove unused `import sqlite3` (line 1).

---

## Final Directory Structure

```
his-analyzer/
├── app.py                              # Router + DI composition root
├── config.py                           # Env vars + path constants
├── .env.example                        # DATABASE_URL template
├── requirements.txt                    # +python-dotenv
│
├── models/                             # Domain dataclasses (unchanged)
│   ├── __init__.py
│   ├── datasource.py
│   ├── migration_config.py
│   └── pipeline_config.py
│
├── protocols/                          # NEW — Abstract interfaces
│   ├── __init__.py
│   ├── repository.py                   # Repository protocols
│   ├── database_dialect.py             # Dialect protocol
│   └── transformer.py                  # Transformer protocol
│
├── repositories/                       # NEW — Data access (replaces database.py)
│   ├── __init__.py
│   ├── connection.py                   # Engine singleton
│   ├── base.py                         # DDL + init_db()
│   ├── datasource_repo.py
│   ├── config_repo.py
│   ├── pipeline_repo.py
│   └── pipeline_run_repo.py
│
├── dialects/                           # NEW — OCP: pluggable DB dialects
│   ├── __init__.py
│   ├── registry.py                     # DialectRegistry
│   ├── base.py                         # BaseDialect ABC
│   ├── mysql.py
│   ├── postgresql.py
│   └── mssql.py
│
├── transformers/                       # NEW — OCP: pluggable transformers
│   ├── __init__.py
│   ├── registry.py                     # @register_transformer
│   ├── base.py                         # DataTransformer class
│   ├── text.py
│   ├── dates.py
│   ├── healthcare.py
│   ├── names.py
│   ├── data_type.py
│   └── lookup.py
│
├── validators/                         # NEW — OCP: pluggable validators
│   ├── __init__.py
│   ├── registry.py                     # @register_validator
│   ├── not_null.py
│   ├── unique.py
│   └── range_check.py
│
├── services/                           # Business logic (cleaned up)
│   ├── db_connector.py                 # Slimmed: engine factory only
│   ├── connection_pool.py              # NEW: extracted from db_connector
│   ├── schema_inspector.py             # NEW: extracted from db_connector
│   ├── migration_executor.py           # ETL execution
│   ├── pipeline_service.py             # Pipeline orchestration (DI-fixed)
│   ├── datasource_repository.py        # Facade (will be deprecated)
│   ├── ml_mapper.py                    # AI mapping (NO streamlit)
│   ├── encoding_helper.py
│   ├── checkpoint_manager.py
│   ├── migration_logger.py
│   └── query_builder.py
│
├── controllers/                        # MVC Controllers (all pages)
│   ├── __init__.py
│   ├── settings_controller.py          # Existing (modified imports)
│   ├── pipeline_controller.py          # Existing (DI-fixed)
│   ├── schema_mapper_controller.py     # NEW — extracted from legacy view
│   ├── migration_engine_controller.py  # NEW — extracted from legacy view
│   └── er_diagram_controller.py        # NEW — extracted from legacy view
│
├── views/                              # MVC Views (pure rendering only)
│   ├── __init__.py
│   ├── settings_view.py                # Clean (no changes)
│   ├── pipeline_view.py                # Clean (no changes)
│   ├── schema_mapper_view.py           # Refactored — no service imports
│   ├── migration_engine_view.py        # Refactored — no service imports
│   ├── er_diagram_view.py              # Refactored — no service imports
│   ├── file_explorer.py                # Minimal changes
│   └── components/                     # Sub-components (pure rendering)
│       ├── shared/
│       │   ├── dialogs.py              # Fixed — no DB calls
│       │   └── styles.py
│       ├── schema_mapper/
│       │   ├── source_selector.py
│       │   ├── mapping_editor.py
│       │   ├── metadata_editor.py
│       │   ├── config_actions.py
│       │   └── history_viewer.py
│       └── migration/
│           ├── step_connections.py
│           ├── step_config.py
│           ├── step_review.py
│           └── step_execution.py
│
├── utils/
│   ├── state_manager.py
│   ├── ui_components.py
│   └── helpers.py
│
├── scripts/
│   └── migrate_sqlite_to_pg.py         # One-time data migration
│
└── analysis_report/                    # External tool (unchanged)
```

---

## Implementation Order

| # | Phase | Task | Dependencies | Risk |
|---|-------|------|-------------|------|
| 1 | 1 | `config.py` + `.env.example` + `python-dotenv` | — | Low |
| 2 | 1 | `repositories/connection.py` — Engine singleton | 1 | Low |
| 3 | 2 | `repositories/base.py` — DDL (PostgreSQL) | 2 | Low |
| 4 | 2 | `repositories/datasource_repo.py` | 3 | Low |
| 5 | 2 | `repositories/config_repo.py` | 3 | Medium (upsert logic) |
| 6 | 2 | `repositories/pipeline_repo.py` | 3 | Low |
| 7 | 2 | `repositories/pipeline_run_repo.py` | 3 | Medium (thread safety) |
| 8 | 3 | `protocols/repository.py` | — | Low (interfaces only) |
| 9 | 4 | `dialects/` — MySQL, PostgreSQL, MSSQL | 8 | Medium (replace 20+ if/elif) |
| 10 | 4 | Refactor `services/db_connector.py` to use dialects | 9 | Medium |
| 11 | 5 | `transformers/` — Split + registry | — | Medium (refactor transformers.py) |
| 12 | 5 | `validators/` — Split + registry | — | Low |
| 13 | 6 | Fix `ml_mapper.py` — remove streamlit import | — | Low |
| 14 | 8 | Fix `pipeline_service.py` — DI injection | 8, 7 | Medium |
| 15 | 9 | Split `db_connector.py` — connection_pool, schema_inspector | 10 | Medium |
| 16 | 7 | `controllers/schema_mapper_controller.py` | 4, 5, 13 | High (largest legacy view) |
| 17 | 7 | `controllers/migration_engine_controller.py` | 4, 6 | High (complex wizard) |
| 18 | 7 | `controllers/er_diagram_controller.py` | 4, 10 | Medium |
| 19 | 7 | Fix `views/components/shared/dialogs.py` | 5 | Low |
| 20 | 7 | Refactor all view components — remove service/DB imports | 16, 17, 18 | High |
| 21 | 7 | Delete `views/settings.py` (legacy) | — | Low |
| 22 | 10 | `scripts/migrate_sqlite_to_pg.py` | 4, 5, 6, 7 | Medium |
| 23 | 10 | Delete `database.py` | 4-20 (all callers migrated) | **Critical** |
| 24 | 10 | `.gitignore` cleanup | — | Low |
| 25 | — | **End-to-end testing** — every Streamlit page | 1-24 | Critical |

---

## Suggested PR Breakdown

| PR | Steps | Scope | Description |
|----|-------|-------|-------------|
| **PR 1** | 1-7 | Repositories + PostgreSQL | New `repositories/` module, all CRUD working with PG |
| **PR 2** | 8-10 | Protocols + Dialects | OCP fixes, `dialects/` module, refactor `db_connector.py` |
| **PR 3** | 11-12 | Transformer/Validator registries | OCP fixes, split `transformers.py` into package |
| **PR 4** | 13-15 | DIP fixes | `ml_mapper`, `pipeline_service`, split `db_connector.py` |
| **PR 5** | 16-21 | Legacy MVC refactoring | All controllers + views cleaned, delete `database.py` |
| **PR 6** | 22-25 | Migration script + cleanup | One-time migration, final cleanup, full E2E testing |

---

## SOLID Violations — Fix Summary

| Violation | Location | Fix | Phase |
|-----------|----------|-----|-------|
| **SRP**: `database.py` God Module (8 responsibilities) | `database.py` | Split into 4 repositories | Phase 2 |
| **SRP**: `db_connector.py` (5 responsibilities) | `services/db_connector.py` | Split into connector + pool + inspector | Phase 9 |
| **SRP**: `transformers.py` (generic + healthcare mixed) | `services/transformers.py` | Split into `transformers/` package | Phase 5 |
| **SRP**: Business logic in view components | `views/components/` (10 files) | Extract into controllers | Phase 7 |
| **OCP**: Hardcoded `if db_type ==` chains (20+ locations) | `db_connector.py`, `query_builder.py` | `dialects/` registry | Phase 4 |
| **OCP**: Static `TRANSFORMER_OPTIONS` list | `config.py` | `transformers/registry.py` | Phase 5 |
| **OCP**: Static `VALIDATOR_OPTIONS` list | `config.py` | `validators/registry.py` | Phase 5 |
| **OCP**: Hardcoded error strategy strings | `pipeline_service.py` | Strategy pattern (future — low priority) | — |
| **DIP**: Controllers import concrete `database` module | `controllers/*.py` | Import protocols + inject repos | Phase 3, 7 |
| **DIP**: `PipelineExecutor` imports concrete `database` | `services/pipeline_service.py` | Constructor injection | Phase 8 |
| **DIP**: Views import `database` and `services` directly | `views/` (10 files) | Controllers mediate | Phase 7 |
| **DIP**: `ml_mapper.py` imports streamlit | `services/ml_mapper.py` | Move `@st.cache_resource` to controller | Phase 6 |
| **ISP**: `Datasource` model defined but never used | `models/datasource.py` | Use it in repositories + controllers | Phase 2 |
| **LSP**: Return type `(bool, list \| str)` ballot pattern | `db_connector.py` | Result objects or exceptions | Phase 9 |

---

## Risk Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| 15 files import `database` — breaking change | High | Migrate callers in batches per PR; keep `database.py` as thin re-export facade during transition |
| `pipeline_service.py` runs in background thread | High | SQLAlchemy Engine is thread-safe; PG handles concurrent writes better than SQLite |
| Transformer split breaks `query_builder.py` dependency | Medium | `transformers/__init__.py` re-exports `DataTransformer` class — zero change for callers |
| `pd.read_sql_query()` to `pd.read_sql()` | Low | Identical API, just accepts engine instead of raw connection |
| SERIAL vs AUTOINCREMENT id mismatch | Low | Both return `int` — no caller code changes needed |
| Large PR scope | High | 6 PRs with clear boundaries; each PR is independently deployable |

---

## Testing Checklist

### PR 1 (Repositories)
- [ ] `init_db()` creates 5 tables in PostgreSQL
- [ ] Datasource CRUD: create, read, update, delete, unique name constraint
- [ ] Config CRUD: save, load, delete, version history, upsert behavior
- [ ] Pipeline CRUD: save, load, delete
- [ ] Pipeline Run CRUD: save, update, get_latest (thread-safe)
- [ ] `compare_config_versions()` diff logic unchanged

### PR 2 (Dialects)
- [ ] `dialects.registry.available_types()` returns 3 types
- [ ] Engine creation works for MySQL, PostgreSQL, MSSQL dialects
- [ ] `query_builder.py` uses dialect for quoting

### PR 3 (Transformers)
- [ ] All existing transformers registered and callable
- [ ] `DataTransformer.transform_batch()` behavior identical
- [ ] `config.get_transformer_options()` returns same options
- [ ] `GENERATE_HN` healthcare logic works correctly

### PR 4 (DIP fixes)
- [ ] `ml_mapper` works without Streamlit runtime
- [ ] `PipelineExecutor` receives repos via constructor
- [ ] `db_connector.py` no longer has 5 responsibilities

### PR 5 (MVC refactoring)
- [ ] Schema Mapper page works end-to-end
- [ ] Migration Engine wizard completes all 4 steps
- [ ] ER Diagram builds graph from datasource
- [ ] No view file imports `database` or `services.*` directly
- [ ] `views/settings.py` deleted

### PR 6 (Migration + cleanup)
- [ ] `scripts/migrate_sqlite_to_pg.py` transfers all data
- [ ] `database.py` deleted with no remaining imports
- [ ] `.gitignore` updated
- [ ] Full E2E: every Streamlit page functional
