# Feature: Data Pipeline

## Context

ปัจจุบัน Migration Engine (`views/migration_engine.py`) รองรับแค่ migrate **1 table ต่อครั้ง** และ config JSON เก็บแค่ `database` (dbname) ไม่ได้เก็บ datasource ID ทำให้ทุกครั้งที่ migrate ต้องเลือก datasource profile ใหม่ใน step 2

**เป้าหมาย**: สร้าง Data Pipeline ที่สามารถ chain หลาย migration configs เข้าด้วยกัน รันตามลำดับ (รองรับ dependency ordering) โดยต่อ datasource ครั้งเดียวแล้ว migrate ทุก table ตาม flow

### 3 Challenges ที่ต้องแก้

| # | Challenge | Problem | Solution |
|---|-----------|---------|----------|
| 1 | **Connection Timeout** | hold single engine ข้าม 10+ tables → `MySQL Server has gone away` | JIT connection per step: สร้าง engine ด้วย `pool_pre_ping=True`, `pool_recycle=3600` แล้ว `dispose()` หลังจบแต่ละ step |
| 2 | **2D Checkpoint** | checkpoint เดิมเก็บแค่ 1 table (flat JSON) | nested JSON per step: `{steps: {config_A: {status, last_batch, rows_processed}}}` |
| 3 | **UI Timeout** | Streamlit block 3+ ชม. → disconnect | background thread + เขียน status ลง `pipeline_runs` table, UI poll ด้วย "Refresh Status" button |

---

## Phase 1: Model Enhancement — เพิ่ม datasource ID ใน config

### 1A. เพิ่ม field ใน MigrationConfig

**File**: `models/migration_config.py`

เพิ่ม 4 fields ใน `MigrationConfig`:
```python
source_datasource_id: int | None = None
source_datasource_name: str = ""
target_datasource_id: int | None = None
target_datasource_name: str = ""
```

อัปเดต `from_dict()` ให้อ่านจาก `d["source"].get("datasource_id")` / `d["source"].get("datasource_name")` (ใช้ `.get()` เพื่อ backward compatible กับ config เก่า)

อัปเดต `to_dict()` ให้ embed ลงใน `source` / `target` dict

### 1B. อัปเดต Schema Mapper ให้บันทึก datasource ID

**File**: `views/components/schema_mapper/config_actions.py`

- `_build_params()` (line 224): resolve datasource display name → ดึง `id` จาก `DSRepo.get_by_name()` เก็บเป็น `source_datasource_id`, `target_datasource_id`
- `generate_json_config()` (line 126): embed `datasource_id` + `datasource_name` ลงใน `source` / `target` object

Config JSON จะเปลี่ยนจาก:
```json
{"source": {"database": "his_db", "table": "patient"}}
```
เป็น:
```json
{"source": {"database": "his_db", "table": "patient", "datasource_id": 3, "datasource_name": "HIS_Source"}}
```

### 1C. Migration Engine auto-populate datasource

**File**: `views/components/migration/step_connections.py`

เมื่อ config มี `datasource_id` → auto-select datasource profile ใน step 2 แทนที่จะให้ user เลือกเอง (fallback เป็น manual select ถ้า config เก่าไม่มี ID)

---

## Phase 2: Extract ETL Service — แยก business logic ออกจาก Streamlit

### 2A. แก้ db_connector.py รับ engine_kwargs

**File**: `services/db_connector.py`

**Line 25**: เพิ่ม `**engine_kwargs` parameter ใน `create_sqlalchemy_engine()`
**Line 85**: pass through → `create_engine(connection_url, **engine_kwargs)`

เปลี่ยน 1 บรรทัด — backward-compatible (callers เดิมไม่ส่ง kwargs ก็ทำงานเหมือนเดิม)

### 2B. สร้าง migration_executor.py (pure Python) — Challenge 1: JIT Connection

**New file**: `services/migration_executor.py`

Extract logic จาก `views/components/migration/step_execution.py` (lines 86-213) ออกมาเป็น:

```python
@dataclass
class MigrationResult:
    status: str              # "success" | "failed"
    rows_processed: int
    batch_count: int
    duration_seconds: float
    error_message: str = ""
    pre_count: int = 0
    post_count: int = 0

def run_single_migration(
    config: dict,
    source_conn_config: dict,    # {db_type, host, port, dbname, username, password, charset}
    target_conn_config: dict,    # same shape
    batch_size: int = 1000,
    truncate_target: bool = False,
    test_mode: bool = False,
    skip_batches: int = 0,
    log_callback=None,           # fn(message: str, icon: str)
    progress_callback=None,      # fn(batch_num: int, rows_processed: int, total_rows: int)
    checkpoint_callback=None,    # fn(config_name: str, batch_num: int, rows: int)
) -> MigrationResult
```

**JIT Connection Pattern** (แก้ Challenge 1):
```python
def run_single_migration(...):
    # สร้าง engine ด้วย pool settings ที่ทน long-running
    src_engine = create_sqlalchemy_engine(
        **source_conn_config, pool_pre_ping=True, pool_recycle=3600
    )
    tgt_engine = create_sqlalchemy_engine(
        **target_conn_config, pool_pre_ping=True, pool_recycle=3600
    )
    try:
        # 1. pre-count target
        # 2. truncate (optional)
        # 3. validate schema compatibility
        # 4. init HN counter
        # 5. batch loop: pd.read_sql → clean_dataframe → transform_batch → batch_insert
        # 6. post-verify counts
        # ใช้ callbacks แทน st.* calls ทั้งหมด
        return MigrationResult(status="success", ...)
    except Exception as e:
        return MigrationResult(status="failed", error_message=str(e), ...)
    finally:
        src_engine.dispose()   # ปิด connection กลับ pool ทุกกรณี
        tgt_engine.dispose()
```

**รับ connection config dict (ไม่ใช่ engine)** — PipelineExecutor ส่งแค่ config, ฟังก์ชันสร้าง engine เอง แล้ว dispose หลังจบ

**Helper functions ย้ายมาด้วย** (ลบ `st.*` dependencies, ใช้ `log_callback` แทน):
- `_get_row_count(engine, table, log_cb) -> int`
- `_truncate_table(engine, table, log_cb)`
- `_validate_schema(src_engine, tgt_engine, src_table, tgt_table, config, log_cb)`
- `_init_hn_counter(tgt_engine, tgt_table, config, log_cb)`
- `_verify_post_migration(tgt_engine, tgt_table, pre_count, total, log_cb) -> int`

**Reuse existing services** (ไม่ต้องเขียนใหม่):
- `services/query_builder.py`: `build_select_query()`, `transform_batch()`, `build_dtype_map()`, `batch_insert()`
- `services/encoding_helper.py`: `clean_dataframe()`
- `services/transformers.py`: `DataTransformer.reset_hn_counter()`
- `services/migration_logger.py`: `create_log_file()`, `write_log()`
- `services/checkpoint_manager.py`: `save_checkpoint()`, `clear_checkpoint()`

### 2C. อัปเดต step_execution.py ให้เรียก service

`step_execution.py` จะเป็น thin wrapper:
1. Resolve datasource → connection config dicts
2. สร้าง Streamlit callbacks (log_callback → `add_log`, progress_callback → update metrics)
3. เรียก `run_single_migration(config, src_conn, tgt_conn, ...)`
4. Handle `MigrationResult` → update session_state

**ผลลัพธ์**: behavior ไม่เปลี่ยน แต่ ETL logic reusable ได้จาก pipeline

---

## Phase 3: Pipeline Model & Database

### 3A. สร้าง PipelineConfig model

**New file**: `models/pipeline_config.py`

```python
@dataclass
class PipelineStep:
    order: int
    config_name: str
    depends_on: list[str] = field(default_factory=list)  # config_names ที่ต้อง complete ก่อน
    enabled: bool = True
    # from_dict(), to_dict()

@dataclass
class PipelineConfig:
    id: str                    # UUID
    name: str
    description: str = ""
    steps: list[PipelineStep] = field(default_factory=list)
    source_datasource_id: int | None = None
    target_datasource_id: int | None = None
    error_strategy: str = "fail_fast"  # fail_fast | continue_on_error | skip_dependents
    batch_size: int = 1000
    truncate_targets: bool = False
    created_at: str = ""
    updated_at: str = ""
    # from_dict(), to_dict() — follow migration_config.py pattern
```

### 3B. Database tables

**File**: `database.py`

เพิ่ม 2 tables ใน `init_db()`:

```sql
CREATE TABLE IF NOT EXISTS pipelines (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT DEFAULT '',
    json_data TEXT,
    source_datasource_id INTEGER,
    target_datasource_id INTEGER,
    error_strategy TEXT DEFAULT 'fail_fast',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id TEXT PRIMARY KEY,
    pipeline_id TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    steps_json TEXT,
    error_message TEXT,
    FOREIGN KEY(pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);
```

**Schema Migration Strategy**: ใช้ `CREATE TABLE IF NOT EXISTS` สำหรับตารางใหม่ทั้ง 2 ตาราง — ปลอดภัยสำหรับ production ที่รันอยู่แล้ว ถ้าในอนาคตต้องเพิ่ม column ใน table ที่มีอยู่ ให้ใช้ `PRAGMA table_info()` เช็คก่อนแล้ว `ALTER TABLE ADD COLUMN` ตาม pattern ของ `ensure_config_histories_table()` ใน `database.py`

CRUD functions (follow existing patterns: `save_datasource`, `get_datasources`, etc.):
- `save_pipeline(name, description, json_data, source_ds_id, target_ds_id, error_strategy) -> (bool, str)`
- `get_pipelines_list() -> pd.DataFrame`
- `get_pipeline_by_name(name) -> dict | None`
- `delete_pipeline(name) -> (bool, str)`
- `save_pipeline_run(pipeline_id, status, steps_json) -> str` — returns run_id (uuid4)
- `update_pipeline_run(run_id, status, steps_json, error_message=None)`
- `get_pipeline_runs(pipeline_id) -> pd.DataFrame`
- `get_latest_pipeline_run(pipeline_id) -> dict | None` — สำหรับ UI polling

---

## Phase 4: Pipeline Service — Challenge 2 & 3

**New file**: `services/pipeline_service.py`

### 4A. Result dataclasses

```python
@dataclass
class StepResult:
    status: str              # success | failed | skipped | skipped_dependency
    config_name: str
    rows_processed: int = 0
    duration_seconds: float = 0.0
    error_message: str = ""

@dataclass
class PipelineResult:
    steps: dict[str, StepResult]
    status: str              # completed | partial | failed
    total_rows: int = 0
    total_duration: float = 0.0
```

### 4B. PipelineExecutor class

```python
class PipelineExecutor:
    def __init__(self, pipeline: PipelineConfig,
                 source_conn_config: dict,   # ส่ง config dict ไม่ใช่ engine (JIT)
                 target_conn_config: dict,
                 log_callback=None,
                 progress_callback=None,
                 run_id: str | None = None): ...

    def execute(self) -> PipelineResult:
        """Main execution loop."""
        ordered = self._resolve_execution_order()
        checkpoint = load_pipeline_checkpoint(self.pipeline.name)
        steps_state = checkpoint["steps"] if checkpoint else {}
        results = {}

        for step in ordered:
            if not step.enabled:
                continue
            # Skip completed steps from 2D checkpoint
            if steps_state.get(step.config_name, {}).get("status") == "completed":
                results[step.config_name] = StepResult("completed", ...)
                continue
            # Check dependency failures
            should_skip, reason = self._should_skip(step, results)
            if should_skip:
                results[step.config_name] = StepResult("skipped_dependency", ...)
                continue

            # Load config from DB, get resume batch from checkpoint
            config = get_config_content(step.config_name)
            skip_batches = steps_state.get(step.config_name, {}).get("last_batch", 0)

            # JIT: pass conn configs, NOT engines
            result = run_single_migration(
                config=config,
                source_conn_config=self.source_conn_config,
                target_conn_config=self.target_conn_config,
                batch_size=self.pipeline.batch_size,
                skip_batches=skip_batches,
                checkpoint_callback=lambda name, batch, rows:
                    self._update_step_checkpoint(name, batch, rows),
            )
            results[step.config_name] = StepResult(...)

            # Update pipeline_runs in DB → UI สามารถ poll ได้
            update_pipeline_run(self.run_id, "running", json.dumps(...))

            # Error strategy
            if result.status == "failed":
                if self.pipeline.error_strategy == "fail_fast":
                    break
                # continue_on_error: ไม่ break, ทำ step ถัดไป
                # skip_dependents: handled by _should_skip()

        return PipelineResult(steps=results, status=..., ...)

    def start_background(self) -> str:
        """Challenge 3: Launch in daemon thread, return run_id for polling."""
        self.run_id = save_pipeline_run(self.pipeline.id, "running", "{}")
        thread = threading.Thread(target=self._background_run, daemon=True)
        thread.start()
        return self.run_id

    def _background_run(self):
        """Thread target: calls execute(), updates DB on completion."""
        try:
            result = self.execute()
            update_pipeline_run(self.run_id, result.status, json.dumps(...))
        except Exception as e:
            update_pipeline_run(self.run_id, "failed", error_message=str(e))
        finally:
            clear_pipeline_checkpoint(self.pipeline.name)

    def _resolve_execution_order(self) -> list[PipelineStep]:
        """Kahn's algorithm — topological sort by depends_on."""
        # Build in-degree map + adjacency list from step.depends_on
        # BFS from nodes with in-degree 0
        # If remaining nodes: raise ValueError("Circular dependency: ...")
        # Return sorted list

    def _should_skip(self, step, results) -> tuple[bool, str]:
        """Check depends_on against failed steps (transitive).

        ใช้ BFS/DFS traversal เพื่อ mark downstream nodes ทั้งหมด:
        ถ้า Step A พัง → Step B (depends A) ถูก skip →
        Step C (depends B) ต้องถูก skip ด้วย แม้ไม่ได้ depend A โดยตรง
        """
        # fail_fast: never reaches here (loop breaks)
        # continue_on_error: don't skip
        # skip_dependents: maintain failed_steps: set + skipped_steps: set
        #   ระหว่าง loop — ถ้า step.depends_on ตัวใดอยู่ใน 2 sets นี้
        #   → skip ทันที + เพิ่มตัวเองลง skipped_steps
        #   ไม่ต้องทำ DFS ใหม่ทุกรอบ เพราะ results dict สะสม status มาแล้ว

    def _update_step_checkpoint(self, config_name, batch_num, rows):
        """Update 2D checkpoint per batch."""
        # Load current checkpoint → update step entry → save back
```

### 4C. Pipeline checkpoint — Challenge 2: 2D Checkpoint

**File**: `services/checkpoint_manager.py` — เพิ่ม functions ต่อจาก line 48:

```python
def save_pipeline_checkpoint(pipeline_name: str, steps_state: dict) -> None
def load_pipeline_checkpoint(pipeline_name: str) -> dict | None
def clear_pipeline_checkpoint(pipeline_name: str) -> None
```

**2D Checkpoint JSON** (แก้ Challenge 2):
```json
{
  "pipeline_name": "my_pipeline",
  "steps": {
    "config_A": {"status": "completed", "last_batch": 10, "rows_processed": 5000},
    "config_B": {"status": "running", "last_batch": 3, "rows_processed": 1500},
    "config_C": {"status": "pending", "last_batch": 0, "rows_processed": 0}
  },
  "timestamp": "2026-03-25T14:30:00"
}
```

เก็บใน `migration_checkpoints/pipeline_<safe_name>.json`

---

## Phase 5: Pipeline Controller + View (MVC)

### 5A. Controller

**New file**: `controllers/pipeline_controller.py`

ตาม pattern ของ `controllers/settings_controller.py`:

```python
_DEFAULTS = {
    "pipeline_wizard_step": 1,
    "pipeline_config": None,
    "pipeline_mode": "new",
    "pipeline_selected": None,
    "pipeline_src_ok": False,
    "pipeline_tgt_ok": False,
    "pipeline_running": False,
    "pipeline_completed": False,
    "pipeline_run_id": None,       # for background thread polling
    "pipeline_results": None,
}

def run() -> None:
    PageState.init(_DEFAULTS)
    pipelines_df = get_pipelines_list()
    configs_df = get_configs_list()
    datasources_df = get_datasources()
    form_state = { ... }
    callbacks = {
        "on_add_step", "on_remove_step", "on_reorder_steps",
        "on_save_pipeline", "on_load_pipeline", "on_delete_pipeline",
        "on_test_connections", "on_start_pipeline", "on_poll_status",
        "on_next_step", "on_prev_step",
    }
    render_pipeline_page(pipelines_df, configs_df, datasources_df, form_state, callbacks)
```

Key callbacks:
- `_on_start_pipeline()`: resolve credentials → `PipelineExecutor(conn_configs)` → `start_background()` → store run_id
- `_on_poll_status()`: `get_latest_pipeline_run(run_id)` → parse steps_json → update form_state
- `_on_force_cancel_run(run_id)`: mark zombie run as failed → `update_pipeline_run(run_id, "failed", error_message="Manually cancelled")`
- `_check_zombie_runs()`: เรียกตอน `run()` init → เช็ค runs ที่ `status='running'` นานกว่า 24 ชม. → set warning flag ใน form_state

### 5B. View — 4-step wizard

**New file**: `views/pipeline_view.py`

| Step | หน้าที่ | รายละเอียด |
|------|---------|------------|
| 1. Design | ออกแบบ pipeline | เลือก configs จาก DB → เพิ่มเป็น step, จัดลำดับ, ตั้ง depends_on, error strategy |
| 2. Connections | ทดสอบ connection | เลือก shared source/target datasource, charset selector, "Test All" button |
| 3. Review | ตรวจสอบ | สรุป table แสดง steps + dependencies, batch size/truncate/test mode, checkpoint resume panel |
| 4. Execute | รัน pipeline | per-step status cards, "Refresh Status" button (poll DB), post-execution summary, "Force Cancel" button สำหรับ zombie runs |

**UI Polling** (Challenge 3): ใช้ "Refresh Status" button → `callbacks["on_poll_status"]()` → `st.rerun()` เพื่อไม่เพิ่ม dependency (ไม่ใช้ `streamlit-autorefresh`)

### 5C. App Router

**File**: `app.py`

เพิ่ม navigation entry:
```python
from controllers import pipeline_controller

elif page == "🔗 Data Pipeline":
    pipeline_controller.run()
```

---

## Implementation Order

เรียงตาม dependency:

| ลำดับ | Phase | งาน | ขึ้นกับ |
|-------|-------|-----|--------|
| 1 | 1A | เพิ่ม datasource fields ใน MigrationConfig | - |
| 2 | 1B | อัปเดต Schema Mapper บันทึก datasource ID | 1 |
| 3 | 1C | Migration Engine auto-populate datasource | 1 |
| 4 | 2A | แก้ `db_connector.py` เพิ่ม `**engine_kwargs` | - |
| 5 | 2B | Extract `services/migration_executor.py` + JIT connection | 4 |
| 6 | 2C | อัปเดต step_execution.py delegate to service | 5 |
| 7 | 3A | สร้าง PipelineConfig model | - |
| 8 | 3B | เพิ่ม DB tables + CRUD (รวม pipeline_runs) | 7 |
| 9 | 4C | เพิ่ม 2D pipeline checkpoint functions | - |
| 10 | 4A-B | สร้าง Pipeline Service + background thread | 5, 8, 9 |
| 11 | 5A | สร้าง Pipeline Controller | 8, 10 |
| 12 | 5B | สร้าง Pipeline View | 11 |
| 13 | 5C | เพิ่ม route ใน app.py | 11, 12 |

**แนะนำแบ่ง PR**:
- **PR 1**: ลำดับ 1-6 (datasource ID + extract ETL service) — refactor ไม่เปลี่ยน behavior, ship ได้เลย
- **PR 2**: ลำดับ 7-13 (pipeline feature ทั้งหมด)

---

## Design Considerations

### Thread Safety กับ SQLite (`check_same_thread`)
Background thread เขียน `pipeline_runs` ขณะ Streamlit main thread อ่าน — SQLite รองรับ concurrent reads แต่ writer ทีละคน.

**Gotcha**: `sqlite3.connect()` default คือ `check_same_thread=True` → ถ้า background thread เรียก `get_connection()` จะได้ connection ใหม่ แต่ต้อง **ไม่โยน connection object ข้าม thread** ไม่งั้นจะเจอ `ProgrammingError: SQLite objects created in a thread can only be used in that same thread`

**Solution**: CRUD functions ทุกตัวที่ background thread เรียก (`update_pipeline_run`, `save_pipeline_run`) ต้องเรียก `get_connection()` ภายในฟังก์ชันเอง (ไม่รับ connection เป็น parameter) — pattern เดิมของ `database.py` ทำแบบนี้อยู่แล้ว ให้รักษา pattern นี้ไว้

### Engine Disposal
`run_single_migration()` ต้อง dispose engines ใน `finally` block **เสมอ** แม้เกิด exception กลาง batch. PipelineExecutor ต้องไม่ share engines ข้าม steps (ตาม JIT pattern)

### HN Counter State (Race Condition ถ้ารัน 2 Pipelines พร้อมกัน)
`DataTransformer.reset_hn_counter()` ใช้ class-level variable (`_hn_counter`). แต่ละ step ที่มี `GENERATE_HN` จะ init counter ใหม่ผ่าน `_init_hn_counter()` ใน `run_single_migration()` — ไม่ conflict ข้าม steps **ตราบใดที่รันแค่ 1 pipeline ในเวลาเดียวกัน**

**Gotcha**: ถ้าในอนาคตรองรับ concurrent pipelines → class-level variable จะถูกเขียนทับ (race condition). ตอนนี้ไม่ได้ออกแบบให้รันพร้อมกัน แต่ควรเพิ่ม guard ใน `_on_start_pipeline()` เช็คว่ามี run ที่ `status='running'` อยู่หรือไม่ → ถ้ามีให้ block ไม่ให้เริ่ม pipeline ใหม่

### Zombie Runs — Daemon Thread + Server Restart
เนื่องจากใช้ `threading.Thread(daemon=True)` หาก Streamlit App ถูก restart (server reboot, deploy ใหม่) ระหว่าง pipeline รันอยู่ → daemon thread ตายพร้อม main process ทันที → `pipeline_runs` ค้างสถานะ `running` ตลอดกาล (zombie run)

**Solution (Phase 5B)**:
1. **Stale run detection**: ตอน controller `run()` ถูกเรียก → เช็ค `pipeline_runs` ที่ `status='running'` แต่ `started_at` เก่ากว่า threshold (เช่น 24 ชม.) → แสดง warning ใน UI
2. **"Force Mark Failed" button**: ใน Execute step (Step 4) แสดงปุ่มสำหรับ run ที่ค้างนานผิดปกติ → เรียก `update_pipeline_run(run_id, "failed", error_message="Manually cancelled — suspected zombie run")`
3. **Startup cleanup** (optional future): ใน `pipeline_controller.run()` เพิ่ม logic ตรวจ zombie runs อัตโนมัติตอนเปิดหน้า

---

## Verification

1. **Backward compat**: `MigrationConfig.from_dict()` กับ config เก่า (ไม่มี datasource_id) → parse ได้ปกติ, return `None`
2. **ETL refactor**: รัน Migration Engine เดิม → behavior เหมือนเดิมทุกประการ (delegate ผ่าน `run_single_migration`)
3. **Unit tests**: topological sort (linear, diamond, circular → `ValueError`), error strategies, 2D checkpoint round-trip
4. **JIT connection**: verify `engine.dispose()` ถูกเรียกใน finally block
5. **Background thread**: สร้าง pipeline → start → poll status → verify `pipeline_runs` table updates
6. **Integration test**: สร้าง config ใหม่ผ่าน Schema Mapper → ตรวจว่า JSON มี `datasource_id`
7. **E2E**: สร้าง 2-3 configs → build pipeline with dependencies → execute → interrupt → resume from 2D checkpoint

---

## Critical Files

| File | Action |
|------|--------|
| `models/migration_config.py` | Modify — add datasource fields |
| `models/pipeline_config.py` | **New** — PipelineConfig + PipelineStep |
| `services/db_connector.py` | Modify — add `**engine_kwargs` (1 line) |
| `services/migration_executor.py` | **New** — extracted ETL logic + JIT connection |
| `services/pipeline_service.py` | **New** — PipelineExecutor + background thread |
| `services/checkpoint_manager.py` | Modify — add 2D pipeline checkpoint |
| `database.py` | Modify — pipeline tables + CRUD + pipeline_runs |
| `controllers/pipeline_controller.py` | **New** — MVC controller |
| `views/pipeline_view.py` | **New** — MVC view (4-step wizard) |
| `views/components/schema_mapper/config_actions.py` | Modify — embed datasource ID |
| `views/components/migration/step_connections.py` | Modify — auto-populate |
| `views/components/migration/step_execution.py` | Modify — delegate to service |
| `app.py` | Modify — add route |
