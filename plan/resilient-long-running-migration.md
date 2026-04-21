# Resilient Long-Running Migration — Implementation Plan

> **Context**: `migration_executor.py` + `pipeline_service.py` handle ETL jobs triggered
> via `/api/v1/jobs`. For configs with millions of rows, migrations can run 1+ days.
> This plan addresses risks of connection timeout, OOM, process crash, and resume gaps.

---

## 1. Risk Assessment (Current State)

### 1.1 Connection Timeout Risk: MEDIUM

| What exists | Gap |
|---|---|
| `pool_pre_ping=True`, `pool_recycle=3600` | `pd.read_sql()` holds **one source connection open** for the entire iteration (hours). If a firewall/LB drops idle connections between batches, the iterator dies mid-stream. |
| `statement_timeout = 0` on PG | Good for long queries, but no upper bound means a stuck query hangs forever with no alarm. |
| JIT engine creation per step | Engines are created once per step, not per batch. A step with 10M rows keeps both engines alive for hours. |

**Impact**: A single TCP reset kills the entire step. No automatic retry.

### 1.2 Memory (RAM) Risk: HIGH

| What exists | Gap |
|---|---|
| `chunksize=batch_size` in `pd.read_sql` | Each batch loads `batch_size` rows into a full DataFrame. `transform_batch()` creates copies (rename, drop, astype). Peak memory = ~3-4x one batch. |
| No memory guard | If `batch_size=10000` with 200+ columns of VARCHAR(MAX), a single batch can be 500MB+. No backpressure or adaptive sizing. |
| `clean_dataframe()` runs on every batch | Good, but allocates a new DataFrame. |

**Impact**: OOM killer terminates the Python process. Daemon thread dies silently. Job status stays "running" forever.

### 1.3 Process Crash / Machine Restart Risk: HIGH

| What exists | Gap |
|---|---|
| File-based checkpoints (`migration_checkpoints/`) | `_CHECKPOINT_INTERVAL = 10` means up to **10 batches of data loss** on crash (rows already inserted but checkpoint not yet saved). |
| Pipeline-level checkpoint (2D) | Good — tracks per-step status. But file writes are **not atomic** (no rename trick), so a crash mid-write corrupts the JSON. |
| `daemon=True` thread | If uvicorn restarts or machine reboots, the thread dies instantly. No graceful shutdown signal. |
| Job status in PostgreSQL | On crash, job stays `status='running'` forever. Only `resume=True` can mark it stale manually. |

**Impact**: After machine restart, stale job blocks new runs (409 Conflict). Resume may re-insert up to 10 batches of duplicate data.

### 1.4 Duplicate Data on Resume Risk: MEDIUM

| What exists | Gap |
|---|---|
| `skip_batches` parameter | Skips N batches from `pd.read_sql` iterator. But if source data changes between runs (INSERT/UPDATE/DELETE), batch boundaries shift. Row 10001 in run 1 is not row 10001 in run 2. |
| No deterministic ordering | `build_select_query()` has no `ORDER BY`. Without it, `OFFSET`-based resume is non-deterministic. |

**Impact**: Resume after crash can skip or duplicate rows if source table is not frozen.

### 1.5 No Observability Risk: MEDIUM

| What exists | Gap |
|---|---|
| `log_callback` | Only reaches Streamlit UI or stdout. No structured logging to file/DB. |
| `batch_event_callback` → Socket.IO | Real-time but ephemeral. If no client is connected, events are lost. |
| No heartbeat | No way to distinguish "slow migration" from "dead thread". |
| No resource metrics | No CPU/RAM/disk monitoring during migration. |

---

## 2. Implementation Plan

### Phase 1: Atomic Checkpoints + Reduce Data Loss Window (Priority: CRITICAL)

**Goal**: Crash at any point loses at most 1 batch, never corrupts checkpoint.

**File**: `services/checkpoint_manager.py`

**Changes**:

1. **Atomic file writes** — write to a temp file, then `os.replace()` (atomic on POSIX):
   ```python
   def save_checkpoint(config_name, batch_num, rows_processed):
       path = _checkpoint_path(config_name)
       tmp = path + ".tmp"
       with open(tmp, "w") as f:
           json.dump(data, f)
           f.flush()
           os.fsync(f.fileno())  # ensure data hits disk
       os.replace(tmp, path)     # atomic rename
   ```

2. **Reduce `_CHECKPOINT_INTERVAL` from 10 to 1** in `migration_executor.py`:
   - With atomic writes, the I/O cost is minimal (one `rename` syscall).
   - Every successful batch is immediately checkpointed.
   - Max data loss on crash = 1 batch (the one currently being inserted).

3. **Same for `save_pipeline_checkpoint`** — atomic write with fsync.

**Effort**: ~1 hour. **Risk**: None (backward compatible).

---

### Phase 2: Deterministic Resume with ORDER BY (Priority: CRITICAL)

**Goal**: Resume produces identical batch boundaries regardless of when it runs.

**Files**: `services/query_builder.py`, `services/migration_executor.py`

**Changes**:

1. **Add `ORDER BY` to `build_select_query()`**:
   - Auto-detect primary key via `sqlalchemy.inspect(engine).get_pk_constraint()`.
   - Append `ORDER BY pk_col1, pk_col2` to the SELECT.
   - If no PK found, log a warning: "No PK detected — resume may produce duplicates".

2. **For `generate_sql`**: User is responsible for adding ORDER BY. Add a validation warning if `ORDER BY` is not present in the SQL string.

3. **Alternative (if ORDER BY is too slow on huge tables)**:
   - Use cursor-based pagination: `WHERE pk > :last_seen_pk ORDER BY pk LIMIT :batch_size`.
   - Store `last_seen_pk` in checkpoint instead of `last_batch` number.
   - This is faster than OFFSET on large tables and immune to source data changes.

**Effort**: ~3-4 hours. **Risk**: ORDER BY adds sort cost. For tables without index on PK, this could be slow. Mitigate with PG `work_mem` tuning (already in place).

---

### Phase 3: Retry with Exponential Backoff (Priority: HIGH)

**Goal**: Transient network errors (TCP reset, connection pool exhaustion) don't kill the entire migration.

**File**: `services/migration_executor.py`

**Changes**:

1. **Add retry wrapper around `batch_insert()`**:
   ```python
   MAX_RETRIES = 3
   RETRY_DELAYS = [5, 15, 45]  # seconds

   def _insert_with_retry(df, target_table, tgt_engine, dtype_map, batch_num, log):
       for attempt in range(MAX_RETRIES):
           try:
               batch_insert(df, target_table, tgt_engine, dtype_map)
               return
           except (OperationalError, DisconnectionError) as e:
               if attempt == MAX_RETRIES - 1:
                   raise
               delay = RETRY_DELAYS[attempt]
               log(f"Batch {batch_num}: transient error, retry {attempt+1}/{MAX_RETRIES} in {delay}s: {e}", "⚠️")
               time.sleep(delay)
               tgt_engine.dispose()  # force new connection
   ```

2. **Retry on `pd.read_sql` chunk failure** (source connection dropped):
   - If the iterator raises `OperationalError`, reconnect and re-seek to the last successful batch using `OFFSET` (or cursor-based if Phase 2 is implemented).
   - This is the hardest part — `pd.read_sql` iterator is not resumable. Solution: replace the single `pd.read_sql(..., chunksize=N)` call with an explicit loop:
     ```python
     while True:
         query = f"{select_query} ORDER BY pk LIMIT {batch_size} OFFSET {offset}"
         df_batch = pd.read_sql(query, src_engine, coerce_float=False)
         if df_batch.empty:
             break
         # process batch...
         offset += batch_size
     ```

**Effort**: ~4-5 hours. **Risk**: OFFSET-based pagination is slow on very large offsets. Cursor-based (Phase 2 alternative) is better for 10M+ rows.

---

### Phase 4: Graceful Shutdown + Heartbeat (Priority: HIGH)

**Goal**: Process restart doesn't leave orphan "running" jobs. Dead threads are detectable.

**Files**: `services/pipeline_service.py`, `api/jobs/service.py`

**Changes**:

1. **Shutdown signal handler**:
   ```python
   import signal
   _shutdown_event = threading.Event()

   def _handle_shutdown(signum, frame):
       _shutdown_event.set()

   signal.signal(signal.SIGTERM, _handle_shutdown)
   signal.signal(signal.SIGINT, _handle_shutdown)
   ```
   - Check `_shutdown_event.is_set()` between batches in `_process_batches()`.
   - If set, save checkpoint and exit gracefully with `status="interrupted"`.

2. **Heartbeat column** on `jobs` table:
   ```sql
   ALTER TABLE jobs ADD COLUMN last_heartbeat TIMESTAMPTZ;
   ```
   - Update `last_heartbeat` every N batches (reuse `_CHECKPOINT_INTERVAL` cadence).
   - On startup (or via a periodic check), any job with `status='running'` and `last_heartbeat < NOW() - INTERVAL '5 minutes'` is marked as `failed` with `error_message='Process died — no heartbeat'`.

3. **Auto-detect stale jobs on POST /api/v1/jobs**:
   - Before returning 409, check if the "running" job has a stale heartbeat.
   - If stale, auto-mark failed and proceed (don't require `resume=True`).

**Effort**: ~4 hours. **Risk**: Signal handlers in threaded Python can be tricky. Only the main thread receives signals — need to use `threading.Event` to propagate.

---

### Phase 5: Memory Guard + Adaptive Batch Size (Priority: HIGH)

**Goal**: Prevent OOM on wide tables or large text columns.

**File**: `services/migration_executor.py`

**Changes**:

1. **Memory check before each batch**:
   ```python
   import psutil

   def _check_memory(log, batch_num):
       mem = psutil.virtual_memory()
       if mem.percent > 90:
           log(f"Batch {batch_num}: Memory at {mem.percent}% — pausing for GC", "⚠️")
           import gc; gc.collect()
           mem = psutil.virtual_memory()
           if mem.percent > 95:
               raise MemoryError(f"Memory critically high ({mem.percent}%) — aborting to prevent OOM kill")
   ```

2. **Adaptive batch size** — measure first batch memory, adjust:
   ```python
   # After first batch:
   batch_memory_mb = df_batch.memory_usage(deep=True).sum() / 1024 / 1024
   if batch_memory_mb > 200:  # > 200MB per batch
       new_batch_size = max(100, batch_size // 2)
       log(f"Batch memory {batch_memory_mb:.0f}MB — reducing batch_size to {new_batch_size}", "⚠️")
       batch_size = new_batch_size
   ```

3. **Explicit `del df_batch` + `gc.collect()`** after each batch insert to release memory immediately (Python's GC is generational and may delay collection of large DataFrames).

**Effort**: ~2-3 hours. **Risk**: `psutil` is a new dependency. Alternative: read `/proc/meminfo` on Linux, `os.sysconf` on macOS.

---

### Phase 6: Connection Keepalive + Per-Batch Reconnect (Priority: MEDIUM)

**Goal**: Survive network interruptions between batches.

**File**: `services/migration_executor.py`, `services/db_connector.py`

**Changes**:

1. **Replace `pd.read_sql(..., chunksize=N)` with explicit pagination loop**:
   - Current: one long-lived connection for the entire iterator.
   - Proposed: open connection per batch (`pd.read_sql` without chunksize, with LIMIT/OFFSET or cursor).
   - Each batch gets a fresh connection from the pool → survives firewall timeouts.

2. **TCP keepalive on SQLAlchemy engines** (especially for PostgreSQL through firewalls):
   ```python
   from sqlalchemy import event

   @event.listens_for(engine, "connect")
   def set_keepalive(dbapi_conn, connection_record):
       # PostgreSQL via psycopg2
       dbapi_conn.set_keepalives(1)
       dbapi_conn.set_keepalives_idle(30)
       dbapi_conn.set_keepalives_interval(10)
       dbapi_conn.set_keepalives_count(5)
   ```

3. **Connection health check before each batch** (lightweight):
   ```python
   try:
       with engine.connect() as conn:
           conn.execute(text("SELECT 1"))
   except Exception:
       engine.dispose()  # clear dead connections
       # pool_pre_ping will handle reconnection on next use
   ```

**Effort**: ~3-4 hours. **Risk**: Changing from chunksize iterator to explicit pagination is a significant refactor. Test thoroughly.

---

### Phase 7: Structured Logging + Audit Trail (Priority: MEDIUM)

**Goal**: Post-mortem analysis possible without being connected to Socket.IO.

**Files**: New `services/migration_logger.py` (already exists but may need enhancement)

**Changes**:

1. **Log to file** — one log file per job: `logs/job_{job_id}.jsonl`
   - Each line: `{"ts": "...", "level": "info", "step": "config_A", "batch": 5, "msg": "Inserted 1000 rows", "memory_mb": 245}`

2. **Log batch timing** — track seconds per batch to detect slowdowns:
   ```
   Batch 1: 2.3s | Batch 2: 2.1s | Batch 50: 45.2s ← something wrong
   ```

3. **Store final summary in `jobs` table**:
   ```sql
   ALTER TABLE jobs ADD COLUMN summary JSONB;
   -- { "total_rows": 5000000, "total_duration_s": 86400, "steps": {...}, "peak_memory_mb": 1200 }
   ```

**Effort**: ~3 hours. **Risk**: Disk space for JSONL logs. Add log rotation or max size.

---

### Phase 8: Idempotent Insert Strategy (Priority: MEDIUM)

**Goal**: Resume never produces duplicate rows, even if checkpoint was stale.

**File**: `services/query_builder.py`

**Changes**:

1. **Option A — Upsert (CONFLICT handling)**:
   - For PostgreSQL: `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`
   - For MSSQL: `MERGE INTO ... USING ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`
   - Requires PK or unique constraint on target table.

2. **Option B — Delete-before-insert per batch**:
   - Before inserting batch N, delete rows that match the PK values in the batch.
   - Simpler to implement, works without UPSERT support.

3. **Option C — Staging table pattern**:
   - Insert into `_staging_{table}` (TRUNCATE on start).
   - After all batches: `INSERT INTO {table} SELECT * FROM _staging WHERE pk NOT IN (SELECT pk FROM {table})`.
   - Cleanest but requires temp table creation.

**Recommendation**: Option A (Upsert) as a config flag `"insert_strategy": "upsert"` per mapping config. Default remains `"append"` for backward compatibility.

**Effort**: ~6-8 hours. **Risk**: Upsert is dialect-specific. Need to implement per dialect.

---

## 3. Implementation Priority

| Phase | Risk Addressed | Effort | Priority |
|-------|---------------|--------|----------|
| 1. Atomic Checkpoints | Data loss on crash | ~1h | **CRITICAL** |
| 2. Deterministic Resume | Duplicate/missing rows | ~4h | **CRITICAL** |
| 3. Retry with Backoff | Transient network errors | ~5h | **HIGH** |
| 4. Graceful Shutdown + Heartbeat | Orphan jobs, dead threads | ~4h | **HIGH** |
| 5. Memory Guard | OOM kill | ~3h | **HIGH** |
| 6. Connection Keepalive | Firewall timeout | ~4h | **MEDIUM** |
| 7. Structured Logging | Observability | ~3h | **MEDIUM** |
| 8. Idempotent Insert | Duplicate rows on resume | ~8h | **MEDIUM** |

**Recommended order**: 1 → 2 → 4 → 5 → 3 → 6 → 8 → 7

---

## 4. Quick Wins (Can Do Now)

These are one-line or few-line changes with immediate impact:

```python
# 1. migration_executor.py — reduce checkpoint interval
_CHECKPOINT_INTERVAL = 1  # was 10

# 2. checkpoint_manager.py — atomic write (add os.replace)
# See Phase 1

# 3. migration_executor.py — explicit gc after each batch
import gc
# After batch_insert() success:
del df_batch
gc.collect()

# 4. db_connector.py — add TCP keepalive for PostgreSQL
# See Phase 6 code snippet

# 5. pipeline_service.py — log memory usage per step
import psutil
mem = psutil.virtual_memory()
self._log(f"[{config_name}] Memory: {mem.percent}%", "📊")
```

---

## 5. Architecture Diagram (After Implementation)

```
POST /api/v1/jobs {pipeline_id, resume: true}
    │
    ├── Check stale jobs (heartbeat < 5min ago → auto-mark failed)
    ├── Load pipeline checkpoint (if resume)
    │
    └── PipelineExecutor.start_background()
            │
            ├── For each step (topological order):
            │   ├── Skip if checkpoint says "completed"
            │   ├── Resume from last_batch if "running"
            │   │
            │   └── run_single_migration()
            │       ├── LOOP (explicit pagination, not pd.read_sql iterator):
            │       │   ├── Check shutdown signal → save & exit
            │       │   ├── Check memory → adaptive batch size
            │       │   ├── SELECT ... ORDER BY pk LIMIT N OFFSET M
            │       │   ├── transform_batch()
            │       │   ├── batch_insert() with retry (3x backoff)
            │       │   ├── Atomic checkpoint (every batch)
            │       │   ├── Update heartbeat (every batch)
            │       │   ├── gc.collect()
            │       │   └── Socket.IO event + JSONL log
            │       │
            │       └── On success: mark step "completed" in checkpoint
            │
            └── On finish: clear checkpoint, update job status
```
