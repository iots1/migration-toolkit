# Migration Bug Fix & Performance Improvement Plan

**Date**: 2026-04-30
**Scope**: `services/migration_executor.py`, `services/query_builder.py`, `services/pipeline_service.py`, `api/jobs/`, `api/pipeline_runs/`, `repositories/pipeline_run_repo.py`, `repositories/job_repo.py`
**Priority**: Critical (bugs) > High (error tracking) > Medium (performance)

---

## 1. Bug Fixes (Critical)

### 1.1 Duplicate `get_by_job()` in `pipeline_run_repo.py`

**File**: `repositories/pipeline_run_repo.py:124` and `:143`
**Severity**: Critical — Python silently overwrites the first definition

The function `get_by_job()` is defined **twice** (lines 124-131 and 143-150). Python uses the second definition, so the first is dead code. This is likely a copy-paste artifact. The duplicate should be removed.

```python
# Lines 124-131: FIRST definition (dead code — overwritten)
def get_by_job(job_id: uuid.UUID) -> list[dict]:
    ...

# Lines 143-150: SECOND definition (the one actually used)
def get_by_job(job_id: uuid.UUID) -> list[dict]:
    ...
```

**Fix**: Delete one of the duplicate definitions.

---

### 1.2 `_background_run()` references `result` in `finally` when it may be `None`

**File**: `services/pipeline_service.py:534-541`
**Severity**: Medium — runtime `AttributeError` on crash

In the `finally` block of `_background_run()`, the code does:
```python
finally:
    if result is not None and result.status == "completed":
        ...
    else:
        cp = load_pipeline_checkpoint(...)
        print(
            f"[JOB] Pipeline '{self._pipeline.name}' "
            f"did not complete — checkpoint preserved for resume"
            f" ({len(cp.get('steps', {}))} step(s) tracked)" if cp else ""
        )
```

When `result` is `None` AND checkpoint is `None`, the `else` branch produces an empty f-string which still prints a blank line. Also, later in the same finally block:
```python
if self._migration_logger:
    summary = self._migration_logger.build_summary(
        result.total_rows if result else 0,  # safe
        result.status if result else "failed",  # safe
    )
```
This is handled correctly with the ternary, but the inconsistent pattern is fragile.

**Fix**: Consolidate the `finally` block with explicit `result is None` handling.

---

### 1.3 `_process_single_batch` returns `None` on transformation error — silently drops batch

**File**: `services/migration_executor.py:1170-1174`
**Severity**: Medium — silent data loss

When `transform_batch()` raises an exception, `_process_single_batch` returns `None`:
```python
except Exception as e:
    log(f"Transformation Error in Batch {batch_num}: {e}", "⚠️")
    return None  # Caller treats None as "skip batch, continue"
```

In both `_process_batches()` (line 861) and `_process_batches_offset()` (line 1079), `None` causes the batch to be **silently skipped** with a `gc.collect()` and `continue`. The rows in that batch are **lost** — no checkpoint, no error record, no quarantine attempt.

**Fix**: Return a `_BatchOutcome(success=False, ...)` instead of `None`, or at minimum save a checkpoint with the error so the batch can be retried on resume.

---

### 1.4 `PipelineRunUpdateRecord` is missing `steps_json` field in repo

**File**: `repositories/pipeline_run_repo.py:56-68`
**Severity**: Low — silent field loss

The `PipelineRunUpdateRecord` dataclass has `steps_json` but the `update()` function in the repo ignores it — the SQL only patches `status` and `error_message`. When `PipelineRunsService.update()` passes `steps_json`, it's silently dropped.

```python
# pipeline_run_repo.py update():
# SQL only updates: status, error_message
# Missing: steps_json
```

**Fix**: Add `steps_json = COALESCE(CAST(:steps_json AS jsonb), steps_json)` to the UPDATE statement.

---

### 1.5 Non-PostgreSQL targets: `to_sql` uses table name as-is

**File**: `services/query_builder.py:427-431`
**Severity**: Low — incorrect table name for schema-qualified tables

For MySQL/MSSQL targets, `batch_insert()` passes the raw `target_table` (e.g., `"dbo.patients"`) to `df.to_sql(name=target_table, ...)`. Pandas' `to_sql` treats the `name` parameter as a plain table name, not a schema-qualified name. This causes errors when the table is schema-qualified.

**Fix**: Parse `target_table` to split schema and name for non-PG backends:
```python
if "." in target_table:
    schema, name = target_table.split(".", 1)
    df.to_sql(name=name, con=conn, schema=schema, if_exists="append", ...)
```

---

### 1.6 `_tune_pg_migration_session` accumulates event listeners

**File**: `services/migration_executor.py:479-493`
**Severity**: Low — memory leak on repeated migrations

Every call to `_tune_pg_migration_session()` registers a **new** `@event.listens_for(engine, "connect")` listener. If the same engine is reused (unlikely with JIT pattern, but possible in testing), listeners accumulate. Same issue with `_set_keepalive()`.

**Fix**: Use `event.remove(engine, "connect", handler)` in the `finally` block, or check if already registered with `event.contains()`.

---

## 2. Error Tracking — Insert Error into `pipeline_runs` / `jobs` on Every Failure

### 2.1 Current Gap

When a migration step fails, the error is logged to console and emitted via Socket.IO, but **not** persisted to the database in several important paths:

| Failure Path | `pipeline_runs` record? | `jobs.error_message`? |
|---|---|---|
| Batch insert failed (non-quarantine) | ✅ via `_save_batch_record` | ❌ |
| Source read error | ❌ | ❌ |
| Transformation error (`None` return) | ❌ | ❌ |
| Top-level `except Exception` in `run_single_migration` | ✅ via `_safe_notify_callback` | ❌ |
| `_background_run` crash (unhandled exception) | ❌ | Partial (run_repo only) |
| Step dependency failed | ❌ | ❌ |

### 2.2 Proposed Fix: Error Persister Layer

Create a helper in `pipeline_service.py` that **always** writes error state to both `pipeline_runs` and `jobs`:

```python
def _persist_step_error(
    self,
    config_name: str,
    error_message: str,
    step_status: str = "failed",
) -> None:
    """Insert a failed batch record AND update the job's error_message."""
    # 1. Insert failed pipeline_runs record
    record = PipelineRunRecord(
        pipeline_id=uuid.UUID(self._pipeline.id),
        job_id=self._job_id_uuid,
        config_name=config_name,
        batch_round=-1,
        status="failed",
        error_message=error_message[:500],
    )
    try:
        self._run_repo.save(record)
    except Exception:
        pass

    # 2. Update job.error_message (append, don't overwrite)
    if self._job_id:
        try:
            from repositories import job_repo as _jr
            _jr.update(
                uuid.UUID(self._job_id),
                JobUpdateRecord(
                    status="running",
                    error_message=error_message[:300],
                ),
            )
        except Exception:
            pass

    # 3. Emit Socket.IO event
    if self._run_event_callback:
        try:
            self._run_event_callback("pipeline_run:failed", {
                "job_id": self._job_id,
                "pipeline_id": self._pipeline.id,
                "config_name": config_name,
                "error_message": error_message,
            })
        except Exception:
            pass
```

### 2.3 Integration Points

Insert `_persist_step_error()` calls at every failure path in `PipelineExecutor.execute()`:

1. **Config not found** (line 271-281) — after `self._log()`, call `_persist_step_error()`
2. **Datasource resolution failed** (line 284-294) — after `self._log()`, call `_persist_step_error()`
3. **Step failed** (non-success `mig_result`, line 368-381) — already partially handled, consolidate
4. **Step interrupted** (line 326-337) — call `_persist_step_error()` with status `"interrupted"`
5. **Unhandled exception** in `_background_run()` (line 505-532) — ensure job status is set to `"failed"`

Also fix in `migration_executor.py`:
- **Source read error** (lines 813-815 and 1045-1047) — currently only saves checkpoint, should also call `batch_insert_callback` with `status="failed"`
- **Transformation error** (`_process_single_batch` returning `None`) — return `_BatchOutcome(success=False)` instead

### 2.4 Jobs Table: Accumulate Multiple Step Errors

Currently `job_repo.update()` uses `COALESCE(:error_message, error_message)` which means the **first** error is kept and subsequent errors are discarded. Change to **append**:

```sql
error_message = CASE
    WHEN error_message IS NULL THEN :error_message
    ELSE error_message || E'\n' || :error_message
END
```

Or limit to last N characters:
```sql
LEFT(
    COALESCE(error_message, '') || E'\n[' || config_name || '] ' || :error_message,
    2000
)
```

---

## 3. Parallel Migration Support

### 3.1 Current State: Strictly Sequential

`PipelineExecutor.execute()` processes steps **sequentially** in topological order. This is correct for dependency chains (A → B → C must be serial), but **independent steps** (A and B have no edge between them) could run in parallel.

### 3.2 Parallel Execution Strategy

The topological sort in `_resolve_execution_order()` produces a flat list. We need to identify **levels** — groups of steps with no inter-dependencies that can run concurrently.

```
Level 0: [patients, departments]    ← independent, run in parallel
Level 1: [visits]                   ← depends on both Level 0 steps
Level 2: [lab_results, prescriptions]  ← independent at this level
```

#### 3.2.1 Modified Topological Sort — Level-Aware

```python
def _resolve_execution_levels(self) -> list[list[dict]]:
    """Return steps grouped by dependency level for parallel execution.
    
    Returns a list of levels, where each level is a list of steps
    that can run concurrently.
    """
    nodes_by_name = {n["config_name"]: n for n in self._pipeline.nodes}
    in_degree = {name: 0 for name in nodes_by_name}
    adjacency = {name: [] for name in nodes_by_name}

    for edge in self._pipeline.edges:
        src = edge.get("source_config_name", "")
        tgt = edge.get("target_config_name", "")
        if src in nodes_by_name and tgt in nodes_by_name:
            adjacency[src].append(tgt)
            in_degree[tgt] += 1

    # Kahn's algorithm with level tracking
    current_level = sorted(
        [name for name, deg in in_degree.items() if deg == 0],
        key=lambda n: nodes_by_name[n].get("order_sort", 0),
    )
    
    levels = []
    while current_level:
        levels.append([nodes_by_name[name] for name in current_level])
        next_level = []
        for name in current_level:
            for neighbor in adjacency[name]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_level.append(neighbor)
        current_level = sorted(next_level, key=lambda n: nodes_by_name[n].get("order_sort", 0))

    return levels
```

#### 3.2.2 Thread Pool Executor for Parallel Steps

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

class PipelineExecutor:
    MAX_PARALLEL_STEPS = 4  # configurable
    
    def execute(self) -> PipelineResult:
        levels = self._resolve_execution_levels()
        
        for level_idx, level_steps in enumerate(levels):
            if len(level_steps) == 1:
                # Single step — run in current thread (no overhead)
                result = self._execute_step(level_steps[0], ...)
            else:
                # Multiple independent steps — run in parallel
                results = self._execute_level_parallel(level_steps, ...)
            
            # Check fail_fast after each level
            if any_failed and self._pipeline.error_strategy == "fail_fast":
                break
        
        return self._aggregate_results(...)
    
    def _execute_level_parallel(self, steps: list[dict], ...) -> dict[str, StepResult]:
        results = {}
        max_workers = min(self.MAX_PARALLEL_STEPS, len(steps))
        
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._execute_step, step, ...): step["config_name"]
                for step in steps
            }
            for future in as_completed(futures):
                config_name = futures[future]
                try:
                    results[config_name] = future.result()
                except Exception as e:
                    results[config_name] = StepResult(
                        status="failed", config_name=config_name, error_message=str(e)
                    )
        
        return results
```

#### 3.2.3 Thread Safety Considerations

| Resource | Thread-Safe? | Action Needed |
|---|---|---|
| `run_single_migration()` | ✅ Yes (JIT engines) | None — each call creates its own engines |
| `save_checkpoint()` | ✅ Yes (atomic `os.replace`) | None |
| `pipeline_run_repo.save()` | ✅ Yes (`get_transaction()` per call) | None |
| `job_repo.update()` | ⚠️ Partial | Use row-level locking or accept last-writer-wins |
| `DataTransformer` (HN counter) | ❌ No (`reset_hn_counter` is global) | Use per-thread counter or `threading.Lock` |
| `self._migration_logger` | ⚠️ Check | Verify `MigrationLogger` is thread-safe |
| `self._log_callback` | ❌ Depends on impl | Ensure callback is thread-safe (Streamlit is not) |
| Socket.IO `emit_from_thread` | ✅ Yes (already thread-safe) | None |

#### 3.2.4 Phase Plan

| Phase | Description | Effort |
|---|---|---|
| Phase A | Add `_resolve_execution_levels()` + `PipelineConfig.parallel_enabled` flag | 1 day |
| Phase B | Implement `_execute_level_parallel()` with `ThreadPoolExecutor` | 1 day |
| Phase C | Fix thread-safety issues (HN counter lock, MigrationLogger) | 0.5 day |
| Phase D | Add `max_parallel_steps` config + API schema update | 0.5 day |
| Phase E | Testing: 3-node pipeline with parallel level + sequential fallback | 1 day |

---

## 4. Performance Improvements

### 4.1 In-Memory Pagination for `find_all()` (API)

**File**: `api/base/service.py:43-59`
**Severity**: High — O(N) memory for all records on every paginated request

`BaseService.find_all()` loads **ALL** records into memory (`_list_all()` → `_apply_query_params()` → `_paginate()`). For `pipeline_runs` with thousands of batch records, this is wasteful.

**Fix**: Implement database-level pagination in repositories:

```python
# pipeline_run_repo.py
def get_page(limit: int, offset: int, job_id: uuid.UUID | None = None) -> list[dict]:
    where = "WHERE job_id = :job_id" if job_id else ""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs {where} "
                 f"ORDER BY created_at DESC LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset, "job_id": job_id},
        )
        return rows_to_dicts(result)
```

**Impact**: Reduces memory from O(total_rows) to O(page_size) per request.

### 4.2 Reduce `pipeline_runs` Writes During Migration

**Current**: Every batch inserts a `pipeline_runs` record AND calls `_update_step_checkpoint()` which writes a JSON file. This means:
- 1 DB INSERT per batch
- 1 filesystem write per batch
- 1 Socket.IO emit per batch

For 1M rows with batch_size=1000, that's **3000 I/O operations per second** on a fast migration.

**Proposed**: Buffer pipeline_runs inserts — write a record every N batches or every M seconds:

```python
_BATCH_RECORD_INTERVAL = 5  # Write to DB every 5 batches

def _save_batch_record(self, ...):
    self._batch_buffer.append({...})
    if len(self._batch_buffer) >= _BATCH_RECORD_INTERVAL or status == "failed":
        self._flush_batch_buffer()
```

**Impact**: Reduces DB writes by ~5x with minimal risk (only last few batches lost on crash).

### 4.3 Batch COPY — Pre-allocate `StringIO` Buffer

**File**: `services/query_builder.py:395-399`
**Severity**: Low

Each `batch_insert()` creates a new `io.StringIO()` and `_csv.writer()`. For high-throughput migrations, pre-allocating a buffer and reusing it saves GC pressure.

```python
# Reusable buffer with reset
_buf = io.StringIO()

def _pg_copy(table, conn, keys, data_iter):
    _buf.seek(0)
    _buf.truncate()
    writer = _csv.writer(_buf, lineterminator="\n")
    writer.writerows(data_iter)
    _buf.seek(0)
    ...
```

Note: This is only safe in single-threaded context. For parallel migration, each thread needs its own buffer.

### 4.4 Connection Pool Sizing for Parallel Steps

**File**: `services/migration_executor.py:192-199`
**Severity**: Medium for parallel execution

Current pool sizing: `pool_size=2, max_overflow=1` per engine. With parallel steps, each thread creates its own engines, so total connections = `parallel_steps × 4` (2 src + 2 tgt per step). This could exhaust source/target DB connection limits.

**Fix**: Add `pool_size` and `max_overflow` parameters to `PipelineExecutor`, derived from `max_parallel_steps`:

```python
per_step_pool = max(1, target_db_max_connections // (max_parallel_steps * 2))
```

### 4.5 `_count_source_rows` — Expensive COUNT for Complex Queries

**File**: `services/migration_executor.py:660-675`
**Severity**: Medium

For complex `generate_sql` with JOINs, `SELECT COUNT(*) FROM (complex_query)` can be very slow. The count is only used for progress display, not for correctness.

**Fix**: Make count optional — skip if it takes longer than 5 seconds, or estimate from first batch size.

```python
def _count_source_rows(engine, select_query, source_table, timeout=5):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"SET statement_timeout = {timeout * 1000}"))
            result = conn.execute(text(f"SELECT COUNT(*) FROM ({select_query}) AS _cnt"))
            return result.scalar() or 0
    except Exception:
        return 0  # Unknown — progress shows "?" instead of percentage
```

---

## 5. Edge Cases

### 5.1 Empty DataFrame After Transformation

If `transform_batch()` removes all columns (e.g., all mappings are `ignore: true`), the resulting DataFrame has columns but the insert target has no matching columns. `batch_insert()` should handle `df.empty` gracefully (already does on line 373, but should also check for empty column set).

### 5.2 UUID PK Values in Cursor Pagination

`_load_last_seen_pk()` deserializes PK values from JSON checkpoint. For UUID columns, the value comes back as a **string** but needs to be compared as a UUID in the cursor WHERE clause. PostgreSQL handles this via implicit cast, but MySQL/MSSQL may not.

**Fix**: Store PK types in checkpoint metadata, or convert strings to UUIDs on load.

### 5.3 Resume After Partial COPY (PostgreSQL)

If the process crashes during `COPY FROM STDIN` (between `copy_expert` and `commit`), the transaction is rolled back but the checkpoint may already show those rows as processed. On resume, those rows are skipped (cursor passes their PK) but they were never actually inserted.

**Fix**: Save checkpoint **before** the COPY (currently correct in `_process_batches` line 885 — saved after successful insert). But `_process_single_batch` saves checkpoint in the caller, and if the caller crashes between `_process_single_batch` return and checkpoint save, we lose the checkpoint. Consider saving checkpoint inside `_process_single_batch` itself.

### 5.4 `GENERATE_HN` + Parallel Steps

`DataTransformer.reset_hn_counter()` is a **global** class variable. If two parallel steps both use `GENERATE_HN`, they'll share and corrupt the counter.

**Fix**: Make HN counter per-instance or use `threading.local()`.

### 5.5 MSSQL OFFSET Pagination — Non-Deterministic Order

**File**: `services/migration_executor.py:1004-1013`
**Severity**: Medium

`ROW_NUMBER() OVER (ORDER BY (SELECT 0))` produces **non-deterministic** ordering. On resume, the same rows may appear in different positions, causing duplicate inserts or skipped rows.

**Fix**: When no PK is available, require the user to specify a sort column in the config, or use `%%PHYSICAL%%` (MSSQL) / `ctid` (PostgreSQL) as a last resort.

---

## 6. Implementation Priority & Timeline

| Priority | Item | Effort | Impact |
|---|---|---|---|
| P0 | 1.1 Duplicate `get_by_job()` | 5 min | Bug fix |
| P0 | 1.3 `_process_single_batch` returns `None` | 2h | Data loss prevention |
| P0 | 2.2-2.4 Error persister for all failure paths | 1 day | Observability |
| P1 | 1.2 `finally` block in `_background_run` | 1h | Crash safety |
| P1 | 1.4 `steps_json` missing from UPDATE | 30 min | Data integrity |
| P1 | 1.5 Non-PG `to_sql` schema handling | 2h | Multi-DB support |
| P1 | 5.3 Checkpoint timing edge case | 2h | Resume correctness |
| P2 | 3.2 Parallel execution (Phase A-E) | 4 days | Throughput |
| P2 | 4.1 DB-level pagination | 1 day | API performance |
| P2 | 4.2 Batch buffer for pipeline_runs | 0.5 day | I/O reduction |
| P3 | 1.6 Event listener accumulation | 1h | Memory leak |
| P3 | 4.3 StringIO reuse | 1h | GC pressure |
| P3 | 4.4 Connection pool sizing | 2h | DB stability |
| P3 | 4.5 Optional source row count | 2h | Startup latency |
| P3 | 5.2 UUID PK in checkpoint | 1h | Cross-DB resume |
| P3 | 5.4 GENERATE_HN thread safety | 2h | Parallel correctness |
| P3 | 5.5 MSSQL non-deterministic pagination | 3h | Resume correctness |

---

## 7. Summary of Changes by File

| File | Changes |
|---|---|
| `repositories/pipeline_run_repo.py` | Remove duplicate `get_by_job()`, add `steps_json` to UPDATE, add `get_page()` |
| `repositories/job_repo.py` | Change `error_message` to append mode |
| `services/migration_executor.py` | Fix `None` return in `_process_single_batch`, add source-read error callback, optional count timeout |
| `services/pipeline_service.py` | Add `_persist_step_error()`, level-aware topological sort, parallel execution, fix `finally` block |
| `services/query_builder.py` | Fix non-PG `to_sql` schema handling, buffer reuse |
| `api/jobs/service.py` | No structural changes — benefits from error persister |
| `api/pipeline_runs/service.py` | Use DB-level pagination |
| `models/pipeline_config.py` | Add `parallel_enabled`, `max_parallel_steps` to `PipelineConfig` |
