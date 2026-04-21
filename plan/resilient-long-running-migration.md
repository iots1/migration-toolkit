# Resilient Long-Running Migration — Implementation Plan

> **Context**: `migration_executor.py` + `pipeline_service.py` handle ETL jobs triggered
> via `/api/v1/jobs`. For configs with millions of rows, migrations can run 1+ days.
> This plan addresses risks of connection timeout, OOM, process crash, and resume gaps.

---

## 1. Risk Assessment (Current State)

### 1.1 Connection Timeout Risk: HIGH

| What exists | Gap |
|---|---|
| `pool_pre_ping=True`, `pool_recycle=3600` in migration_executor calls | But `db_connector.py:121` defaults `pool_pre_ping=False` — inconsistent. `pd.read_sql()` holds **one source connection open** for the entire iteration (hours). If a firewall/LB drops idle connections between batches, the iterator dies mid-stream. |
| `statement_timeout = 0` on PG | No upper bound means a stuck query hangs forever with no alarm. |
| JIT engine creation per step | Engines are created once per step, not per batch. A step with 10M rows keeps both engines alive for hours. |
| Server-side cursor via `pd.read_sql(chunksize=N)` | Holds a PostgreSQL transaction snapshot open for the entire step, preventing VACUUM and blocking DDL on the source table. |

**Impact**: A single TCP reset kills the entire step. No automatic retry. Long-held snapshots prevent maintenance.

### 1.2 Memory (RAM) Risk: HIGH

| What exists | Gap |
|---|---|
| `chunksize=batch_size` in `pd.read_sql` | Each batch loads `batch_size` rows into a full DataFrame. `transform_batch()` creates copies (rename, drop, astype). Peak memory = ~3-4x one batch. |
| No memory guard | If `batch_size=10000` with 200+ columns of VARCHAR(MAX), a single batch can be 500MB+. No backpressure or adaptive sizing. |
| `clean_dataframe()` runs on every batch | Good, but allocates a new DataFrame. |
| Single `batch_size` for all steps | Wide tables (200+ cols) need smaller batches than narrow tables (5 cols), but currently use the same value from `pipeline.batch_size`. |

**Impact**: OOM killer terminates the Python process. Daemon thread dies silently. Job status stays "running" forever.

### 1.3 Process Crash / Machine Restart Risk: HIGH

| What exists | Gap |
|---|---|
| File-based checkpoints (`migration_checkpoints/`) | `_CHECKPOINT_INTERVAL = 10` means up to **10 batches of data loss** on crash (rows already inserted but checkpoint not yet saved). |
| Pipeline-level checkpoint (2D) | Good — tracks per-step status. But file writes are **not atomic** (no rename trick), so a crash mid-write corrupts the JSON. |
| `daemon=True` thread | If uvicorn restarts or machine reboots, the thread dies instantly. No graceful shutdown signal. |
| Job status in PostgreSQL | On crash, job stays `status='running'` forever. Only `resume=True` can mark it stale manually. |

**Impact**: After machine restart, stale job blocks new runs (409 Conflict). Resume may re-insert up to 10 batches of duplicate data.

### 1.4 Duplicate Data on Resume Risk: HIGH

| What exists | Gap |
|---|---|
| `skip_batches` parameter | Skips N batches from `pd.read_sql` iterator. But if source data changes between runs (INSERT/UPDATE/DELETE), batch boundaries shift. Row 10001 in run 1 is not row 10001 in run 2. |
| No deterministic ordering | `build_select_query()` has no `ORDER BY`. Without it, `OFFSET`-based resume is non-deterministic. |
| No idempotent insert | `batch_insert()` uses `if_exists="append"` — duplicate rows on resume are silently accepted. PostgreSQL COPY FROM STDIN is not inherently idempotent. |
| No batch-level transaction | `to_sql(if_exists="append")` uses autocommit by default — if COPY fails mid-stream, partial rows may already be committed with no rollback path. |

**Impact**: Resume after crash can skip or duplicate rows if source table is not frozen.

### 1.5 No Observability Risk: HIGH

| What exists | Gap |
|---|---|
| `log_callback` | Only reaches Streamlit UI or stdout. No structured logging to file/DB. |
| `batch_event_callback` → Socket.IO | Real-time but ephemeral. If no client is connected, events are lost. |
| No heartbeat | No way to distinguish "slow migration" from "dead thread". |
| No resource metrics | No CPU/RAM/disk monitoring during migration. |
| No ETA estimation | For 1+ day migrations, no way to estimate completion time. |

### 1.6 Row-Level Error Risk: MEDIUM (NEW)

| What exists | Gap |
|---|---|
| Batch-level error handling | If 1 row in a batch has data corruption (e.g., invalid encoding, constraint violation), the **entire batch** fails and migration stops. No mechanism to quarantine bad rows and continue. |

**Impact**: A single corrupt row among millions can halt a 24-hour migration with no recovery path other than manual investigation.

---

## 2. Implementation Plan

### Phase 1: Atomic Checkpoints + Reduce Data Loss Window (Priority: CRITICAL)

**Goal**: Crash at any point loses at most 1 batch, never corrupts checkpoint.

**File**: `services/checkpoint_manager.py`

**Changes**:

1. **Atomic file writes** — write to a temp file, then `os.replace()` (atomic on POSIX):
   ```python
   _FSYNC_INTERVAL = 50  # fsync every N batches

   def save_checkpoint(config_name, batch_num, rows_processed, last_seen_pk=None):
       path = _checkpoint_path(config_name)
       tmp = path + ".tmp"
       with open(tmp, "w") as f:
           json.dump(data, f)
           f.flush()
           if batch_num % _FSYNC_INTERVAL == 0:
               os.fsync(f.fileno())  # durable every 50 batches (~500ms overhead total)
       os.replace(tmp, path)     # atomic rename on POSIX
   ```

   > **Design note**: `os.replace()` atomically swaps the directory entry, but data blocks
   > may still reside in OS page cache. On power loss (not process crash), the file content
   > could be incomplete. We use `fsync()` every 50 batches as a compromise — for
   > batch_size=1000, that's ~10ms every 50 batches (50,000 rows), adding negligible
   > overhead while bounding worst-case data loss to 50 batches on power failure.
   > For process crashes (not power loss), `os.replace()` alone is sufficient since the
   > OS flushes page cache on orderly shutdown.

2. **Reduce `_CHECKPOINT_INTERVAL` from 10 to 1** in `migration_executor.py`:
   - With atomic writes, the I/O cost is minimal (one `rename` syscall).
   - Every successful batch is immediately checkpointed.
   - Max data loss on crash = 1 batch (the one currently being inserted).

3. **Same for `save_pipeline_checkpoint`** — atomic write (no fsync).

4. **Store `last_seen_pk` alongside `last_batch`** in checkpoint (prep for Phase 2):
   ```json
   {
     "config_name": "patients",
     "last_batch": 42,
     "last_seen_pk": "HN0000012345",
     "rows_processed": 42000,
     "timestamp": "2026-04-22T10:30:00"
   }
   ```

**Effort**: ~1.5 hours. **Risk**: None (backward compatible).

---

### Phase 2: Cursor-Based Pagination (Priority: CRITICAL)

**Goal**: Replace `pd.read_sql(chunksize=N)` with per-batch explicit queries that use
cursor-based pagination (`WHERE pk > :last_pk`). This eliminates OFFSET O(N) slowdown
and releases the source connection between batches.

**Why not OFFSET**: PostgreSQL OFFSET is O(N) — at batch 900 of a 10M-row table, it scans
9M rows then discards them. A 24-hour migration becomes 48-72 hours. Cursor-based is O(1)
per batch regardless of position.

**Files**: `services/query_builder.py`, `services/migration_executor.py`

**Changes**:

1. **New function `build_paginated_select()`** in `query_builder.py`:
   ```python
   from sqlalchemy import text

   def build_paginated_select(
       base_query: str,
       pk_columns: list[str],
       last_seen_pk: tuple | None = None,
       batch_size: int = 1000,
   ) -> tuple[text, dict]:
       """Wrap a SELECT query with cursor-based pagination.

       Uses WHERE pk > :last_pk ORDER BY pk LIMIT :batch_size.
       Returns a sqlalchemy.text() query and parameter dict for last_seen_pk.

       Cross-dialect: uses row-value comparison (a,b) > (:x,:y) for PostgreSQL,
       expanded OR-chain for MySQL/MSSQL compatibility.
       """
       order_clause = ", ".join(f'"{c}"' for c in pk_columns)
       pk_params: dict = {"batch_size": batch_size}

       if last_seen_pk is not None:
           for i, v in enumerate(last_seen_pk):
               pk_params[f"pk_{i}"] = v

           # Row-value comparison: (col1, col2) > (:pk_0, :pk_1)
           # Works in PostgreSQL, MySQL 8+, MariaDB 10.3+
           # For older MySQL/MSSQL, expand to OR-chain (see below)
           pk_placeholders = ", ".join(f":pk_{i}" for i in range(len(pk_columns)))
           where_clause = f"WHERE ({order_clause}) > ({pk_placeholders})"
       else:
           where_clause = ""

       return text(
           f"SELECT * FROM ({base_query}) AS _paginated_src "
           f"{where_clause} "
           f"ORDER BY {order_clause} "
           f"LIMIT :batch_size"
       ), pk_params

   def build_paginated_select_expanded(
       base_query: str,
       pk_columns: list[str],
       last_seen_pk: tuple | None = None,
       batch_size: int = 1000,
   ) -> tuple[text, dict]:
       """Cross-dialect cursor pagination using expanded OR-chain.

       For composite PK (a, b), generates:
         WHERE a > :pk_0 OR (a = :pk_0 AND b > :pk_1)
       This works on ALL databases including MySQL 5.x and MSSQL.
       """
       pk_params: dict = {"batch_size": batch_size}

       if last_seen_pk is not None:
           for i, v in enumerate(last_seen_pk):
               pk_params[f"pk_{i}"] = v

           conditions = []
           for depth in range(len(pk_columns)):
               eq_parts = []
               for i in range(depth):
                   eq_parts.append(f'"{pk_columns[i]}" = :pk_{i}')
               gt_part = f'"{pk_columns[depth]}" > :pk_{depth}'
               if eq_parts:
                   conditions.append(f"({' AND '.join(eq_parts)} AND {gt_part})")
               else:
                   conditions.append(gt_part)
           where_clause = f"WHERE ({' OR '.join(conditions)})"
       else:
           where_clause = ""

       order_clause = ", ".join(f'"{c}"' for c in pk_columns)
       return text(
           f"SELECT * FROM ({base_query}) AS _paginated_src "
           f"{where_clause} "
           f"ORDER BY {order_clause} "
           f"LIMIT :batch_size"
       ), pk_params
   ```

   > **Dialect choice**: Use `build_paginated_select()` (row-value) for PostgreSQL targets.
   > Use `build_paginated_select_expanded()` (OR-chain) for MySQL/MSSQL targets.
   > Detect via `engine.dialect.name` at runtime.

2. **Auto-detect primary key** (fallback to unique index) in `migration_executor.py`:
   ```python
   def _detect_pk_columns(engine, source_table: str) -> list[str] | None:
       try:
           insp = sqlalchemy.inspect(engine)
           pk = insp.get_pk_constraint(source_table)
           if pk and pk.get("constrained_columns"):
               return pk["constrained_columns"]
       except Exception:
           pass

       # Fallback: find smallest unique index (prefer short indexes for cursor pagination)
       try:
           insp = sqlalchemy.inspect(engine)
           unique_indexes = insp.get_unique_constraints(source_table)
           if unique_indexes:
               shortest = min(unique_indexes, key=lambda u: len(u["column_names"]))
               return shortest["column_names"]
       except Exception:
           pass

       return None
   ```

3. **Rewrite `_process_batches()`** — replace `pd.read_sql(chunksize=N)` with explicit loop:
   ```python
   def _process_batches(*, src_engine, select_query, config, ...):
       pk_columns = _detect_pk_columns(src_engine, source_table)

       if pk_columns is None:
           log("WARNING: No PK or unique index detected. "
               "Cursor-based pagination is not possible — migration will use OFFSET "
               "which is non-deterministic for resume. Consider adding a PK or unique "
               "index to the source table, or specify pk_columns in config.", "⚠️")
           return _process_batches_offset(...)

       last_seen_pk = _load_last_seen_pk(checkpoint, config_name)

       while True:
           query, params = build_paginated_select(
               select_query, pk_columns, last_seen_pk, batch_size
           )

           # Each batch gets a fresh connection from the pool
           with src_engine.connect() as conn:
               # Set source isolation level for consistent snapshot
               conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
               df_batch = pd.read_sql(
                   query, conn, params=params, coerce_float=False
               )

           if df_batch.empty:
               break

           last_seen_pk = tuple(df_batch[pk].iloc[-1] for pk in pk_columns)

           # ... process batch (transform, insert, checkpoint) ...
           save_checkpoint(config_name, batch_num, total_rows, last_seen_pk=last_seen_pk)

           del df_batch
           gc.collect()
   ```

   > **Source isolation level**: `REPEATABLE READ` ensures each batch sees a consistent
   > snapshot of the source data. Without it, rows inserted/updated between batches on
   > the source could cause duplicates or missed rows. For frozen source tables (no writes
   > during migration), this has no performance impact. Note: this only works for
   > PostgreSQL source; MySQL/MSSQL use their own isolation level syntax.

4. **For `generate_sql`**: User is responsible for including ORDER BY on the PK.
    - Add a validation warning if `ORDER BY` is not detected at the top level.
    - Use `sqlparse` to extract top-level tokens (skip subqueries/comments) and check
      for `ORDER BY` keyword. Note: `sqlparse` is a **syntax-level** parser, not
      semantic — it may produce false positives for `ORDER BY` inside CTEs or
      subqueries. Accept this limitation and add a secondary heuristic: if the
      ORDER BY columns match the detected PK columns, proceed; otherwise warn.
    - Fall back to OFFSET pagination with a warning if no ORDER BY found.

5. **OFFSET fallback** — for tables without PK or unique index:
   ```python
   def _process_batches_offset(*, src_engine, select_query, ...):
       """Fallback: OFFSET-based pagination for tables without PK.

       For PostgreSQL source: use ctid (physical row ID) for stable ordering.
       For other databases: fail early and require user to specify pk_columns.
       """
       dialect = src_engine.dialect.name

       if dialect == "postgresql":
           # ctid provides deterministic physical ordering (no index needed)
           order_col = "ctid"
           wrapped = (
               f"SELECT *, ctid FROM ({select_query}) AS _offset_src "
               f"ORDER BY ctid LIMIT :batch_size OFFSET :offset"
           )
       else:
           raise ValueError(
               f"Cannot paginate table '{source_table}': no primary key or unique "
               f"index found. For {dialect}, specify 'pk_columns' in the config or "
               f"add a primary key to the source table."
           )

       offset = skip_batches * batch_size
       while True:
           df_batch = pd.read_sql(
               text(wrapped), src_engine,
               params={"batch_size": batch_size, "offset": offset},
               coerce_float=False,
           )
           if df_batch.empty:
               break
           # Drop ctid column before transform/insert
           if "ctid" in df_batch.columns:
               df_batch = df_batch.drop(columns=["ctid"])
           offset += batch_size
           # ... process batch ...
   ```

   > **Note**: `ctid`-based OFFSET is still O(N) but at least deterministic. Physical
   > row order is stable if no VACUUM FULL or concurrent writes occur. For long-running
   > migrations on tables without PK, strongly recommend adding a PK or unique index.

**Effort**: ~8 hours. **Risk**: Significant refactor of `_process_batches()`. The explicit loop
changes the connection lifecycle — each batch gets a fresh connection. Test with:
   - Tables with composite PKs (multi-column)
   - Tables with unique index but no PK (fallback to unique index)
   - Tables with no PK or unique index (ctid fallback / fail early)
   - `generate_sql` with JOINs and WHERE clauses
   - Resume after crash (cursor-based vs OFFSET)
   - MySQL/MSSQL source with composite PK (expanded OR-chain)
   - Cross-dialect parameter binding with `sqlalchemy.text()` + subqueries

---

### Phase 3: Batch-Level Transaction Isolation (Priority: CRITICAL)

**Goal**: Each batch is atomic — on failure, no partial rows are committed to the target.

**File**: `services/query_builder.py`

**Changes**:

1. **Wrap `batch_insert()` in explicit transaction**:
   ```python
   from sqlalchemy import text as sa_text

   def batch_insert(df, target_table, engine, dtype_map=None) -> int:
       if df.empty:
           return 0

       is_pg = "postgresql" in str(engine.url)
       quoted_table = f'"{target_table}"'  # properly quote table name

       if is_pg:
           dbapi_conn = None
           try:
               conn = engine.connect()
               dbapi_conn = conn.connection
               dbapi_conn.autocommit = False

               # Set per-connection timeout (not pool-level)
               with dbapi_conn.cursor() as cur:
                   cur.execute(
                       f"SET statement_timeout = {BATCH_STATEMENT_TIMEOUT_MS}"
                   )

               buf = io.StringIO()
               writer = csv.writer(buf, lineterminator="\n")
               cols = list(df.columns)
               writer.writerows(df[cols].itertuples(index=False, name=None))
               buf.seek(0)

               col_list = ", ".join(f'"{c}"' for c in cols)
               with dbapi_conn.cursor() as cur:
                   cur.copy_expert(
                       f"COPY {quoted_table} ({col_list}) FROM STDIN WITH CSV",
                       buf,
                   )
               dbapi_conn.commit()
           except Exception:
               if dbapi_conn:
                   dbapi_conn.rollback()
               raise
           finally:
               if dbapi_conn:
                   dbapi_conn.autocommit = True
               if conn:
                   conn.close()
       else:
           # MySQL / MSSQL: use explicit BEGIN/COMMIT
           with engine.begin() as conn:
               df.to_sql(
                   name=target_table, con=conn, if_exists="append",
                   index=False, method="multi", dtype=dtype_map or None,
               )

       return len(df)
   ```

   > **Note**: `conn.connection` reaches into the raw DBAPI connection, bypassing
   > SQLAlchemy's transaction tracking. We must explicitly `conn.close()` in `finally`
   > to return the connection to the pool. The `autocommit` toggle is fragile —
   > consider using SQLAlchemy's `conn.execute(sa_text("BEGIN"))` pattern instead
   > if raw DBAPI access causes issues with SQLAlchemy 2.0's connection management.

2. **Set `statement_timeout` per-connection** (not pool-level):
   ```python
   BATCH_STATEMENT_TIMEOUT_MS = 300_000  # 5 minutes — configurable per step

   # Applied inside batch_insert() per connection, NOT as a pool-level event listener.
   # A pool-level listener would apply the timeout to ALL connections from the engine,
   # including schema inspection and PK detection queries that may need longer.
   #
   # For migrations using complex generate_sql with JOINs on large tables,
   # override BATCH_STATEMENT_TIMEOUT_MS via config:
   #   {"statement_timeout_ms": 600000}  # 10 minutes
   ```

   > **Rationale**: `statement_timeout = 0` (current) means a stuck query hangs forever.
   > A per-batch timeout of 5 minutes guarantees each batch either completes or fails
   > within a bounded time. The retry mechanism (Phase 4) handles transient failures.
   > Setting per-connection (not pool-level) avoids interfering with non-migration queries
   > like PK detection and schema inspection that share the same engine.

**Effort**: ~4 hours. **Risk**: Changing transaction semantics for COPY is the most sensitive
part. Must verify that rollback after partial COPY actually removes all inserted rows.
Also verify that raw DBAPI access (`conn.connection`) works correctly with SQLAlchemy 2.0's
connection pool — test that connections are properly returned to the pool after `close()`.

---

### Phase 4: Retry with Backoff + UPSERT Strategy (Priority: HIGH)

**Goal**: Transient errors don't kill the migration. Resume never produces duplicate rows.

**Files**: `services/migration_executor.py`, `services/query_builder.py`

**Changes**:

1. **Retry wrapper around `batch_insert()`**:
   ```python
   MAX_RETRIES = 3
   RETRY_DELAYS = [5, 15, 45]  # seconds

   def _insert_with_retry(df, target_table, tgt_engine, dtype_map, batch_num, log):
       for attempt in range(MAX_RETRIES):
           try:
               batch_insert(df, target_table, tgt_engine, dtype_map)
               return
           except (OperationalError, DisconnectionError, InterfaceError) as e:
               if attempt == MAX_RETRIES - 1:
                   raise
               delay = RETRY_DELAYS[attempt]
               log(f"Batch {batch_num}: transient error, retry {attempt+1}/{MAX_RETRIES} "
                   f"in {delay}s: {e}", "⚠️")
               time.sleep(delay)
               tgt_engine.dispose()  # force new connection pool
   ```

   > **COPY + retry safety**: Since Phase 3 wraps each COPY in a transaction with rollback,
   > a failed COPY is fully rolled back before retry. No partial data risk.

2. **Configurable insert strategy** — `"insert_strategy"` in config:
   - `"append"` (default, backward compatible) — plain INSERT / COPY
   - `"upsert"` — PostgreSQL `INSERT ... ON CONFLICT (pk) DO UPDATE SET ...`
   - `"upsert_ignore"` — PostgreSQL `INSERT ... ON CONFLICT (pk) DO NOTHING`

   ```python
   # In query_builder.py
   def _make_pg_upsert_method(target_table, pk_columns, insert_strategy="upsert"):
       """COPY into staging CTE, then INSERT ... ON CONFLICT."""
       def _upsert(table, conn, keys, data_iter):
           cols = ", ".join(f'"{k}"' for k in keys)
           pk_cols = ", ".join(f'"{k}"' for k in pk_columns)
           updates = ", ".join(
               f'"{k}" = EXCLUDED."{k}"' for k in keys if k not in pk_columns
           )

           buf = io.StringIO()
           writer = csv.writer(buf, lineterminator="\n")
           writer.writerows(data_iter)
           buf.seek(0)

           dbapi_conn = conn.connection
           with dbapi_conn.cursor() as cur:
               if insert_strategy == "upsert":
                   sql = (
                       f"INSERT INTO {quoted} ({cols}) "
                       f"SELECT * FROM temp_csv ON CONFLICT ({pk_cols}) "
                       f"DO UPDATE SET {updates}"
                   )
               else:  # upsert_ignore
                   sql = (
                       f"INSERT INTO {quoted} ({cols}) "
                       f"SELECT * FROM temp_csv ON CONFLICT ({pk_cols}) DO NOTHING"
                   )
               cur.copy_expert(
                   f"COPY temp_csv({cols}) FROM STDIN WITH CSV", buf
               )
               cur.execute(sql)
       return _upsert
   ```

   > **Note**: UPSERT requires knowing the target PK columns. These are detected at the
   > start of each step (same mechanism as Phase 2 source PK detection) and stored
   > alongside the step config for the duration of the migration.

3. **Retry on source read failure** — since Phase 2 uses per-batch queries, a failed
   `pd.read_sql` simply retries the same cursor-based query (same `last_seen_pk`):
   ```python
   def _read_batch_with_retry(src_engine, query, params, batch_num, log, max_retries=3):
       for attempt in range(max_retries):
           try:
               with src_engine.connect() as conn:
                   return pd.read_sql(text(query), conn, params=params, coerce_float=False)
           except (OperationalError, DisconnectionError) as e:
               if attempt == max_retries - 1:
                   raise
               log(f"Batch {batch_num}: source read error, retry {attempt+1}/{max_retries}: {e}", "⚠️")
               time.sleep(RETRY_DELAYS[attempt])
               src_engine.dispose()
   ```

**Effort**: ~8 hours. **Risk**: UPSERT is dialect-specific. Implement PostgreSQL first
(most common target), add MySQL (`INSERT ... ON DUPLICATE KEY UPDATE`) and MSSQL
(`MERGE`) later. COPY-into-temp-CTE pattern needs testing for large batches.

---

### Phase 5: Graceful Shutdown + File-Based Heartbeat (Priority: HIGH)

**Goal**: Process restart doesn't leave orphan "running" jobs. Dead threads are detectable.

**Files**: `services/pipeline_service.py`, `api/jobs/router.py`, `repositories/job_repo.py`

**Changes**:

1. **Shutdown signal handler** — register in FastAPI startup:
   ```python
   # In api/main.py or a lifecycle module
   _shutdown_event = threading.Event()

   @asynccontextmanager
   async def lifespan(app):
       loop = asyncio.get_running_loop()

       def _handle_shutdown():
           _shutdown_event.set()
           # Also interrupt any running migrations
           for thread in threading.enumerate():
               if thread.name.startswith("pipeline-"):
                   pass  # thread checks _shutdown_event between batches

       # Python signal handlers run in the main thread
       # Use loop.add_signal_handler for async compatibility
       for sig in (signal.SIGTERM, signal.SIGINT):
           loop.add_signal_handler(sig, _handle_shutdown)

       yield

   # In migration_executor.py — check between batches:
   def _process_batches(..., shutdown_event=None):
       if shutdown_event and shutdown_event.is_set():
           save_checkpoint(config_name, batch_num, total_rows)
           raise MigrationInterrupted("Shutdown requested")
   ```

2. **File-based heartbeat** (lightweight, no DB write per batch):
   ```python
   HEARTBEAT_DIR = "/tmp/migration_heartbeats"

   def _write_heartbeat(job_id: str, step: str, batch: int) -> None:
       path = os.path.join(HEARTBEAT_DIR, f"{job_id}.heartbeat")
       os.makedirs(HEARTBEAT_DIR, exist_ok=True)
       with open(path, "w") as f:
           f.write(f"{step}|{batch}|{time.time()}")

   def _read_heartbeat(job_id: str) -> dict | None:
       path = os.path.join(HEARTBEAT_DIR, f"{job_id}.heartbeat")
       if not os.path.exists(path):
           return None
       try:
           with open(path) as f:
               step, batch, ts = f.read().strip().split("|")
           return {"step": step, "batch": int(batch), "timestamp": float(ts)}
       except Exception:
           return None
   ```

   > **Design note**: We use file-based heartbeat instead of DB-based to avoid
   > 10,000+ DB writes per migration (one per batch). The heartbeat file is ~50 bytes
   > and written via `open()` which is near-instant. A periodic checker (or stale job
   > detection on POST) reads this file to detect dead threads.

3. **Heartbeat + `last_heartbeat` column** — periodic flush to DB (every 30s):
   ```sql
   ALTER TABLE jobs ADD COLUMN last_heartbeat TIMESTAMPTZ;
   ```

   ```python
   # In pipeline_service.py — background heartbeat flusher
   def _heartbeat_flusher(self, job_id: str, stop_event: threading.Event):
       """Flush heartbeat to DB every 30 seconds."""
       while not stop_event.wait(30):
           hb = _read_heartbeat(job_id)
           if hb and time.time() - hb["timestamp"] < 60:
               self._run_repo.update(job_id, PipelineRunUpdateRecord(
                   last_heartbeat=datetime.utcnow()
               ))
   ```

4. **Stale job detection on POST /api/v1/jobs**:
   ```python
   # In jobs router — before returning 409:
   running_job = job_repo.get_running_for_pipeline(pipeline_id)
   if running_job:
       hb = _read_heartbeat(str(running_job["id"]))
       if hb and (time.time() - hb["timestamp"]) > 300:  # 5 min stale
           job_repo.update(running_job["id"], {
               "status": "failed",
               "error_message": "Process died — no heartbeat for 5 minutes"
           })
           # Clean up heartbeat file
           os.remove(f"{HEARTBEAT_DIR}/{running_job['id']}.heartbeat")
           # Proceed with new job creation
       else:
           return 409  # genuinely running
   ```

5. **New job status: `"interrupted"`**:
   - Set when `_shutdown_event` is detected between batches.
   - Resume from interrupted job works the same as failed (reads checkpoint).

**Effort**: ~5 hours. **Risk**: Signal handling in threaded Python — only the main thread
receives signals. We use `asyncio.loop.add_signal_handler()` (available in uvicorn's
event loop) + `threading.Event` to propagate to worker threads.

---

### Phase 6: Memory Guard + Adaptive Batch Size (Priority: HIGH)

**Goal**: Prevent OOM on wide tables or large text columns.

**File**: `services/migration_executor.py`

**Changes**:

1. **Memory check before each batch**:
   ```python
   import psutil

   MEMORY_WARN_THRESHOLD = 85   # percent — trigger gc.collect()
   MEMORY_ABORT_THRESHOLD = 95  # percent — abort to prevent OOM kill
   MEMORY_PER_BATCH_TARGET_MB = 200  # target max memory per batch

   def _check_memory(log, batch_num, batch_size):
       mem = psutil.virtual_memory()
       if mem.percent > MEMORY_WARN_THRESHOLD:
           log(f"Batch {batch_num}: Memory at {mem.percent}% — running GC", "⚠️")
           gc.collect()
           mem = psutil.virtual_memory()
           if mem.percent > MEMORY_ABORT_THRESHOLD:
               raise MemoryError(
                   f"Memory critically high ({mem.percent}%) — aborting to prevent OOM kill. "
                   f"Reduce batch_size or add more RAM."
               )
       return mem
   ```

   > **psutil dependency**: Already a de-facto standard in production Python (used by
   > celery, gunicorn, etc.). Add to `requirements.txt`.

2. **Adaptive batch size** — measure first batch memory, adjust:
   ```python
   # After first successful batch:
   batch_memory_mb = df_batch.memory_usage(deep=True).sum() / (1024 * 1024)
   if batch_memory_mb > MEMORY_PER_BATCH_TARGET_MB:
       # Calculate new batch size to stay under target
       scale_factor = MEMORY_PER_BATCH_TARGET_MB / batch_memory_mb
       new_batch_size = max(100, int(batch_size * scale_factor))
       log(f"Batch 1 memory: {batch_memory_mb:.0f}MB — "
           f"reducing batch_size from {batch_size} to {new_batch_size}", "⚠️")
       batch_size = new_batch_size
   ```

3. **Explicit `del df_batch` + `gc.collect()`** after each batch insert:
   ```python
   # After successful batch_insert():
   total_rows += rows_in_batch
   save_checkpoint(config_name, batch_num, total_rows, last_seen_pk=last_seen_pk)
   del df_batch
   gc.collect()
   ```

4. **Per-step batch_size override** (optional, in pipeline node config):
   ```python
   # In pipeline node JSON:
   # {"config_name": "wide_table", "batch_size_override": 500}

   effective_batch_size = node.get("batch_size_override") or pipeline.batch_size
   ```

**Effort**: ~3 hours. **Risk**: Low. `psutil` is cross-platform. Adaptive sizing only
reduces batch_size (never increases), so it's safe.

---

### Phase 7: Connection Keepalive + Per-Batch Reconnect (Priority: HIGH)

**Goal**: Survive network interruptions between batches. Each batch gets a fresh connection.

> **Note**: Phase 2 (cursor-based pagination) already replaces `pd.read_sql(chunksize=N)`
> with per-batch explicit queries, so each batch naturally gets a fresh connection from
> the pool. This phase adds TCP keepalive as a defense-in-depth measure.

**Files**: `services/migration_executor.py`, `services/db_connector.py`

**Changes**:

1. **Fix `pool_pre_ping` default** in `db_connector.py:121`:
   ```python
   # Before:
   engine_kwargs["pool_pre_ping"] = False
   # After:
   engine_kwargs["pool_pre_ping"] = True
   ```

2. **TCP keepalive on SQLAlchemy engines** (especially for PostgreSQL through firewalls):
   ```python
   from sqlalchemy import event

   @event.listens_for(engine, "connect")
   def set_keepalive(dbapi_conn, connection_record):
       if "postgresql" in str(engine.url):
           dbapi_conn.set_keepalives(1)
           dbapi_conn.set_keepalives_idle(30)
           dbapi_conn.set_keepalives_interval(10)
           dbapi_conn.set_keepalives_count(5)
       elif hasattr(dbapi_conn, "ping"):  # pymysql
           # pymysql uses reconnect=True in URL params
           pass
   ```

3. **Connection pool sizing** — set reasonable limits for migration workloads:
   ```python
   # In migration_executor.py engine creation:
   engine = connector.create_sqlalchemy_engine(
       **conn_config,
       pool_pre_ping=True,
       pool_recycle=1800,    # 30 min (not 3600 — align with typical firewall timeouts)
       pool_size=2,          # only need 1-2 concurrent connections per step
       max_overflow=1,
   )
   ```

4. **No explicit health check needed** — `pool_pre_ping=True` handles this:
   SQLAlchemy's `pool_pre_ping` issues a lightweight `SELECT 1` before handing out a
   connection. If it fails, the connection is discarded and a fresh one is created.
   This is more efficient than a separate health check call.

**Effort**: ~2 hours. **Risk**: Low. Mostly config changes.

---

### Phase 8: Structured Logging + ETA Estimation (Priority: MEDIUM)

**Goal**: Post-mortem analysis possible. Users can estimate when migration will finish.

**Files**: `services/migration_logger.py` (enhance existing), `services/pipeline_service.py`

**Changes**:

1. **JSONL log file per job**: `logs/job_{job_id}.jsonl`
   ```python
   import json, os
   from datetime import datetime, timezone

   class MigrationLogger:
       def __init__(self, job_id: str):
           self.job_id = job_id
           os.makedirs("logs", exist_ok=True)
           self._path = f"logs/job_{job_id}.jsonl"
           self._file = open(self._path, "a")
           self._step_stats: dict[str, dict] = {}

       def log(self, step: str, batch: int, event: str, **extra):
           entry = {
               "ts": datetime.now(timezone.utc).isoformat(),
               "job_id": self.job_id,
               "step": step,
               "batch": batch,
               "event": event,
               **extra,
           }
           self._file.write(json.dumps(entry, ensure_ascii=False) + "\n")
           self._file.flush()

       def close(self):
           self._file.close()
   ```

2. **Batch timing + ETA estimation**:
   ```python
   def _estimate_eta(self, step: str, batch_num: int, total_rows: int,
                     rows_processed: int) -> str | None:
       """Calculate ETA from rolling average of recent batch durations."""
       stats = self._step_stats.get(step, {})
       if not stats.get("batch_times"):
           return None

       # Rolling average of last 10 batches
       recent = stats["batch_times"][-10:]
       avg_seconds = sum(recent) / len(recent)
       remaining_rows = total_rows - rows_processed
       remaining_batches = remaining_rows / stats.get("avg_rows_per_batch", 1)
       eta_seconds = remaining_batches * avg_seconds

       if eta_seconds < 60:
           return f"~{int(eta_seconds)}s"
       elif eta_seconds < 3600:
           return f"~{int(eta_seconds / 60)}m"
       else:
           hours = int(eta_seconds // 3600)
           mins = int((eta_seconds % 3600) // 60)
           return f"~{hours}h {mins}m"
   ```

3. **Store final summary in `jobs` table**:
   ```sql
   ALTER TABLE jobs ADD COLUMN summary JSONB;
   ```
   ```json
   {
     "total_rows": 5000000,
     "total_duration_s": 86400,
     "steps": {
       "patients": {"rows": 2000000, "duration_s": 36000, "batches": 2000},
       "visits": {"rows": 3000000, "duration_s": 50400, "batches": 3000}
     },
     "peak_memory_pct": 72,
     "retries": 3
   }
   ```

4. **Log rotation** — cap log file at 50MB per job, or 7 days retention:
   ```python
   MAX_LOG_SIZE = 50 * 1024 * 1024  # 50MB

   def _rotate_if_needed(self):
       if os.path.getsize(self._path) > MAX_LOG_SIZE:
           self._file.close()
           os.rename(self._path, self._path + ".old")
           self._file = open(self._path, "a")
   ```

**Effort**: ~4 hours. **Risk**: Low. Disk space is the main concern — 50MB cap per job
with automatic rotation keeps it bounded.

---

### Phase 9: Dead Letter Queue (Priority: MEDIUM)

**Goal**: A single corrupt row doesn't halt the entire migration. Bad rows are quarantined
for later investigation.

**Files**: `services/migration_executor.py` (new option), `services/query_builder.py`

**Changes**:

1. **Config flag `"error_handling"` per mapping config**:
   - `"fail"` (default, backward compatible) — stop migration on any batch error
   - `"skip_bad_rows"` — quarantine bad rows, continue migration

2. **Row-level error quarantine** when `error_handling: "skip_bad_rows"`:
   ```python
   def _insert_with_quarantine(
       df, target_table, tgt_engine, dtype_map,
       batch_num, config_name, log, quarantine_callback
   ):
       """Try full batch insert. On failure, insert row-by-row to isolate bad rows."""
       try:
           batch_insert(df, target_table, tgt_engine, dtype_map)
           return len(df), 0  # (inserted, quarantined)
       except Exception as batch_error:
           if config.get("error_handling") != "skip_bad_rows":
               raise  # default: fail fast

           log(f"Batch {batch_num}: Batch insert failed — "
               f"switching to row-by-row quarantine mode", "⚠️")

           inserted = 0
           quarantined = 0
           for idx, row in df.iterrows():
               try:
                   row_df = df.iloc[[idx]]
                   batch_insert(row_df, target_table, tgt_engine, dtype_map)
                   inserted += 1
               except Exception as row_error:
                   quarantined += 1
                   quarantine_callback(
                       config_name, batch_num, idx, row.to_dict(), str(row_error)
                   )
                   if quarantined >= MAX_QUARANTINED_PER_BATCH:
                       log(f"Batch {batch_num}: Too many bad rows ({quarantined}), aborting batch", "❌")
                       break

           return inserted, quarantined
   ```

3. **Quarantine storage** — write to `logs/quarantine_{job_id}.jsonl`:
   ```jsonl
   {"ts": "2026-04-22T10:30:00Z", "step": "patients", "batch": 42, "row_idx": 5,
    "row": {"hn": "HN0001234", "name": null, ...}, "error": "violates not-null constraint"}
   ```

4. **Quarantine summary in job completion**:
   ```python
   # Include in job.summary:
   "quarantined_rows": {"patients": 15, "visits": 3}
   ```

   > **Performance note**: Row-by-row fallback is slow (1 DB round-trip per row).
   > Only triggered when a batch fails. For tables with frequent bad data, users
   > should fix the data or add transformers rather than relying on quarantine.

**Effort**: ~5 hours. **Risk**: Row-by-row fallback is slow by design. Limit
`MAX_QUARANTINED_PER_BATCH` (e.g., 100) to prevent runaway processing. Quarantine
is opt-in via config flag — default behavior unchanged.

---

## 3. Implementation Priority

| Phase | Risk Addressed | Effort | Priority |
|-------|---------------|--------|----------|
| 1. Atomic Checkpoints | Data loss on crash, checkpoint corruption | ~1.5h | **CRITICAL** |
| 2. Cursor-Based Pagination | OFFSET O(N) slowdown, connection hold, resume correctness | ~6h | **CRITICAL** |
| 3. Batch Transaction Isolation | Partial data on COPY failure, stuck queries | ~3h | **CRITICAL** |
| 4. Retry + UPSERT | Transient errors, duplicate rows on resume | ~8h | **HIGH** |
| 5. Graceful Shutdown + Heartbeat | Orphan jobs, dead thread detection | ~5h | **HIGH** |
| 6. Memory Guard | OOM kill on wide tables | ~3h | **HIGH** |
| 7. Connection Keepalive | Firewall timeout, pool_pre_ping fix | ~2h | **HIGH** |
| 8. Structured Logging + ETA | Observability, progress estimation | ~4h | **MEDIUM** |
| 9. Dead Letter Queue | Single bad row halting migration | ~5h | **MEDIUM** |

**Total estimated effort**: ~37.5 hours

**Recommended order**: 1 → 2 → 3 → 5 → 6 → 4 → 7 → 8 → 9

> **Rationale**: Phases 1-3 form the foundation (pagination + transactions + checkpoints).
> Phase 5 (shutdown) should come before retry (Phase 4) because the shutdown mechanism
> needs to work before we add retry complexity. Phase 6 (memory) and Phase 7 (connections)
> are independent and can be done in parallel. Phases 8-9 are enhancements that build on
> the stable foundation.

---

## 4. Quick Wins (Can Do Now)

These are small changes with immediate impact — can be done before starting the phases:

```python
# 1. migration_executor.py — reduce checkpoint interval
_CHECKPOINT_INTERVAL = 1  # was 10

# 2. checkpoint_manager.py — atomic write (Phase 1)
# Add os.replace() — see Phase 1

# 3. migration_executor.py — explicit gc after each batch
import gc
# After batch_insert() success:
del df_batch
gc.collect()

# 4. db_connector.py — fix pool_pre_ping default (Phase 7)
# Change line 121: pool_pre_ping=True

# 5. pipeline_service.py — remove dead code
# Delete unreachable line 650: return False, ""
```

---

## 5. Database Schema Changes

All schema changes needed across phases:

```sql
-- Phase 5: Heartbeat tracking
ALTER TABLE jobs ADD COLUMN last_heartbeat TIMESTAMPTZ;

-- Phase 8: Migration summary
ALTER TABLE jobs ADD COLUMN summary JSONB;

-- New job status values
-- existing: 'pending', 'running', 'completed', 'failed'
-- add:       'interrupted'
-- (No DDL needed — just use the string value)
```

Migration script: `scripts/migrate_add_resilience_columns.py`

---

## 6. New Dependencies

```
psutil>=5.9.0    # Phase 6: memory monitoring (cross-platform, widely used)
sqlparse>=0.4.0  # Phase 2: ORDER BY validation in generate_sql (lightweight)
```

---

## 7. Architecture Diagram (After Implementation)

```
POST /api/v1/jobs {pipeline_id, resume: true}
    │
    ├── Check stale jobs (heartbeat file stale > 5min → auto-mark failed)
    ├── Load pipeline checkpoint (if resume)
    │
    └── PipelineExecutor.start_background()
            │
            ├── Register shutdown handler (threading.Event)
            ├── Start heartbeat flusher (file → DB every 30s)
            │
            └── For each step (topological order):
                ├── Skip if checkpoint says "completed"
                ├── Resume from last_seen_pk if "running"
                │
                └── run_single_migration()
                    │
                    ├── Detect PK columns (source + target)
                    ├── Set statement_timeout (per-batch, not 0)
                    │
                    └── LOOP (cursor-based pagination):
                        ├── Check shutdown signal → save checkpoint & exit
                        ├── Check memory → adaptive batch_size
                        ├── SELECT * FROM (base_query)
                        │   WHERE (pk) > (:last_pk)
                        │   ORDER BY pk LIMIT :batch_size
                        │   [fresh connection per batch from pool]
                        │
                        ├── transform_batch()
                        ├── _insert_with_retry()
                        │   ├── batch_insert() [wrapped in transaction]
                        │   │   ├── append (default)
                        │   │   ├── upsert (INSERT ... ON CONFLICT DO UPDATE)
                        │   │   └── upsert_ignore (INSERT ... ON CONFLICT DO NOTHING)
                        │   └── retry 3x with exponential backoff
                        │       └── rollback on failure, new connection pool on retry
                        │
                        ├── Update last_seen_pk
                        ├── Atomic checkpoint (every batch)
                        ├── Write heartbeat file
                        ├── del df_batch + gc.collect()
                        ├── Socket.IO event
                        └── JSONL log entry + ETA update
                        │
                    └── On step success: mark "completed" in checkpoint
                        On step failure: quarantine bad rows if configured
```

---

## 8. Testing Strategy

Each phase should be verified with:

| Test Scenario | How to Verify |
|---|---|
| **Crash recovery** (Phase 1, 2) | Start migration, `kill -9` the process mid-batch, restart, resume. Verify no duplicates via `SELECT COUNT(*)` and checksum. |
| **Network failure** (Phase 4, 7) | Block source port with `iptables` for 30s during migration. Verify retry succeeds after port is restored. |
| **OOM prevention** (Phase 6) | Use a wide table (200+ VARCHAR cols) with batch_size=10000. Verify adaptive sizing kicks in. |
| **Graceful shutdown** (Phase 5) | Send `SIGTERM` during migration. Verify checkpoint is saved and job status = "interrupted". |
| **Dead thread detection** (Phase 5) | Start migration, `kill -9`, then POST new job. Verify stale job auto-fails. |
| **UPSERT correctness** (Phase 4) | Migrate, then resume. Verify row count matches source exactly (no duplicates). |
| **Bad row quarantine** (Phase 9) | Insert a row with NULL in NOT NULL column. Verify migration continues and bad row appears in quarantine file. |
| **ETA accuracy** (Phase 8) | Start migration, check ETA after 100 batches, compare with actual completion time. |
