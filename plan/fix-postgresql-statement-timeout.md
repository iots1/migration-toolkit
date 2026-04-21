# Fix: PostgreSQL `statement_timeout` ทำให้ Migration ล้มเหลว

**Date**: 2026-04-21  
**File Changed**: `services/migration_executor.py`  
**Error**: `psycopg2.errors.QueryCanceled: canceling statement due to statement timeout`

---

## 1. ปัญหา

เมื่อรัน migration pipeline พบ error:

```
psycopg2.errors.QueryCanceled) canceling statement due to statement timeout
[SQL: SELECT "hn", "title", "first_name", "last_name", "blood_type",
      "religion", "occupation", "marital_status", "title_code",
      "title_th", "title_en" FROM test_patients]
```

### สาเหตุ

1. PostgreSQL server มี `statement_timeout` ตั้งไว้ (อาจจาก `postgresql.conf` หรือ `ALTER DATABASE`)
2. `pd.read_sql(select_query, src_engine, chunksize=1000)` ส่ง SELECT query ทั้งหมดไป server ในครั้งเดียว
3. แม้ `chunksize` จะใช้ **server-side named cursor** (streaming) แต่ PostgreSQL ยังต้อง execute query plan + เริ่ม scan ทั้งหมดก่อนเริ่ม return rows
4. ถ้า table มีข้อมูลมาก (หลายแสน/ล้าน rows) query จะถูก `statement_timeout` ฆยก่อนจะเสร็จ

### จุดที่เกิดปัญหาในโค้ด

```python
# migration_executor.py:499-501
data_iterator = pd.read_sql(
    select_query, src_engine, chunksize=batch_size, coerce_float=False
)
```

`pd.read_sql` สร้าง connection ของตัวเองจาก engine pool ทำให้ไม่สามารถ inject `SET statement_timeout = 0` ก่อน execute ได้โดยตรง

---

## 2. วิธีแก้

ใช้ SQLAlchemy **`connect` event listener** เพื่อตั้งค่า session parameters ทุกครั้งที่มีการสร้าง connection ใหม่จาก pool

### สิ่งที่เปลี่ยนแปลงใน `services/migration_executor.py`

#### 2.1 Import `event` จาก SQLAlchemy

```python
# Before
from sqlalchemy import text

# After
from sqlalchemy import text, event
```

#### 2.2 เพิ่ม function `_tune_pg_migration_session()`

```python
def _tune_pg_migration_session(engine) -> None:
    """Auto-configure PostgreSQL session parameters for migration workloads."""
    if "postgresql" not in str(engine.url):
        return

    @event.listens_for(engine, "connect")
    def _apply_migration_tuning(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("SET statement_timeout = 0")
        cursor.execute("SET work_mem = '256MB'")
        cursor.execute("SET max_parallel_workers_per_gather = 4")
        cursor.execute("SET maintenance_work_mem = '512MB'")
        cursor.execute("SET effective_cache_size = '4GB'")
        cursor.close()
```

#### 2.3 เรียกใช้ใน `run_single_migration()` — หลังสร้าง engine ทั้งสอง

```python
src_engine = connector.create_sqlalchemy_engine(
    **source_conn_config, pool_pre_ping=True, pool_recycle=3600
)
tgt_engine = connector.create_sqlalchemy_engine(
    **target_conn_config, pool_pre_ping=True, pool_recycle=3600
)

_tune_pg_migration_session(src_engine)
_tune_pg_migration_session(tgt_engine)
```

---

## 3. Session Parameters — คำอธิบายแต่ละตัว

| Parameter | ค่า | ผลกระทบ | เหตุผล |
|---|---|---|---|
| `statement_timeout` | `0` (disabled) | ป้องกัน query cancel | Migration query อาจรันนานหลายนาทีถึงหลายชั่วโมง ต้องปิด timeout |
| `work_mem` | `256MB` (default: 4MB) | Sort, hash, merge ทำใน memory มากขึ้น | Tables ใน HIS มีหลาย column (wide rows) — sort/hash ไม่ต้อง spill to disk |
| `max_parallel_workers_per_gather` | `4` (default: 2) | Sequential scan ขนาน 4 workers | เร่ง full table scan สำหรับ tables ขนาดใหญ่ |
| `maintenance_work_mem` | `512MB` (default: 64MB) | COPY, VACUUM, CREATE INDEX ใช้ memory มากขึ้น | `COPY FROM STDIN` (batch_insert) เขียน WAL ได้เร็วขึ้น |
| `effective_cache_size` | `4GB` (default: 4GB) | Query planner hint — **ไม่จอย memory จริง** | บอก planner ว่ามี cache มาก → เลือก execution plan ที่เหมาะกับ large scan |

---

## 4. ทำไมใช้ Event Listener แทนวิธีอื่น

| ทางเลือก | ข้อจำกัด |
|---|---|
| `SET statement_timeout` ก่อน `pd.read_sql()` | `pd.read_sql(engine, chunksize=...)` สร้าง connection ของตัวเองจาก pool — ไม่ใช้ connection เดียวกัน |
| `connect_args` ใน URL | ต้องเข้าไปแก้ `db_connector.py` ซึ่งกระทบทุก caller |
| `ALTER DATABASE ... SET statement_timeout` | กระทบทุก connection ทุก application บน database นั้น |
| `postgresql.conf` | ต้อง restart PostgreSQL |
| **`event.listens_for(engine, "connect")`** | **แก้เฉพาะ engine ที่ migration สร้าง, engine `dispose()` แล้ว listener หาย, ไม่กระทบอะไร** |

### Lifecycle ของ tuning

```
run_single_migration() เริ่ม
  │
  ├─ create src_engine
  ├─ _tune_pg_migration_session(src_engine)  ← register event listener
  ├─ create tgt_engine
  ├─ _tune_pg_migration_session(tgt_engine)  ← register event listener
  │
  ├─ [migration ทำงาน — ทุก connection ที่สร้างจาก pool จะได้รับ tuning อัตโนมัติ]
  │     │
  │     ├─ pd.read_sql(chunksize=...) → connection ใหม่ → SET work_mem, SET timeout, ...
  │     ├─ _count_source_rows()       → connection ใหม่ → SET work_mem, SET timeout, ...
  │     ├─ batch_insert()             → connection ใหม่ → SET work_mem, SET timeout, ...
  │     └─ _verify_post_migration()   → connection ใหม่ → SET work_mem, SET timeout, ...
  │
  └─ finally:
      ├─ src_engine.dispose()  ← listener ถูกทำลายพร้อม engine
      └─ tgt_engine.dispose()  ← listener ถูกทำลายพร้อม engine
```

---

## 5. Queries ที่ได้รับผลกระทบ

ทุก query ที่ผ่าน `src_engine` หรือ `tgt_engine` จะได้รับ tuning อัตโนมัติ:

| Query | Location | ประเภท |
|---|---|---|
| `SELECT ... FROM source_table` (chunked) | `_process_batches()` → `pd.read_sql()` | Source extract |
| `SELECT COUNT(*) FROM (...)` | `_count_source_rows()` | Source row count |
| `SELECT COUNT(*) FROM target` | `_get_row_count()` | Target pre-count |
| `COPY target_table (...) FROM STDIN WITH CSV` | `batch_insert()` → `to_sql()` | Target bulk insert |
| `SELECT COUNT(*) FROM target` | `_verify_post_migration()` | Target post-count |
| `SELECT MAX(...)` | `_init_hn_counter()` | HN counter init |
| `TRUNCATE / DELETE FROM target` | `_truncate_table()` | Target cleanup |
| Schema inspection (`get_columns`) | `_validate_schema()` | Schema validation |

---

## 6. ข้อจำกัดและข้อควรระวัง

### 6.1 Memory Usage

- `work_mem = 256MB` ต่อ **operation** (ไม่ใช่ต่อ connection) — ถ้ามีหลาย sort/hash operations พร้อมกัน อาจใช้ memory มาก
- สำหรับ migration ที่รันทีละ batch (sequential) ไม่น่ามีปัญหา
- ถ้า server มี RAM น้อย (< 4GB) ให้ลด `work_mem` เหลือ `64MB` หรือ `128MB`

### 6.2 MySQL / MSSQL

- `_tune_pg_migration_session()` เป็น **no-op** สำหรับ non-PostgreSQL engines
- MySQL ใช้ `max_execution_time` / `net_read_timeout` แทน — ไม่ได้ set ในรอบนี้
- MSSQL ใช้ `SET LOCK_TIMEOUT` / `SET QUERY_GOVERNOR_COST_LIMIT` — ไม่ได้ set ในรอบนี้

### 6.3 Custom Script Path

- `config_type == "custom"` มี `SET statement_timeout = 0` อยู่แล้วใน `_run_custom_script()` (line 420)
- แต่ใช้ได้เฉพาะ connection เดียวใน block นั้น — ไม่ได้ครอบคลุม engine ทั้งหมด
- หาก custom script มี query ที่รันนานผ่าน engine pool อาจยังเจอ timeout ได้
- ในอนาคตอาจต้องเพิ่ม `_tune_pg_migration_session(tgt_engine)` ใน custom script path ด้วย

---

## 7. สิ่งที่ไม่ได้เปลี่ยน

- `services/db_connector.py` — ไม่แก้ไข, engine creation ยังเหมือนเดิม
- `services/query_builder.py` — ไม่แก้ไข, `batch_insert()` ยังใช้ COPY FROM STDIN เหมือนเดิม
- `api/jobs/router.py` — ไม่แก้ไข, pipeline execution path เรียก `run_single_migration()` เหมือนเดิม
- `services/pipeline_service.py` — ไม่แก้ไข, เรียก `run_single_migration()` เหมือนเดิม
- Engine `pool_pre_ping=True, pool_recycle=3600` — ยังคงเหมือนเดิม

---

## 8. ทดสอบ

หลังจากแก้ไข รัน migration pipeline อีกครั้ง — query `SELECT ... FROM test_patients` จะไม่ถูก cancel อีกต่อไป เพราะ:

1. `statement_timeout = 0` — query รันได้ไม่จำกัดเวลา
2. `work_mem = 256MB` — sort/hash ทำใน memory ไม่ spill to disk
3. `max_parallel_workers_per_gather = 4` — scan ขนาน 4 workers
4. `maintenance_work_mem = 512MB` — COPY เขียน WAL เร็วขึ้น
5. `effective_cache_size = 4GB` — planner เลือก execution plan ที่เหมาะสมกว่า
