# Feature: Pipeline Resume — กู้คืนเมื่อ Background Process ดับกลางทาง

**Date**: 2026-04-21  
**Files Changed**: `services/pipeline_service.py`, `api/jobs/service.py`, `api/jobs/schemas.py`  
**Problem**: Background process ดับ (OOM, SIGKILL, server restart) → migration ไม่ทำต่อ, ต้องเริ่มใหม่ทั้งหมด

---

## 1. ปัญหา

เมื่อ background process ดับกลางทาง (เช่น OOM kill, server restart, network drop) เกิดปัญหา 4 อย่าง:

| # | ปัญหา | ผลกระทบ |
|---|---|---|
| 1 | `_background_run.finally` ลบ checkpoint **เสมอ** แม้ตอน fail | Resume ไม่ได้ — ข้อมูลที่บันทึกไว้หาย |
| 2 | Resume แล้ว `truncate_target=True` ลบข้อมูลที่ insert ไปแล้ว | Data loss — ต้องเริ่มต้นใหม่จริง ๆ |
| 3 | ไม่มี Resume API | ต้อง POST /jobs ใหม่จาก scratch |
| 4 | Job status ติด "running" ตลอดกาล | สร้าง job ใหม่ไม่ได้เพราะ guard block (409 Conflict) |

### ตัวอย่างสถานการณ์

```
Pipeline: patient_migration (5 steps)
  Step 1: config_patients     → 500,000 rows ✅ completed
  Step 2: config_visits       → 1,200,000 rows → batch 450/1200 → 💀 process died
  
  Step 3-5: ยังไม่ได้ทำ
```

**ก่อนแก้**: ต้องเริ่มใหม่ทั้ง 5 steps, checkpoint ถูกลบใน `finally`
**หลังแก้**: Resume → Step 1 ข้าม (completed), Step 2 ทำต่อจาก batch 450, Steps 3-5 ทำต่อ

---

## 2. Architecture — Resume Flow

```
┌─────────────────────────────────────────────────────┐
│  Process crashes at Step 2 batch 450                │
│                                                     │
│  checkpoint_manager saves:                          │
│  {                                                  │
│    "config_patients": {                             │
│      "status": "completed", "last_batch": -1,       │
│      "rows_processed": 500000                       │
│    },                                               │
│    "config_visits": {                               │
│      "status": "running", "last_batch": 450,        │
│      "rows_processed": 450000                       │
│    }                                                │
│  }                                                  │
│                                                     │
│  Job status in DB: "running" (stale)               │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│  User calls POST /api/v1/jobs { resume: true }      │
│                                                     │
│  1. Detect stale "running" job → mark as "failed"   │
│  2. Verify checkpoint exists (else 404)             │
│  3. Create new job record                           │
│  4. start_background() → execute()                  │
│                                                     │
│  execute() reads checkpoint:                        │
│    config_patients → status=completed → SKIP ✅     │
│    config_visits   → status=running                 │
│                   → skip_batches=450                │
│                   → truncate_target=False (!)        │
│                   → RESUME from batch 451 🚀        │
│    config_xxx      → no checkpoint → START FRESH    │
└─────────────────────────────────────────────────────┘
```

---

## 3. สิ่งที่เปลี่ยนแปลง

### 3.1 `services/pipeline_service.py`

#### Fix A: `_background_run()` — เก็บ checkpoint ตอน fail

```python
# Before (BUG): always deletes checkpoint
finally:
    clear_pipeline_checkpoint(self._pipeline.name)

# After (FIX): only clear on success
finally:
    if result is not None and result.status == "completed":
        clear_pipeline_checkpoint(self._pipeline.name)
    else:
        # Checkpoint preserved — log for debugging
        cp = load_pipeline_checkpoint(self._pipeline.name)
        if cp:
            print(f"[JOB] checkpoint preserved ({len(cp['steps'])} steps)")
```

**ทำไม**: `result` เป็น `None` เฉพาะเมื่อ `execute()` พังใน `except` block (unhandled exception) ถ้า `execute()` return ปกติ `result.status` จะเป็น `"completed"`, `"partial"`, หรือ `"failed"` — ทั้งสองอย่างหลังต้องเก็บ checkpoint

#### Fix B: `execute()` — ไม่ truncate เมื่อ resume

```python
# Before (BUG): always truncates based on pipeline config
truncate_target=self._pipeline.truncate_targets,

# After (FIX): skip truncate for running steps
step_state = steps_state.get(config_name, {})
is_resuming = step_state.get("status") == "running"
should_truncate = self._pipeline.truncate_targets and not is_resuming

run_single_migration(
    ...,
    truncate_target=should_truncate,
    skip_batches=step_state.get("last_batch", 0),
)
```

**ทำไม**: ถ้า step กำลังทำงาน (status="running") แปลว่า target table มีข้อมูลบางส่วนแล้ว ถ้า truncate จะเสียข้อมูลที่ insert ไปแล้ว

### 3.2 `api/jobs/service.py`

#### Fix C: Resume mode + stale job detection

```python
def create(self, data: dict) -> dict:
    resume_mode = data.get("resume", False)
    
    # ... load pipeline ...
    
    # Stale running job guard
    if recent and recent[0]["status"] == "running":
        if resume_mode:
            self._mark_stale_job_failed(uuid.UUID(recent[0]["id"]))
        else:
            raise HTTPException(409, "Use resume=true to force resume.")
    
    # Verify checkpoint exists when resuming
    if resume_mode:
        checkpoint = load_pipeline_checkpoint(pc.name)
        if not checkpoint:
            raise HTTPException(404, "No checkpoint found.")
    
    # ... create job + start background ...
```

#### New: `_mark_stale_job_failed()`

```python
def _mark_stale_job_failed(self, job_id: uuid.UUID) -> None:
    """Mark a stale 'running' job as 'failed' so a new run can start."""
    job_repo.update(job_id, JobUpdateRecord(
        status="failed",
        error_message="Marked stale — process died or was restarted",
    ))
```

### 3.3 `api/jobs/schemas.py`

```python
class CreateJobSchema(BaseModel):
    pipeline_id: str = Field(..., description="UUID of the pipeline to execute")
    resume: bool = Field(
        False,
        description="Resume from last checkpoint instead of starting fresh.",
    )
```

---

## 4. API Usage

### เริ่ม pipeline ใหม่ (เหมือนเดิม)

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"pipeline_id": "abc-123-def"}'
```

### Resume pipeline ที่ดับกลางทาง

```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"pipeline_id": "abc-123-def", "resume": true}'
```

**Response (เหมือนกันทั้งสองกรณี):**
```json
{
  "job_id": "new-job-uuid",
  "run_id": "new-run-uuid",
  "pipeline_id": "abc-123-def",
  "status": "running"
}
```

**Error cases:**

| Status | เงื่อนไข | Message |
|---|---|---|
| `409` | มี job กำลังรันอยู่ + ไม่ได้ส่ง `resume=true` | "A job is already running. Use resume=true to force resume." |
| `404` | `resume=true` แต่ไม่มี checkpoint | "No checkpoint found. Cannot resume." |

---

## 5. Checkpoint Data Structure

ไฟล์: `migration_checkpoints/pipeline_<name>.json`

```json
{
  "pipeline_name": "patient_migration",
  "steps": {
    "config_patients": {
      "status": "completed",
      "last_batch": -1,
      "rows_processed": 500000
    },
    "config_visits": {
      "status": "running",
      "last_batch": 450,
      "rows_processed": 450000
    },
    "config_diagnoses": {
      "status": "pending",
      "last_batch": 0,
      "rows_processed": 0
    }
  },
  "timestamp": "2026-04-21T14:30:00"
}
```

### Resume logic ต่อ step status:

| Step Status | Action | Truncate? | Skip Batches |
|---|---|---|---|
| `completed` | SKIP ทั้ง step | N/A | N/A |
| `running` | RESUME จาก `last_batch` | **No** | `last_batch` |
| ไม่มีใน checkpoint | START FRESH | ตาม pipeline config | 0 |

---

## 6. Lifecycle — สิ่งที่เกิดขึ้นเมื่อ process ดับ

### Scenario A: Process crash (OOM, SIGKILL)

```
1. Process dies immediately — no finally, no cleanup
2. Checkpoint file stays on disk ✅
3. Job status in DB = "running" (stale)
4. User calls POST /jobs { resume: true }
5. _mark_stale_job_failed() → old job → "failed"
6. New job created → execute() reads checkpoint
7. Completed steps skipped, running step resumed
```

### Scenario B: Step fails (error/exception)

```
1. execute() catches error, returns PipelineResult(status="partial"|"failed")
2. _background_run.finally: result.status != "completed" → checkpoint KEPT ✅
3. Job status updated to "partial" or "failed"
4. User calls POST /jobs { resume: true }
5. No stale running guard (status != "running") → proceed normally
6. Checkpoint exists → execute() reads it → resumes
```

### Scenario C: All steps complete

```
1. execute() returns PipelineResult(status="completed")
2. _background_run.finally: result.status == "completed" → checkpoint DELETED ✅
3. Job status updated to "completed"
4. Next POST /jobs starts fresh (no checkpoint to resume from)
```

---

## 7. ข้อจำกัดและสิ่งที่ควรรู้

### 7.1 `skip_batches` อ่าน rows ซ้ำ

Resume ใช้ `skip_batches` ซึ่ง iterate ผ่าน rows ที่อ่านไปแล้ว (skip โดยไม่ insert):
```python
for df_batch in data_iterator:
    batch_num += 1
    if batch_num <= skip_batches:
        total_rows += rows_in_batch
        continue  # skip already-processed batches
```

สำหรับ table ขนาดใหญ่ (ล้าน rows) การ re-read อาจใช้เวลานาน เพราะ PostgreSQL server-side cursor ต้อง scan ผ่าน rows ทั้งหมดจนถึง offset

**Future optimization**: ใช้ keyset pagination (WHERE pk > last_pk) แทน offset-based skip

### 7.2 Checkpoint เป็น file-based

Checkpoint เก็บเป็น JSON file ใน `migration_checkpoints/` directory:
- ถ้า container/pod restart และ volume ไม่ persistent → checkpoint หาย
- แนะนำ mount volume ที่ `migration_checkpoints/` สำหรับ production

### 7.3 Single-instance assumption

ระบบนี้ออกแบบสำหรับ **single process** — ไม่รองรับหลาย worker resume pipeline เดียวกันพร้อมกัน ถ้าต้องการ distributed execution ต้องเพิ่ม distributed lock (เช่น pg_advisory_lock)

### 7.4 `truncate_targets` กับ resume

เมื่อ `resume=true`:
- Steps ที่ **กำลังทำ** (status="running") → **ไม่ truncate** แม้ pipeline config จะตั้ง truncate_targets=True
- Steps ที่ **ยังไม่เริ่ม** (ไม่มีใน checkpoint) → truncate ตามปกติ
- Steps ที่ **เสร็จแล้ว** (status="completed") → skip ทั้ง step
