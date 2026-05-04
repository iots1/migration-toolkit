# Fix: ข้อมูลไม่เข้า target เมื่อใช้ generate_sql + TRIM ไม่ทำงาน

**Date**: 2026-05-05
**Severity**: CRITICAL — ข้อมูลไม่เข้า target เลย (0 rows) แต่ log รายงานสำเร็จ + ข้อมูลที่เข้าแล้ว query ไม่เจอเพราะ trailing spaces
**Affected**: migration ทุก config ที่ใช้ `generate_sql` ร่วมกับ ignored mappings ที่มี target name ซ้ำกับ active columns

---

## Bug #1: transform_batch ทำลาย columns ทั้งหมด (0 rows inserted)

### ลำดับเหตุการณ์

```
generate_sql alias columns เป็น target names
    ↓
DataFrame columns = ['old_hn', 'national_id', 'first_name', ...]  (target names)
    ↓
transform_batch() → rename step ไม่ทำอะไร (เพราะ src == tgt อยู่แล้ว)
    ↓
drop ignored step: รวบรวมทุก m["target"] ที่ ignore=true
    → ได้ ['title', 'old_hn', 'national_id', 'first_name', 'last_name', ...]
    ↓
df.drop(columns=ignored)
    → ทุก column ถูก drop เพราะ target names ซ้ำกับ ignored targets
    ↓
DataFrame = (100 rows, 0 columns)
    ↓
batch_insert() → df.empty check ผ่าน (มี rows) แต่ COPY ไม่มี columns → return 0
    ↓
Log รายงาน: "✅ Batch inserted 100 rows" (เป็นการนับจาก source read ไม่ใช่ insert จริง)
    ↓
Target table = 0 rows
```

### สาเหตุเชิงลึก

Config `cnPatient_patients_config` มี mappings 2 ประเภท:

**Active mappings** (source → target, ignore=false):
```json
{"source":"HospitalNumber","target":"old_hn","ignore":false}
{"source":"IdCardNo","target":"national_id","ignore":false}
{"source":"FirstName","target":"first_name","ignore":false}
```

**Ignored identity mappings** (target → target, ignore=true) — มีไว้บอกว่า "ถ้ามี column นี้อยู่แล้วใน source ไม่ต้องแตะ":
```json
{"source":"old_hn","target":"old_hn","ignore":true}
{"source":"national_id","target":"national_id","ignore":true}
{"source":"first_name","target":"first_name","ignore":true}
```

**ปัญหา**: `generate_sql` ใช้ alias `HospitalNumber AS old_hn` → column ชื่อ `old_hn` มาจาก active mapping แต่ ignored mapping `{"source":"old_hn","target":"old_hn","ignore":true}` ก็ match `old_hn` → โดน drop

Code เดิม (`query_builder.py:233-234`):
```python
ignored = [m["target"] for m in config.get("mappings", []) if m.get("ignore", False)]
df = df.drop(columns=[c for c in ignored if c in df.columns], errors="ignore")
```

drop ทุก column ที่ชื่อตรงกับ ignored target **โดยไม่ตรวจสอบ** ว่า column นั้นกำลังถูก active mapping ใช้อยู่หรือไม่

### วิธีแก้ไข: `services/query_builder.py`

เปลี่ยน logic drop ignored จาก 1 step เป็น 2 steps:

**ก่อน (บั๊ก)**:
```python
if rename_map:
    df.rename(columns=rename_map, inplace=True)

ignored = [m["target"] for m in config.get("mappings", []) if m.get("ignore", False)]
df = df.drop(columns=[c for c in ignored if c in df.columns], errors="ignore")
```

**หลัง (แก้แล้ว)**:
```python
# Step 1: Drop ignored source columns ก่อน rename (กรณี src != tgt)
ignored_sources = [
    m["source"] for m in config.get("mappings", [])
    if m.get("ignore", False) and m.get("source") in df.columns and m["source"] != m.get("target")
]
df = df.drop(columns=ignored_sources, errors="ignore")

if rename_map:
    df.rename(columns=rename_map, inplace=True)

# Step 2: Drop ignored target columns หลัง rename — เฉพาะที่ไม่ได้ถูก active mapping ใช้
active_in_df = {
    m["target"] for m in config.get("mappings", [])
    if not m.get("ignore", False) and m.get("target") and m["target"] in df.columns
}
ignored_targets = [
    m["target"] for m in config.get("mappings", [])
    if m.get("ignore", False) and m.get("target") in df.columns and m["target"] not in active_in_df
]
df = df.drop(columns=ignored_targets, errors="ignore")
```

### Logic การแก้

| Step | ทำอะไร | ทำไม |
|------|--------|------|
| 1 | Drop source columns ที่ ignore (เมื่อ src != tgt) | ลบ column ต้นทางที่ไม่ต้องการก่อน rename |
| 2 | Rename source → target | แปลงชื่อ columns |
| 3 | Drop target columns ที่ ignore **เฉพาะที่ไม่ active** | ป้องกันไม่ให้ drop column ที่กำลังถูก active mapping ใช้ |

---

## Bug #2: Transformer (TRIM) ไม่ทำงานทำให้ query ไม่เจอข้อมูล

### อาการ

```sql
-- Source (MSSQL): เจอ
SELECT * FROM dbo.cnPatientDudeeV1 WHERE HospitalNumber = '660008276';

-- Target (PostgreSQL): ไม่เจอ (หลัง fix Bug #1 แล้ว)
SELECT * FROM public.test_patients WHERE old_hn = '660008276';
```

### สาเหตุ

Source column `HospitalNumber` เป็น `CHAR(13)` → มี trailing spaces: `'660008276    '` (13 chars)

Config มี `"transformers":["TRIM"]` บน mapping `HospitalNumber → old_hn` แต่:

1. `generate_sql` alias `HospitalNumber AS old_hn` → DataFrame column = `old_hn`
2. `DataTransformer.apply_transformers_to_batch()` ที่ `transformers.py:50-52`:
   ```python
   if source_col not in available_cols:
       continue
   ```
   `source_col = "HospitalNumber"` ไม่เจอใน DataFrame (มีแค่ `old_hn`) → **skip TRIM** → trailing spaces ติดมาด้วย

ผลลัพธ์: `old_hn = '660008276    '` → `WHERE old_hn = '660008276'` ไม่ match

### วิธีแก้ไข: `services/transformers.py`

**ก่อน (บั๊ก)**:
```python
# Skip if source column doesn't exist
if source_col not in available_cols:
    continue
```

**หลัง (แก้แล้ว)**:
```python
# Skip if source column doesn't exist
# When generate_sql aliases columns to target names, source_col
# won't be found but target_col may already be present.
if source_col not in available_cols:
    if target_col in available_cols and not transformers:
        continue
    elif target_col in available_cols:
        source_col = target_col
    else:
        continue
```

**Logic**: เมื่อ `source_col` ไม่เจอ แต่ `target_col` มีอยู่ (จาก `generate_sql` alias) → fallback ใช้ `target_col` เป็นตัวรัน transformer (TRIM, BIT_CAST ฯลฯ)

### ผลลัพธ์หลังแก้

```
old_hn='660008276', len=9    ← TRIM ทำงานแล้ว (เดิม len=13)
Query old_hn=660008276: 1 row ← เจอข้อมูลแล้ว
```

---

## สรุปไฟล์ที่แก้ไข

| ไฟล์ | บรรทัด | Bug | การแก้ |
|------|--------|-----|--------|
| `services/query_builder.py` | 228-247 | #1 Drop ignored ทำลาย active columns | แบ่งเป็น 2 steps: drop source ก่อน rename, drop target เฉพาะที่ไม่ active หลัง rename |
| `services/transformers.py` | 50-57 | #2 Transformer ข้ามเมื่อ source_col ไม่เจอแต่ target_col มีอยู่ | Fallback ใช้ target_col เมื่อ source_col ไม่เจอ |

---

## ผลกระทบ

### Scenarios ที่ได้รับผลกระทบ

| Scenario | ก่อน fix | หลัง fix |
|----------|---------|---------|
| `generate_sql` + ignored identity mappings | **ทุก column ถูก drop → 0 rows** | เฉพาะ ignored columns ที่ไม่ active ถูก drop |
| `generate_sql` + transformers (TRIM, BIT_CAST) | **Transformer ไม่ทำงาน → trailing spaces / ค่าผิด** | Transformer ทำงานบน target column ได้ |
| Dynamic SELECT (ไม่มี `generate_sql`) | ทำงานปกติ | ยังทำงานปกติ |
| `generate_sql` ไม่มี ignored mappings | ทำงานปกติ | ยังทำงานปกติ |

### Config ที่ได้รับผลกระทบ

Config ที่มีทั้ง 3 เงื่อนไขพร้อมกันจะได้รับผล:
1. มี `generate_sql` ที่ alias columns เป็น target names
2. มี active mappings (source → target, ignore=false)
3. มี ignored identity mappings (target → target, ignore=true) ที่ target name ซ้ำกับ active

---

## การทดสอบ

### Manual test ที่ผ่าน

```bash
# Bug #1: generate_sql → transform_batch → batch_insert → verify rows in PostgreSQL
# ผล: 10 rows inserted successfully, data visible in target (ก่อน fix = 0 rows)

# Bug #2: TRIM transformer → query exact match
# ผล: old_hn='660008276' (len=9) → WHERE old_hn = '660008276' เจอ 1 row
```

### Automated tests ที่ผ่าน

```
tests/test_query_builder.py — 10/10 passed
```

---

## คำแนะนำเพิ่มเติม

1. **เพิ่ม `pk_columns` ใน config** — เช่น `["HospitalNumber"]` เพื่อใช้ cursor-based pagination แทน OFFSET (ลด risk ของ data duplication/skipping)
2. **ลบ ignored identity mappings ที่ซ้ำซ้อน** — เช่น `{"source":"old_hn","target":"old_hn","ignore":true}` ไม่จำเป็นถ้า `generate_sql` ควบคุม columns อยู่แล้ว
3. **เพิ่ม guard ใน `batch_insert`** — ควร log warning เมื่อ df มี 0 columns แต่มี rows (เป็น signal ของ bug)
4. **เพิ่ม TRIM ใน `generate_sql` โดยตรง** — เช่น `TRIM(HospitalNumber) AS old_hn` เพื่อ double safety
