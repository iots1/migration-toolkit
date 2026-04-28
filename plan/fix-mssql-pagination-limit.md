# Fix: MSSQL Pagination — LIMIT Syntax & PK Column Mismatch

**Date**: 2026-04-28  
**Status**: ✅ Fixed  
**Files Changed**:
- `services/query_builder.py`
- `services/migration_executor.py`

---

## Root Cause

### Bug 1 — MSSQL ไม่รองรับ `LIMIT`

Cursor-based pagination ทุก dialect ที่ไม่ใช่ PostgreSQL ใช้ `build_paginated_select_expanded`  
ซึ่งต่อท้าย query ด้วย `LIMIT :batch_size` — syntax ที่ PostgreSQL/MySQL รองรับแต่ **MSSQL ไม่รองรับ**

```sql
-- ❌ Query ที่ถูก generate (ทำให้ error บน MSSQL)
SELECT * FROM (...) AS _paginated_src
ORDER BY "ItemID"
LIMIT %(batch_size)s   -- ← syntax ผิดสำหรับ MSSQL
```

MSSQL ต้องใช้ `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` แทน

---

### Bug 2 — PK Column ไม่มีอยู่ใน Subquery ของ `generate_sql`

เมื่อ config มี `generate_sql` (custom SELECT):

```sql
SELECT Code AS code, Name AS name_thai FROM dbSpecialClinic;
```

ระบบยัง auto-detect PK จาก schema ของตาราง (`ItemID`) แล้วใส่ `ORDER BY "ItemID"` ใน pagination wrapper:

```sql
-- ❌ ORDER BY column ที่ไม่มีใน subquery
SELECT * FROM (
    SELECT Code AS code, Name AS name_thai FROM dbo.dbSpecialClinic
) AS _paginated_src
ORDER BY "ItemID"   -- ← ItemID ไม่มีใน subquery → error
OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY
```

เกิด error: `Invalid column name 'ItemID'`

**Root cause**: `_detect_pk_columns()` inspect schema ของตารางต้นฉบับ ได้ PK ถูกต้อง  
แต่ `generate_sql` ที่ user เขียนเองไม่จำเป็นต้อง SELECT PK column นั้นออกมา  
Cursor pagination จึง wrap ด้วย ORDER BY column ที่ไม่มีใน result set

---

## วิธีแก้

### Fix 1 — เพิ่ม `build_paginated_select_mssql` (`query_builder.py`)

```python
def build_paginated_select_mssql(base_query, pk_columns, last_seen_pk=None, batch_size=1000):
    # ใช้ expanded OR-chain เหมือนกัน แต่เปลี่ยน LIMIT → FETCH NEXT
    ...
    return text(
        f"SELECT * FROM ({base_query}) AS _paginated_src "
        f"{where_clause} "
        f"ORDER BY {order_clause} "
        f"OFFSET 0 ROWS FETCH NEXT :batch_size ROWS ONLY"  # ✅ MSSQL syntax
    ), pk_params
```

`select_pagination_builder()` route แยก dialect:
- `postgresql` → `build_paginated_select` (row-value comparison + LIMIT)
- `mssql` → `build_paginated_select_mssql` (expanded OR + FETCH NEXT)
- others → `build_paginated_select_expanded` (expanded OR + LIMIT)

---

### Fix 2 — Skip cursor pagination เมื่อ `generate_sql` ไม่มี `pk_columns` (`migration_executor.py`)

```python
config_pk = config.get("pk_columns")

# generate_sql ที่ user เขียนเองอาจไม่ SELECT PK ออกมา
# → ใช้ cursor pagination ได้ก็ต่อเมื่อ user ระบุ pk_columns ไว้ใน config เอง
if not config_pk and config.get("generate_sql"):
    pk_columns = None   # → fallback to offset pagination
else:
    pk_columns = config_pk if config_pk else _detect_pk_columns(src_engine, source_table)
```

เมื่อ `pk_columns = None` → ตกไปใช้ `_process_batches_offset()`  
ซึ่งสำหรับ MSSQL ใช้ `ROW_NUMBER() OVER (ORDER BY (SELECT 0))` — ไม่ต้องการ column ใดๆ เป็นพิเศษ

---

## Decision: ทำไมถึง fallback แทนที่จะ inject PK เข้า subquery

| แนวทาง | ข้อดี | ข้อเสีย |
|--------|-------|---------|
| Fallback to offset | ปลอดภัย, ไม่แตะ SQL ของ user | Non-deterministic resume (ถ้า crash กลางทาง) |
| Inject PK into SELECT | Cursor pagination ยังทำงานได้ | ต้อง parse SQL, อาจ conflict กับ alias/CTE |
| ใช้ cursor ตาม schema PK | ไม่ต้องเปลี่ยนอะไร | Fail ถ้า PK ไม่ได้ SELECT ออกมา (Bug นี้) |

**เลือก fallback** เพราะ: user เขียน `generate_sql` เองหมายความว่าต้องการควบคุม query — เราไม่ควรแก้ SQL ของ user โดยไม่ได้รับอนุญาต

---

## วิธีใช้ Cursor Pagination กับ `generate_sql`

ถ้าต้องการ cursor pagination (deterministic resume) บน custom SQL:

1. ต้อง SELECT PK ออกมาใน `generate_sql`
2. ระบุ `pk_columns` ใน config JSON

```json
{
  "generate_sql": "SELECT ItemID, Code AS code, Name AS name_thai FROM dbSpecialClinic",
  "pk_columns": ["ItemID"],
  ...
}
```
