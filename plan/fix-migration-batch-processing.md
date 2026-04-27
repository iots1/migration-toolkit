# Fix: Migration Batch Processing — Semicolon Bug & Simplification

> **Date**: 2026-04-27
> **Status**: Applied (staged, pending commit)
> **Files**: `services/migration_executor.py`, `services/checkpoint_manager.py`

---

## Bug: Trailing Semicolon in `generate_sql` Causes Subquery Syntax Error

### Symptom

```
Source read error: (psycopg2.errors.SyntaxError) syntax error at or near ";"
LINE 15: FROM test_patients;) AS _offset_src ORDER BY ctid LIMIT 1000...
```

Migration fails when a config has `generate_sql` ending with `;`.

### Root Cause

`_prepare_select_query()` returned `generate_sql` verbatim. The OFFSET fallback
(`_process_batches_offset`) wrapped it as a subquery:

```sql
SELECT *, ctid FROM (
    SELECT ... FROM test_patients;   -- ← semicolon breaks subquery syntax
) AS _offset_src ORDER BY ctid LIMIT 1000 OFFSET 0
```

PostgreSQL does not allow `;` inside a subquery expression.

### Fix

Strip trailing semicolons before returning:

```python
# services/migration_executor.py — _prepare_select_query()
return generate_sql.rstrip(";"), config
```

---

## Simplification: Removed OFFSET Fallback in `_process_batches()`

### Before

`_process_batches()` had two code paths:

1. **Cursor-based pagination** (PK detected) → `build_paginated_select()` with `WHERE pk > :last_pk`
2. **OFFSET fallback** (no PK) → `_process_batches_offset()` wrapping query with `ctid` / `ROW_NUMBER()`

**Problems**:
- OFFSET fallback was complex (separate function, dialect-specific SQL wrapping, ctid handling)
- The `;` bug was hard to diagnose — wrapping happened in a different function from where the query was prepared
- `_process_batches_offset` generated dynamic SQL (`SELECT *, ctid FROM (...) AS _offset_src`) that was fragile

### After

Simplified to a single path using `pd.read_sql(chunksize=N)`:

```python
def _process_batches(...):
    is_pg = "postgresql" in str(src_engine.url)
    if is_pg:
        src_read_target = src_engine.connect()
        src_read_target.execute(text("SET statement_timeout = 0"))
        src_read_target.commit()
    else:
        src_read_target = src_engine

    try:
        data_iterator = pd.read_sql(
            select_query, src_read_target, chunksize=batch_size, coerce_float=False
        )
        for df_batch in data_iterator:
            # ... process batch ...
    finally:
        if is_pg:
            src_read_target.close()
```

### Why This Works

`pd.read_sql(chunksize=N)` uses a server-side cursor on PostgreSQL, which fetches
rows in chunks without OFFSET. This is simpler, equally efficient, and avoids the
SQL wrapping that caused the bug.

### Trade-off

Cursor-based pagination (`WHERE pk > :last_pk`) is more efficient for resume
(O(1) vs repositioning server-side cursor). Can be re-introduced from the
[resilient migration plan](resilient-long-running-migration.md) Phase 2 if needed.

---

## Changes Summary

| File | Change |
|------|--------|
| `services/migration_executor.py:630` | `return generate_sql` → `return generate_sql.rstrip(";")` |
| `services/migration_executor.py` `_process_batches()` | Replaced cursor + OFFSET dual-path with `pd.read_sql(chunksize=N)` |
| `services/migration_executor.py` `_process_batches_offset()` | Removed (no longer needed) |
