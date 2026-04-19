# Feature: Use `generate_sql` in Migration Engine

**Status:** Planning
**Branch:** `feature/postgresql`
**Scope:** Single file change — `services/migration_executor.py`

---

## Background

The `configs` table stores a `generate_sql` column containing a hand-crafted `SELECT`
(with custom `JOIN`s, `WHERE`, and column aliasing). Currently the Migration Engine
ignores this field and always builds a dynamic `SELECT` from the `mappings` array.

This feature makes the engine **use `generate_sql` when present**, falling back to
the dynamic query when the field is empty.

### Example config record
```
config_name : cnPatient_patients_config
generate_sql: SELECT TOP 1000
                  HospitalNumber AS old_hn,
                  BirthDate AS birth_date,
                  ...
              FROM cnPatientDudeeV1
              LEFT JOIN dbo.dbReligion ON religion = dbReligion.CODE
              WHERE Religion <> 1
condition   : Religion <> 1
lookup      : LEFT JOIN dbReligion ON religion = dbReligion.CODE
```

---

## Root Cause Analysis

### Current ETL flow

```
build_select_query(config)
  → "SELECT HospitalNumber, IdCardNo, BirthDate, ... FROM cnPatientDudeeV1"
  → DataFrame columns = SOURCE names (HospitalNumber, BirthDate, ...)

transform_batch(df, config)
  → looks up mapping["source"] = "HospitalNumber"  ✅ found
  → applies BUDDHIST_TO_ISO to "BirthDate"          ✅ found
  → renames source → target

batch_insert(df, target_table)
  → inserts with TARGET column names
```

### Problem with naive generate_sql substitution

`generate_sql` aliases columns to **target names** (`HospitalNumber AS old_hn`).
After `pd.read_sql(generate_sql)`, DataFrame columns = `old_hn`, `birth_date`, ...
`transform_batch` then looks for `mapping["source"]` = `HospitalNumber` → **not found**.

### Solution: remap `source = target` for active mappings

Since the SQL already aliased columns to target names, patch the in-memory config
so each active mapping has `source = target`. `transform_batch` then finds columns
by their aliased name. The rename step becomes a no-op. Nothing else changes.

---

## Phase 1 — Core Logic

**File:** `services/migration_executor.py`
**Function:** `run_single_migration()`
**Location:** just before `build_select_query` is called (~line 88)

### Change

```python
# ── BEFORE ──────────────────────────────────────────
select_query = build_select_query(config, source_table, src_db_type)
log(f"SELECT Query: {select_query}", "🔍")

# ── AFTER ────────────────────────────────────────────
generate_sql = (config.get("generate_sql") or "").strip()
if generate_sql:
    select_query = generate_sql
    log("Using pre-built generate_sql (custom SQL with JOIN/WHERE)", "📋")
    # generate_sql already aliases columns to target names.
    # Remap active mappings so transform_batch can locate them by target name.
    remapped = [
        {**m, "source": m["target"]}
        for m in config.get("mappings", [])
        if not m.get("ignore", False) and m.get("target")
    ]
    config = {**config, "mappings": remapped}
else:
    select_query = build_select_query(config, source_table, src_db_type)
log(f"SELECT Query: {select_query}", "🔍")
```

### Why this is safe

| Concern | Answer |
|---|---|
| Configs without `generate_sql` | `empty string` → falls through to `build_select_query` as before |
| Transformers (BUDDHIST_TO_ISO, etc.) | Still run — `transform_batch` finds `birth_date` via remapped `source = target` |
| `config` mutation | `{**config, ...}` creates a shallow copy — original dict is not modified |
| Ignored mappings | Excluded from `remapped` — they won't be in the DataFrame from `generate_sql` |
| Rename step in `transform_batch` | Becomes no-op (source == target) — no error, minimal overhead |

---

## Phase 2 — ETL Flow After Change

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP START                                                      │
│  config = get_content("cnPatient_patients_config")             │
│  → generate_sql = "SELECT TOP 1000 HospitalNumber AS old_hn…" │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 1 — QUERY DECISION  (new)                                 │
│                                                                 │
│  generate_sql present?                                          │
│  ├─ YES → select_query = generate_sql                          │
│  │         remap mappings: source = target                     │
│  └─ NO  → select_query = build_select_query(...)  (unchanged)  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 2 — FETCH  (unchanged)                                    │
│  pd.read_sql(select_query, src_engine, chunksize=batch_size)   │
│                                                                 │
│  Without generate_sql → columns: HospitalNumber, BirthDate … │
│  With    generate_sql → columns: old_hn, birth_date …         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ (per-batch loop)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 3 — TRANSFORM  (unchanged function)                       │
│  transform_batch(df, config)                                    │
│                                                                 │
│  apply BUDDHIST_TO_ISO → "birth_date"  ✅ found via remap      │
│  apply BIT_CAST        → "is_death"    ✅ found via remap      │
│  apply MAP_GENDER      → "gender"      ✅ found via remap      │
│  rename source→target  → no-op        ✅ (source == target)    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 4 — INSERT  (unchanged)                                   │
│  batch_insert(df, "accidents", tgt_engine)                     │
│  → df.to_sql("accidents", ...)                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 3 — Performance Notes

No regression. Potential improvement:

| Metric | Without generate_sql | With generate_sql |
|---|---|---|
| JOIN filtering | None — all rows fetched | DB-level — fewer rows |
| WHERE filtering | None | DB-level — fewer rows |
| Python transform overhead | Full source dataset | Only matching rows |
| Code path overhead | — | Dict lookup + list comprehension **once** per step |

Using `generate_sql` with WHERE/JOIN can **reduce data volume** fetched into Python,
making the overall migration faster for large source tables.

---

## Phase 4 — Verification Checklist

```
[ ] Config with generate_sql set:
    - Log shows "Using pre-built generate_sql" (not the dynamic SELECT)
    - Transformers (BUDDHIST_TO_ISO, MAP_GENDER, BIT_CAST) apply correctly
    - Data inserts into target table with correct column mapping

[ ] Config without generate_sql (empty / null):
    - Log shows the dynamic SELECT unchanged
    - Behaviour identical to before this change

[ ] Edge cases:
    - generate_sql with only whitespace → treated as empty, falls back to dynamic
    - Config where all mappings are ignored → remapped = [] → empty insert (same as before)
```

---

## Files Modified

| File | Change |
|---|---|
| `services/migration_executor.py` | **~6 lines added** around `build_select_query` call |

## Files NOT Modified

| File | Reason |
|---|---|
| `services/query_builder.py` | `build_select_query` still used for configs without `generate_sql` |
| `repositories/config_repo.py` | `get_content()` already returns `generate_sql` key |
| `services/pipeline_service.py` | Config loading unchanged |
| `models/migration_config.py` | `ConfigRecord.generate_sql` already exists |
