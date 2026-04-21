# ETL Transformer Default Value Pattern

## Overview

**Fallback Chain Pattern** for handling null/empty values in data transformers.

When a transformer encounters `null` or empty string, it follows a **declarative fallback chain** instead of silently returning null. This prevents silent data loss and makes ETL logic explicit and debuggable.

---

## Fallback Chain Logic

```
value is NULL or EMPTY?
    │
    ├─→ TRANSFORMER.default_value exists?
    │   └─→ YES: Use it
    │   └─→ NO: Continue ↓
    │
    ├─→ DEFAULT_VALUE.value exists (global fallback)?
    │   └─→ YES: Use it
    │   └─→ NO: Continue ↓
    │
    └─→ Return NULL
        └─→ If column has NOT NULL constraint → Database Error (fail-safe)
```

---

## Implementation Details

### 1. Transformer-Specific Default Value

Each transformer can declare its own default value in `transformer_params`:

```json
{
  "source": "CreateDate",
  "target": "created_at",
  "transformers": ["BUDDHIST_TO_ISO"],
  "transformer_params": {
    "BUDDHIST_TO_ISO": {
      "default_value": "1970-01-01 00:00:00"
    }
  }
}
```

**When it triggers:**
- Transformer encounters `null` or empty string
- Specific transformer has `default_value` parameter
- Falls back to that value immediately

**Precedence:** Highest (checked first)

---

### 2. Global Default Value Fallback

If transformer doesn't have its own default, it looks for global `DEFAULT_VALUE`:

```json
{
  "source": "BirthDate",
  "target": "birth_date",
  "transformers": ["ENG_DATE_TO_ISO", "DEFAULT_VALUE"],
  "transformer_params": {
    "DEFAULT_VALUE": {
      "value": "1900-01-01"
    }
  }
}
```

**When it triggers:**
- Transformer encounters `null` or empty string
- Transformer has NO `default_value` parameter
- Global `DEFAULT_VALUE.value` exists
- Falls back to that value

**Precedence:** Medium (checked second)

**Multiple transformers can share:**
```json
{
  "transformers": ["BUDDHIST_TO_ISO", "TRIM", "DEFAULT_VALUE"],
  "transformer_params": {
    "DEFAULT_VALUE": {
      "value": "N/A"
    }
  }
}
```

All three transformers will use `"N/A"` for null handling.

---

### 3. No Default Value → Error (Fail-Safe)

If neither fallback exists:

```json
{
  "source": "CriticalDate",
  "target": "critical_date",
  "transformers": ["BUDDHIST_TO_ISO"]
}
```

**Behavior:**
- `null` values pass through as `null`
- If column has `NOT NULL` constraint → **Database error**
- **Fail-safe**: Prevents silent data loss; forces explicit handling

---

## Config Examples

### Example 1: Date with Transformer-Specific Default

```json
{
  "source": "CreateDate",
  "target": "created_at",
  "ignore": false,
  "transformers": ["BUDDHIST_TO_ISO"],
  "transformer_params": {
    "BUDDHIST_TO_ISO": {
      "default_value": "1970-01-01 00:00:00"
    }
  }
}
```

✅ Any `null` in `CreateDate` → becomes `"1970-01-01 00:00:00"`

---

### Example 2: Date with Global Fallback

```json
{
  "source": "BirthDate",
  "target": "birth_date",
  "ignore": false,
  "transformers": ["ENG_DATE_TO_ISO"],
  "transformer_params": {
    "DEFAULT_VALUE": {
      "value": "1900-01-01"
    }
  }
}
```

✅ Any `null` in `BirthDate` → becomes `"1900-01-01"`
✅ Works for any transformer in the mapping

---

### Example 3: Multi-Step Transform with Shared Default

```json
{
  "source": "VisitDate",
  "target": "visit_date",
  "ignore": false,
  "transformers": ["TRIM", "ENG_DATE_TO_ISO", "DEFAULT_VALUE"],
  "transformer_params": {
    "DEFAULT_VALUE": {
      "value": "2000-01-01"
    }
  }
}
```

**Order of operations:**
1. `TRIM` — strip whitespace
2. `ENG_DATE_TO_ISO` — convert to ISO format
3. `DEFAULT_VALUE` — if step 1-2 resulted in null, fill with `"2000-01-01"`

---

### Example 4: Transformer-Specific Override Global Fallback

```json
{
  "source": "ModifyDate",
  "target": "modified_at",
  "transformers": ["BUDDHIST_TO_ISO"],
  "transformer_params": {
    "BUDDHIST_TO_ISO": {
      "default_value": "2020-01-01 12:00:00"
    },
    "DEFAULT_VALUE": {
      "value": "2000-01-01 00:00:00"
    }
  }
}
```

✅ `BUDDHIST_TO_ISO` encounters null → uses `"2020-01-01 12:00:00"` (transformer-specific)
✅ Ignores global `DEFAULT_VALUE` because transformer-specific exists

---

## Implementation in Code

### Transform Flow

**services/transformers.py → `transform_value()`**

```python
if value is None or pd.isna(value):
    # Fallback chain for null values:
    # 1. Check transformer-specific default_value
    if transformer_name in transformer_params and 'default_value' in transformer_params[transformer_name]:
        return transformer_params[transformer_name]['default_value']
    # 2. Fall back to global DEFAULT_VALUE
    if 'DEFAULT_VALUE' in transformer_params:
        return transformer_params['DEFAULT_VALUE'].get('value', None)
    # 3. Return null (may cause DB constraint error)
    return None
```

### Data Transformer Registration

**data_transformers/dates.py**

```python
@register_transformer("BUDDHIST_TO_ISO", ...)
def buddhist_to_iso(series: pd.Series, params=None) -> pd.Series:
    """Convert Thai BE years to ISO"""
    
    def get_default():
        # Same fallback chain
        if 'BUDDHIST_TO_ISO' in params and 'default_value' in params.get('BUDDHIST_TO_ISO', {}):
            return params['BUDDHIST_TO_ISO']['default_value']
        if 'DEFAULT_VALUE' in params:
            return params['DEFAULT_VALUE'].get('value', None)
        return None
    
    def convert_year(date_str):
        if pd.isna(date_str) or date_str == '':
            return get_default()
        # ... conversion logic
```

---

## Benefits

| Benefit | Explanation |
|---------|------------|
| **Declarative** | Config explicitly shows how nulls are handled |
| **Fine-grained** | Can override defaults per transformer |
| **Flexible** | Global fallback for multiple transformers |
| **Fail-safe** | No default → error (prevents silent data loss) |
| **Debuggable** | Clear chain of decision-making |
| **DRY** | Reuse global `DEFAULT_VALUE` across mappings |

---

## Comparison: Before vs After

### ❌ Before (No Fallback)
```python
def transform_value(value, transformer_name):
    if pd.isna(value):
        return None  # Silent null pass-through
    # ... transform logic
```

**Problems:**
- Silent null propagation
- NOT NULL constraint errors are hard to debug
- No way to specify defaults per mapping

---

### ✅ After (Fallback Chain)
```python
if pd.isna(value):
    # Fallback 1: Transformer-specific
    if transformer_name in transformer_params and 'default_value' in ...:
        return transformer_params[transformer_name]['default_value']
    # Fallback 2: Global
    if 'DEFAULT_VALUE' in transformer_params:
        return transformer_params['DEFAULT_VALUE'].get('value', None)
    # Fallback 3: Explicit error
    return None
```

**Improvements:**
- Explicit null handling
- Transformer-specific or global defaults
- Better error diagnostics

---

## Use Cases

### Use Case 1: Historical Data with Missing Dates

**Problem:** Source system has null `created_date` for legacy records

**Solution:**
```json
{
  "source": "created_date",
  "target": "created_at",
  "transformers": ["BUDDHIST_TO_ISO"],
  "transformer_params": {
    "BUDDHIST_TO_ISO": {
      "default_value": "1970-01-01 00:00:00"
    }
  }
}
```

✅ Legacy records get `1970-01-01` instead of breaking the pipeline

---

### Use Case 2: Multiple Date Columns, Single Default

**Problem:** Many date columns, all need same fallback

**Solution:**
```json
[
  {
    "source": "created_date",
    "target": "created_at",
    "transformers": ["BUDDHIST_TO_ISO"]
  },
  {
    "source": "updated_date",
    "target": "updated_at",
    "transformers": ["BUDDHIST_TO_ISO"]
  },
  {
    "source": "deleted_date",
    "target": "deleted_at",
    "transformers": ["BUDDHIST_TO_ISO"]
  }
]
```

With shared `transformer_params`:
```json
"transformer_params": {
  "DEFAULT_VALUE": {
    "value": "1970-01-01 00:00:00"
  }
}
```

✅ All three dates share same default, DRY principle

---

### Use Case 3: Different Defaults per Transformer

**Problem:** Date and birth year have different fallback requirements

**Solution:**
```json
[
  {
    "source": "created_date",
    "target": "created_at",
    "transformers": ["BUDDHIST_TO_ISO"],
    "transformer_params": {
      "BUDDHIST_TO_ISO": {
        "default_value": "1970-01-01"
      }
    }
  },
  {
    "source": "birth_year",
    "target": "birth_year_iso",
    "transformers": ["BUDDHIST_TO_ISO"],
    "transformer_params": {
      "BUDDHIST_TO_ISO": {
        "default_value": "1900-01-01"
      }
    }
  }
]
```

✅ Each mapping has its own default strategy

---

## Migration Checklist

- [x] Implement fallback chain in `services/transformers.py`
- [x] Implement fallback chain in registered transformers (`data_transformers/dates.py`)
- [x] Add `DEFAULT_VALUE` transformer in `data_transformers/text.py`
- [x] Update config schema docs
- [ ] Update migration executor docs
- [ ] Add integration tests for fallback chain
- [ ] Update user-facing error messages

---

## See Also

- `services/transformers.py` — `transform_value()` implementation
- `data_transformers/dates.py` — `BUDDHIST_TO_ISO` with fallback
- `data_transformers/text.py` — `DEFAULT_VALUE` transformer
- Migration config schema in API docs

---

**Last Updated:** 2026-04-22  
**Pattern:** Declarative Fallback Chain  
**Status:** ✅ Implemented
