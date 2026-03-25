# CODE_OF_CONDUCT.md

This document establishes strict MVC (Model-View-Controller) architectural conventions for the HIS Migration Toolkit. **All code contributions must adhere to these rules.** This is not optional — it ensures consistency, testability, and maintainability.

---

## Quick Reference: The Three Layers

| Layer | Location | What It Does | What It CANNOT Do |
|-------|----------|--------------|------------------|
| **Model** | `models/`, `services/` | Pure Python: data structures, business logic, DB queries | ❌ NO `import streamlit` |
| **View** | `views/`, `views/components/` | Streamlit rendering ONLY: `st.button`, `st.text_input`, etc. | ❌ NO `import database`, `import services`, direct `st.session_state` manipulation |
| **Controller** | `controllers/` | Orchestrate: init state, fetch data, define callbacks, call view | ✅ CAN import models/services/database, CAN manipulate `st.session_state` |

---

## Layer 1: Models & Services (Pure Python)

### Rule 1.1: NO Streamlit Imports
**Files in `models/` and `services/` MUST NEVER import `streamlit`.**

```python
# ❌ WRONG
import streamlit as st
from services.transformers import DataTransformer

# ✅ CORRECT
from services.transformers import DataTransformer
```

### Rule 1.2: Services Are Pure Functions & Classes
Services handle business logic without side effects. All I/O (database, APIs) is explicit.

```python
# ✅ GOOD
class DataTransformer:
    def apply_trim(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.apply(lambda x: x.str.strip() if isinstance(x, str) else x)

# ❌ BAD (no side effects, but this pollutes the service layer)
def fetch_and_display_results():
    results = db.query(...)
    st.dataframe(results)  # Rendering code in service layer
```

### Rule 1.3: Models Are Dataclasses With Optional Conversion Methods

```python
# ✅ GOOD
@dataclass
class MigrationConfig:
    config_name: str
    source_database: str
    mappings: list[MappingItem] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "MigrationConfig":
        return cls(
            config_name=d.get("config_name"),
            source_database=d.get("source_database"),
            mappings=[MappingItem.from_dict(m) for m in d.get("mappings", [])]
        )

    def to_dict(self) -> dict:
        return {
            "config_name": self.config_name,
            "source_database": self.source_database,
            "mappings": [m.to_dict() for m in self.mappings]
        }
```

### Rule 1.4: Database Access Is Handled by `database.py` or Repository Services

The controller imports from `database.py` — services do NOT call database directly (except `services/datasource_repository.py` which is a query helper).

```python
# ✅ CONTROLLER imports database
import database as db
datasources = db.get_datasources()

# ❌ SERVICE should NOT import database
# services/my_service.py
import database as db  # DON'T DO THIS
```

---

## Layer 2: Views (Dumb Rendering)

### Rule 2.1: Views Are Pure Streamlit Renderers
Views MUST ONLY contain `st.*` calls. No business logic, no database imports.

```python
# ✅ GOOD
def render_settings_page(datasources_df, form_state: dict, callbacks: dict) -> None:
    st.subheader("Settings")
    if st.button("Save", type="primary"):
        callbacks["on_save"]()

# ❌ BAD (business logic in view)
def render_settings_page():
    datasources = db.get_datasources()  # Business logic
    if len(datasources) > 10:  # Decision logic
        st.warning("Too many datasources")
```

### Rule 2.2: Views Receive All Data as Arguments
Views must be 100% determined by their arguments. No global state, no assumptions.

```python
# ✅ GOOD
def render_datasource_tab(datasources_df, form_state: dict, callbacks: dict) -> None:
    is_edit_mode = form_state["is_edit_mode"]
    if is_edit_mode:
        st.write("Editing mode")

# ❌ BAD (accessing global state)
def render_datasource_tab():
    if st.session_state.is_edit_mode:  # View shouldn't access session_state directly
        st.write("Editing mode")
```

### Rule 2.3: Views Accept Callbacks for All Actions
Button clicks, form submissions, etc. delegate to callbacks provided by the controller.

```python
# ✅ GOOD
if st.button("Save Changes", type="primary", use_container_width=True):
    if form_name and form_host:
        ok, msg = callbacks["on_update"](form_id, form_name, form_host, ...)
        if not ok:
            st.error(msg)

# ❌ BAD (view doing the database call)
if st.button("Save Changes", type="primary"):
    db.update_datasource(...)  # NO! Controller should do this
    st.success("Updated!")
```

### Rule 2.4: Views Are Private Functions (Prefix with `_`)
Private render functions prevent accidental direct imports.

```python
# views/settings_view.py

def render_settings_page(...) -> None:
    """Public entry point called by controller."""

def _render_datasource_tab(...) -> None:
    """Private helper, called only by render_settings_page."""
```

### Rule 2.5: Widget Keys Must Use Session State Keys from Controller

```python
# ✅ GOOD
ds_name = st.text_input("Name", key="new_ds_name")

# ❌ BAD (hardcoded keys outside session_state management)
ds_name = st.text_input("Name", key="some_random_key")
```

---

## Layer 3: Controllers (Orchestration)

### Rule 3.1: Controllers Own All Session State for Their Feature
Controllers initialize, read, and modify session state for their page.

```python
# controllers/settings_controller.py

_DEFAULTS: dict = {
    "new_ds_name": "",
    "new_ds_host": "",
    "is_edit_mode": False,
    "edit_ds_id": None,
}

def run() -> None:
    PageState.init(_DEFAULTS)

    # Now session state is safe to use
    is_edit_mode = PageState.get("is_edit_mode")
```

### Rule 3.2: Controllers Fetch All Data
Controllers call `database.py`, `services/`, and `models/` to gather data before rendering.

```python
def run() -> None:
    PageState.init(_DEFAULTS)

    # Fetch data
    datasources_df = db.get_datasources()
    configs_df = db.get_configs_list()

    # Assemble state snapshot
    form_state = {
        "is_edit_mode": PageState.get("is_edit_mode"),
        "edit_ds_id": PageState.get("edit_ds_id"),
    }

    # Pass to view
    render_settings_page(datasources_df, configs_df, form_state, callbacks)
```

### Rule 3.3: Controllers Define All Action Callbacks
Every button click, form submission, or data mutation flows through a callback defined in the controller.

```python
callbacks = {
    "on_row_select": _on_row_select,
    "on_save_new": _on_save_new,
    "on_update": _on_update,
    "on_delete": _on_delete,
    "on_cancel": _reset_to_new_mode,
    "on_get_data": _on_get_config_content,
}

def _on_row_select(ds_id: int) -> None:
    full_data = db.get_datasource_by_id(ds_id)
    PageState.set("is_edit_mode", True)
    PageState.set("edit_ds_id", ds_id)
    st.rerun()

def _on_save_new(name: str, host: str, ...) -> tuple[bool, str]:
    ok, msg = db.save_datasource(name, host, ...)
    if ok:
        PageState.set("trigger_reset", True)
        st.rerun()
    return ok, msg
```

### Rule 3.4: Controllers Call the View's Public Render Function
The view is called exactly once, at the end of the controller.

```python
def run() -> None:
    PageState.init(_DEFAULTS)

    # ... fetch data, define callbacks ...

    render_settings_page(datasources_df, configs_df, form_state, callbacks)  # ← ONLY HERE
```

### Rule 3.5: Controllers Manage State Mutations, Not Views
Only controllers can call `PageState.set()`. Views never touch session state.

```python
# ✅ CONTROLLER
def _on_update(...):
    PageState.set("trigger_reset", True)

# ❌ VIEW (NEVER)
def render_form(...):
    # Views NEVER manipulate state
    st.session_state.trigger_reset = True  # ❌ WRONG
```

---

## File Structure & Naming Conventions

### Naming Pattern
```
feature/
  models/feature_model.py              (if needed, contains @dataclass)
  services/feature_service.py          (if needed, contains logic)
  controllers/feature_controller.py    (owns state, fetches data, calls view)
  views/feature_view.py                (pure rendering)
  views/components/feature/
    sub_component.py                   (reusable sub-components)
```

### Example: Settings Feature (PoC)
```
✅ controllers/settings_controller.py
✅ views/settings_view.py
✅ views/components/shared/dialogs.py  (reusable dialog components)
✅ views/components/shared/styles.py   (reusable CSS)
```

---

## Shared Components (Views)

### Rule 4.1: Dialogs Live in `views/components/shared/dialogs.py`
All `@st.dialog` components should be reusable and receive pre-fetched data.

```python
# ✅ GOOD: Controller fetches, dialog renders
@st.dialog("Preview Configuration")
def preview_config_dialog(config_name: str, content: dict | None) -> None:
    if content:
        st.json(content, expanded=True)
    else:
        st.error("Could not load configuration.")

# In controller:
content = db.get_config_content(config_name)
preview_config_dialog(config_name, content)

# ❌ BAD: Dialog fetches data itself
@st.dialog("Preview Configuration")
def preview_config_dialog(config_name: str) -> None:
    content = db.get_config_content(config_name)  # Dialog shouldn't fetch
```

### Rule 4.2: Shared CSS Goes in `views/components/shared/styles.py`
Global CSS that affects multiple pages should be centralized.

```python
# views/components/shared/styles.py
def inject_global_css() -> None:
    st.markdown("""<style>...</style>""", unsafe_allow_html=True)

# Called from any view
from views.components.shared.styles import inject_global_css
inject_global_css()
```

---

## Migration Rules: Refactoring Legacy Code

When refactoring a legacy page (e.g., `schema_mapper.py`), follow this checklist:

### Pre-Refactoring
- [ ] Read the entire view file to understand all state keys
- [ ] Identify all database calls (`db.*`, `database.*`)
- [ ] Identify all helper functions (these become controller actions)
- [ ] List all buttons/form interactions (these become callbacks)

### During Refactoring
- [ ] Create `controllers/feature_controller.py` with:
  - `_DEFAULTS: dict` (all session state keys)
  - `run()` function (entry point)
  - Private action callbacks (`_on_*` functions)
- [ ] Create `views/feature_view.py` with:
  - Public `render_feature_page(data, form_state, callbacks)` function
  - Private `_render_*` helper functions
  - ZERO database imports
  - ZERO business logic
- [ ] Update `app.py` to import controller and call `controller.run()`

### Post-Refactoring
- [ ] View should have NO `db.` calls
- [ ] View should have NO `PageState.set()` calls (reads only via `form_state` dict)
- [ ] View should have NO conditional logic beyond "is field empty?"
- [ ] Test all CRUD operations (create, read, update, delete)
- [ ] Test all edge cases (empty data, invalid input, etc.)

---

## Testing & Validation

### Unit Test Pattern: Services First
```python
# tests/test_my_service.py
def test_data_transformer():
    transformer = DataTransformer()
    df = pd.DataFrame({"name": [" Alice ", " Bob "]})
    result = transformer.apply_trim(df)
    assert result["name"].tolist() == ["Alice", "Bob"]  # Pure Python test
```

### Integration Test Pattern: Controller + View
```python
# Tests should verify:
# 1. Controller initializes state correctly
# 2. Controller fetches data correctly
# 3. Callbacks modify state and trigger rerun correctly
# 4. View renders with provided data and callbacks

# Note: Full E2E Streamlit testing is complex — focus on service/controller unit tests
```

---

## Code Review Checklist

Before merging any controller/view refactor, verify:

- [ ] **Model Layer**: No `import streamlit` in `models/` or `services/`
- [ ] **View Layer**: No `import database`, `db.*` calls, or `PageState.set()`
- [ ] **Controller Layer**: All state mutations happen here, all data fetches happen here
- [ ] **Naming**: Functions prefixed with `_render`, `_on_*`, `run()`, etc.
- [ ] **Callbacks**: All callbacks defined in controller, not hardcoded in view
- [ ] **Session State**: Only controller initializes and mutates session state
- [ ] **App.py**: Routes to `controller.run()` not `view.render_*()`
- [ ] **Comments**: Docstrings on public functions explaining the contract (what data/callbacks they expect)

---

## Examples & Anti-Patterns

### ✅ Good: Dialog with Pre-Fetched Data

```python
# controllers/settings_controller.py
def _on_preview_config(config_name: str) -> None:
    content = db.get_config_content(config_name)
    preview_config_dialog(config_name, content)

# views/settings_view.py
if st.button("Preview"):
    callbacks["on_preview_config"](config_name)

# views/components/shared/dialogs.py
@st.dialog("Preview")
def preview_config_dialog(config_name: str, content: dict | None) -> None:
    st.json(content)  # Just render
```

### ❌ Bad: Dialog Fetching Data

```python
# ❌ WRONG: Dialog is fetching data
@st.dialog("Preview")
def preview_config_dialog(config_name: str) -> None:
    content = db.get_config_content(config_name)  # NO!
    st.json(content)
```

### ✅ Good: Callback Returns Data to View

```python
# views/settings_view.py
if st.button("Save"):
    ok, msg = callbacks["on_save"](form_data)
    if not ok:
        st.error(msg)

# controllers/settings_controller.py
def _on_save(...) -> tuple[bool, str]:
    ok, msg = db.save_datasource(...)
    if ok:
        PageState.set("trigger_reset", True)
        st.rerun()
    return ok, msg  # View gets result
```

### ❌ Bad: View Showing Success/Error without Callback Return

```python
# ❌ WRONG: View assumes success
if st.button("Save"):
    callbacks["on_save"](form_data)
    st.success("Saved!")  # What if save failed?
```

### ✅ Good: State Snapshot in Form State Dict

```python
# controllers/settings_controller.py
form_state = {
    "is_edit_mode": PageState.get("is_edit_mode"),
    "edit_ds_id": PageState.get("edit_ds_id"),
}
render_settings_page(..., form_state, ...)

# views/settings_view.py
def render_settings_page(..., form_state: dict, ...) -> None:
    if form_state["is_edit_mode"]:
        st.write("Edit mode")
```

### ❌ Bad: View Accessing Session State Directly

```python
# ❌ WRONG: View shouldn't touch session_state
def render_settings_page(...) -> None:
    if st.session_state.is_edit_mode:  # NO!
        st.write("Edit mode")
```

---

## Summary

The three rules of MVC here:

1. **Models/Services** — Pure Python, zero Streamlit, zero side effects
2. **Views** — Only Streamlit rendering, receive all data + callbacks as arguments, NEVER fetch data
3. **Controllers** — Orchestrate everything: init state, fetch data, define callbacks, call view once

When in doubt: **"If it's data or business logic, it belongs in the controller. If it's a button or text input, it belongs in the view. If it's a pure function, it belongs in services."**

---

**Last Updated**: 2026-03-25
**Status**: ✅ Active — enforced on all new code
