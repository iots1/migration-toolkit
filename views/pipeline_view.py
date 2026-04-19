"""
Pipeline View — pure Streamlit rendering for the Data Pipeline page.

Receives all data and callbacks from controllers/pipeline_controller.py.

Rules (strict MVC):
    - MUST NOT import database, services, or models.
    - MUST NOT access st.session_state directly (widget key= params are OK).
    - MUST NOT contain business logic.
    - All mutations are delegated through callbacks.
"""
from __future__ import annotations
import streamlit as st

from views.components.shared.dialogs import generic_confirm_dialog
from views.components.shared.styles import inject_global_css

_CHARSET_MAP = {
    "utf8mb4 (Default)": None,
    "tis620 (Thai Legacy)": "tis620",
    "latin1 (Raw Bytes)": "latin1",
}

_STRATEGY_LABELS = {
    "fail_fast": "Fail Fast — stop immediately on first error",
    "continue_on_error": "Continue — run all steps regardless of failures",
    "skip_dependents": "Skip Dependents — skip steps whose parent failed",
}

_STATUS_ICONS = {
    "success": "✅",
    "failed": "❌",
    "skipped": "⏭️",
    "skipped_dependency": "⏭️",
    "running": "⏳",
    "pending": "🔵",
    "completed": "✅",
    "partial": "⚠️",
}


def render_pipeline_page(
    pipelines_df,
    configs_df,
    datasources_df,
    form_state: dict,
    callbacks: dict,
) -> None:
    inject_global_css()
    st.subheader("🔗 Data Pipeline")

    wizard_step = form_state["wizard_step"]

    # --- Wizard progress bar ---
    _render_wizard_header(wizard_step)

    st.divider()

    if wizard_step == 1:
        _render_step_design(pipelines_df, configs_df, form_state, callbacks)
    elif wizard_step == 2:
        _render_step_connections(datasources_df, form_state, callbacks)
    elif wizard_step == 3:
        _render_step_review(form_state, callbacks)
    elif wizard_step == 4:
        _render_step_execute(form_state, callbacks)


# ---------------------------------------------------------------------------
# Wizard header
# ---------------------------------------------------------------------------

def _render_wizard_header(step: int) -> None:
    labels = ["1. Design", "2. Connections", "3. Review", "4. Execute"]
    cols = st.columns(len(labels))
    for i, (col, label) in enumerate(zip(cols, labels), start=1):
        style = "**" if i == step else ""
        icon = "🔵" if i == step else ("✅" if i < step else "⚪")
        col.markdown(f"{icon} {style}{label}{style}", unsafe_allow_html=False)


# ---------------------------------------------------------------------------
# Step 1: Design
# ---------------------------------------------------------------------------

def _render_step_design(pipelines_df, configs_df, form_state, callbacks) -> None:
    st.markdown("### Step 1: Design Pipeline")

    left, right = st.columns([1, 1])

    with left:
        _render_pipeline_metadata(form_state, callbacks)

    with right:
        _render_pipeline_loader(pipelines_df, callbacks)

    st.divider()
    _render_step_builder(configs_df, form_state, callbacks)

    st.divider()
    _render_design_nav(form_state, callbacks)


def _render_pipeline_metadata(form_state, callbacks) -> None:
    st.markdown("#### Pipeline Settings")

    name = st.text_input(
        "Pipeline Name *",
        value=form_state["form_name"],
        key="pl_form_name",
        placeholder="e.g. his_full_migration",
    )
    desc = st.text_input(
        "Description",
        value=form_state["form_desc"],
        key="pl_form_desc",
    )

    strategy_keys = list(_STRATEGY_LABELS.keys())
    current_strategy = form_state["form_error_strategy"]
    strategy_idx = strategy_keys.index(current_strategy) if current_strategy in strategy_keys else 0

    strategy = st.selectbox(
        "Error Strategy",
        options=strategy_keys,
        index=strategy_idx,
        format_func=lambda k: _STRATEGY_LABELS[k],
        key="pl_form_strategy",
    )

    c1, c2 = st.columns(2)
    batch_size = c1.number_input(
        "Batch Size", min_value=100, max_value=50000,
        value=form_state["form_batch_size"], step=100, key="pl_form_batch",
    )
    truncate = c2.checkbox(
        "Truncate Targets Before Insert",
        value=form_state["form_truncate"],
        key="pl_form_truncate",
    )

    if st.button("💾 Save Pipeline", type="primary", use_container_width=True):
        ok, msg = callbacks["on_save_pipeline"](name, desc, strategy, int(batch_size), truncate)
        if not ok:
            st.error(msg)


def _render_pipeline_loader(pipelines_df, callbacks) -> None:
    st.markdown("#### Load Existing Pipeline")

    if pipelines_df.empty:
        st.info("No saved pipelines yet.")
        return

    options = pipelines_df["name"].tolist()
    selected = st.selectbox("Select Pipeline", ["-- Select --"] + options, key="pl_load_sel")

    c1, c2 = st.columns(2)
    if c1.button("📂 Load", use_container_width=True, disabled=(selected == "-- Select --")):
        callbacks["on_load_pipeline"](selected)

    if c2.button("🗑️ Delete", use_container_width=True, disabled=(selected == "-- Select --")):
        generic_confirm_dialog(
            title=f"Delete pipeline: {selected}?",
            message="This will also delete all run history for this pipeline.",
            confirm_label="Delete Pipeline",
            on_confirm_func=callbacks["on_delete_pipeline"],
            name=selected,
        )

    if st.button("✨ New Pipeline", use_container_width=True):
        callbacks["on_new_pipeline"]()


def _render_step_builder(configs_df, form_state, callbacks) -> None:
    st.markdown("#### Pipeline Steps")

    steps: list[dict] = form_state["form_steps"]
    all_step_names = [s["config_name"] for s in steps]

    col_add, col_steps = st.columns([1, 2])

    with col_add:
        st.markdown("**Available Configs**")
        if configs_df.empty:
            st.info("No saved configs.")
        else:
            available = [
                r for r in configs_df["config_name"].tolist()
                if r not in all_step_names
            ]
            if available:
                to_add = st.selectbox("Config to add", available, key="pl_add_cfg_sel")
                if st.button("➕ Add Step", use_container_width=True):
                    callbacks["on_add_step"](to_add)
            else:
                st.success("All configs are in the pipeline.")

    with col_steps:
        st.markdown("**Current Steps**")
        if not steps:
            st.info("No steps added yet. Pick a config and click Add Step →")
        else:
            for i, step in enumerate(steps):
                cname = step["config_name"]
                enabled = step["enabled"]
                depends = step.get("depends_on", [])

                with st.container(border=True):
                    h1, h2, h3, h4, h5 = st.columns([3, 1, 1, 1, 1])
                    label = f"{'✅' if enabled else '⬜'} **{i + 1}. {cname}**"
                    h1.markdown(label)

                    if h2.button("↑", key=f"pl_up_{cname}", use_container_width=True, disabled=(i == 0)):
                        callbacks["on_move_step_up"](cname)
                    if h3.button("↓", key=f"pl_dn_{cname}", use_container_width=True, disabled=(i == len(steps) - 1)):
                        callbacks["on_move_step_down"](cname)
                    if h4.button("En/Dis", key=f"pl_tog_{cname}", use_container_width=True):
                        callbacks["on_toggle_step"](cname)
                    if h5.button("🗑️", key=f"pl_rm_{cname}", use_container_width=True):
                        callbacks["on_remove_step"](cname)

                    other_steps = [s["config_name"] for s in steps if s["config_name"] != cname]
                    if other_steps:
                        new_deps = st.multiselect(
                            "depends_on",
                            options=other_steps,
                            default=[d for d in depends if d in other_steps],
                            key=f"pl_dep_{cname}",
                            label_visibility="collapsed",
                            placeholder="Select dependencies…",
                        )
                        if new_deps != depends:
                            callbacks["on_set_depends"](cname, new_deps)


def _render_design_nav(form_state, callbacks) -> None:
    steps = form_state["form_steps"]
    can_proceed = bool(form_state["form_name"].strip()) and bool(steps)

    c1, _, c3 = st.columns([1, 3, 1])
    if c3.button(
        "Next: Connections →",
        type="primary",
        use_container_width=True,
        disabled=not can_proceed,
    ):
        callbacks["on_next_step"]()

    if not can_proceed:
        st.caption("Save the pipeline with a name and at least one step to continue.")


# ---------------------------------------------------------------------------
# Step 2: Connections
# ---------------------------------------------------------------------------

def _render_step_connections(datasources_df, form_state, callbacks) -> None:
    st.markdown("### Step 2: Verify Connections")

    ds_options = ["-- Select --"] + (datasources_df["name"].tolist() if not datasources_df.empty else [])

    col_src, col_tgt = st.columns(2)

    with col_src:
        _render_conn_panel(
            label="Source Database",
            ds_options=ds_options,
            sel_key="pl_src_ds_sel",
            current_name=form_state["src_ds_name"],
            is_ok=form_state["src_ok"],
            show_charset=True,
            test_callback=callbacks["on_test_source"],
        )

    with col_tgt:
        _render_conn_panel(
            label="Target Database",
            ds_options=ds_options,
            sel_key="pl_tgt_ds_sel",
            current_name=form_state["tgt_ds_name"],
            is_ok=form_state["tgt_ok"],
            show_charset=False,
            test_callback=callbacks["on_test_target"],
        )

    st.divider()
    c_back, _, c_next = st.columns([1, 3, 1])

    if c_back.button("← Back", use_container_width=True):
        callbacks["on_prev_step"]()

    can_proceed = form_state["src_ok"] and form_state["tgt_ok"]
    if c_next.button(
        "Next: Review →",
        type="primary",
        use_container_width=True,
        disabled=not can_proceed,
    ):
        callbacks["on_next_step"]()

    if not can_proceed:
        st.caption("Test both connections successfully to continue.")


def _render_conn_panel(label, ds_options, sel_key, current_name, is_ok,
                       show_charset, test_callback) -> None:
    st.markdown(f"#### {label}")

    default_idx = 0
    if current_name and current_name in ds_options:
        default_idx = ds_options.index(current_name)

    selected = st.selectbox("Profile", ds_options, index=default_idx, key=sel_key)

    charset_val = None
    if show_charset:
        charset_label = st.selectbox(
            "Source Charset",
            list(_CHARSET_MAP.keys()),
            key=f"{sel_key}_charset",
        )
        charset_val = _CHARSET_MAP[charset_label]

    if selected != "-- Select --":
        if st.button(f"🔍 Test {label.split()[0]}", key=f"{sel_key}_test", use_container_width=True):
            with st.spinner("Connecting…"):
                if show_charset:
                    ok, msg = test_callback(selected, charset_val)
                else:
                    ok, msg = test_callback(selected)
            if not ok:
                st.error(f"❌ {msg}")

    if is_ok:
        st.success(f"✅ {label} Connected")


# ---------------------------------------------------------------------------
# Step 3: Review
# ---------------------------------------------------------------------------

def _render_step_review(form_state, callbacks) -> None:
    st.markdown("### Step 3: Review & Configure")

    steps: list[dict] = form_state["form_steps"]

    # Steps summary table
    st.markdown("#### Pipeline Steps")
    for step in steps:
        enabled_icon = "✅" if step["enabled"] else "⬜"
        deps = ", ".join(step["depends_on"]) if step["depends_on"] else "—"
        st.markdown(
            f"{enabled_icon} **{step['order']}. {step['config_name']}** "
            f"&nbsp;&nbsp; *depends_on:* `{deps}`"
        )

    st.divider()
    st.markdown("#### Settings")
    c1, c2, c3 = st.columns(3)
    c1.metric("Batch Size", f"{form_state['form_batch_size']:,}")
    c2.metric("Error Strategy", form_state["form_error_strategy"])
    c3.metric("Truncate Targets", "Yes" if form_state["form_truncate"] else "No")

    st.markdown(
        f"**Source:** `{form_state['src_ds_name'] or '—'}`  "
        f"→  **Target:** `{form_state['tgt_ds_name'] or '—'}`"
    )

    if form_state["has_checkpoint"]:
        st.warning(
            "⚠️ **Checkpoint Detected** — a previous run was interrupted. "
            "The pipeline will resume from where it left off. "
            "Steps marked *completed* in the checkpoint will be skipped."
        )

    st.divider()
    c_back, _, c_start = st.columns([1, 3, 1])

    if c_back.button("← Back", use_container_width=True):
        callbacks["on_prev_step"]()

    if c_start.button("🚀 Start Pipeline", type="primary", use_container_width=True):
        ok, msg = callbacks["on_start_pipeline"]()
        if not ok:
            st.error(msg)


# ---------------------------------------------------------------------------
# Step 4: Execute
# ---------------------------------------------------------------------------

def _render_step_execute(form_state, callbacks) -> None:
    st.markdown("### Step 4: Execute")

    is_running = form_state["running"]
    is_completed = form_state["completed"]
    run_result = form_state["run_result"]
    run_id = form_state["run_id"]

    # Zombie run warning
    if form_state["zombie_warning"] and run_id:
        st.warning(
            "⚠️ **Possible Zombie Run** — this run has been in 'running' state for over 24 hours. "
            "The server may have restarted while the pipeline was active."
        )
        if st.button("🛑 Force Mark as Failed", type="secondary"):
            callbacks["on_force_cancel"](run_id)

    # Action buttons row
    c_refresh, c_cancel, c_new = st.columns([1, 1, 1])

    if c_refresh.button("🔄 Refresh Status", use_container_width=True, type="primary"):
        callbacks["on_poll_status"]()

    if is_running and run_id:
        if c_cancel.button("🛑 Force Cancel", type="secondary", use_container_width=True):
            callbacks["on_force_cancel"](run_id)

    if c_new.button("✨ New Pipeline", use_container_width=True):
        callbacks["on_reset"]()

    st.divider()

    # Status display — explicit state at every branch
    if not run_id:
        st.info("Click 🚀 Start Pipeline on the Review step to begin.")
        return

    if is_running and not run_result:
        st.info("⏳ Pipeline is running in the background… click **🔄 Refresh Status** to check progress.")
        return

    if not run_result:
        st.info("No run data yet — click **🔄 Refresh Status**.")
        return

    # Show run result
    overall_status = run_result.get("status", "unknown")
    overall_icon = _STATUS_ICONS.get(overall_status, "❓")

    st.markdown(f"**Overall Status:** {overall_icon} `{overall_status.upper()}`")

    if run_result.get("error_message"):
        st.error(f"Pipeline error: {run_result['error_message']}")

    # get_latest_pipeline_run() already parses steps_json → dict under key "steps"
    steps: dict = run_result.get("steps") or {}

    if steps:
        st.markdown("#### Step Results")
        for step_name, info in steps.items():
            status = info.get("status", "pending")
            icon = _STATUS_ICONS.get(status, "❓")
            rows = info.get("rows_processed", 0)
            duration = info.get("duration_seconds", 0.0)
            err_msg = info.get("error_message", "")

            with st.container(border=True):
                h1, h2, h3 = st.columns([3, 1, 1])
                h1.markdown(f"{icon} **{step_name}** — `{status}`")
                h2.metric("Rows", f"{rows:,}")
                h3.metric("Duration", f"{duration:.1f}s")
                if err_msg:
                    st.error(err_msg)
    else:
        if is_running:
            st.info("Run started — no step data yet. Click **🔄 Refresh Status** to check.")
        else:
            st.info("No step data available.")

    if is_completed:
        st.divider()
        total_rows = sum(info.get("rows_processed", 0) for info in steps.values())
        st.success(
            f"Pipeline finished with status **{overall_status}**. "
            f"Total rows migrated: **{total_rows:,}**"
        )
