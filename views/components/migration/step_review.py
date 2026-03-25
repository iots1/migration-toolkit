"""
Step 3 — Review & Settings.

Shows config summary, self-migration guard, checkpoint resume option,
batch size, truncate checkbox, and test mode toggle.
Updates session_state:
    batch_size                int
    truncate_target           bool
    migration_test_sample     bool
    resume_from_checkpoint    bool
    checkpoint_batch          int
    migration_running         bool (reset to False)
    migration_completed       bool (reset to False)
    migration_step            → 2 (back) | 4 (start)
"""
import streamlit as st
from services.checkpoint_manager import load_checkpoint, clear_checkpoint
from services.datasource_repository import DatasourceRepository as DSRepo


def render_step_review() -> None:
    st.markdown("### Step 3: Review & Settings")

    config = st.session_state.migration_config
    config_name = config.get("config_name", "migration")
    checkpoint = load_checkpoint(config_name)

    with st.expander("📄 View Configuration JSON", expanded=False):
        st.json(config)

    is_self_migration = _check_self_migration(config)
    if is_self_migration:
        src_tbl = config.get("source", {}).get("table", "")
        tgt_tbl = config.get("target", {}).get("table", "")
        st.error(
            f"🚨 **Source และ Target เป็นตารางเดียวกัน!**  \n"
            f"`{src_tbl}` → `{tgt_tbl}` บน DB เดียวกัน  \n"
            f"Migration จะ insert กลับเข้าหาตัวเอง — กรุณาแก้ไข config ก่อน"
        )

    col_set1, col_set2 = st.columns(2)
    with col_set1:
        _render_mapping_summary(config)
    with col_set2:
        _render_execution_settings(checkpoint)

    if checkpoint:
        _render_checkpoint_panel(config_name, checkpoint)

    st.divider()
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("← Back"):
            st.session_state.migration_step = 2
            st.rerun()
    with col_btn2:
        btn_label = (
            "🔄 Resume Migration"
            if (checkpoint and st.session_state.resume_from_checkpoint)
            else "🚀 Start Migration Engine"
        )
        if st.button(btn_label, type="primary", use_container_width=True, disabled=is_self_migration):
            st.session_state.migration_running = False
            st.session_state.migration_completed = False
            st.session_state.checkpoint_batch = (
                checkpoint["last_batch"]
                if checkpoint and st.session_state.resume_from_checkpoint
                else 0
            )
            st.session_state.migration_step = 4
            st.rerun()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _check_self_migration(config: dict) -> bool:
    src_tbl = config.get("source", {}).get("table", "")
    tgt_tbl = config.get("target", {}).get("table", "")
    src_profile = st.session_state.get("migration_src_profile", "")
    tgt_profile = st.session_state.get("migration_tgt_profile", "")
    src_ds = DSRepo.get_by_name(src_profile) if src_profile else None
    tgt_ds = DSRepo.get_by_name(tgt_profile) if tgt_profile else None
    same_conn = (
        src_ds and tgt_ds
        and src_ds["host"] == tgt_ds["host"]
        and src_ds["port"] == tgt_ds["port"]
        and src_ds["dbname"] == tgt_ds["dbname"]
    )
    return bool(same_conn and src_tbl == tgt_tbl)


def _render_mapping_summary(config: dict) -> None:
    st.markdown("#### Mapping Summary")
    st.info(f"Source Table: **{config['source']['table']}**")
    st.info(f"Target Table: **{config['target']['table']}**")
    st.write(f"Columns Mapped: {len(config.get('mappings', []))}")


def _render_execution_settings(checkpoint) -> None:
    st.markdown("#### Execution Settings")
    batch_size = st.number_input("Batch Size (Rows per chunk)", value=1000, step=500, min_value=100)
    st.session_state.batch_size = batch_size

    st.markdown("#### Data Options")
    st.session_state.truncate_target = st.checkbox(
        "🗑️ **Truncate Target Table** before starting",
        value=st.session_state.truncate_target,
        help="⚠️ WARNING: This will DELETE ALL DATA in the target table before migration begins.",
        disabled=checkpoint is not None and st.session_state.resume_from_checkpoint,
    )
    st.session_state.migration_test_sample = st.checkbox(
        "🧪 **Test Mode** (Process only 1 batch)",
        value=st.session_state.migration_test_sample,
    )
    if st.session_state.migration_test_sample:
        st.warning("Running in Test Mode: Migration will stop after the first batch.")


def _render_checkpoint_panel(config_name: str, checkpoint: dict) -> None:
    st.divider()
    st.warning("⚠️ **Previous migration was interrupted!**")
    col_ck1, col_ck2 = st.columns(2)
    with col_ck1:
        st.markdown(
            f"- **Last Batch:** {checkpoint['last_batch']}\n"
            f"- **Rows Processed:** {checkpoint['rows_processed']:,}\n"
            f"- **Saved:** {checkpoint['timestamp']}"
        )
    with col_ck2:
        st.session_state.resume_from_checkpoint = st.checkbox(
            "🔄 **Resume from checkpoint**",
            value=st.session_state.resume_from_checkpoint,
            help="Continue from where the migration stopped",
        )
        if st.button("🗑️ Clear Checkpoint", type="secondary"):
            clear_checkpoint(config_name)
            st.session_state.resume_from_checkpoint = False
            st.success("Checkpoint cleared!")
            st.rerun()
