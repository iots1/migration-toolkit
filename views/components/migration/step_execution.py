"""
Step 4 — ETL Execution.

Thin Streamlit wrapper around services.migration_executor.run_single_migration.

Responsibilities:
    1. Resolve datasource names → connection config dicts
    2. Build Streamlit callbacks (log, progress)
    3. Delegate ETL to run_single_migration()
    4. Render MigrationResult to session_state + UI

Reads from session_state:
    migration_config, migration_src_profile, migration_tgt_profile,
    checkpoint_batch, src_charset, batch_size, truncate_target,
    migration_test_sample, migration_log_file

Updates session_state:
    migration_running, migration_completed, last_migration_info,
    migration_log_file, migration_step (reset on "New Migration")
"""
import time
import streamlit as st
from datetime import datetime
from sqlalchemy import text

from services.datasource_repository import DatasourceRepository as DSRepo
from services.migration_logger import create_log_file, write_log, read_log_file
from services.migration_executor import run_single_migration


def render_step_execution() -> None:
    # Guard: already running (hot-reload protection)
    if st.session_state.migration_running:
        st.warning("⏳ Migration is already running. Please wait...")
        st.info("If you believe this is stuck, click 'Start New Migration' below.")
        if st.button("🔄 Start New Migration", use_container_width=True):
            _reset_and_restart()
        st.stop()

    if st.session_state.migration_completed:
        st.success("✅ Migration already completed!")
        if st.button("🔄 Start New Migration", use_container_width=True):
            _reset_and_restart()
        st.stop()

    st.session_state.migration_running = True
    st.markdown("### ⚙️ Migration in Progress")

    col_m1, col_m2, col_m3 = st.columns(3)
    metric_processed = col_m1.metric("Rows Processed", "0")
    metric_batch = col_m2.metric("Current Batch", "0")
    metric_time = col_m3.metric("Elapsed Time", "0s")
    progress_bar = st.progress(0)

    with st.status("Initializing...", expanded=True) as status_box:
        log_container = st.empty()
        logs: list[str] = []

        def add_log(msg: str, icon: str = "ℹ️") -> None:
            timestamp = datetime.now().strftime("%H:%M:%S")
            logs.append(f"{icon} `[{timestamp}]` {msg}")
            log_container.markdown("\n\n".join(logs[-20:]))
            write_log(st.session_state.get("migration_log_file"), msg)

        try:
            _run_migration(
                add_log, status_box,
                metric_processed, metric_batch, metric_time,
                progress_bar,
            )
        except Exception as e:
            st.session_state.migration_running = False
            status_box.update(label="Critical Error", state="error", expanded=True)
            st.error(f"Critical Error: {str(e)}")
            add_log(f"CRITICAL ERROR: {str(e)}", "💀")

    st.divider()
    _render_post_migration_controls()


# ---------------------------------------------------------------------------
# Private — thin ETL wrapper
# ---------------------------------------------------------------------------

def _run_migration(add_log, status_box, metric_processed, metric_batch, metric_time, progress_bar):
    config = st.session_state.migration_config
    config_name = config.get("config_name", "migration")

    log_file = create_log_file(config_name)
    st.session_state.migration_log_file = log_file
    add_log(f"Log File created: `{log_file}`", "📂")

    skip_batches = st.session_state.get("checkpoint_batch", 0)
    if skip_batches > 0:
        add_log(f"Resuming from checkpoint: skipping first {skip_batches} batches", "🔄")

    # Resolve credentials
    add_log("Connecting to databases...", "🔗")
    src_ds = DSRepo.get_by_name(st.session_state.migration_src_profile)
    tgt_ds = DSRepo.get_by_name(st.session_state.migration_tgt_profile)
    if not src_ds or not tgt_ds:
        raise ValueError("Could not retrieve datasource credentials.")

    src_charset = st.session_state.get("src_charset")
    if src_ds["db_type"] == "PostgreSQL" and src_charset == "tis620":
        add_log("Auto-adjusting encoding: 'tis620' -> 'WIN874' (PostgreSQL Standard)", "🔧")
        src_charset = "WIN874"

    source_conn_config = {
        "db_type": src_ds["db_type"], "host": src_ds["host"], "port": src_ds["port"],
        "db_name": src_ds["dbname"], "user": src_ds["username"], "password": src_ds["password"],
        "charset": src_charset,
    }
    target_conn_config = {
        "db_type": tgt_ds["db_type"], "host": tgt_ds["host"], "port": tgt_ds["port"],
        "db_name": tgt_ds["dbname"], "user": tgt_ds["username"], "password": tgt_ds["password"],
    }

    migration_start_time = datetime.now()
    wall_start = time.time()

    def progress_callback(batch_num, total_rows, rows_in_batch):
        elapsed = time.time() - wall_start
        metric_processed.metric("Rows Processed", f"{total_rows:,}")
        metric_batch.metric("Current Batch", batch_num)
        metric_time.metric("Elapsed Time", f"{elapsed:.1f}s")
        progress_bar.progress(min(batch_num * 5, 95))
        status_box.update(label=f"Processing Batch {batch_num} ({rows_in_batch:,} rows)...", state="running")

    result = run_single_migration(
        config=config,
        source_conn_config=source_conn_config,
        target_conn_config=target_conn_config,
        batch_size=st.session_state.batch_size,
        truncate_target=st.session_state.get("truncate_target", False),
        test_mode=st.session_state.migration_test_sample,
        skip_batches=skip_batches,
        log_callback=add_log,
        progress_callback=progress_callback,
    )

    target_table = config["target"]["table"]
    st.session_state["last_migration_info"] = {
        "table": target_table,
        "tgt_profile": st.session_state.migration_tgt_profile,
        "start_time": migration_start_time.isoformat(),
        "pre_count": result.pre_count,
    }

    if result.status == "failed":
        status_box.update(label="Migration Failed", state="error", expanded=True)
        short_msg, failed_sql = _parse_db_error(result.error_message or "")
        st.error(f"**Migration Failed:** {short_msg}")
        if failed_sql:
            with st.expander("🔍 Failed SQL Query", expanded=True):
                st.code(failed_sql, language="sql")
        with st.expander("📋 Full Error Details"):
            st.code(result.error_message or "No details", language="text")
        col_err1, _ = st.columns(2)
        with col_err1:
            if st.button("🗑️ Emergency Truncate Target Table", key="emergency_truncate"):
                tgt_engine = DSRepo.get_engine(st.session_state.migration_tgt_profile)
                _emergency_truncate(tgt_engine, target_table, add_log)
    else:
        progress_bar.progress(100)
        status_box.update(label="Migration Complete!", state="complete", expanded=False)
        if result.post_count >= 0:
            actual_inserted = result.post_count - result.pre_count
            if actual_inserted == result.rows_processed:
                st.success(
                    f"✅ Migration Verified! Inserted **{actual_inserted:,}** rows "
                    f"into `{target_table}` (total: {result.post_count:,})"
                )
            else:
                st.warning(
                    f"⚠️ Count Mismatch! Processed: {result.rows_processed:,} | "
                    f"Actually in DB: {actual_inserted:,}  \n"
                    f"Target now has {result.post_count:,} rows total."
                )
            st.session_state["last_migration_info"]["inserted"] = actual_inserted
            st.session_state["last_migration_info"]["post_count"] = result.post_count
        else:
            st.success(f"✅ Migration Finished! Total Rows Processed: {result.rows_processed:,}")
        st.session_state.migration_completed = True
        st.balloons()

    st.session_state.migration_running = False


# ---------------------------------------------------------------------------
# Private — emergency truncate (UI-triggered, runs after migration ends)
# ---------------------------------------------------------------------------

def _emergency_truncate(engine, table: str, add_log) -> None:
    try:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
            except Exception:
                conn.execute(text(f"DELETE FROM {table}"))
        st.success(f"Table '{table}' truncated!")
        add_log(f"User triggered Emergency Truncate on {table}", "🗑️")
    except Exception as e:
        st.error(f"Failed to truncate: {e}")


# ---------------------------------------------------------------------------
# Private — post-migration controls
# ---------------------------------------------------------------------------

def _render_post_migration_controls() -> None:
    col_end1, col_end2, col_end3 = st.columns(3)

    with col_end1:
        if st.button("🔄 Start New Migration", use_container_width=True):
            _reset_and_restart()

    with col_end2:
        _render_rollback_button()

    with col_end3:
        _render_log_download()


def _render_rollback_button() -> None:
    migration_info = st.session_state.get("last_migration_info")
    if not migration_info:
        st.button("🔙 Rollback", disabled=True, use_container_width=True, help="ไม่มีข้อมูล migration ล่าสุด")
        return

    inserted = migration_info.get("inserted", 0)
    label = f"🔙 Rollback ({inserted:,} rows)" if inserted else "🔙 Rollback Last Migration"
    if st.button(label, type="secondary", use_container_width=True):
        try:
            rb_engine = DSRepo.get_engine(migration_info["tgt_profile"])
            rb_table = migration_info["table"]
            rb_start = migration_info["start_time"]

            with rb_engine.begin() as conn:
                try:
                    result = conn.execute(
                        text(f"DELETE FROM {rb_table} WHERE created_at >= :ts RETURNING *"),
                        {"ts": rb_start},
                    )
                    st.success(f"✅ Rollback สำเร็จ — ลบ {result.rowcount:,} rows (created_at >= {rb_start[:19]})")
                except Exception:
                    result = conn.execute(
                        text(f"DELETE FROM {rb_table} WHERE ctid IN "
                             f"(SELECT ctid FROM {rb_table} ORDER BY ctid DESC LIMIT :n)"),
                        {"n": inserted},
                    )
                    st.success(f"✅ Rollback สำเร็จ — ลบ {result.rowcount:,} rows")

            st.session_state.pop("last_migration_info", None)
            st.rerun()
        except Exception as e:
            st.error(f"Rollback failed: {e}")


def _render_log_download() -> None:
    log_content = read_log_file(st.session_state.get("migration_log_file"))
    if log_content:
        st.download_button("📥 Download Full Log", data=log_content, file_name="migration.log")


def _reset_and_restart() -> None:
    st.session_state.migration_running = False
    st.session_state.migration_completed = False
    st.session_state.resume_from_checkpoint = False
    st.session_state.checkpoint_batch = 0
    st.session_state.migration_step = 1
    st.rerun()


def _parse_db_error(raw: str) -> tuple[str, str | None]:
    """
    Extract a readable summary and the failed SQL from a SQLAlchemy/pymssql error string.

    Returns (short_summary, failed_sql | None).

    Raw pymssql format:
        (pymssql.exceptions.ProgrammingError) (207, b"Invalid column name 'old_hn'.
        DB-Lib error message 20018, severity 16:\\nGeneral SQL Server error...\\n" * N)
        [SQL: SELECT ...]
        (Background on this error at: ...)
    """
    import re

    if not raw:
        return "Unknown error", None

    # ── Extract failed SQL ────────────────────────────────────────────────────
    failed_sql: str | None = None
    sql_marker = "[SQL:"
    bg_marker = "(Background on this error"
    if sql_marker in raw:
        sql_start = raw.index(sql_marker) + len(sql_marker)
        sql_end = raw.find(bg_marker, sql_start)
        if sql_end == -1:
            sql_end = len(raw)
        failed_sql = raw[sql_start:sql_end].strip().rstrip("]").strip()

    # ── Build short summary ───────────────────────────────────────────────────
    first_line = raw.split("\n")[0]

    # pymssql pattern: (207, b"Invalid column name 'old_hn'.DB-Lib...)
    match = re.search(r'\((\d+),\s*b[\'"](.+?)(?:DB-Lib|\\n|[\'"])', first_line)
    if match:
        code = match.group(1)
        msg = match.group(2).rstrip(".").strip()
        # Exception class name from e.g. "(pymssql.exceptions.ProgrammingError)"
        cls_match = re.search(r'\([\w.]*?(\w+Error)\)', first_line)
        exc = cls_match.group(1) if cls_match else "DatabaseError"
        short_summary = f"{exc} [{code}]: {msg}"
    else:
        # Generic fallback — first 200 chars of the first line
        short_summary = first_line[:200]

    return short_summary, failed_sql
