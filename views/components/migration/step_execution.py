"""
Step 4 — ETL Execution.

Runs the full migration pipeline:
    connect → pre-count → truncate → schema check → HN init →
    batch loop (encode → transform → insert) → verify → rollback UI

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
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

from services.datasource_repository import DatasourceRepository as DSRepo
from services.migration_logger import create_log_file, write_log, read_log_file
from services.checkpoint_manager import save_checkpoint, clear_checkpoint
from services.encoding_helper import clean_dataframe
from services.query_builder import build_select_query, transform_batch, build_dtype_map, batch_insert
from services.transformers import DataTransformer


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
# Private — ETL pipeline
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

    # --- Connect ---
    add_log("Connecting to databases...", "🔗")
    src_ds = DSRepo.get_by_name(st.session_state.migration_src_profile)
    tgt_ds = DSRepo.get_by_name(st.session_state.migration_tgt_profile)
    if not src_ds or not tgt_ds:
        raise ValueError("Could not retrieve datasource credentials.")

    src_charset = st.session_state.get("src_charset")
    if src_ds["db_type"] == "PostgreSQL" and src_charset == "tis620":
        add_log("Auto-adjusting encoding: 'tis620' -> 'WIN874' (PostgreSQL Standard)", "🔧")
        src_charset = "WIN874"

    src_engine = DSRepo.get_engine(st.session_state.migration_src_profile, charset=src_charset)
    tgt_engine = DSRepo.get_engine(st.session_state.migration_tgt_profile)
    add_log(f"Source connected: {src_ds['db_type']} (charset: {src_charset or 'default'})", "✅")
    add_log(f"Target connected: {tgt_ds['db_type']}", "✅")

    target_table = config["target"]["table"]
    source_table = config["source"]["table"]
    migration_start_time = datetime.now()

    # --- Pre-count ---
    pre_migration_count = _get_row_count(tgt_engine, target_table, add_log)
    st.session_state["last_migration_info"] = {
        "table": target_table,
        "tgt_profile": st.session_state.migration_tgt_profile,
        "start_time": migration_start_time.isoformat(),
        "pre_count": pre_migration_count,
    }

    # --- Truncate ---
    if st.session_state.get("truncate_target", False):
        _truncate_table(tgt_engine, target_table, add_log)

    # --- Schema validation ---
    _validate_schema(src_engine, tgt_engine, source_table, target_table, config, add_log)

    # --- HN counter ---
    _init_hn_counter(tgt_engine, target_table, config, add_log)

    # --- Batch loop ---
    batch_size = st.session_state.batch_size
    select_query = build_select_query(config, source_table, src_ds["db_type"])
    add_log(f"SELECT Query: {select_query}", "🔍")
    add_log(f"Starting Batch Processing (Size: {batch_size})...", "🚀")

    data_iterator = pd.read_sql(select_query, src_engine, chunksize=batch_size, coerce_float=False)
    total_rows = 0
    batch_num = 0
    start_time = time.time()
    migration_failed = False

    for df_batch in data_iterator:
        batch_num += 1
        rows_in_batch = len(df_batch)

        if batch_num <= skip_batches:
            total_rows += rows_in_batch
            add_log(f"Batch {batch_num}: Skipped (checkpoint)", "⏭️")
            continue

        status_box.update(label=f"Processing Batch {batch_num} ({rows_in_batch} rows)...", state="running")
        df_batch = clean_dataframe(df_batch)

        try:
            df_batch, bit_columns = transform_batch(df_batch, config)
        except Exception as e:
            add_log(f"Transformation Error in Batch {batch_num}: {e}", "⚠️")
            continue

        try:
            dtype_map = build_dtype_map(bit_columns, df_batch, tgt_ds["db_type"])
            batch_insert(df_batch, target_table, tgt_engine, dtype_map)

            total_rows += rows_in_batch
            elapsed = time.time() - start_time
            metric_processed.metric("Rows Processed", f"{total_rows:,}")
            metric_batch.metric("Current Batch", batch_num)
            metric_time.metric("Elapsed Time", f"{elapsed:.1f}s")
            progress_bar.progress(min(batch_num * 5, 95))
            save_checkpoint(config_name, batch_num, total_rows)
            add_log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

        except Exception as e:
            save_checkpoint(config_name, batch_num - 1, total_rows)
            short_err = str(e).split("[SQL:")[0].strip()[:300]
            add_log(f"Insert Failed: {short_err}", "❌")
            st.error(f"Migration Failed at Batch {batch_num}: {short_err}")

            col_err1, _ = st.columns(2)
            with col_err1:
                if st.button("🗑️ Emergency Truncate Target Table", key="emergency_truncate"):
                    _emergency_truncate(tgt_engine, target_table, add_log)
            with st.expander("🔴 View Full Error Details", expanded=False):
                st.code(str(e), language="sql")

            status_box.update(label="Migration Failed", state="error", expanded=True)
            st.session_state.migration_running = False
            migration_failed = True
            break

        if st.session_state.migration_test_sample:
            add_log("Stopping after first batch (Test Mode)", "🛑")
            break

    # --- Post-migration ---
    if not migration_failed:
        progress_bar.progress(100)
        status_box.update(label="Migration Complete!", state="complete", expanded=False)
        _verify_post_migration(tgt_engine, target_table, pre_migration_count, total_rows, add_log)
        clear_checkpoint(config_name)
        add_log("Checkpoint cleared (migration complete)", "🧹")
        st.session_state.migration_completed = True
        st.balloons()

    st.session_state.migration_running = False


# ---------------------------------------------------------------------------
# Private — sub-operations
# ---------------------------------------------------------------------------

def _get_row_count(engine, table: str, add_log) -> int:
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar() or 0
        add_log(f"Pre-migration count: {count:,} rows in `{table}`", "📊")
        return count
    except Exception as e:
        add_log(f"Could not get pre-migration count (non-critical): {e}", "⚠️")
        return 0


def _truncate_table(engine, table: str, add_log) -> None:
    add_log(f"Cleaning target table: {table}...", "🧹")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        add_log("Target table truncated successfully.", "✅")
    except Exception as e:
        add_log(f"TRUNCATE failed, trying DELETE FROM... ({e})", "⚠️")
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table}"))
            add_log("Target table cleared using DELETE.", "✅")
        except Exception as e2:
            add_log(f"Failed to clean table: {e2}", "❌")
            raise e2


def _validate_schema(src_engine, tgt_engine, source_table: str, target_table: str, config: dict, add_log) -> None:
    add_log("Validating Schema Compatibility...", "🧐")
    try:
        src_insp = sqlalchemy.inspect(src_engine)
        tgt_insp = sqlalchemy.inspect(tgt_engine)

        def _get_cols(insp, table):
            parts = table.split(".")
            t, s = parts[-1], parts[0] if len(parts) > 1 else None
            try:
                return {c["name"]: c["type"] for c in insp.get_columns(t, schema=s)}
            except Exception:
                return {c["name"]: c["type"] for c in insp.get_columns(table)}

        src_cols = _get_cols(src_insp, source_table)
        tgt_cols = _get_cols(tgt_insp, target_table)

        warnings = []
        for m in config.get("mappings", []):
            if m.get("ignore", False):
                continue
            sc, tc = m["source"], m["target"]
            if sc in src_cols and tc in tgt_cols:
                src_len = getattr(src_cols[sc], "length", None)
                tgt_len = getattr(tgt_cols[tc], "length", None)
                if tgt_len is not None:
                    if src_len is None:
                        warnings.append(f"- **{sc}** (Unknown/Text) ➔ **{tc}** (Limit: {tgt_len})")
                    elif src_len > tgt_len:
                        warnings.append(f"- **{sc}** (Limit: {src_len}) ➔ **{tc}** (Limit: {tgt_len})")

        if warnings:
            add_log("⚠️ Potential Truncation Detected:\n" + "\n".join(warnings).replace("**", ""), "⚠️")
            st.warning("⚠️ **Potential Truncation Detected!** check logs for details.")
            time.sleep(1)
        else:
            add_log("Schema compatibility check passed.", "✅")
    except Exception as e:
        add_log(f"Skipping schema check (Non-critical): {e}", "⚠️")


def _init_hn_counter(tgt_engine, target_table: str, config: dict, add_log) -> None:
    import re as _re
    for mapping in config.get("mappings", []):
        if mapping.get("ignore", False) or "GENERATE_HN" not in mapping.get("transformers", []):
            continue
        ghn_params = mapping.get("transformer_params", {}).get("GENERATE_HN", {})
        auto_detect = ghn_params.get("auto_detect_max", True)
        start_from = int(ghn_params.get("start_from", 0))

        if auto_detect:
            hn_col = mapping.get("target", mapping.get("source"))
            add_log(f"Auto-detecting max HN from `{target_table}.{hn_col}`...", "🔍")
            try:
                with tgt_engine.connect() as conn:
                    result = conn.execute(text(f'SELECT MAX("{hn_col}") FROM {target_table}'))
                    max_val = result.scalar()
                if max_val:
                    digits = _re.sub(r"\D", "", str(max_val))
                    start_from = int(digits) if digits else 0
                    add_log(f"Max HN found: `{max_val}` → counter starts at {start_from}", "✅")
                else:
                    add_log(f"No existing HN in target → counter starts at {start_from}", "ℹ️")
            except Exception as e:
                add_log(f"Auto-detect HN failed: {e} → using start_from={start_from}", "⚠️")

        DataTransformer.reset_hn_counter(start_from)
        add_log(f"HN Counter initialized at {start_from} (next: HN{str(start_from+1).zfill(9)})", "🔢")
        break  # Only one GENERATE_HN per config


def _verify_post_migration(tgt_engine, target_table: str, pre_count: int, total_processed: int, add_log) -> None:
    try:
        with tgt_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
            post_count = result.scalar() or 0
        actual_inserted = post_count - pre_count
        add_log(f"Post-migration count: {post_count:,} rows (inserted: {actual_inserted:,})", "📊")

        if actual_inserted == total_processed:
            st.success(f"✅ Migration Verified! Inserted **{actual_inserted:,}** rows into `{target_table}` (total: {post_count:,})")
        else:
            st.warning(
                f"⚠️ Count Mismatch! Processed: {total_processed:,} | Actually in DB: {actual_inserted:,}  \n"
                f"Target now has {post_count:,} rows total."
            )
        if "last_migration_info" in st.session_state:
            st.session_state["last_migration_info"]["inserted"] = actual_inserted
            st.session_state["last_migration_info"]["post_count"] = post_count
    except Exception as e:
        st.success(f"✅ Migration Finished! Total Rows Processed: {total_processed:,}")
        add_log(f"Could not verify post-count: {e}", "⚠️")


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
