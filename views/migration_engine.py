import streamlit as st
import json
import time
from datetime import datetime
from config import DB_TYPES
import services.db_connector as connector
import database as db
import pandas as pd
import sqlalchemy
from sqlalchemy import text

from services.checkpoint_manager import save_checkpoint, load_checkpoint, clear_checkpoint
from services.migration_logger import create_log_file as create_migration_log_file, write_log, read_log_file
from services.encoding_helper import clean_dataframe as clean_batch_encoding
from services.query_builder import build_select_query as generate_select_query, transform_batch, build_dtype_map, batch_insert

# --- Main Page Renderer ---

def render_migration_engine_page():
    st.subheader("🚀 Data Migration Execution Engine")

    # --- Session State Initialization ---
    if "migration_step" not in st.session_state: st.session_state.migration_step = 1
    if "migration_config" not in st.session_state: st.session_state.migration_config = None
    if "migration_src_profile" not in st.session_state: st.session_state.migration_src_profile = None
    if "migration_tgt_profile" not in st.session_state: st.session_state.migration_tgt_profile = None
    if "migration_src_ok" not in st.session_state: st.session_state.migration_src_ok = False
    if "migration_tgt_ok" not in st.session_state: st.session_state.migration_tgt_ok = False
    if "migration_test_sample" not in st.session_state: st.session_state.migration_test_sample = False
    if "truncate_target" not in st.session_state: st.session_state.truncate_target = False
    # Prevent hot reload re-execution
    if "migration_running" not in st.session_state: st.session_state.migration_running = False
    if "migration_completed" not in st.session_state: st.session_state.migration_completed = False
    # Resume from checkpoint
    if "resume_from_checkpoint" not in st.session_state: st.session_state.resume_from_checkpoint = False
    if "checkpoint_batch" not in st.session_state: st.session_state.checkpoint_batch = 0

    # ==========================================
    # STEP 1: Select Configuration
    # ==========================================
    if st.session_state.migration_step == 1:
        st.markdown("### Step 1: Select Configuration")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📚 Load from Project DB", use_container_width=True):
                st.session_state.migration_mode = "load_db"
                st.rerun()
        with col2:
            if st.button("📂 Upload JSON File", use_container_width=True):
                st.session_state.migration_mode = "upload_file"
                st.rerun()

        st.divider()

        if st.session_state.get("migration_mode") == "load_db":
            configs_df = db.get_configs_list()
            if not configs_df.empty:
                sel_config = st.selectbox("Select Saved Config", configs_df['config_name'])
                if st.button("Proceed to Connection Test", type="primary"):
                    st.session_state.migration_config = db.get_config_content(sel_config)
                    st.session_state.migration_step = 2
                    st.rerun()
            else:
                st.warning("No saved configurations found.")

        elif st.session_state.get("migration_mode") == "upload_file":
            uploaded = st.file_uploader("Upload .json config", type=["json"])
            if uploaded:
                st.session_state.migration_config = json.load(uploaded)
                if st.button("Proceed to Connection Test", type="primary"):
                    st.session_state.migration_step = 2
                    st.rerun()

    # ==========================================
    # STEP 2: Test Connections
    # ==========================================
    elif st.session_state.migration_step == 2:
        st.markdown("### Step 2: Verify Connections")
        datasources = db.get_datasources()
        ds_options = ["Select Profile..."] + datasources['name'].tolist()

        col_src, col_tgt = st.columns(2)

        with col_src:
            st.markdown("#### Source Database")
            src_sel = st.selectbox("Source Profile", ds_options, key="src_sel")
            st.session_state.migration_src_profile = src_sel
            
            charset_options = ["utf8mb4 (Default)", "tis620 (Thai Legacy)", "latin1 (Raw Bytes)"]
            src_charset_sel = st.selectbox(
                "Source Charset (ถ้าภาษาไทยเพี้ยนให้ลอง tis620)", 
                charset_options, 
                key="src_charset_sel"
            )
            charset_map = {
                "utf8mb4 (Default)": None,
                "tis620 (Thai Legacy)": "tis620",
                "latin1 (Raw Bytes)": "latin1"
            }
            st.session_state.src_charset = charset_map.get(src_charset_sel)
            
            if src_sel != "Select Profile...":
                if st.button("🔍 Test Source"):
                    with st.spinner("Connecting..."):
                        row = datasources[datasources['name'] == src_sel].iloc[0]
                        ds = db.get_datasource_by_id(int(row['id']))
                        ok, msg = connector.test_db_connection(ds['db_type'], ds['host'], ds['port'], ds['dbname'], ds['username'], ds['password'])
                        if ok: st.session_state.migration_src_ok = True
                        else: st.error(msg)
            if st.session_state.migration_src_ok: st.success("✅ Source Connected")

        with col_tgt:
            st.markdown("#### Target Database")
            tgt_sel = st.selectbox("Target Profile", ds_options, key="tgt_sel")
            st.session_state.migration_tgt_profile = tgt_sel
            if tgt_sel != "Select Profile...":
                if st.button("🔍 Test Target"):
                    with st.spinner("Connecting..."):
                        row = datasources[datasources['name'] == tgt_sel].iloc[0]
                        ds = db.get_datasource_by_id(int(row['id']))
                        ok, msg = connector.test_db_connection(ds['db_type'], ds['host'], ds['port'], ds['dbname'], ds['username'], ds['password'])
                        if ok: st.session_state.migration_tgt_ok = True
                        else: st.error(msg)
            if st.session_state.migration_tgt_ok: st.success("✅ Target Connected")

        st.divider()
        c1, c2 = st.columns([1, 4])
        if c1.button("← Back"):
            st.session_state.migration_step = 1
            st.rerun()
        if st.session_state.migration_src_ok and st.session_state.migration_tgt_ok:
            if c2.button("Next: Review & Execute →", type="primary", use_container_width=True):
                st.session_state.migration_step = 3
                st.rerun()

    # ==========================================
    # STEP 3: Review & Settings
    # ==========================================
    elif st.session_state.migration_step == 3:
        st.markdown("### Step 3: Review & Settings")
        config = st.session_state.migration_config
        config_name = config.get('config_name', 'migration')

        # Check for existing checkpoint
        checkpoint = load_checkpoint(config_name)

        with st.expander("📄 View Configuration JSON", expanded=False):
            st.json(config)

        # --- VALIDATION: ห้าม source == target ---
        src_db_cfg = config.get('source', {}).get('database', '')
        tgt_db_cfg = config.get('target', {}).get('database', '')
        src_tbl_cfg = config.get('source', {}).get('table', '')
        tgt_tbl_cfg = config.get('target', {}).get('table', '')

        _src_profile = st.session_state.get('migration_src_profile', '')
        _tgt_profile = st.session_state.get('migration_tgt_profile', '')
        _src_ds_row = db.get_datasource_by_name(_src_profile) if _src_profile else None
        _tgt_ds_row = db.get_datasource_by_name(_tgt_profile) if _tgt_profile else None

        _same_conn = (_src_ds_row and _tgt_ds_row and
                      _src_ds_row['host'] == _tgt_ds_row['host'] and
                      _src_ds_row['port'] == _tgt_ds_row['port'] and
                      _src_ds_row['dbname'] == _tgt_ds_row['dbname'])
        _same_table = src_tbl_cfg == tgt_tbl_cfg
        _is_self_migration = _same_conn and _same_table

        if _is_self_migration:
            st.error(f"🚨 **Source และ Target เป็นตารางเดียวกัน!**  \n"
                     f"`{src_tbl_cfg}` → `{tgt_tbl_cfg}` บน DB เดียวกัน  \n"
                     f"Migration จะ insert กลับเข้าหาตัวเอง — กรุณาแก้ไข config ก่อน")

        col_set1, col_set2 = st.columns(2)
        with col_set1:
            st.markdown("#### Mapping Summary")
            st.info(f"Source Table: **{config['source']['table']}**")
            st.info(f"Target Table: **{config['target']['table']}**")
            st.write(f"Columns Mapped: {len(config.get('mappings', []))}")

        with col_set2:
            st.markdown("#### Execution Settings")
            batch_size = st.number_input("Batch Size (Rows per chunk)", value=1000, step=500, min_value=100)
            st.session_state.batch_size = batch_size

            st.markdown("#### Data Options")
            st.session_state.truncate_target = st.checkbox(
                "🗑️ **Truncate Target Table** before starting",
                value=st.session_state.truncate_target,
                help="⚠️ WARNING: This will DELETE ALL DATA in the target table before migration begins.",
                disabled=checkpoint is not None and st.session_state.resume_from_checkpoint
            )

            st.session_state.migration_test_sample = st.checkbox(
                "🧪 **Test Mode** (Process only 1 batch)",
                value=st.session_state.migration_test_sample
            )
            if st.session_state.migration_test_sample:
                st.warning("Running in Test Mode: Migration will stop after the first batch.")

        # Show checkpoint resume option if exists
        if checkpoint:
            st.divider()
            st.warning(f"⚠️ **Previous migration was interrupted!**")
            col_ck1, col_ck2 = st.columns(2)
            with col_ck1:
                st.markdown(f"""
                - **Last Batch:** {checkpoint['last_batch']}
                - **Rows Processed:** {checkpoint['rows_processed']:,}
                - **Saved:** {checkpoint['timestamp']}
                """)
            with col_ck2:
                st.session_state.resume_from_checkpoint = st.checkbox(
                    "🔄 **Resume from checkpoint**",
                    value=st.session_state.resume_from_checkpoint,
                    help="Continue from where the migration stopped"
                )
                if st.button("🗑️ Clear Checkpoint", type="secondary"):
                    clear_checkpoint(config_name)
                    st.session_state.resume_from_checkpoint = False
                    st.success("Checkpoint cleared!")
                    st.rerun()

        st.divider()
        col_btn1, col_btn2 = st.columns([1, 4])
        with col_btn1:
            if st.button("← Back"):
                st.session_state.migration_step = 2
                st.rerun()
        with col_btn2:
            btn_label = "🔄 Resume Migration" if (checkpoint and st.session_state.resume_from_checkpoint) else "🚀 Start Migration Engine"
            if st.button(btn_label, type="primary", use_container_width=True, disabled=_is_self_migration):
                st.session_state.migration_running = False
                st.session_state.migration_completed = False
                if checkpoint and st.session_state.resume_from_checkpoint:
                    st.session_state.checkpoint_batch = checkpoint['last_batch']
                else:
                    st.session_state.checkpoint_batch = 0
                st.session_state.migration_step = 4
                st.rerun()

    # ==========================================
    # STEP 4: Execution (Improved UI)
    # ==========================================
    elif st.session_state.migration_step == 4:
        # Prevent hot reload from re-running migration
        if st.session_state.migration_running:
            st.warning("⏳ Migration is already running. Please wait...")
            st.info("If you believe this is stuck, click 'Start New Migration' below.")
            if st.button("🔄 Start New Migration", use_container_width=True):
                st.session_state.migration_running = False
                st.session_state.migration_completed = False
                st.session_state.migration_step = 1
                st.rerun()
            st.stop()

        if st.session_state.migration_completed:
            st.success("✅ Migration already completed!")
            if st.button("🔄 Start New Migration", use_container_width=True):
                st.session_state.migration_running = False
                st.session_state.migration_completed = False
                st.session_state.migration_step = 1
                st.rerun()
            st.stop()

        # Mark migration as running
        st.session_state.migration_running = True

        st.markdown("### ⚙️ Migration in Progress")

        # --- Metrics Dashboard ---
        col_m1, col_m2, col_m3 = st.columns(3)
        metric_processed = col_m1.metric("Rows Processed", "0")
        metric_batch = col_m2.metric("Current Batch", "0")
        metric_time = col_m3.metric("Elapsed Time", "0s")

        # --- Progress Bar ---
        progress_bar = st.progress(0)

        # --- Status Container & Logger ---
        with st.status("Initializing...", expanded=True) as status_box:
            log_container = st.empty()
            logs = []

            def add_log(msg, icon="ℹ️"):
                """Helper to append log to UI and file"""
                timestamp = datetime.now().strftime("%H:%M:%S")
                ui_msg = f"{icon} `[{timestamp}]` {msg}"
                logs.append(ui_msg)
                display_logs = logs[-20:] if len(logs) > 20 else logs
                log_container.markdown("\n\n".join(display_logs))
                if 'migration_log_file' in st.session_state:
                    write_log(st.session_state.migration_log_file, msg)

            try:
                config = st.session_state.migration_config
                config_name = config.get('config_name', 'migration')
                log_file = create_migration_log_file(config_name)
                st.session_state.migration_log_file = log_file

                # Get checkpoint info
                skip_batches = st.session_state.get('checkpoint_batch', 0)
                if skip_batches > 0:
                    add_log(f"Resuming from checkpoint: skipping first {skip_batches} batches", "🔄")

                add_log(f"Log File created: `{log_file}`", "📂")

                # Connect to DBs
                src_profile_name = st.session_state.migration_src_profile
                tgt_profile_name = st.session_state.migration_tgt_profile
                
                add_log("Connecting to databases...", "🔗")
                src_ds = db.get_datasource_by_name(src_profile_name)
                tgt_ds = db.get_datasource_by_name(tgt_profile_name)

                if not src_ds or not tgt_ds:
                    raise ValueError("Could not retrieve datasource credentials.")

                src_charset = st.session_state.get('src_charset', None)
                
                # FIX: Handle PostgreSQL compatibility with Thai legacy encoding
                if src_ds['db_type'] == 'PostgreSQL' and src_charset == 'tis620':
                    add_log("Auto-adjusting encoding: 'tis620' -> 'WIN874' (PostgreSQL Standard)", "🔧")
                    src_charset = 'WIN874'

                src_engine = connector.create_sqlalchemy_engine(
                    src_ds['db_type'], src_ds['host'], src_ds['port'], src_ds['dbname'], src_ds['username'], src_ds['password'],
                    charset=src_charset
                )
                tgt_engine = connector.create_sqlalchemy_engine(
                    tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password']
                )
                
                add_log(f"Source connected: {src_ds['db_type']} (charset: {src_charset or 'default'})", "✅")
                add_log(f"Target connected: {tgt_ds['db_type']}", "✅")

                target_table = config['target']['table']

                # --- PRE-MIGRATION COUNT (for verification & rollback) ---
                migration_start_time = datetime.now()
                pre_migration_count = 0
                try:
                    with tgt_engine.connect() as conn:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
                        pre_migration_count = result.scalar() or 0
                    add_log(f"Pre-migration count: {pre_migration_count:,} rows in `{target_table}`", "📊")
                    # Store for rollback
                    st.session_state['last_migration_info'] = {
                        'table': target_table,
                        'tgt_profile': tgt_profile_name,
                        'start_time': migration_start_time.isoformat(),
                        'pre_count': pre_migration_count
                    }
                except Exception as e:
                    add_log(f"Could not get pre-migration count (non-critical): {e}", "⚠️")

                # --- NEW: TRUNCATE EXECUTION ---
                if st.session_state.get('truncate_target', False):
                    add_log(f"Cleaning target table: {target_table}...", "🧹")
                    try:
                        with tgt_engine.begin() as conn:
                            conn.execute(text(f"TRUNCATE TABLE {target_table}"))
                        add_log("Target table truncated successfully.", "✅")
                    except Exception as e:
                        add_log(f"TRUNCATE failed, trying DELETE FROM... ({str(e)})", "⚠️")
                        try:
                            with tgt_engine.begin() as conn:
                                conn.execute(text(f"DELETE FROM {target_table}"))
                            add_log("Target table cleared using DELETE.", "✅")
                        except Exception as e2:
                            add_log(f"Failed to clean table: {str(e2)}", "❌")
                            raise e2

                # --- NEW: Schema Validation (Pre-flight check) ---
                add_log("Validating Schema Compatibility...", "🧐")
                try:
                    source_table = config['source']['table']
                    
                    src_inspector = sqlalchemy.inspect(src_engine)
                    tgt_inspector = sqlalchemy.inspect(tgt_engine)
                    
                    try:
                        src_parts = source_table.split('.')
                        tgt_parts = target_table.split('.')
                        src_t = src_parts[-1]
                        src_s = src_parts[0] if len(src_parts) > 1 else None
                        tgt_t = tgt_parts[-1]
                        tgt_s = tgt_parts[0] if len(tgt_parts) > 1 else None

                        src_col_defs = {col['name']: col['type'] for col in src_inspector.get_columns(src_t, schema=src_s)}
                        tgt_col_defs = {col['name']: col['type'] for col in tgt_inspector.get_columns(tgt_t, schema=tgt_s)}
                    except Exception as e:
                        src_col_defs = {col['name']: col['type'] for col in src_inspector.get_columns(source_table)}
                        tgt_col_defs = {col['name']: col['type'] for col in tgt_inspector.get_columns(target_table)}

                    warnings = []
                    for mapping in config.get('mappings', []):
                        if mapping.get('ignore', False): continue
                        
                        src_col = mapping['source']
                        tgt_col = mapping['target']
                        
                        if src_col in src_col_defs and tgt_col in tgt_col_defs:
                            src_type = src_col_defs[src_col]
                            tgt_type = tgt_col_defs[tgt_col]
                            
                            src_len = getattr(src_type, 'length', None)
                            tgt_len = getattr(tgt_type, 'length', None)
                            
                            if tgt_len is not None:
                                if src_len is None:
                                    warnings.append(f"- **{src_col}** (Unknown/Text) ➔ **{tgt_col}** (Limit: {tgt_len})")
                                elif src_len > tgt_len:
                                    warnings.append(f"- **{src_col}** (Limit: {src_len}) ➔ **{tgt_col}** (Limit: {tgt_len})")
                    
                    if warnings:
                        warn_msg_log = "⚠️ Potential Truncation Detected:\n" + "\n".join(warnings).replace("**", "")
                        # Log to memory so it appears BEFORE insert logs
                        add_log(warn_msg_log, "⚠️")
                        st.warning("⚠️ **Potential Truncation Detected!** check logs for details.")
                        time.sleep(1)
                    else:
                        add_log("Schema compatibility check passed.", "✅")

                except Exception as e:
                     add_log(f"Skipping schema check (Non-critical): {e}", "⚠️")

                # --- GENERATE_HN: Auto-detect max HN counter from target DB ---
                for mapping in config.get('mappings', []):
                    if mapping.get('ignore', False): continue
                    if 'GENERATE_HN' not in mapping.get('transformers', []): continue

                    ghn_params = mapping.get('transformer_params', {}).get('GENERATE_HN', {})
                    auto_detect = ghn_params.get('auto_detect_max', True)
                    start_from = int(ghn_params.get('start_from', 0))

                    if auto_detect:
                        target_hn_col = mapping.get('target', mapping.get('source'))
                        add_log(f"Auto-detecting max HN from `{target_table}.{target_hn_col}`...", "🔍")
                        try:
                            with tgt_engine.connect() as conn:
                                result = conn.execute(text(f'SELECT MAX("{target_hn_col}") FROM {target_table}'))
                                max_val = result.scalar()

                            if max_val:
                                # Parse numeric part from HN format e.g. "HN000000123" → 123
                                import re as _re
                                digits = _re.sub(r'\D', '', str(max_val))
                                start_from = int(digits) if digits else 0
                                add_log(f"Max HN found: `{max_val}` → counter starts at {start_from}", "✅")
                            else:
                                add_log(f"No existing HN in target (empty table) → counter starts at {start_from}", "ℹ️")
                        except Exception as e:
                            add_log(f"Auto-detect HN failed: {e} → using start_from={start_from}", "⚠️")

                    DataTransformer.reset_hn_counter(start_from)
                    add_log(f"HN Counter initialized at {start_from} (next HN: HN{str(start_from+1).zfill(9)})", "🔢")
                    break  # Only one GENERATE_HN per config

                # Prepare Query
                batch_size = st.session_state.batch_size
                select_query = generate_select_query(config, source_table, src_ds['db_type'])
                add_log(f"SELECT Query: {select_query}", "🔍")

                add_log(f"Starting Batch Processing (Size: {batch_size})...", "🚀")
                
                # Start Iteration
                data_iterator = pd.read_sql(
                    select_query, 
                    src_engine, 
                    chunksize=batch_size,
                    coerce_float=False
                )
                
                total_rows_processed = 0
                batch_num = 0
                start_time = time.time()
                migration_failed = False

                for df_batch in data_iterator:
                    batch_num += 1
                    rows_in_batch = len(df_batch)

                    # Skip batches if resuming from checkpoint
                    if batch_num <= skip_batches:
                        total_rows_processed += rows_in_batch
                        add_log(f"Batch {batch_num}: Skipped (checkpoint)", "⏭️")
                        continue

                    status_box.update(label=f"Processing Batch {batch_num} ({rows_in_batch} rows)...", state="running")

                    # Clean encoding issues
                    df_batch = clean_batch_encoding(df_batch)

                    # Transform batch
                    try:
                        df_batch, bit_columns = transform_batch(df_batch, config)
                    except Exception as e:
                        add_log(f"Transformation Error in Batch {batch_num}: {e}", "⚠️")
                        continue

                    # Insert batch
                    try:
                        dtype_map = build_dtype_map(bit_columns, df_batch, tgt_ds['db_type'])
                        batch_insert(df_batch, target_table, tgt_engine, dtype_map)

                        total_rows_processed += rows_in_batch
                        elapsed = time.time() - start_time

                        metric_processed.metric("Rows Processed", f"{total_rows_processed:,}")
                        metric_batch.metric("Current Batch", batch_num)
                        metric_time.metric("Elapsed Time", f"{elapsed:.1f}s")
                        progress_bar.progress(min(batch_num * 5, 95))

                        # Save checkpoint after each successful batch
                        save_checkpoint(config_name, batch_num, total_rows_processed)

                        add_log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

                    except Exception as e:
                        # Save checkpoint before failing
                        save_checkpoint(config_name, batch_num - 1, total_rows_processed)

                        error_msg_full = str(e)
                        short_error = error_msg_full.split("[SQL:")[0].strip() if "[SQL:" in error_msg_full else error_msg_full[:300]

                        add_log(f"Insert Failed: {short_error}", "❌")
                        add_log(f"Checkpoint saved at batch {batch_num - 1}", "💾")
                        st.error(f"Migration Failed at Batch {batch_num}: {short_error}")

                        col_err1, col_err2 = st.columns([1, 1])
                        with col_err1:
                            if st.button("🗑️ Emergency Truncate Target Table", key="emergency_truncate"):
                                try:
                                    with tgt_engine.begin() as conn:
                                        try:
                                            conn.execute(text(f"TRUNCATE TABLE {target_table}"))
                                        except Exception:
                                            conn.execute(text(f"DELETE FROM {target_table}"))
                                    st.success(f"Table '{target_table}' truncated!")
                                    add_log(f"User triggered Emergency Truncate on {target_table}", "🗑️")
                                except Exception as e_trunc:
                                    st.error(f"Failed to truncate: {e_trunc}")

                        with st.expander("🔴 View Full Error Details", expanded=False):
                            st.code(error_msg_full, language="sql")

                        status_box.update(label="Migration Failed", state="error", expanded=True)
                        st.session_state.migration_running = False
                        migration_failed = True
                        break

                    if st.session_state.migration_test_sample:
                        add_log("Stopping after first batch (Test Mode)", "🛑")
                        break

                # Loop finished (either naturally or by break in Test Mode)
                if not migration_failed:
                    progress_bar.progress(100)
                    status_box.update(label="Migration Complete!", state="complete", expanded=False)

                    # --- POST-MIGRATION VERIFICATION ---
                    try:
                        with tgt_engine.connect() as conn:
                            result = conn.execute(text(f"SELECT COUNT(*) FROM {target_table}"))
                            post_count = result.scalar() or 0
                        actual_inserted = post_count - pre_migration_count
                        add_log(f"Post-migration count: {post_count:,} rows (inserted: {actual_inserted:,})", "📊")

                        if actual_inserted == total_rows_processed:
                            st.success(f"✅ Migration Verified! Inserted **{actual_inserted:,}** rows into `{target_table}` (total: {post_count:,})")
                        else:
                            st.warning(f"⚠️ Count Mismatch! Processed: {total_rows_processed:,} | Actually in DB: {actual_inserted:,}  \n"
                                       f"Target now has {post_count:,} rows total.")
                        # Update rollback info with actual inserted count
                        if 'last_migration_info' in st.session_state:
                            st.session_state['last_migration_info']['inserted'] = actual_inserted
                            st.session_state['last_migration_info']['post_count'] = post_count
                    except Exception as e:
                        st.success(f"✅ Migration Finished! Total Rows Processed: {total_rows_processed:,}")
                        add_log(f"Could not verify post-count: {e}", "⚠️")

                    # Clear checkpoint on success
                    clear_checkpoint(config_name)
                    add_log("Checkpoint cleared (migration complete)", "🧹")
                    st.session_state.migration_completed = True
                    st.balloons()

                st.session_state.migration_running = False

            except Exception as e:
                st.session_state.migration_running = False
                status_box.update(label="Critical Error", state="error", expanded=True)
                st.error(f"Critical Error: {str(e)}")
                add_log(f"CRITICAL ERROR: {str(e)}", "💀")

        st.divider()
        col_end1, col_end2, col_end3 = st.columns(3)
        with col_end1:
            if st.button("🔄 Start New Migration", use_container_width=True):
                st.session_state.migration_running = False
                st.session_state.migration_completed = False
                st.session_state.resume_from_checkpoint = False
                st.session_state.checkpoint_batch = 0
                st.session_state.migration_step = 1
                st.rerun()

        with col_end2:
            # --- ROLLBACK BUTTON ---
            migration_info = st.session_state.get('last_migration_info')
            if migration_info:
                inserted = migration_info.get('inserted', 0)
                rb_label = f"🔙 Rollback ({inserted:,} rows)" if inserted else "🔙 Rollback Last Migration"
                if st.button(rb_label, type="secondary", use_container_width=True, help="ลบ rows ที่ insert ใน migration นี้"):
                    try:
                        rb_tgt_ds = db.get_datasource_by_name(migration_info['tgt_profile'])
                        rb_engine = connector.create_sqlalchemy_engine(
                            rb_tgt_ds['db_type'], rb_tgt_ds['host'], rb_tgt_ds['port'],
                            rb_tgt_ds['dbname'], rb_tgt_ds['username'], rb_tgt_ds['password']
                        )
                        rb_table = migration_info['table']
                        rb_start = migration_info['start_time']
                        rb_pre_count = migration_info['pre_count']

                        deleted = 0
                        with rb_engine.begin() as conn:
                            # Strategy 1: ลบโดย created_at >= migration_start_time
                            try:
                                result = conn.execute(text(
                                    f"DELETE FROM {rb_table} WHERE created_at >= :ts RETURNING *"
                                ), {"ts": rb_start})
                                deleted = result.rowcount
                                st.success(f"✅ Rollback สำเร็จ — ลบ {deleted:,} rows (created_at >= {rb_start[:19]})")
                            except Exception:
                                # Strategy 2: ลบโดย row count (ctid สูงสุด)
                                result = conn.execute(text(
                                    f"DELETE FROM {rb_table} WHERE ctid IN "
                                    f"(SELECT ctid FROM {rb_table} ORDER BY ctid DESC LIMIT :n)"
                                ), {"n": inserted})
                                deleted = result.rowcount
                                st.success(f"✅ Rollback สำเร็จ — ลบ {deleted:,} rows")

                        st.session_state.pop('last_migration_info', None)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Rollback failed: {e}")
            else:
                st.button("🔙 Rollback", disabled=True, use_container_width=True, help="ไม่มีข้อมูล migration ล่าสุด")

        with col_end3:
            log_content = read_log_file(st.session_state.get('migration_log_file'))
            if log_content:
                st.download_button("📥 Download Full Log", data=log_content, file_name="migration.log")