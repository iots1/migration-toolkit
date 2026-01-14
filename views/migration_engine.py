import streamlit as st
import json
import time
import os
from datetime import datetime
from config import DB_TYPES
import services.db_connector as connector
from services.transformers import DataTransformer
import database as db
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# --- Helper Functions ---

def generate_select_query(config_data, source_table):
    """
    Generate a SELECT query based on configuration.
    """
    try:
        if not config_data or 'mappings' not in config_data:
            return f"SELECT * FROM {source_table}"

        selected_cols = [
            mapping['source']
            for mapping in config_data.get('mappings', [])
            if not mapping.get('ignore', False)
            and 'GENERATE_HN' not in mapping.get('transformers', [])
        ]

        if not selected_cols:
            has_generate_hn = any(
                'GENERATE_HN' in mapping.get('transformers', [])
                for mapping in config_data.get('mappings', [])
                if not mapping.get('ignore', False)
            )
            if has_generate_hn:
                first_col = next(
                    (m['source'] for m in config_data.get('mappings', []) if not m.get('ignore', False)),
                    None
                )
                if first_col:
                    return f'SELECT "{first_col}" FROM {source_table}'
            return f"SELECT * FROM {source_table}"

        columns_str = ", ".join([f'"{col}"' for col in selected_cols])
        return f"SELECT {columns_str} FROM {source_table}"
    except Exception as e:
        return f"SELECT * FROM {source_table}"

def create_migration_log_file(config_name: str) -> str:
    """Create a unique log file for this migration run."""
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "migration_logs")
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in config_name)
        log_file = os.path.join(log_dir, f"migration_{safe_name}_{timestamp}.log")
        return log_file
    except Exception as e:
        return None

def write_log(log_file: str, message: str):
    """Write message to log file and flush immediately."""
    if log_file:
        try:
            with open(log_file, "a", encoding="utf-8", errors="replace") as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"[{timestamp}] {message}\n")
        except Exception as e:
            print(f"Error writing to log: {e}")


def clean_encoding_issues(value):
    """Clean encoding issues from a single value."""
    if value is None:
        return value
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError:
            try:
                value = value.decode('latin-1')
            except UnicodeDecodeError:
                value = str(value)
    if isinstance(value, str):
        value = value.replace('\xa0', ' ').replace('\x00', '').replace('\x85', '...')
        value = ''.join(c if c in '\t\n\r' or ord(c) >= 32 else '' for c in value)
    return value


def clean_batch_encoding(df_batch: pd.DataFrame) -> pd.DataFrame:
    """Clean encoding issues for all object columns in the batch."""
    for col in df_batch.select_dtypes(include=['object']).columns:
        df_batch[col] = df_batch[col].apply(clean_encoding_issues)
    return df_batch


def transform_batch(df_batch: pd.DataFrame, config: dict) -> tuple:
    """
    Apply transformations, rename columns, and prepare batch for insert.
    Returns (transformed_df, bit_columns).
    """
    df_batch = DataTransformer.apply_transformers_to_batch(df_batch, config)

    # Build rename map from source to target columns
    rename_map = {
        m['source']: m['target']
        for m in config.get('mappings', [])
        if not m.get('ignore', False) and 'target' in m and m['source'] in df_batch.columns
    }
    if rename_map:
        df_batch.rename(columns=rename_map, inplace=True)

    # Drop ignored columns
    ignored_cols = [m['target'] for m in config.get('mappings', []) if m.get('ignore', False)]
    df_batch = df_batch.drop(columns=[c for c in ignored_cols if c in df_batch.columns], errors='ignore')

    # Normalize column names
    df_batch.columns = df_batch.columns.str.lower()
    df_batch = df_batch.loc[:, ~df_batch.columns.duplicated(keep='first')]

    # Identify and convert BIT columns
    bit_columns = [
        mapping.get('target', '').lower()
        for mapping in config.get('mappings', [])
        if 'transformers' in mapping and 'BIT_CAST' in mapping['transformers'] and mapping.get('target')
    ]

    for col in bit_columns:
        if col in df_batch.columns:
            df_batch[col] = df_batch[col].apply(
                lambda x: '1' if x in (True, 1, '1') or str(x).lower() == 'true' else '0'
            )

    return df_batch, bit_columns


def build_dtype_map(bit_columns: list, df_batch: pd.DataFrame, db_type: str) -> dict:
    """Build SQLAlchemy dtype map for BIT columns based on target database type."""
    if not bit_columns:
        return {}

    dtype_map = {}
    if db_type == 'PostgreSQL':
        from sqlalchemy.dialects.postgresql import BIT
        for col in bit_columns:
            if col in df_batch.columns:
                dtype_map[col] = BIT(1)
    elif db_type == 'MySQL':
        from sqlalchemy.types import Integer
        for col in bit_columns:
            if col in df_batch.columns:
                dtype_map[col] = Integer()
    elif db_type == 'MSSQL':
        from sqlalchemy.dialects.mssql import BIT as MSSQL_BIT
        for col in bit_columns:
            if col in df_batch.columns:
                dtype_map[col] = MSSQL_BIT()
    return dtype_map


def batch_insert(df_batch: pd.DataFrame, target_table: str, engine, dtype_map: dict = None):
    """
    Batch insert using pandas to_sql with multi-row INSERT for speed.
    """
    if df_batch.empty:
        return 0

    df_batch.to_sql(
        name=target_table,
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500,
        dtype=dtype_map if dtype_map else None
    )
    return len(df_batch)

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
    # Checkbox state
    if "truncate_target" not in st.session_state: st.session_state.truncate_target = False

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
        with st.expander("📄 View Configuration JSON", expanded=False):
            st.json(config)

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
            
            # --- NEW: TRUNCATE OPTION ---
            st.markdown("#### Data Options")
            st.session_state.truncate_target = st.checkbox(
                "🗑️ **Truncate Target Table** before starting", 
                value=st.session_state.truncate_target,
                help="⚠️ WARNING: This will DELETE ALL DATA in the target table before migration begins."
            )
            
            st.session_state.migration_test_sample = st.checkbox(
                "🧪 **Test Mode** (Process only 1 batch)", 
                value=st.session_state.migration_test_sample
            )
            if st.session_state.migration_test_sample:
                st.warning("Running in Test Mode: Migration will stop after the first batch.")

        st.divider()
        if st.button("🚀 Start Migration Engine", type="primary", use_container_width=True):
            st.session_state.migration_step = 4
            st.rerun()

    # ==========================================
    # STEP 4: Execution (Improved UI)
    # ==========================================
    elif st.session_state.migration_step == 4:
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
                # Format for UI (Markdown)
                ui_msg = f"{icon} `[{timestamp}]` {msg}"
                logs.append(ui_msg)
                
                # Show only last 20 lines in the preview to keep UI clean
                display_logs = logs[-20:] if len(logs) > 20 else logs
                log_container.markdown("\n\n".join(display_logs))
                
                # Write plain text to file
                if 'migration_log_file' in st.session_state:
                    file_msg = f"{msg}" # Timestamp is added in write_log
                    write_log(st.session_state.migration_log_file, file_msg)

            try:
                config = st.session_state.migration_config
                log_file = create_migration_log_file(config.get('config_name', 'migration'))
                st.session_state.migration_log_file = log_file
                
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

                # Prepare Query
                batch_size = st.session_state.batch_size
                select_query = generate_select_query(config, source_table)
                
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

                        add_log(f"Batch {batch_num}: Inserted {rows_in_batch} rows", "💾")

                    except Exception as e:
                        error_msg_full = str(e)
                        short_error = error_msg_full.split("[SQL:")[0].strip() if "[SQL:" in error_msg_full else error_msg_full[:300]

                        add_log(f"Insert Failed: {short_error}", "❌")
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
                        migration_failed = True
                        break

                    if st.session_state.migration_test_sample:
                        add_log("Stopping after first batch (Test Mode)", "🛑")
                        break
            
                # Loop finished (either naturally or by break in Test Mode)
                if not migration_failed:
                    # Finish Progress Bar
                    progress_bar.progress(100)
                    status_box.update(label="Migration Complete!", state="complete", expanded=False)
                    st.success(f"✅ Migration Finished Successfully! Total Rows: {total_rows_processed}")
                    st.balloons()
            
            except Exception as e:
                status_box.update(label="Critical Error", state="error", expanded=True)
                st.error(f"Critical Error: {str(e)}")
                add_log(f"CRITICAL ERROR: {str(e)}", "💀")

        st.divider()
        col_end1, col_end2 = st.columns(2)
        with col_end1:
            if st.button("🔄 Start New Migration", use_container_width=True):
                st.session_state.migration_step = 1
                st.rerun()
        with col_end2:
            if st.session_state.migration_log_file and os.path.exists(st.session_state.migration_log_file):
                log_content = None
                for encoding in ['utf-8', 'cp874', 'tis-620', 'latin-1']:
                    try:
                        with open(st.session_state.migration_log_file, "r", encoding=encoding, errors="replace") as f:
                            log_content = f.read()
                        break
                    except: continue
                
                if log_content:
                    st.download_button("📥 Download Full Log", data=log_content, file_name="migration.log")