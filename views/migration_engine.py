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

# --- Helper Functions ---

def generate_select_query(config_data, source_table):
    """
    Generate a SELECT query based on configuration.
    It selects specific columns to minimize data transfer overhead.
    Excludes columns that use GENERATE_HN transformer since they don't need source data.
    """
    try:
        if not config_data or 'mappings' not in config_data:
            return f"SELECT * FROM {source_table}"

        # เลือกเฉพาะ Column ที่ไม่ได้ถูก Ignore และไม่ใช้ GENERATE_HN transformer
        selected_cols = [
            mapping['source']
            for mapping in config_data.get('mappings', [])
            if not mapping.get('ignore', False)
            and 'GENERATE_HN' not in mapping.get('transformers', [])
        ]

        # If no columns selected (all ignored or GENERATE_HN), select all to maintain row count
        # GENERATE_HN needs to know how many rows to generate
        if not selected_cols:
            # Check if there are any GENERATE_HN columns
            has_generate_hn = any(
                'GENERATE_HN' in mapping.get('transformers', [])
                for mapping in config_data.get('mappings', [])
                if not mapping.get('ignore', False)
            )
            if has_generate_hn:
                # Select first non-ignored column or use * to maintain row count
                first_col = next(
                    (m['source'] for m in config_data.get('mappings', []) if not m.get('ignore', False)),
                    None
                )
                if first_col:
                    return f'SELECT "{first_col}" FROM {source_table}'
            return f"SELECT * FROM {source_table}"

        # สร้าง Query String (ใส่ Quote เพื่อรองรับชื่อ column ที่มี space หรือ keyword)
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
            
            # Charset option for legacy Thai databases
            charset_options = ["utf8mb4 (Default)", "tis620 (Thai Legacy)", "latin1 (Raw Bytes)"]
            src_charset_sel = st.selectbox(
                "Source Charset (ถ้าภาษาไทยเพี้ยนให้ลอง tis620)", 
                charset_options, 
                key="src_charset_sel"
            )
            # Map selection to actual charset value
            charset_map = {
                "utf8mb4 (Default)": None,
                "tis620 (Thai Legacy)": "tis620",
                "latin1 (Raw Bytes)": "latin1"
            }
            st.session_state.src_charset = charset_map.get(src_charset_sel)
            
            if src_sel != "Select Profile...":
                if st.button("🔍 Test Source"):
                    with st.spinner("Connecting..."):
                        # Using existing test function
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
    # STEP 4: Execution (Real Batch Processing)
    # ==========================================
    elif st.session_state.migration_step == 4:
        st.markdown("### ⚙️ Migration in Progress")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.container()
        log_placeholder = log_container.empty()
        logs = []

        def add_log(msg):
            logs.append(msg)
            # Fix: Added explicit label "Log Output" to prevent Streamlit error
            log_str = "\n".join(logs)
            log_placeholder.text_area("Log Output", value=log_str, height=300, disabled=True, label_visibility="visible")
            if 'migration_log_file' in st.session_state:
                write_log(st.session_state.migration_log_file, msg)

        try:
            config = st.session_state.migration_config
            log_file = create_migration_log_file(config.get('config_name', 'migration'))
            st.session_state.migration_log_file = log_file
            
            add_log(f"[{datetime.now().time()}] 🚀 Initialization started")
            add_log(f"   Log File: {log_file}")

            # 2. Connect to DBs
            src_profile_name = st.session_state.migration_src_profile
            tgt_profile_name = st.session_state.migration_tgt_profile
            
            add_log(f"[{datetime.now().time()}] 🔗 Fetching credentials...")
            src_ds = db.get_datasource_by_name(src_profile_name)
            tgt_ds = db.get_datasource_by_name(tgt_profile_name)

            if not src_ds or not tgt_ds:
                raise ValueError("Could not retrieve datasource credentials.")

            add_log(f"[{datetime.now().time()}] 🔗 Creating Database Engines...")
            
            # Get charset from session state (set in Step 2)
            src_charset = st.session_state.get('src_charset', None)
            
            # Use SQLAlchemy Engine for Pandas
            src_engine = connector.create_sqlalchemy_engine(
                src_ds['db_type'], src_ds['host'], src_ds['port'], src_ds['dbname'], src_ds['username'], src_ds['password'],
                charset=src_charset
            )
            tgt_engine = connector.create_sqlalchemy_engine(
                tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password']
            )
            
            add_log(f"   ✅ Connected to Source: {src_ds['db_type']} @ {src_ds['host']} (charset: {src_charset or 'default'})")
            add_log(f"   ✅ Connected to Target: {tgt_ds['db_type']} @ {tgt_ds['host']}")

            # 3. Prepare Query & Parameters
            source_table = config['source']['table']
            target_table = config['target']['table']
            batch_size = st.session_state.batch_size
            
            select_query = generate_select_query(config, source_table)
            add_log(f"   📝 Generated Query: {select_query}")
            
            # 4. START BATCH PROCESSING
            add_log(f"[{datetime.now().time()}] 🔄 Starting REAL Data Transfer...")
            
            # Use pd.read_sql with chunksize -> Returns an Iterator
            # coerce_float=False ป้องกัน pandas แปลง numeric ผิด
            data_iterator = pd.read_sql(
                select_query, 
                src_engine, 
                chunksize=batch_size,
                coerce_float=False  # ป้องกัน auto-convert ที่อาจทำให้ encoding พัง
            )
            
            total_rows_processed = 0
            batch_num = 0
            start_time = time.time()

            for df_batch in data_iterator:
                batch_num += 1
                rows_in_batch = len(df_batch)
                
                status_text.text(f"Processing Batch {batch_num} ({rows_in_batch} rows)...")
                add_log(f"   ▶ Batch {batch_num}: Fetched {rows_in_batch} rows")

                # --- FIX: Clean problematic characters (0xa0 = non-breaking space, etc.) ---
                # แปลง non-breaking space และ special bytes เป็น space ปกติ
                # รองรับทั้ง str และ bytes ที่อาจ decode ไม่ได้
                def clean_encoding_issues(x):
                    if x is None:
                        return x
                    if isinstance(x, bytes):
                        # ถ้าเป็น bytes ให้ decode ด้วย utf-8 หรือ latin-1 fallback
                        try:
                            x = x.decode('utf-8')
                        except (UnicodeDecodeError, AttributeError):
                            try:
                                x = x.decode('latin-1')  # latin-1 รองรับทุก byte 0x00-0xFF
                            except:
                                x = str(x)
                    if isinstance(x, str):
                        # แทนที่ problematic characters
                        x = x.replace('\xa0', ' ')  # non-breaking space → space
                        x = x.replace('\x00', '')   # null byte → remove
                        x = x.replace('\x85', '...')  # NEL → ellipsis
                        x = x.replace('\x96', '-')   # en-dash → hyphen
                        x = x.replace('\x97', '-')   # em-dash → hyphen
                        # ลบ control characters อื่นๆ (0x00-0x1F ยกเว้น tab, newline)
                        x = ''.join(c if c in '\t\n\r' or ord(c) >= 32 else '' for c in x)
                    return x
                
                for col in df_batch.select_dtypes(include=['object']).columns:
                    df_batch[col] = df_batch[col].apply(clean_encoding_issues)

                # --- A. TRANSFORM (In-Memory) ---
                try:
                    df_batch = DataTransformer.apply_transformers_to_batch(df_batch, config)
                    
                    # Rename Column to match Target
                    rename_map = {}
                    for m in config.get('mappings', []):
                        if not m.get('ignore', False) and 'target' in m and m['source'] in df_batch.columns:
                            rename_map[m['source']] = m['target']
                    if rename_map:
                        df_batch.rename(columns=rename_map, inplace=True)
                    
                    # ลบ column ที่ถูก ignore
                    ignored_cols = [m['target'] for m in config.get('mappings', []) if m.get('ignore', False)]
                    df_batch = df_batch.drop(columns=[c for c in ignored_cols if c in df_batch.columns], errors='ignore')
                    
                    # บังคับ lowercase ทุก column เพื่อรองรับ PostgreSQL (case-sensitive)
                    df_batch.columns = df_batch.columns.str.lower()
                    
                    # ตรวจสอบและลบ duplicate columns
                    if df_batch.columns.duplicated().any():
                        dup_cols = df_batch.columns[df_batch.columns.duplicated()].tolist()
                        df_batch = df_batch.loc[:, ~df_batch.columns.duplicated(keep='first')]

                    # อ่าน BIT columns จาก config (ตรวจหา BIT_CAST transformers)
                    bit_columns = []    
                    for mapping in config.get('mappings', []):
                        if 'transformers' in mapping and 'BIT_CAST' in mapping['transformers']:
                            target_col = mapping.get('target', '').lower()
                            if target_col:
                                bit_columns.append(target_col)
                    
                    # แปลง Boolean/Integer → '0'/'1' string สำหรับ BIT columns (PostgreSQL ต้องการ string)
                    for col in bit_columns:
                        if col in df_batch.columns:
                            # แปลง Boolean/Integer เป็น '1' หรือ '0' (string) สำหรับ PostgreSQL BIT
                            df_batch[col] = df_batch[col].apply(
                                lambda x: '1' if (x == True or x == 1 or x == '1' or str(x).lower() == 'true') else '0'
                            )
                        
                except Exception as e:
                    add_log(f"     ⚠️ Transformation Warning in Batch {batch_num}: {e}")

                # --- B. LOAD (Bulk Insert) ---
                try:
                    # สร้าง dtype mapping สำหรับ BIT columns
                    from sqlalchemy import text
                    from sqlalchemy.types import String
                    
                    dtype_map = {}
                    if bit_columns:
                        if tgt_ds['db_type'] == 'PostgreSQL':
                            # PostgreSQL: ใช้ BIT type
                            from sqlalchemy.dialects.postgresql import BIT
                            for col in bit_columns:
                                if col in df_batch.columns:
                                    dtype_map[col] = BIT(1)
                        elif tgt_ds['db_type'] == 'MySQL':
                            # MySQL: ใช้ BIT type
                            from sqlalchemy.types import Integer
                            for col in bit_columns:
                                if col in df_batch.columns:
                                    dtype_map[col] = Integer()  # MySQL BIT(1) = TINYINT(1)
                        elif tgt_ds['db_type'] == 'MSSQL':
                            # MSSQL: ใช้ BIT type
                            from sqlalchemy.dialects.mssql import BIT as MSSQL_BIT
                            for col in bit_columns:
                                if col in df_batch.columns:
                                    dtype_map[col] = MSSQL_BIT()
                    
                    df_batch.to_sql(
                        name=target_table,
                        con=tgt_engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=500,
                        dtype=dtype_map if dtype_map else None
                    )
                    total_rows_processed += rows_in_batch
                    add_log(f"     💾 Inserted {rows_in_batch} rows successfully")
                    
                except Exception as e:
                    st.error(f"Insert Failed: {e}")
                    add_log(f"     ❌ Insert Failed: {e}")
                    break 

                prog = min(batch_num * 5, 95)
                progress_bar.progress(prog)

                if st.session_state.migration_test_sample:
                    add_log("   🛑 Stopping after first batch (Test Mode Enabled)")
                    break
            
            end_time = time.time()
            duration = end_time - start_time
            
            progress_bar.progress(100)
            status_text.success("Migration Complete!")
            st.success(f"✅ Migration Finished Successfully in {duration:.2f} seconds")
            add_log(f"SUMMARY: Total Records: {total_rows_processed}")
            st.balloons()

        except Exception as e:
            st.error(f"Critical Error: {str(e)}")
            add_log(f"❌ CRITICAL ERROR: {str(e)}")

        st.divider()
        col_end1, col_end2 = st.columns(2)
        with col_end1:
            if st.button("🔄 Start New Migration", use_container_width=True):
                st.session_state.migration_step = 1
                st.rerun()
        with col_end2:
            if st.session_state.migration_log_file and os.path.exists(st.session_state.migration_log_file):
                # ลองอ่านด้วย utf-8 ก่อน ถ้าไม่ได้ให้ลอง cp874 (Windows-874 สำหรับภาษาไทย)
                log_content = None
                for encoding in ['utf-8', 'cp874', 'tis-620', 'latin-1']:
                    try:
                        with open(st.session_state.migration_log_file, "r", encoding=encoding, errors="replace") as f:
                            log_content = f.read()
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                if log_content is None:
                    # Fallback: อ่านแบบ binary แล้ว decode ด้วย errors='ignore'
                    with open(st.session_state.migration_log_file, "rb") as f:
                        log_content = f.read().decode('utf-8', errors='ignore')
                st.download_button("📥 Download Log", data=log_content, file_name="migration.log")