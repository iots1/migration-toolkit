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
    """
    try:
        if not config_data or 'mappings' not in config_data:
            return f"SELECT * FROM {source_table}"

        # เลือกเฉพาะ Column ที่ไม่ได้ถูก Ignore
        selected_cols = [
            mapping['source']
            for mapping in config_data.get('mappings', [])
            if not mapping.get('ignore', False)
        ]

        if not selected_cols:
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
            with open(log_file, "a", encoding="utf-8") as f:
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
            
            # Use SQLAlchemy Engine for Pandas
            src_engine = connector.create_sqlalchemy_engine(
                src_ds['db_type'], src_ds['host'], src_ds['port'], src_ds['dbname'], src_ds['username'], src_ds['password']
            )
            tgt_engine = connector.create_sqlalchemy_engine(
                tgt_ds['db_type'], tgt_ds['host'], tgt_ds['port'], tgt_ds['dbname'], tgt_ds['username'], tgt_ds['password']
            )
            
            add_log(f"   ✅ Connected to Source: {src_ds['db_type']} @ {src_ds['host']}")
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
            data_iterator = pd.read_sql(select_query, src_engine, chunksize=batch_size)
            
            total_rows_processed = 0
            batch_num = 0
            start_time = time.time()

            for df_batch in data_iterator:
                batch_num += 1
                rows_in_batch = len(df_batch)
                
                status_text.text(f"Processing Batch {batch_num} ({rows_in_batch} rows)...")
                add_log(f"   ▶ Batch {batch_num}: Fetched {rows_in_batch} rows")

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
                    bit_cast_types = ['MYSQL_BIT_CAST', 'POSTGRES_BIT_CAST', 'MSSQL_BIT_CAST']
                    for mapping in config.get('mappings', []):
                        if 'transformers' in mapping:
                            for bit_type in bit_cast_types:
                                if bit_type in mapping['transformers']:
                                    target_col = mapping.get('target', '').lower()
                                    if target_col:
                                        bit_columns.append((target_col, bit_type))
                                    break
                    
                    # แปลง Boolean/Integer → 0/1 สำหรับ BIT columns (จะใช้ CAST ใน SQL)
                    for col_info in bit_columns:
                        col = col_info[0] if isinstance(col_info, tuple) else col_info
                        if col in df_batch.columns:
                            # แปลง Boolean/Integer เป็น 0 หรือ 1
                            df_batch[col] = df_batch[col].apply(
                                lambda x: 1 if (x == True or x == 1 or x == '1') else 0
                            ).astype('int64')
                        
                except Exception as e:
                    add_log(f"     ⚠️ Transformation Warning in Batch {batch_num}: {e}")

                # --- B. LOAD (Bulk Insert) with BIT column handling ---
                try:
                    # ถ้ามี BIT columns ให้ใช้ custom method, ไม่งั้นใช้ method='multi'
                    if bit_columns:
                        # สร้าง dict สำหรับ lookup bit column types
                        bit_col_map = {col: cast_type for col, cast_type in bit_columns}
                        
                        # Custom insert method สำหรับ CAST integer → BIT
                        def bit_insert_method(table, conn, keys, data_iter):
                            from sqlalchemy import text
                            
                            # สร้าง INSERT statement พร้อม CAST สำหรับ BIT columns
                            columns = ', '.join([f'"{k}"' for k in keys])
                            placeholders = []
                            for k in keys:
                                if k in bit_col_map:
                                    cast_type = bit_col_map[k]
                                    # PostgreSQL ต้อง CAST, MySQL/MSSQL ไม่ต้อง
                                    if cast_type == 'POSTGRES_BIT_CAST':
                                        placeholders.append(f"CAST(:{k} AS BIT)")
                                    else:
                                        placeholders.append(f":{k}")
                                else:
                                    placeholders.append(f":{k}")
                            placeholders_str = ', '.join(placeholders)
                            
                            insert_stmt = f'INSERT INTO "{table.name}" ({columns}) VALUES ({placeholders_str})'
                            
                            # Execute batch insert
                            data_list = [dict(zip(keys, row)) for row in data_iter]
                            conn.execute(text(insert_stmt), data_list)
                        
                        df_batch.to_sql(
                            name=target_table,
                            con=tgt_engine,
                            if_exists='append',
                            index=False,
                            method=bit_insert_method,
                            chunksize=500 
                        )
                    else:
                        # ไม่มี BIT columns ใช้ method='multi' ธรรมดา
                        df_batch.to_sql(
                            name=target_table,
                            con=tgt_engine,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=500 
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
                with open(st.session_state.migration_log_file, "r", encoding="utf-8") as f:
                    st.download_button("📥 Download Log", data=f, file_name="migration.log")