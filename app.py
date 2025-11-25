import streamlit as st
import os
import database as db

# Import Views
from views import schema_mapper, migration_engine, file_explorer, settings

# --- CONFIGURATION ---
st.set_page_config(page_title="HIS Migration Toolkit", layout="wide", page_icon="🏥")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- INITIALIZATION ---
db.init_db()

# --- UI LAYOUT ---
st.title("🏥 HIS Migration Toolkit Center")

with st.sidebar:
    st.header("Navigate")
    page = st.radio("Go to", ["📊 Schema Mapper", "🚀 Migration Engine", "📁 File Explorer", "⚙️ Datasource & Config"])
    st.divider()
    st.caption(f"📂 Root: {BASE_DIR}")
    st.caption("💾 Storage: SQLite")

# --- ROUTING ---
if page == "📊 Schema Mapper":
    schema_mapper.render_schema_mapper_page()
    
elif page == "🚀 Migration Engine":
    migration_engine.render_migration_engine_page()
    
elif page == "📁 File Explorer":
    file_explorer.render_file_explorer_page(BASE_DIR)
    
elif page == "⚙️ Datasource & Config":
    settings.render_settings_page()