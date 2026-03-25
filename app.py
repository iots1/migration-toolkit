import streamlit as st
import os
import database as db

# Import Views (legacy — to be migrated to controllers progressively)
from views import schema_mapper, migration_engine, file_explorer, er_diagram

# Import Controllers (MVC-refactored pages)
from controllers import settings_controller

# --- CONFIGURATION ---
st.set_page_config(page_title="HIS Migration Toolkit", layout="wide", page_icon="🏥")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# --- INITIALIZATION ---
db.init_db()

# --- UI LAYOUT ---
st.title("🏥 HIS Migration Toolkit Center")

with st.sidebar:
    st.header("Navigate")
    page = st.radio(
        "Go to", 
        [
            "📊 Schema Mapper", 
            "🚀 Migration Engine", 
            "🗺️ ER Diagram",
            "📁 File Explorer", 
            "⚙️ Datasource & Config"
        ]
    )
    st.divider()
    st.caption(f"📂 Root: {BASE_DIR}")
    st.caption("💾 Storage: SQLite")

# --- ROUTING ---
if page == "📊 Schema Mapper":
    schema_mapper.render_schema_mapper_page()
    
elif page == "🚀 Migration Engine":
    migration_engine.render_migration_engine_page()
    
elif page == "🗺️ ER Diagram":
    er_diagram.render_er_diagram_page()
    
elif page == "📁 File Explorer":
    file_explorer.render_file_explorer_page(BASE_DIR)
    
elif page == "⚙️ Datasource & Config":
    settings_controller.run()