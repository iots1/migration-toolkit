import streamlit as st
import os
from dotenv import load_dotenv
import database as db

# Import Controllers (MVC-refactored pages)
from controllers import (
    settings_controller,
    pipeline_controller,
    file_explorer_controller,
    er_diagram_controller,
    schema_mapper_controller,
    migration_engine_controller,
)

# --- CONFIGURATION ---
st.set_page_config(page_title="HIS Migration Toolkit", layout="wide", page_icon="🏥")

# Load environment variables from .env file
load_dotenv()

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
            "🔗 Data Pipeline",
            "🗺️ ER Diagram",
            "📁 File Explorer",
            "⚙️ Datasource & Config"
        ]
    )
    st.divider()
    st.caption(f"📂 Root: {BASE_DIR}")
    st.caption("💾 Storage: PostgreSQL")

# --- ROUTING ---
if page == "📊 Schema Mapper":
    schema_mapper_controller.run()

elif page == "🚀 Migration Engine":
    migration_engine_controller.run()

elif page == "🔗 Data Pipeline":
    pipeline_controller.run()

elif page == "🗺️ ER Diagram":
    er_diagram_controller.run()

elif page == "📁 File Explorer":
    file_explorer_controller.run()

elif page == "⚙️ Datasource & Config":
    settings_controller.run()