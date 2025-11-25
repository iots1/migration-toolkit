import streamlit as st
import os
from config import ANALYSIS_DIR

def render_file_explorer_page(base_dir):
    st.subheader("Project Files")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📂 Analysis Report")
        if os.path.exists(ANALYSIS_DIR): 
            files = os.listdir(ANALYSIS_DIR)
            st.code("\n".join(files))
        else:
            st.info("No analysis report directory found.")

    with col2:
        st.markdown("### 📂 Mini HIS (Mockup)")
        mini_his_dir = os.path.join(base_dir, "mini_his")
        if os.path.exists(mini_his_dir): 
            files = os.listdir(mini_his_dir)
            st.code("\n".join(files))
        else:
            st.info("No mini_his directory found.")