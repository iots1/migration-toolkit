import streamlit as st
import time

def inject_global_css():
    """Injects custom CSS for buttons and dialogs globally."""
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sarabun:wght@400;500&display=swap');

        .block-container {padding-top: 1rem;}

        /* Thai font support for data editor / dataframe */
        [data-testid="stDataFrame"] *,
        [data-testid="stDataEditor"] *,
        .dvn-scroller *,
        .gdg-cell,
        .gdg-growing-entry,
        canvas {
            font-family: 'Sarabun', 'Noto Sans Thai', sans-serif !important;
        }

        /* Thai HTML table (used in SQL preview result) */
        table.thai-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'Sarabun', 'Noto Sans Thai', sans-serif;
            font-size: 0.9rem;
        }
        table.thai-table th {
            background: #f0f2f6;
            padding: 6px 10px;
            text-align: left;
            border-bottom: 2px solid #d0d3da;
            white-space: nowrap;
        }
        table.thai-table td {
            padding: 5px 10px;
            border-bottom: 1px solid #e8eaed;
            word-break: break-word;
        }
        table.thai-table tr:hover td {
            background: #f7f8fc;
        }
        
        /* --- 1. Global Primary Button (Save/Add) -> Green Filled --- */
        div[data-testid="stButton"] > button[kind="primary"] {
            background-color: #28a745 !important;
            border-color: #28a745 !important; 
            color: white !important;
        }
        div[data-testid="stButton"] > button[kind="primary"]:hover {
            background-color: #218838 !important;
            border-color: #1e7e34 !important;
        }
        div[data-testid="stButton"] > button[kind="primary"]:focus {
            box-shadow: 0 0 0 0.2rem rgba(40, 167, 69, 0.5) !important;
        }

        /* --- 2. Dialog Primary Button (Delete/Confirm) -> Red Filled --- */
        /* Override specific to buttons inside dialogs */
        div[data-testid="stDialog"] button[kind="primary"] {
            background-color: #dc3545 !important;
            border: 1px solid #dc3545 !important;
            color: white !important;
        }
        div[data-testid="stDialog"] button[kind="primary"]:hover {
            background-color: #c82333 !important;
            border-color: #bd2130 !important;
        }

        /* --- 3. Dialog Secondary Button (Cancel) -> Outline --- */
        div[data-testid="stDialog"] button[kind="secondary"] {
            background-color: transparent !important;
            border: 1px solid #6c757d !important;
            color: #343a40 !important;
        }
        div[data-testid="stDialog"] button[kind="secondary"]:hover {
            background-color: #f8f9fa !important;
            border-color: #343a40 !important;
        }
        </style>
    """, unsafe_allow_html=True)

@st.dialog("Please Confirm")
def generic_confirm_dialog(title, message, confirm_label, on_confirm_func, *args, **kwargs):
    """
    Reusable confirmation dialog with Red Primary Button.
    """
    st.markdown(f"### {title}")
    st.write(message)
    st.write("") # Spacer
    
    # Layout: Spacer | Cancel | Confirm (Red)
    # ปรับสัดส่วนให้กว้างพอสำหรับคำว่า 'Delete Datasource' บรรทัดเดียว
    c_spacer, c_cancel, c_confirm = st.columns([2, 1, 2.5])
    
    with c_cancel:
        if st.button("Cancel", type="secondary", use_container_width=True):
            st.rerun()
            
    with c_confirm:
        # ปุ่มนี้จะเป็นสีแดงตาม CSS ที่เรา Override ไว้
        if st.button(confirm_label, type="primary", use_container_width=True):
            try:
                on_confirm_func(*args, **kwargs)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")