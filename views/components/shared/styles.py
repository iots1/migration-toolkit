"""
Shared Styles — global CSS injected once per page render.

Usage:
    from views.components.shared.styles import inject_global_css
    inject_global_css()
"""
import streamlit as st


def inject_global_css() -> None:
    """Injects custom CSS for buttons and dialogs globally."""
    st.markdown("""
        <style>
        .block-container {padding-top: 1rem;}

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
