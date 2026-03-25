"""
State Manager — centralised session_state helpers.

Replaces the scattered `if "key" not in st.session_state` boilerplate
and the `_mapper_needs_rerun` deferred-rerun pattern.
"""
import streamlit as st


class PageState:
    """Utility for initialising and managing Streamlit session_state keys."""

    @staticmethod
    def init(defaults: dict) -> None:
        """Set session_state keys that are not yet initialised."""
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    @staticmethod
    def get(key: str, default=None):
        return st.session_state.get(key, default)

    @staticmethod
    def set(key: str, value) -> None:
        st.session_state[key] = value

    @staticmethod
    def pop(key: str, default=None):
        return st.session_state.pop(key, default)

    @staticmethod
    def trigger_rerun(key: str = "_needs_rerun") -> None:
        """Mark that a rerun should happen after leaving column context."""
        st.session_state[key] = True

    @staticmethod
    def flush_rerun(key: str = "_needs_rerun") -> None:
        """If a rerun was requested, execute it now (call outside column context)."""
        if st.session_state.pop(key, False):
            st.rerun()
