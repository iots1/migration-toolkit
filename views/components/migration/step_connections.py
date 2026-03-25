"""
Step 2 — Connection Testing.

Lets the user pick source/target datasource profiles and verify connectivity.
Updates session_state:
    migration_src_profile   str (datasource display name)
    migration_tgt_profile   str
    migration_src_ok        bool
    migration_tgt_ok        bool
    src_charset             str | None
    migration_step          → 1 (back) | 3 (next)
"""
import streamlit as st
import database as db
import services.db_connector as connector

_CHARSET_MAP = {
    "utf8mb4 (Default)": None,
    "tis620 (Thai Legacy)": "tis620",
    "latin1 (Raw Bytes)": "latin1",
}


def render_step_connections() -> None:
    st.markdown("### Step 2: Verify Connections")

    datasources = db.get_datasources()
    ds_options = ["Select Profile..."] + datasources["name"].tolist()

    col_src, col_tgt = st.columns(2)

    with col_src:
        _render_source_panel(datasources, ds_options)

    with col_tgt:
        _render_target_panel(datasources, ds_options)

    st.divider()
    c1, c2 = st.columns([1, 4])
    if c1.button("← Back"):
        st.session_state.migration_step = 1
        st.rerun()
    if st.session_state.migration_src_ok and st.session_state.migration_tgt_ok:
        if c2.button("Next: Review & Execute →", type="primary", use_container_width=True):
            st.session_state.migration_step = 3
            st.rerun()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _render_source_panel(datasources, ds_options) -> None:
    st.markdown("#### Source Database")
    src_sel = st.selectbox("Source Profile", ds_options, key="src_sel")
    st.session_state.migration_src_profile = src_sel

    charset_sel = st.selectbox(
        "Source Charset (ถ้าภาษาไทยเพี้ยนให้ลอง tis620)",
        list(_CHARSET_MAP.keys()),
        key="src_charset_sel",
    )
    st.session_state.src_charset = _CHARSET_MAP[charset_sel]

    if src_sel != "Select Profile...":
        if st.button("🔍 Test Source"):
            with st.spinner("Connecting..."):
                row = datasources[datasources["name"] == src_sel].iloc[0]
                ds = db.get_datasource_by_id(int(row["id"]))
                ok, msg = connector.test_db_connection(
                    ds["db_type"], ds["host"], ds["port"],
                    ds["dbname"], ds["username"], ds["password"],
                )
                if ok:
                    st.session_state.migration_src_ok = True
                else:
                    st.error(msg)
    if st.session_state.migration_src_ok:
        st.success("✅ Source Connected")


def _render_target_panel(datasources, ds_options) -> None:
    st.markdown("#### Target Database")
    tgt_sel = st.selectbox("Target Profile", ds_options, key="tgt_sel")
    st.session_state.migration_tgt_profile = tgt_sel

    if tgt_sel != "Select Profile...":
        if st.button("🔍 Test Target"):
            with st.spinner("Connecting..."):
                row = datasources[datasources["name"] == tgt_sel].iloc[0]
                ds = db.get_datasource_by_id(int(row["id"]))
                ok, msg = connector.test_db_connection(
                    ds["db_type"], ds["host"], ds["port"],
                    ds["dbname"], ds["username"], ds["password"],
                )
                if ok:
                    st.session_state.migration_tgt_ok = True
                else:
                    st.error(msg)
    if st.session_state.migration_tgt_ok:
        st.success("✅ Target Connected")
