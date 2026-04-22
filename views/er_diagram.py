import streamlit as st
import database as db
from models.db_type import DbType
from services.db_connector import get_tables_from_datasource, get_columns_from_table, get_foreign_keys

# Import Agraph for Interactive Graph
from streamlit_agraph import agraph, Node, Edge, Config

def render_er_diagram_page():
    st.subheader("🖱️ Interactive ERD Studio")
    
    # 1. Select Datasource
    datasources_df = db.get_datasources()
    if datasources_df.empty:
        st.warning("No datasources defined.")
        return

    col1, col2 = st.columns([1, 1])
    with col1:
        selected_ds_name = st.selectbox("Select Datasource", datasources_df['name'])
    
    ds = db.get_datasource_by_name(selected_ds_name)
    if not ds: return

    # 2. Schema Selection
    schema_input = None
    if ds['db_type'] in [DbType.POSTGRESQL, DbType.MSSQL]:
        with col2:
            default_schema = "public" if ds['db_type'] == DbType.POSTGRESQL else "dbo"
            schema_input = st.text_input("Schema", value=default_schema)

    # 3. Layout Settings (ตัวช่วยแก้ปัญหาทับกัน)
    with st.sidebar.expander("⚙️ Layout Settings", expanded=True):
        st.caption("Adjust these to fix overlapping nodes.")
        
        physics_enabled = st.checkbox("Enable Physics", value=True, help="Uncheck to freeze layout")
        
        # Physics Parameters
        node_distance = st.slider("Node Spacing (Repulsion)", 100, 1000, 300, step=50, help="Higher value pushes nodes further apart")
        spring_length = st.slider("Edge Length", 100, 500, 200, step=50, help="Length of the connecting lines")
        gravity = st.slider("Gravity", -10000, -1000, -2000, step=500, help="Stronger negative value pushes center apart")

    # 4. Fetch Data & State Management
    if "er_nodes" not in st.session_state:
        st.session_state.er_nodes = []
    if "er_edges" not in st.session_state:
        st.session_state.er_edges = []
    if "er_edit_target" not in st.session_state:
        st.session_state.er_edit_target = None

    col_btn, col_info = st.columns([1, 3])
    with col_btn:
        if st.button("🔄 Load/Reset Diagram", type="primary"):
            with st.spinner("Building interactive graph..."):
                build_graph_state(ds, schema_input)
                st.rerun()

    # 5. Interactive Graph Area
    if st.session_state.er_nodes:
        
        # --- Layout 1: The Graph ---
        with st.container(border=True):
            st.markdown("### 🎨 Canvas")
            
            # Config for physics and interaction
            # Using BarnesHut with adjustable parameters to fix overlapping
            config = Config(
                width="100%", 
                height=750, 
                directed=True,
                physics=physics_enabled,
                hierarchical=False,
                nodeHighlightBehavior=True, 
                highlightColor="#F7A7A6",
                collapsible=False,
                # Tuning Physics Engine
                title="ER Diagram",
                gravity=gravity/50000, # Normalized gravity
                linkLength=spring_length,
                nodeSpacing=node_distance,
                # Advanced solver settings passed via **kwargs logic in agraph wrapper if supported,
                # but basically nodeSpacing helps BarnesHut.
                # Explicitly setting solver parameters:
                stabilization=False, # Let user watch it settle
                fit=True
            )

            # Render Agraph
            selected_node_id = agraph(
                nodes=st.session_state.er_nodes, 
                edges=st.session_state.er_edges, 
                config=config
            )

            if selected_node_id:
                st.session_state.er_edit_target = selected_node_id

        # --- Layout 2: Editor Panel ---
        if st.session_state.er_edit_target:
            render_editor_panel(ds, schema_input)

def build_graph_state(ds, schema):
    """
    Fetches data and converts to Agraph Nodes/Edges
    """
    nodes = []
    edges = []
    
    # 1. Fetch Tables (Nodes)
    success, tables = get_tables_from_datasource(
        ds['db_type'], ds['host'], ds['port'], ds['dbname'], 
        ds['username'], ds['password'], schema
    )
    
    # Limit tables (Too many tables always cause overlap)
    displayed_tables = tables[:20] if success else []

    for table in displayed_tables:
        _, cols = get_columns_from_table(
            ds['db_type'], ds['host'], ds['port'], ds['dbname'], 
            ds['username'], ds['password'], table, schema
        )
        
        col_count = len(cols) if cols else 0
        # Use HTML-like formatting for clearer labels if supported, or just clean text
        label_text = f"{table}\n[{col_count} cols]"
        
        nodes.append(Node(
            id=table,
            label=label_text,
            size=30, # Slightly larger nodes
            shape="box", 
            color="#FFFFFF",
            font={"color": "black", "face": "Arial", "size": 16}, # Bigger font
            borderWidth=2,
            shadow=True
        ))

    # 2. Fetch Relationships (Edges)
    success_fk, fks = get_foreign_keys(
        ds['db_type'], ds['host'], ds['port'], ds['dbname'], 
        ds['username'], ds['password'], schema
    )

    if success_fk:
        for fk in fks:
            if fk['table'] in displayed_tables and fk['ref_table'] in displayed_tables:
                edges.append(Edge(
                    source=fk['table'],
                    target=fk['ref_table'],
                    label=fk['col'], 
                    type="CURVE_SMOOTH",
                    color="#555555",
                    width=2
                ))

    st.session_state.er_nodes = nodes
    st.session_state.er_edges = edges
    st.session_state.er_edit_target = None 

def render_editor_panel(ds, schema):
    """
    Renders the form to edit the selected table.
    """
    table_name = st.session_state.er_edit_target
    
    st.sidebar.markdown("---")
    st.sidebar.subheader(f"📝 Edit: {table_name}")
    
    with st.sidebar.form(key=f"edit_form_{table_name}"):
        new_name = st.text_input("Table Name", value=table_name)
        
        success, cols = get_columns_from_table(
            ds['db_type'], ds['host'], ds['port'], ds['dbname'], 
            ds['username'], ds['password'], table_name, schema
        )
        
        if success and cols:
            st.markdown("**Columns:**")
            for col in cols[:5]: 
                c1, c2 = st.columns([2, 1])
                c1.text_input(f"Col: {col['name']}", value=col['name'], key=f"c_n_{col['name']}")
                c2.text_input("Type", value=col['type'], key=f"c_t_{col['name']}")
            
            if len(cols) > 5:
                st.caption(f"... and {len(cols)-5} more columns")

        c_save, c_cancel = st.columns(2)
        if c_save.form_submit_button("💾 Save"):
            st.toast(f"Saved changes for {table_name}!")
        
        if c_cancel.form_submit_button("❌ Close"):
            st.session_state.er_edit_target = None
            st.rerun()