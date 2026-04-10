"""ER Diagram View - Pure rendering component for ER diagram page."""
import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config


def render_er_diagram_page(datasources_df, form_state: dict, callbacks: dict) -> None:
    """
    Render ER diagram page.

    Args:
        datasources_df: DataFrame of available datasources
        form_state: dict with keys:
            - er_edit_target: Currently selected node
            - er_loaded: Whether graph has been loaded
        callbacks: dict with keys:
            - on_build_graph: Callback to build graph
    """
    st.subheader("🖱️ Interactive ERD Studio")

    if datasources_df.empty:
        st.warning("No datasources defined.")
        return

    # 1. Select Datasource
    col1, col2 = st.columns([1, 1])
    with col1:
        selected_ds_name = st.selectbox("Select Datasource", datasources_df['name'])

    # 2. Schema Selection
    ds_row = datasources_df[datasources_df['name'] == selected_ds_name]
    if not ds_row.empty:
        ds_type = ds_row.iloc[0]['db_type']
        schema_input = None
        if ds_type in ["PostgreSQL", "Microsoft SQL Server"]:
            with col2:
                default_schema = "public" if ds_type == "PostgreSQL" else "dbo"
                schema_input = st.text_input("Schema", value=default_schema)

    # 3. Build Graph Button
    if st.button("🔄 Load/Reset Diagram", type="primary"):
        with st.spinner("Building interactive graph..."):
            success, msg, nodes, edges = callbacks["on_build_graph"](selected_ds_name, schema_input)
            st.rerun()

    # 4. Layout Settings
    with st.sidebar.expander("⚙️ Layout Settings", expanded=True):
        st.caption("Adjust these to fix overlapping nodes.")
        physics_enabled = st.checkbox("Enable Physics", value=True)
        node_distance = st.slider("Node Spacing", 100, 1000, 300, step=50)
        spring_length = st.slider("Edge Length", 100, 500, 200, step=50)
        gravity = st.slider("Gravity", -10000, -1000, -2000, step=500)

    # 5. Interactive Graph
    if form_state["er_loaded"] and form_state["er_nodes"]:
        with st.container(border=True):
            st.markdown("### 🎨 Canvas")

            config = Config(
                width="100%",
                height=750,
                directed=True,
                physics=physics_enabled,
                hierarchical=False,
                nodeHighlightBehavior=True,
                highlightColor="#F7A7A6",
                collapsible=False,
                gravity=gravity/50000,
                linkLength=spring_length,
                nodeSpacing=node_distance,
                stabilization=False,
                fit=True
            )

            # Convert dicts to Node/Edge objects
            nodes = [
                Node(id=n["id"], label=n["label"], title=n.get("title", ""), size=n.get("size", 25))
                for n in form_state["er_nodes"]
            ]
            edges = [
                Edge(source=e["source"], target=e["target"], label=e.get("label", ""))
                for e in form_state["er_edges"]
            ]

            selected_node_id = agraph(nodes=nodes, edges=edges, config=config)

            if selected_node_id:
                # Update state in controller via callback
                callbacks.get("on_node_selected", lambda x: None)(selected_node_id)

    # 6. Editor Panel
    if form_state["er_edit_target"]:
        st.markdown(f"### 📝 Editing: {form_state['er_edit_target']}")
        st.info("Column details panel - feature coming soon")
