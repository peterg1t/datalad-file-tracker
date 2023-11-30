"""
Docstring
"""
import argparse
import cProfile

import networkx as nx
import streamlit as st
from bokeh.transform import linear_cmap

from . import graphs, utilities

profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the database provenance tracker!
"""
)


def export_graph(**kwargs):
    """! This function will print an exception if"""
    try:
        kwargs["graph"].graph_export(kwargs["filename"])
    except ValueError as expt:
        st.sidebar.text(f"{expt}")


def git_log_parse(ds_name, ds_branch):
    """! This function will generate the graph of the entire project
    Args:
        ds_name (str): An absolute path to the dataset name
        a_option (str): An analysis mode for the node calculation
    """
    try:
        nodes_provenance, edges_provenance = graphs.prov_scan(ds_name, ds_branch)
        gdb_provenance = nx.DiGraph()
        gdb_provenance.add_nodes_from(nodes_provenance)
        gdb_provenance.add_edges_from(edges_provenance)
    except ValueError as err:
        st.warning(
            f"Error creating graph object. Please check that your path contains a valid Datalad dataset {err}"  # noqa: E501
        )
        st.stop()

    # plot_db = gdb.graph_object_plot()
    # st.bokeh_chart(plot_db, use_container_width=True)

    export_name = st.sidebar.text_input("Path for provenance graph export")
    st.sidebar.button(
        "Save",
        on_click=export_graph,
        kwargs={"graph": gdb_provenance, "filename": export_name},
    )

    return gdb_provenance


def plot_attributes(prov_graph, node_attributes):
    """Visualize the provenance graph using Bokeh based on provided node attributes.

    Args:
        prov_graph (ProvenanceGraph): The provenance graph to be visualized.
        node_attributes (dict): A dictionary mapping node identifiers
          to attribute values.
    """
    nx.set_node_attributes(prov_graph.graph, node_attributes, name="node_a")
    attr_fill_col = linear_cmap(
        "node_a",
        "Spectral8",
        min(list(node_attributes.values())),
        max(list(node_attributes.values())),
    )
    graph_plot_abstract = prov_graph.graph_object_plot_abstract(attr_fill_col)
    st.bokeh_chart(graph_plot_abstract, use_container_width=True)


def calculate_attribute(attr, dataset, branch):
    """Visualize graph attributes for the provenance graph based on
    the specified attribute.

    Args:
        attr (str): The graph attribute to visualize. Options: "None",
        "Betweenness Centrality", "Degree Centrality", "Bonacich Centrality",
                      "Closeness Centrality".
        dataset (str): The name of the dataset.
        branch (str): The branch of the dataset in version control.
    """
    provenance_graph = git_log_parse(dataset, branch)
    if len(provenance_graph.nodes) != 0:
        if attr == "None":
            graph_plot_abstract = graphs.graph_object_plot_provenance(provenance_graph)
            st.bokeh_chart(graph_plot_abstract, use_container_width=True)

        elif attr == "Betweenness Centrality":
            node_attr = graphs.calc_betw_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Degree Centrality":
            node_attr = graphs.deg_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Bonacich Centrality":
            node_attr = graphs.eigen_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Closeness Centrality":
            node_attr = graphs.close_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

    else:
        st.warning("This dataset does not contains any Datalad run instances")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dspath", help="Path to dataset")
    parser.add_argument(
        "-a",
        "--analysis",
        help="Analysis to apply to nodes",
        choices=["Centrality", "Betweenness", "Bonacich", "Closeness"],
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dataset_name = args.dspath
        analysis_type = args.analysis

        branches_project = utilities.get_branches(dataset_name)
        branch_select = st.selectbox("Branches", branches_project)

        calculate_attribute(analysis_type, dataset_name, branch_select)

    else:
        description = {
            "None": "None",
            "Degree Centrality": """Degree centrality, is the simplest measure
              of node connectivity. It assigns an importance score based
              simply on the number of links held by each node.""",
            "Betweenness Centrality": """Betweenness centrality measures the
            number of times a node lies on the shortest path between other
            nodes. Betweenness centrality of a node v is the sum of the
            fraction of all-pairs shortest paths that pass through node v""",
            "Bonacich Centrality": """EigenCentrality measures a node's
            influence based on the number of links it has to other nodes in
            the network. EigenCentrality then goes a step further by also
              taking into account how well connected a node is, and how many
              links their connections have, and so on through the network.""",
            "Closeness Centrality": """In a connected graph, closeness
            centrality (or closeness) of a node is a measure of centrality in
            a network, calculated as the reciprocal of the sum of the length of
            the shortest paths between the node and all other nodes in the
            graph. Thus, the more central a node is, the closer it is
            to all other nodes.""",
        }

        print(
            "Not all command line arguments were used\
              as input, results might be wrong"
        )
        dataset_name = st.text_input("Input the dataset to track")

        with st.sidebar:
            analysis_type = st.selectbox(
                "Analysis mode",
                [
                    "None",
                    "Degree Centrality",
                    "Betweenness Centrality",
                    "Bonacich Centrality",
                    "Closeness Centrality",
                ],
            )

            description = st.text_area(
                "Description", description[analysis_type], disabled=True
            )

        # # Sreamlit UI implementation
        if utilities.exists_case_sensitive(dataset_name):
            branches_project = utilities.get_branches(dataset_name)
            branch_select = st.selectbox("Branches", branches_project)

            calculate_attribute(analysis_type, dataset_name, branch_select)
        else:
            st.warning("Invalid path to dataset")
