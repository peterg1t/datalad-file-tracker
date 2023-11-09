"""
Docstring
"""
import os
import argparse
from pathlib import Path
import cProfile
import streamlit as st
import graphs
import networkx as nx
from bokeh.transform import linear_cmap

import utils


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
    except Exception as expt:
        st.sidebar.text(f"{expt}")


def git_log_parse(ds_name, ds_branch):
    """! This function will generate the graph of the entire project
    Args:
        ds_name (str): An absolute path to the dataset name
        a_option (str): An analysis mode for the node calculation
    """
    try:
        # gdb = graphs.GraphProvenance(ds_name, ds_branch)
        gdb = graphs.GraphProvenanceTasks(ds_name, ds_branch)
    except Exception as err:
        st.warning(
            f"Error creating graph object. Please check that your path contains a valid Datalad dataset {err}"
        )
        st.stop()

    # plot_db = gdb.graph_object_plot()
    # st.bokeh_chart(plot_db, use_container_width=True)

    export_name = st.sidebar.text_input("Path for provenance graph export")
    st.sidebar.button(
        "Save", on_click=export_graph, kwargs={"graph": gdb, "filename": export_name}
    )

    return gdb


def plot_attributes(prov_graph, node_attributes):
    nx.set_node_attributes(prov_graph.graph, node_attributes, name="node_a")
    attr_fill_col = linear_cmap(
        "node_a",
        "Spectral8",
        min(list(node_attributes.values())),
        max(list(node_attributes.values())),
    )
    graph_plot_abstract = prov_graph.graph_object_plot(attr_fill_col)
    st.bokeh_chart(graph_plot_abstract, use_container_width=True)


def calculate_attribute(attr, dataset_name, branch):
    provenance_graph = git_log_parse(dataset_name, branch)
    if len(provenance_graph.node_list) != 0:
        if attr == "None":
            graph_plot_abstract = provenance_graph.graph_object_plot()
            st.bokeh_chart(graph_plot_abstract, use_container_width=True)

        elif attr == "Betweeness Centrality":
            node_attr = utils.calc_betw_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Degree Centrality":
            node_attr = utils.deg_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Bonacich Centrality":
            node_attr = utils.eigen_centrl(provenance_graph.graph)
            plot_attributes(provenance_graph, node_attr)

        elif attr == "Closeness Centrality":
            node_attr = utils.close_centrl(provenance_graph.graph)
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
        choices=["Centrality", "Betweeness", "Bonacich", "Closeness"],
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dataset_name = args.dspath
        analysis_type = args.analysis
        calculate_attribute(analysis_type, dataset_name)

    else:
        description = {
            "None": "None",
            "Degree Centrality": """Degree centrality, is the simplest measure of node connectivity. It assigns an importance score based simply on the number of links held by each node.""",
            "Betweeness Centrality": """Betweenness centrality measures the number of times a node lies on the shortest path between other nodes. Betweenness centrality of a node v is the sum of the fraction of all-pairs shortest paths that pass through node v""",
            "Bonacich Centrality": """EigenCentrality measures a node's influence based on the number of links it has to other nodes in the network. EigenCentrality then goes a step further by also taking into account how well connected a node is, and how many links their connections have, and so on through the network.""",
            "Closeness Centrality": """In a connected graph, closeness centrality (or closeness) of a node is a measure of centrality in a network, calculated as the reciprocal of the sum of the length of the shortest paths between the node and all other nodes in the graph. Thus, the more central a node is, the closer it is to all other nodes.""",
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
                    "Betweeness Centrality",
                    "Bonacich Centrality",
                    "Closeness Centrality",
                ],
            )

            description = st.text_area(
                "Description", description[analysis_type], disabled=True
            )

        # # Sreamlit UI implementation
        if utils.exists_case_sensitive(dataset_name):
            branches_project = utils.get_branches(dataset_name)
            branch_select = st.selectbox("Branches", branches_project)

            calculate_attribute(analysis_type, dataset_name, branch_select)
        else:
            st.warning("Invalid path to dataset")
