"""
Docstring
"""
import argparse
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


def git_log_parse(ds_name):
    """! This function will generate the graph of the entire project
    Args:
        ds_name (str): An absolute path to the dataset name
        a_option (str): An analysis mode for the node calculation
    """
    try:
        gdb = graphs.GraphProvenance(ds_name)
    except Exception as exept:
        st.warning(f"{exept}")
        st.stop()

    # plot_db = gdb.graph_object_plot()
    # st.bokeh_chart(plot_db, use_container_width=True)

    # export_name = st.sidebar.text_input("Path for provenance graph export")
    # st.sidebar.button(
    #     "Save", on_click=export_graph, kwargs={"graph": gdb, "filename": export_name}
    # )

    return gdb




def calculate_attribute(attr, dataset_name):
    print("calculating graph attribute", attr)
    if dataset_name:
        provenance_graph = git_log_parse(dataset_name)

        if attr == "None":
            graph_plot_abstract = provenance_graph.graph_object_plot()
            st.bokeh_chart(graph_plot_abstract, use_container_width=True)

        elif attr == "Betweeness Centrality":
            node_attr = utils.calc_betw_centrl(provenance_graph.graph)
            nx.set_node_attributes(provenance_graph.graph, node_attr, name="node_a")
            fill_col = linear_cmap(
                    "node_a",
                    "Spectral8",
                    min(list(node_attr.values())),
                    max(list(node_attr.values())),
                )
            graph_plot_abstract = provenance_graph.graph_object_plot(fill_col)
            st.bokeh_chart(graph_plot_abstract, use_container_width=True)

        elif attr == "Degree Centrality":
            node_attr = utils.deg_centrl(provenance_graph.graph)
            nx.set_node_attributes(provenance_graph.graph, node_attr, name="node_a")
            fill_col = linear_cmap(
                    "node_a",
                    "Spectral8",
                    min(list(node_attr.values())),
                    max(list(node_attr.values())),
                )
            graph_plot_abstract = provenance_graph.graph_object_plot(fill_col)
            st.bokeh_chart(graph_plot_abstract, use_container_width=True)
    



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dspath", help="Path to dataset")
    parser.add_argument(
        "-a",
        "--analysis",
        help="Analysis to apply to nodes",
        choices=["Centrality", "Betweeness"],
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dataset_name = args.dspath
        analysis_type = args.analysis
    else:
        print(
            "Not all command line arguments were used\
              as input, results might be wrong"
        )
        dataset_name = st.text_input("Input the dataset to track")
        with st.sidebar:
            analysis_type = st.selectbox(
                "Analysis mode", ["None", "Degree Centrality", "Betweeness Centrality"]
            )
            st.sidebar.button(
            "Calculate attribute",
            on_click=calculate_attribute,
            args=(analysis_type, dataset_name,)
            )

    # # Sreamlit UI implementation
    # if dataset_name:
    #     git_log_parse(dataset_name)
