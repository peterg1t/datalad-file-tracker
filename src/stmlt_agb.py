"""
Docstring
"""
import os
import argparse
import copy
import cProfile
import csv
import import_export
import git
import glob
from datetime import datetime
import difflib

import streamlit as st
import networkx as nx
from bokeh.io import export_png
import datalad.api as dl
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

import utilities
import graphs
import match
import import_export


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)


def plot_graph(plot):
    """! Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot
    """
    st.bokeh_chart(plot, use_container_width=True)


def match_graphs(provenance_ds_path, gdb_abstract, ds_branch):
    """! Function to match the graphs loaded with Streamlit interface

    Args:
        provenance_ds_path (str)`: The path to the provenance dataset
        gdb_abstract (graph): An abstract graph
    """
    
    repo = git.Repo(provenance_ds_path)
    branch = repo.heads[ds_branch]
    branch.checkout()
    node_mapping = import_export.translation_file_process(f"{provenance_ds_path}/tf.csv")

    if utilities.exists_case_sensitive(provenance_ds_path):
        nodes_provenance, edges_provenance = graphs.prov_scan(provenance_ds_path, ds_branch)
        gdb_provenance = nx.DiGraph()
        gdb_provenance.add_nodes_from(nodes_provenance)
        gdb_provenance.add_edges_from(edges_provenance)

        gdb_abstract = match.graph_remap_command(gdb_abstract, node_mapping)
        gdb_abstract = match.graph_ID_relabel(gdb_abstract, node_mapping)
        gdb_abstract, gdb_difference = match.graph_diff(gdb_abstract, gdb_provenance)
        # print("abstract", gdb_abstract.nodes(data=True), "\n")
        # print("provenance", gdb_provenance.nodes(data=True), "\n")
        # print("difference", gdb_difference.nodes(data=True), "\n")

        graph_plot_diff = graphs.graph_object_plot_provenance(gdb_provenance)
        plot_graph(graph_plot_diff)

        if gdb_difference:
            next_nodes_requirements = match.next_nodes_run(gdb_difference)

        if not st.session_state["next_nodes_req"] or "next_nodes_req" not in st.session_state:
            st.session_state["next_nodes_req"] = next_nodes_requirements

    else:
        st.warning(f"Path {provenance_ds_path} does not exist.")
        st.stop()

    # print("session state", st.session_state)
    # return gdb_difference
    st.session_state["gdb_diff"] = gdb_difference


def run_pending_nodes(provenance_ds_path, gdb_difference, branch):
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs_dict = {}
    outputs_dict = {}
    inputs = []
    # we need to use the translation file so the nodes in the difference tree have the file names instead of the abstract names. From the nodes we can extract the list of inputs and outputs for the job that is going to run
    node_mapping = import_export.translation_file_process(f"{provenance_ds_path}/tf.csv")

    gdb_difference = match.graph_ID_relabel(gdb_difference, node_mapping)
    print("graph_diff", gdb_difference.nodes(data=True),"\n")

    try:
        next_nodes_run = st.session_state["next_nodes_req"]
        print("next_nodes_run", next_nodes_run, st.session_state ,'\n', gdb_difference.nodes(data=True))

        for item in next_nodes_run:
            inputs = gdb_difference.nodes[item]['inputs']
            outputs = gdb_difference.nodes[item]['outputs']

            
            command = gdb_difference.nodes[item]["cmd"]
            message = "test"

            print("submit_job", provenance_ds_path, inputs, outputs, message, "command=", command)
            scheduler.add_job(
                utilities.job_submit,
                args=[provenance_ds_path, branch, inputs, outputs, message, command],
            )

    except Exception as err:  # pylint: disable = bare-except
        st.warning(
            f"No provance graph has been matched to this abstract graph, match one first {err}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Added argument parser to parse a file with a workflow from a text file, the text file
    # format will be the following format
    # {inputs}<>{task}<>{outputs}
    parser.add_argument(
        "-a",
        "--agraph",
        type=str,
        help="Path to graph txt file. \
                        Content must have the F<>{files}<>{prec_nodes} format per line\
                        or  T<>{task}<>{prec_nodes}<>{command}<>{workflow}   ",
    )
    parser.add_argument(
        "-p", "--pgraph", type=str, help="Path to project to extract provenance"
    )
    parser.add_argument(
        "-gml",
        "--gml_export",
        type=str,
        help="Flag to export abstract graph to GML format",
    )
    parser.add_argument(
        "-png",
        "--png_export",
        type=str,
        help="Flag to export abstract graph to png format",
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    # We now start the background scheduler
    # scheduler = BackgroundScheduler()
    # This will get you a BackgroundScheduler with a MemoryJobStore named
    # “default” and a ThreadPoolExecutor named “default” with a default
    # maximum thread count of 10.

    # Lets cutomize the scheduler a little bit lets keep the default
    # MemoryJobStore but define a ProcessPoolExecutor
    jobstores = {
        "default": SQLAlchemyJobStore(
            url="sqlite:////Users/pemartin/Projects/datalad-file-tracker/src/jobstore.sqlite"
        )
    }
    executors = {
        "default": ThreadPoolExecutor(8),
    }
    job_defaults = {"coalesce": False, "max_instances": 3}
    scheduler = BackgroundScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )
    scheduler.start()  # We start the scheduler

    next_nodes_req = []

    node_list = None  # pylint: disable=invalid-name
    edge_list = None  # pylint: disable=invalid-name

    if args.agraph:
        node_list, edge_list = graphs.graph_components_generator_from_file(args.agraph)

    else:
        tasks_number = st.number_input("Please define a number of stages", min_value=1)
        node_list, edge_list = graphs.graph_components_generator(tasks_number)

    try:
        gdb = nx.DiGraph()
        gdb.add_nodes_from(node_list)
        gdb.add_edges_from(edge_list)
        st.success("Graph created")

    except:
        st.warning(
            f"There was a problem in the creation of the graph verify\
                   that all node names match along the edges"
        )
        st.stop()

    graph_plot_abstract = graphs.graph_object_plot_abstract(gdb)
    plot_graph(graph_plot_abstract)
    if args.png_export:
        export_png(graph_plot_abstract, filename=args.png_export)

    export_name = st.sidebar.text_input("Path for abstract graph export")

    st.sidebar.button(
        "Save",
        on_click=import_export.export_graph,
        kwargs={"graph": gdb, "filename": export_name},
    )

    # The provenance graph name is the path to any
    # directory in a project where provenance is recorded.
    # When the button is clicked a full provenance graph
    # for all the project is generated and matched
    # to the abstract graph
    provenance_graph_path = st.sidebar.text_input("Path to the dataset with provenance")

    if st.sidebar.button("Generate code"):
        code = import_export.generate_code(gdb)
        st.text_area("Prefect code", code)

    if utilities.exists_case_sensitive(provenance_graph_path):
        branches_project = utilities.get_branches(provenance_graph_path)
        branch_select = st.sidebar.selectbox("Branches", branches_project)

        match_button = st.sidebar.button("Match")
        if match_button:
            match_graphs(provenance_graph_path, gdb, branch_select)
        
        run_next_button = st.sidebar.button("Run pending nodes")
        if run_next_button:
            print(st.session_state)
            run_pending_nodes(provenance_graph_path, st.session_state.gdb_diff, branch_select)

            
