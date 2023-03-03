"""
Docstring
"""
import os
import re
import argparse
import copy
import cProfile
import streamlit as st
import networkx as nx

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

from graphs.graph_base import GraphBase
from graphs.graph_provenance import GraphProvenance

import utils


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)


def graph_components_generator_from_file(filename):
    """! This function generate a networkx graph from a file containing an abstract graph

    Args:
        filename (str): Path to the abstract graph

    Returns:
        nodes: A list of nodes
        edges: A list of edges
    """
    nodes = []
    edges = []
    with open(filename, encoding="utf-8") as file_abstract:
        read_data = file_abstract.readlines()
        for item in read_data:
            stage_type = item.split("<>")[0].strip()
            if stage_type == "T":
                task, command, prec_nodes, transform = utils.line_process_task(item)
                nodes.append(
                    (
                        task,
                        {
                            "name": task,
                            "label": task,
                            "path": "",
                            "type": "task",
                            "cmd": command,
                            "status": "pending",
                            "node_color": "grey",
                            "transform": transform,
                            "ID": "",
                        },
                    )
                )
                for node in prec_nodes:
                    if node:
                        edges.append((node, task))

            elif stage_type == "F":
                files, prec_nodes = utils.line_process_file(item)
                for file in files:
                    nodes.append(
                        (
                            os.path.basename(file).split(".")[0],
                            {
                                "name": file,
                                "label": os.path.basename(file).split(".")[0],
                                "path": os.path.dirname(file),
                                "type": "file",
                                "status": "pending",
                                "node_color": "grey",
                                "ID": utils.encode(file),
                            },
                        )
                    )
                    for node in prec_nodes:
                        if node:
                            edges.append((node, os.path.basename(file).split(".")[0]))

    return nodes, edges


def graph_components_generator(number_of_tasks):
    """! This function will generate the graph of the entire project

    Args:
        number_of_tasks (int): A number describing the number of tasks to be added

    Returns:
        inputs: A list of input files
        commands: A list of commands (ideally one per task)
        outputs: A list of output files
    """
    nodes = []
    edges = []
    for i in range(number_of_tasks):
        container = st.container()
        with container:
            col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 2, 2])
            stage_type = col1.selectbox(
                "Select node type", ["file", "task"], key=f"stage_{i}"
            )
            prec_nodes_grp = utils.remove_space(
                col4.text_input(f"Preceding node(s) for stage{i}", key=f"node(s)_{i}")
            ).split(",")

            print('prec_nodes_grp',prec_nodes_grp)
            prec_nodes = []
            for prec_nodes_item in prec_nodes_grp:
                # for file definition lets check if we have defined multiple files with regex
                nodes_expanded = utils.file_name_expansion(prec_nodes_item)
                # if (
                #     len(prec_nodes_item.rstrip()) == 0
                # ):  # if there is no file (or there is an empty file) stop the execution
                #     st.stop()

                prec_nodes.extend(nodes_expanded)



            if stage_type == "file":
                file_grp = utils.remove_space(
                    col2.text_input(
                        f"File(s) for stage {i}",
                        key=f"name_{i}",
                        placeholder="File(s) Name (comma sepparated)",
                    )
                ).split(",")

                files = []
                for file_item in file_grp:
                    # for file definition lets check if we have defined multiple files with regex
                    files_expanded = utils.file_name_expansion(file_item)
                    
                    if (
                        len(file_item.rstrip()) == 0
                    ):  # if there is no file (or there is an empty file) stop the execution
                        st.stop()

                    files.extend(files_expanded)


                for file in files:
                    nodes.append(
                        (
                            os.path.basename(file).split(".")[0],
                            {
                                "name": file,
                                "label": os.path.basename(file).split(".")[0],
                                "path": os.path.dirname(file),
                                "type": stage_type,
                                "status": "pending",
                                "node_color": "grey",
                                "ID": utils.encode(file),
                            },
                        )
                    )


                    for node in prec_nodes:
                        if node:
                            edges.append((node, os.path.basename(file).split(".")[0]))


            elif stage_type == "task":
                task = col2.text_input(
                    f"Task for stage {i}", key=f"name_{i}", placeholder="Task Name"
                )

                if not task:  # if there is no task stop the execution
                    st.stop()

                command = col3.text_input(
                    f"Command for stage {i}", key=f"cmd_{i}", placeholder="Command"
                )
                transform = col5.text_input(
                    f"Data transform for task {i}", key=f"trf_{i}"
                )

                if "*" not in transform:
                    st.text("Special character * not in Data transform string")
                    # st.stop()

                nodes.append(
                    (
                        task,
                        {
                            "name": task,
                            "label": task,
                            "path": "",
                            "type": stage_type,
                            "cmd": command,
                            "status": "pending",
                            "node_color": "grey",
                            "transform": transform,
                            "ID": "",
                        },
                    )
                )


                for node in prec_nodes:
                    if node:
                        edges.append((node, task))

    return nodes, edges


def plot_graph(plot):
    """! Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot
    """
    st.bokeh_chart(plot, use_container_width=True)


def export_graph(**kwargs):
    """! This function call the graph_export method of the graph object and
    throws an exception to streamlit
    """
    try:
        kwargs["graph"].graph_export(kwargs["filename"])
    except Exception as exception_graph:
        st.sidebar.text(f"{exception_graph}")


def workflow_diff(abstract, provenance):
    """! Calculate the difference of the abstract and provenance graphs

    Args:
        abstract (_type_): _description_
        provenance (_type_): _description_

    Returns:
        _type_: _description_
    """
    abs_graph_id = list(nx.get_node_attributes(abstract.graph, "ID").values())
    prov_graph_id = list(nx.get_node_attributes(provenance.graph, "ID").values())
    nodes_abs = list(abstract.graph.nodes())

    print("ids", abs_graph_id, prov_graph_id, nodes_abs)

    nodes_update = [
        n for n, v in abstract.graph.nodes(data=True) if v["ID"] in prov_graph_id
    ]

    for node in nodes_update:
        nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
        if abstract.graph.nodes()[node]["type"] == "task":
            nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
        elif abstract.graph.nodes()[node]["type"] == "file":
            nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")

    graph_plot_diff = abstract.graph_object_plot()
    plot_graph(graph_plot_diff)

    gdb_difference = copy.deepcopy(gdb)
    gdb_difference.graph.remove_nodes_from(
        n for n, v in abstract.graph.nodes(data=True) if v["status"] == "complete"
    )
    # In the difference graph the start_nodes is the list of nodes that can be
    # started (these should usually be a task)

    return gdb_difference


def match_graphs(provenance_ds_path, gdb_abstract):
    """! Function to match the graphs loaded with Streamlit interface

    Args:
        provenance_ds_path (str): The path to the provenance dataset
        gdb_abstract (graph): An abstract graph
    """
    if provenance_ds_path:
        gdb_provenance = GraphProvenance(provenance_ds_path)
        gdb_difference = workflow_diff(gdb_abstract, gdb_provenance)
        next_nodes_requirements = gdb_difference.next_nodes_run()

        if "next_nodes_req" not in st.session_state:
            st.session_state["next_nodes_req"] = next_nodes_requirements


def run_pending_nodes(gdb_difference):
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs_dict = {}
    outputs_dict = {}

    try:
        next_nodes_req = st.session_state["next_nodes_req"]
        for item in next_nodes_req:
            for predecessors in gdb_difference.graph.predecessors(item):
                inputs_dict[predecessors] = gdb_difference.graph.nodes[predecessors][
                    "name"
                ]

            for successors in gdb_difference.graph.successors(item):
                outputs_dict[successors] = gdb_difference.graph.nodes[successors][
                    "name"
                ]

            inputs = list(inputs_dict.values())
            outputs = list(outputs_dict.values())
            dataset = utils.get_git_root(os.path.dirname(inputs[0]))
            command = gdb_difference.graph.nodes[item]["cmd"]
            message = "test"

            print("submit_job", dataset, inputs, outputs, message, command)
            # scheduler.add_job(utils.job_submit, args=[dataset, inputs, outputs, message, command])

    except: # pylint: disable = bare-except
        st.warning(
            "No provance graph has been matched to this abstract graph, match one first"
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
                        Content must have the {type}<>{name}<>{params} format per line",
    )
    parser.add_argument(
        "-p", "--pgraph", type=str, help="Path to project to extract provenance"
    )
    parser.add_argument(
        "-e", "--export", type=str, help="Flag to export abstract graph to GML format"
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
        node_list, edge_list = graph_components_generator_from_file(args.agraph)

    else:
        tasks_number = st.number_input("Please define a number of stages", min_value=1)
        # file_inputs, commands, file_outputs = graph_components_generator(tasks_number)
        node_list, edge_list = graph_components_generator(tasks_number)

    try:
        gdb = GraphBase(node_list, edge_list)
        graph_plot_abstract = gdb.graph_object_plot()
        plot_graph(graph_plot_abstract)
        export_name = st.sidebar.text_input("Path for abstract graph export")

        st.sidebar.button(
            "Save",
            on_click=export_graph,
            kwargs={"graph": gdb, "filename": export_name},
        )
        # The provenance graph name is the path to any
        # directory in a project where provenance is recorded.
        # When the button is clicked a full provenance graph
        # for all the project is generated and matched
        # to the abstract graph
        provenance_graph_path = st.sidebar.text_input("Path to the dataset with provenance")
        match_button = st.sidebar.button("Match")
        if match_button:
            match_graphs(provenance_graph_path, gdb)
        run_next_button = st.sidebar.button("Run pending nodes")
        if run_next_button:
            run_pending_nodes(gdb)

        st.success("Graph created")
    
    except:
        st.warning(f"There was a problem in the creation of the graph verify\
                   that all node names match along the edges")
        st.stop()

    
    
    
