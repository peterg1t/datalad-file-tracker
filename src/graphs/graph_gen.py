"""Utilities for graph generation"""
import os

import streamlit as st

<<<<<<< HEAD
from . import (encode,
               file_name_expansion,
    line_process_file,
    line_process_task,
    line_process_task_v2,
    remove_space,
)
=======
import utilities  # pylint: disable=import-error
>>>>>>> e135d909ccdac54dabe0319fbb2a49507c2d4dc4


def process_task_node(
    task, prec_nodes, command, workflow, nodes, edges
):  # pylint: disable=too-many-arguments
    """
    Process a task node in a workflow graph.

    This function adds a task node to the workflow graph, including its metadata
    such as name, label, command, status, and connections to predecessor nodes.

    Args:
        task (str): The task node to be processed.
        prec_nodes (List[str]): The predecessor nodes of the task.
        command (str): The command associated with the task.
        workflow (str): The workflow to which the task belongs.
        nodes (List[Tuple[str, Dict[str, Any]]]): The list of nodes in
        the workflow graph.
        edges (List[Tuple[str, str]]): The list of edges connecting nodes
          in the workflow graph.

    Returns:
        None
    """
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
                "predecessor": prec_nodes,
                "workflow": workflow,
                "ID": "",
            },
        )
    )
    for node in prec_nodes:
        if node:
            edges.append((node, task))


def process_file_node(files, prec_nodes, nodes, edges):
    """_summary_

    Args:
        files (_type_): _description_
        prec_nodes (_type_): _description_
        nodes (_type_): _description_
        edges (_type_): _description_
    """
    for file in files:
        nodes.append(
            (
                os.path.basename(file),
                {
                    "name": file,
                    "label": os.path.basename(file),
                    "path": os.path.dirname(file),
                    "type": "file",
                    "status": "pending",
                    "node_color": "grey",
                    "predecessor": prec_nodes,
                    "ID": utilities.encode(file),
                },
            )
        )
        for node in prec_nodes:
            if node:
                edges.append((node, os.path.basename(file)))


def graph_components_generator_from_file(filename):
    """Process file nodes and update the graph.

    This function takes a list of file paths, predecessor nodes,
    current nodes, and edges
    and updates the graph by adding new file nodes with their corresponding attributes.

    Args:
        files (List[str]): A list of file paths to be processed.
        prec_nodes (List[str]): A list of predecessor nodes.
        nodes (List[Tuple[str, dict]]): List containing tuples of node name
          and its attributes.
        edges (List[Tuple[str, str]]): List containing tuples of edge
         connections between nodes.

    Returns:
        None: The function directly updates the 'nodes' and 'edges' lists.
    """
    nodes = []
    edges = []
    with open(filename, encoding="utf-8") as file_abstract:
        read_data = file_abstract.readlines()
        for line in read_data:
            stage_type = line.split("<>")[0].strip()
            if stage_type == "T":
                (
                    task,
                    prec_nodes,
                    command,
                    workflow,
                ) = utilities.line_process_task(  # noqa: E501
                    line
                )
                process_task_node(task, prec_nodes, command, workflow, nodes, edges)
            elif stage_type == "F":
                files, prec_nodes = utilities.line_process_file(line)
                process_file_node(files, prec_nodes, nodes, edges)

    return nodes, edges


def graph_components_generator(number_of_tasks):  # pylint: disable=too-many-locals
    """! This function will generate the graph of the entire project

    Args:
        number_of_tasks (int): A number describing the number of
        tasks to be added

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
            prec_nodes_grp = utilities.remove_space(
                col3.text_input(f"Preceding node(s) for stage{i}", key=f"node(s)_{i}")
            ).split(",")

            prec_nodes = []
            for prec_nodes_item in prec_nodes_grp:
                # for file definition lets check if we have defined
                # multiple files with regex
                nodes_expanded = utilities.file_name_expansion(prec_nodes_item)
                prec_nodes.extend(nodes_expanded)

            if stage_type == "file":
                file_grp = utilities.remove_space(
                    col2.text_input(
                        f"File(s) for stage {i}",
                        key=f"name_{i}",
                        placeholder="File(s) Name (comma sepparated)",
                    )
                ).split(",")

                files = []
                for file_item in file_grp:
                    # for file definition lets check if we have defined
                    #  multiple files with regex
                    files_expanded = utilities.file_name_expansion(file_item)

                    if (
                        len(file_item.rstrip()) == 0
                    ):  # if there is no file (or there is an empty file) stop the execution # noqa: E501
                        st.stop()

                    files.extend(files_expanded)
                    process_file_node(files, prec_nodes, nodes, edges)

            elif stage_type == "task":
                task = col2.text_input(
                    f"Task for stage {i}", key=f"name_{i}", placeholder="Task Name"
                )

                if not task:  # if there is no task stop the execution
                    st.stop()

                command = col4.text_input(
                    f"Command for stage {i}", key=f"cmd_{i}", placeholder="Command"
                )
                workflow = col5.text_input(
                    f"Workflow for stage {i}",
                    key=f"wrkf_{i}",
                    placeholder="Subworkflow",
                )
                if not workflow:
                    workflow = "main"

                process_task_node(task, prec_nodes, command, workflow, nodes, edges)

    return nodes, edges


def gcg_processing_tasks(filename):  # pylint: disable=too-many-locals
    """! This function generate a networkx graph from a file containing an
    abstract graph

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
            (
                task,
                inputs,
                outputs,
                command,
                pce,
                subworkflow,
                message,
            ) = utilities.line_process_task_v2(item)
            nodes.append(
                (
                    task,
                    {
                        "description": task,
                        "command": command,
                        "inputs": inputs,
                        "outputs": outputs,
                        "message": message,
                        "PCE": pce,
                        "subworkflow": subworkflow,
                    },
                )
            )

        for node1 in nodes:
            for node2 in nodes:
                diff_set = set(node1[1]["outputs"]).intersection(
                    set(node2[1]["inputs"])
                )
                if diff_set:
                    edges.append((node1[0], node2[0]))

    return nodes, edges
