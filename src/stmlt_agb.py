"""
Docstring
"""
import os
import argparse
import copy
import shutil
import cProfile
import streamlit as st
import networkx as nx
from pytz import utc


from graphs.graph_abstract import graph_abstract
from graphs.graph_provenance import graph_provenance
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from multiprocessing import Pool
from functools import partial
from itertools import repeat
import time
import utils


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)


def graph_components_generator_from_file(filename):
    inputs = []
    commands = []
    outputs = []
    with open(filename, encoding="utf-8") as f:
        read_data = f.readlines()
        for item in read_data:
            inputs.append(item.split("<>")[0].strip())
            # commands.append(item.strip().split('<>')[1])
            commands.append(item.split("<>")[1])
            outputs.append(item.split("<>")[2].strip())

    return inputs, commands, outputs


def graph_components_generator(tasks_number):
    """This function will generate the graph of the entire project

    Args:
        tasks_number (int): A number describing the number of tasks to be added

    Returns:
        inputs: A list of input files
        commands: A list of commands (ideally one per task)
        outputs: A list of output files
    """

    nodes = []
    edges = []
    for i in range(tasks_number):
        container = st.container()
        with container:
            col1, col2, col3, col4, col5 = st.columns([1, 2, 2, 2, 2])
            level_type = col1.selectbox(
                "Select node type", ["file", "task"], key=f"level_{i}"
            )
            prec_nodes = utils.remove_space(
                col4.text_input(f"Preceding node(s) for stage{i}", key=f"node(s)_{i}")
            ).split(",")

            if level_type == "file":
                files = utils.remove_space(
                    col2.text_input(
                        f"File(s) for stage {i}",
                        key=f"name_{i}",
                        placeholder="File(s) Name (comma sepparated)",
                    )
                ).split(",")

                for file in files:
                    if (
                        len(file.rstrip()) == 0
                    ):  # if there is no file (or there is an empty file) stop the execution
                        st.stop()

                for file in files:
                    nodes.append(
                        (
                            os.path.basename(file).split('.')[0],
                            {
                                "name": file,
                                "label": os.path.basename(file).split('.')[0],
                                "path": os.path.dirname(file),
                                "type": level_type,
                                "status": "pending",
                                "node_color": "grey",
                                "ID": utils.encode(file),
                            },
                        )
                    )
                    for node in prec_nodes:
                        if node:
                            edges.append((node, os.path.basename(file).split('.')[0]))

            elif level_type == "task":
                task = col2.text_input(
                    f"Task for stage {i}", key=f"name_{i}", placeholder="Task Name"
                )

                if not task:  # if there is no task stop the execution
                    st.stop()

                command = col3.text_input(
                    f"Command for task {i}", key=f"cmd_{i}", placeholder="Command"
                )
                transform = col5.text_input(
                    f"Data transform for task {i}", key=f"trf_{i}"
                )

                if '*' not in transform:
                    st.text('Special character * not in Data transform string')
                    # st.stop()

                nodes.append(
                    (
                        task,
                        {
                            "name": task,
                            "label": task,
                            "path": "",
                            "type": level_type,
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
    """Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot
    """
    st.bokeh_chart(plot, use_container_width=True)




def export_graph(**kwargs):
    try:
        kwargs["graph"]._graph_export(kwargs["filename"])
    except Exception as e:
        st.sidebar.text(f"{e}")




def provenance_graph(dataset):
    """This function will return a provenance graph

    Returns:
        **: _description_
    """
    return graph_provenance(dataset)




def workflow_diff(abstract, provenance):
    abs_graph_id = list(nx.get_node_attributes(abstract.graph, 'ID').values())
    prov_graph_id = list(nx.get_node_attributes(provenance.graph, 'ID').values())
    nodes_abs = list(abstract.graph.nodes())
    
    nodes_update = [n for n,v in abstract.graph.nodes(data=True) if v['ID'] in prov_graph_id]

    for node in nodes_update:
        nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
        if abstract.graph.nodes()[node]["type"] == "task":
            nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
        elif abstract.graph.nodes()[node]["type"] == "file":
            nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")



    graph_plot = abstract.graph_object_plot()
    plot_graph(graph_plot)

    gdb_diff = copy.deepcopy(gdb)
    gdb_diff.graph.remove_nodes_from(n for n,v in abstract.graph.nodes(data=True) if v['status']=='complete')


    # In the difference graph the start_nodes is the list of nodes that can be started (these should usually be a task)
    next_nodes = gdb_diff.start_nodes()
    print('next_nodes',next_nodes)

    return next_nodes











if __name__ == "__main__":
    """Sreamlit UI implementation"""
    parser = argparse.ArgumentParser()
    # Added argument parser to parse a file with a workflow from a text file, the text file
    # format will be the following format
    # {inputs}<>{task}<>{outputs}
    parser.add_argument(
        "-a",
        "--agraph",
        type=str,
        help="Path to graph txt file. \
                        Content must have the {inputs}<>{task}<>{outputs} format per line",
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
    # This will get you a BackgroundScheduler with a MemoryJobStore named “default” and a ThreadPoolExecutor named “default” with a default maximum thread count of 10.

    # Lets cutomize the scheduler a little bit lets keep the default MemoryJobStore but define a ProcessPoolExecutor
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
    if args.agraph:
        pass

    

    else:
        tasks_number = st.number_input("Please define a number of levels", min_value=1)
        # file_inputs, commands, file_outputs = graph_components_generator(tasks_number)
        node_list, edge_list = graph_components_generator(tasks_number)

        gdb = graph_abstract(node_list, edge_list)
        graph_plot = gdb.graph_object_plot()
        plot_graph(graph_plot)



        export_name = st.sidebar.text_input("Path for abstract graph export")
        st.sidebar.button(
            "Save",
            on_click=export_graph,
            kwargs={"graph": gdb, "filename": export_name},
        )

        

        # The provenance graph name is the path to any directory in a project where provenance is recorded. 
        # When the button is clicked a full provenance graph for all the project is generated and matched 
        # to the abstract graph
        provenance_graph_name = st.sidebar.text_input(
            "Path for concrete provenance graph"
        )
        button_clicked = st.sidebar.button("Match")

        if button_clicked:
            gdb_prov = provenance_graph(provenance_graph_name)
            next_nodes_req = workflow_diff(gdb, gdb_prov)

        inputs_dict = {}
        outputs_dict = {}
        if next_nodes_req:
            for item in next_nodes_req:
                for predecessors in gdb.graph.predecessors(item):
                    inputs_dict[predecessors] = gdb.graph.nodes[predecessors]['name']

                for successors in gdb.graph.successors(item):
                    outputs_dict[successors] = gdb.graph.nodes[successors]['name']

                inputs = list(inputs_dict.values())
                outputs = list(outputs_dict.values())
        
                dataset = utils.get_git_root(os.path.dirname(inputs[0]))
                command  = gdb.graph.nodes[item]['cmd']
                message = "test"
                scheduler.add_job(utils.job_submit, args=[dataset, inputs, outputs, message, command])
                
