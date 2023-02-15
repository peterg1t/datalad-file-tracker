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
            inputs.append(item.split('<>')[0].strip())
            # commands.append(item.strip().split('<>')[1])
            commands.append(item.split('<>')[1])
            outputs.append(item.split('<>')[2].strip())

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

    col1, col2, col3 = st.columns(3)
    inputs = []
    commands = []
    outputs = []
    for i in range(tasks_number):
        expander = st.expander(label=f"expander{i}")
        with expander:
            inputs.append(
                utils.remove_space(
                    col1.text_input(
                        f"Write the inputs for task {i}, separated by commas",
                        key=f"inputs_{i}",
                    )
                )
            )
            commands.append(
                col2.text_input(f"Write the command for task {i}", key=f"cmd_{i}")
            )
            outputs.append(
                utils.remove_space(
                    col3.text_input(
                        f"Write the outputs for task {i}, separated by commas",
                        key=f"outputs_{i}",
                    )
                )
            )

    return inputs, commands, outputs


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
    for node in provenance.graph.nodes():
        if node in abstract.graph.nodes():
            nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
            if abstract.graph.nodes()[node]["type"] == "task":
                nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
            elif abstract.graph.nodes()[node]["type"] == "file":
                nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")


    graph_plot = abstract.graph_object_plot()
    plot_graph(graph_plot)

    gdb_diff = copy.deepcopy(gdb)
    gdb_diff.graph.remove_nodes_from(n for n in abstract.graph if n in provenance.graph)

    # In the difference graph the start_nodes is the list of nodes that can be started (these should usually be a task)
    next_nodes = gdb_diff.start_nodes()

    tasks_requirements=[]

    for node in next_nodes:
        inputs_task=[]
        predecesor = abstract.graph.predecessors(node)
        for item in predecesor:
            inputs_task.append(provenance.graph.nodes[item]['literal_name'])
        tasks_requirements.append((inputs_task, abstract.graph.nodes[node]["name"]))

    return tasks_requirements






if __name__ == "__main__":
    """Sreamlit UI implementation"""
    parser = argparse.ArgumentParser() 
    # Added argument parser to parse a file with a workflow from a text file, the text file
    #format will be the following format
    # {inputs}<>{task}<>{outputs}
    parser.add_argument("-a", "--agraph", type=str, help="Path to graph txt file. \
                        Content must have the {inputs}<>{task}<>{outputs} format per line")
    parser.add_argument("-p", "--pgraph", type=str, help="Path to project to extract provenance")
    parser.add_argument("-e", "--export", type=str, help="Flag to export abstract graph to GML format")
    
    args = parser.parse_args()  # pylint: disable = invalid-name


    

    # We now start the background scheduler
    # scheduler = BackgroundScheduler()
    # This will get you a BackgroundScheduler with a MemoryJobStore named “default” and a ThreadPoolExecutor named “default” with a default maximum thread count of 10.

    # Lets cutomize the scheduler a little bit lets keep the default MemoryJobStore but define a ProcessPoolExecutor
    jobstores = {
        'default': SQLAlchemyJobStore(url='sqlite:////Users/pemartin/Projects/datalad-file-tracker/src/jobstore.sqlite')
    }
    executors = {
    'default': ThreadPoolExecutor(8),
    }
    job_defaults = {
    'coalesce': False,
    'max_instances': 3
    }
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.start() #We start the scheduler

    


    
    next_nodes_req = []
    if args.agraph:
        
        gdb_prov = None
        
        file_inputs, commands, file_outputs = graph_components_generator_from_file(args.agraph)
        graph_plot = None
 
        gdb = graph_abstract(file_inputs, commands, file_outputs)
        graph_plot = gdb.graph_object_plot()
        plot_graph(graph_plot)

        if args.pgraph:
            gdb_prov = provenance_graph(args.pgraph)
            next_nodes_req = workflow_diff(gdb, gdb_prov)

        if args.export:
            export_graph(graph=gdb, filename=args.export)

        if next_nodes_req:
            for req in next_nodes_req:
                for idx,input in enumerate(req[0]):
                    print('req',req, input)
                    message = 'this is a test'
                    print('folder to search', os.path.dirname(input))
                    dataset = utils.get_git_root(os.path.dirname(input))
                    print('dataset', dataset)
                    command = req[1]
                    print('command',command)
                    # command = "cat {inputs} >> {outputs} &&echo '6' >> {outputs}"
                    output_filename = req[2][idx]
                    print('output filename', output_filename)
                    output = f"{os.path.dirname(input)}/{output_filename}"
                    print('output', output)
                    scheduler.add_job(utils.job_submit, args=[dataset, input, output, message, command])
            # print('Scheduled Jobs', scheduler.get_jobs())

        


    else:
        tasks_number = st.number_input("Please define a number of tasks", min_value=1)
        file_inputs, commands, file_outputs = graph_components_generator(tasks_number)

        graph_plot = None
        if "" in commands:
            st.text(
                "Empty task on abstract tree. Add a task or remove the row to proceed with the abstract graph generation."
            )
            pass

        else:
            gdb = graph_abstract(file_inputs, commands, file_outputs)
            graph_plot = gdb.graph_object_plot()
            plot_graph(graph_plot)

            export_name = st.sidebar.text_input("Path for abstract graph export")
            st.sidebar.button(
                "Save",
                on_click=export_graph,
                kwargs={"graph": gdb, "filename": export_name},
            )

            # The provenance graph name is the path to any directory in a project where provenance is recorded. When the button is clicked a full provenance graph for all the project is generated and matched to the abstract graph
            provenance_graph_name = st.sidebar.text_input(
                "Path for concrete provenance graph"
            )
            button_clicked = st.sidebar.button("Match")

            if button_clicked:
                gdb_prov = provenance_graph(provenance_graph_name)
                next_nodes = workflow_diff(gdb, gdb_prov)

    

    