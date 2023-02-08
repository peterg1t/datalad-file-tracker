"""
Docstring
"""
import argparse
import copy
import cProfile
import streamlit as st
import networkx as nx
from graphAbs import graphAbs
from graphProvDB import graphProvDB


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write(
    """
Welcome to the abstract graph builder!
"""
)


def remove_space(input):
    """This funcition remove spaces in strings

    Args:
        input (str): A string

    Returns:
        str: The string without spaces
    """
    return "".join(input.split())


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
                remove_space(
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
                remove_space(
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
    return graphProvDB(dataset)


def workflow_diff(abstract, provenance):
    for node in provenance.graph.nodes():
        print("node", node)
        if node in abstract.graph.nodes():
            nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
            if abstract.graph.nodes()[node]["type"] == "task":
                nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
            elif abstract.graph.nodes()[node]["type"] == "file":
                nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")

        print(abstract.graph.nodes()[node]["status"])

    graph_plot = abstract.graph_object_plot()
    plot_graph(graph_plot)

    gdb_diff = copy.deepcopy(gdb)
    gdb_diff.graph.remove_nodes_from(n for n in abstract.graph if n in provenance.graph)

    # In the difference graph the start_nodes is the list of nodes that can be started (these should usually be a task)
    start_nodes = gdb_diff.start_nodes()
    print("start nodes", start_nodes)

    print("diff", nx.get_node_attributes(gdb_diff.graph, "literal_name"))


if __name__ == "__main__":
    """Sreamlit UI implementation"""

    tasks_number = st.number_input("Please define a number of tasks", min_value=1)
    file_inputs, commands, file_outputs = graph_components_generator(tasks_number)

    graph_plot = None
    if "" in commands:
        st.text(
            "Empty task on abstract tree. Add a task or remove the row to proceed with the abstract graph generation."
        )
        pass

    else:
        gdb = graphAbs(file_inputs, commands, file_outputs)
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
            workflow_diff(gdb, gdb_prov)
