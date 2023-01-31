"""
Docstring
"""
import argparse
import cProfile
import streamlit as st
import networkx as nx
import utils
from utils.encode import encode
from graphAbs import graphAbs


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write("""
Welcome to the abstract graph builder!
""")



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
            inputs.append(col1.text_input(f"Write the inputs for task {i}, separated by commas", key=f"inputs_{i}"))
            commands.append(col2.text_input(f"Write the command for task {i}", key=f"cmd_{i}"))
            outputs.append(col3.text_input(f"Write the outputs for task {i}, separated by commas", key=f"outputs_{i}"))
        
    return inputs, commands, outputs


def plot_graph(plot):
    """Function to generate a bokeh chart

    Args:
        plot (plot): A networkx plot 
    """
    st.bokeh_chart(plot, use_container_width=True)

    

if __name__ == "__main__":
    """Sreamlit UI implementation
    """
        
    tasks_number = st.number_input('Please define a number of tasks', min_value=1)
    file_inputs, commands, file_outputs = graph_components_generator(tasks_number)

    
    graph_plot=None
    if '' in commands:
        st.text('Empty task on abstract tree. Add a task or remove the row to proceed with the abstract graph generation.')
        pass
        
    else: 
        gdb = graphAbs(file_inputs, commands, file_outputs)
        graph_plot = gdb.graph_object_plot()
        plot_graph(graph_plot)

        

        
    
