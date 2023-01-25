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



def graph_components_generator(num_tasks):
    """! This function will generate the graph of the entire project
    Args:
    """

    col1, col2, col3 = st.columns(3)
    inputs = []
    cmds = []
    outputs = []
    for i in range(num_tasks):
        expander = st.expander(label=f"expander{i}")
        with expander:
            inputs.append(col1.text_input(f"Write the inputs for task {i}, separated by commas", key=f"inputs_{i}"))
            cmds.append(col2.text_input(f"Write the command for task {i}", key=f"cmd_{i}"))
            outputs.append(col3.text_input(f"Write the outputs for task {i}, separated by commas", key=f"outputs_{i}"))
        
    return inputs, cmds, outputs


def plot_graph(plot):
    st.bokeh_chart(plot, use_container_width=True)

    

if __name__ == "__main__":
    # Sreamlit UI implementation
    
    num_tasks = st.number_input('Please define a number of tasks', min_value=1)
    file_inputs, commands, file_outputs = graph_components_generator(num_tasks)
    gdb = graphAbs(file_inputs, commands, file_outputs)
    graph_plot = gdb.graph_ObjPlot()
    st.button('Generate',on_click=plot_graph(graph_plot))
    
