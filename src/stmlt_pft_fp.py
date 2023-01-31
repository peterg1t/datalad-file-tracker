"""
Docstring
"""
import argparse
import cProfile
import streamlit as st
from graphProvDB import graphProvDB


profiler = cProfile.Profile()

st.set_page_config(layout="wide")
st.write("""
Welcome to the database provenance tracker!
""")



def git_log_parse(ds_name):
    """! This function will generate the graph of the entire project
    Args:
        ds_name (str): An absolute path to the dataset name
        a_option (str): An analysis mode for the node calculation
    """
    gdb = graphProvDB(ds_name)
    plot_db = gdb.graph_object_plot()
    st.bokeh_chart(plot_db, use_container_width=True)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dspath", help="Path to dataset")
    parser.add_argument("-a", "--analysis", help="Analysis to apply to nodes", choices=['Centrality','Betweeness'])

    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dataset_name = args.dspath
        analysis_type = args.analysis
    else:
        print("Not all command line arguments were used as input, results might be wrong")
        dataset_name = st.text_input('Input the dataset to track')
        with st.sidebar:
            analysis_type = st.selectbox('Analysis mode', ['None', 'Degree Centrality', 'Betweeness Centrality'])


    # Sreamlit UI implementation
    if dataset_name:
        git_log_parse(dataset_name)
