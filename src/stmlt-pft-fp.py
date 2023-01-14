import cmd
import os
from os import curdir
from pathlib import Path
import glob 
import profile
from tkinter.messagebox import NO
import copy


import git
import re
import ast
from fileinput import filename
from importlib.resources import path
from importlib_metadata import method_cache
from sqlalchemy import between
import streamlit as st



import datalad.api as dl
from datalad_metalad.extractors import runprov
from datalad_metalad import extract
from datalad_metalad.extract import Extract
from datalad_metalad.aggregate import Aggregate
import argparse

import networkx as nx



import utils

import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (BoxZoomTool, Circle, HoverTool, ResetTool, ColumnDataSource, LabelSet, DataRange1d)
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis

import cProfile, pstats

from graphProvDB import graphProvDB



    




profiler = cProfile.Profile()

st.write("""
Welcome to file provenance tracker!
""")




   


def git_log_parse(dsname, a_option):
    """! This function will generate the graph of the entire project
    Args:
        dsname (str): An absolute path to the filename
        a_option (str): An analysis mode for the node calculation 
    """
    gdb = graphProvDB(dsname)
    plot_db = gdb.graph_plot()
    st.bokeh_chart(plot_db, use_container_width=True)
        














if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--dspath", help="Path to dataset")
    parser.add_argument("-a", "--analysis", help="Analysis to apply to nodes", choices=['Centrality','Betweeness'])
    
    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dsnm = args.dspath
        analysis_type = args.analysis
    else: 
        print("Not all command line arguments were used as input, results might be wrong")
        dsnm = st.text_input('Input the dataset to track')
        with st.sidebar:
            analysis_type = st.selectbox('Analysis mode', ['None', 'Degree Centrality', 'Betweeness Centrality'])



    # Sreamlit UI implementation
    

    if dsnm:
        git_log_parse(dsnm, analysis_type)