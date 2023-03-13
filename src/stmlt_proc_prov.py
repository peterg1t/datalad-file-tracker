"""
Docstring
"""
import os
import argparse
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor as Executor
import cProfile

import graphs
import networkx as nx
from bokeh.transform import linear_cmap

import utils


profiler = cProfile.Profile()

def match_run(abstract, provenance, run):
    print(abstract, provenance, run)
    pass



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--abspath", help="Path to abstract workflow", required=True)
    parser.add_argument("-p", "--provpath", help="Path to provenance dataset (superdataset)", required=True)
    parser.add_argument("-r", "--runs", nargs='+', help="Run number to match", required=True)
    

    args = parser.parse_args()  # pylint: disable = invalid-name

    abspath = args.abspath
    provpath = args.provpath
    runs = args.runs

    #Match run
    for run in runs:
        match_run(abspath, provpath, run)
    
            
