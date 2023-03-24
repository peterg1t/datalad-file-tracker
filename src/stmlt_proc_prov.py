"""
Docstring
"""
import os
import argparse
import git
from itertools import repeat
from multiprocessing import Pool
from concurrent import futures
import datalad.api as dl
from bokeh.io import export_png
import csv
import time

import cProfile

import graphs
from bokeh.transform import linear_cmap

import utils


profiler = cProfile.Profile()



def run_preparation_worktree(ds, run):
    utils.job_prepare(ds, run)


def run_cleaning_worktree(ds):
    utils.job_clean(ds)


def graph_diff_calc(gdb_abs, ds, run):
    
    node_mapping = {}
    repo = git.Repo(ds)
    tree = repo.heads[run].commit.tree
    
 
    
    for blob in tree.blobs:
        if blob.name == 'tf.csv':
            translation_file_data = blob.data_stream.read().decode('utf-8').split('\n')
 
            for row in translation_file_data[:-1]:
                row_splitted = row.split(',')
                node_mapping[row_splitted[0]] = f"{ds}/{row_splitted[1]}"
            
            gdb_abs_proc = utils.graph_relabel(gdb_abs,node_mapping)
            print('node_mapping', node_mapping)

            gdb_conc = graphs.GraphProvenance(ds, run)
            # gplot_concrete = gdb_conc.graph_object_plot()
            # export_png(gplot_concrete, filename=f"/tmp/graph_concrete_{run}.png")
            gdb_abstract, gdb_difference = utils.graph_diff(gdb_abs_proc, gdb_conc)
            # print('run->', run, gdb_difference.graph.nodes, gdb_abstract.graph.nodes(data=True))

            #We now need to get the input file/files for this job so it can be passed to the pending nodes job
            job_dataset = f"/tmp/test_{run}"

            utils.sub_clone_flock(ds, job_dataset, run)
            utils.sub_get(job_dataset, True)
            utils.sub_dead_here(job_dataset)

            utils.run_pending_nodes(job_dataset, gdb_abstract, gdb_difference, run)
            # utils.sub_push_flock(job_dataset, 'origin')
            








def match_run(abstract, provenance_path, runs):
    """This function will match and run pending nodes

    Args:
        abstract (graph): Abstract graph
        provenance_path (graph): Concrete graph
        runs (str): branch
    """
    node_abstract_list, edge_abstract_list = utils.gcg_processing(abstract)
    gdb_abs = graphs.GraphBase(node_abstract_list, edge_abstract_list)

    # for several runs we are going to create a pool

    # with Pool(4) as p:
        # p.starmap(graph_diff_calc, zip(repeat(gdb_abs), repeat(provenance_path), runs))

    
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        fs = [executor.submit(graph_diff_calc, gdb_abs, provenance_path, run) for run in runs]
        
        for future in futures.as_completed(fs):
            print(f"result: {future.result()}")

    # for run in runs:
        # graph_diff_calc(gdb_abs, provenance_path, run)


    #perform merge




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a",
        "--agraph",
        type=str,
        help="Path to graph txt file. \
                        Content must have the F<>{files}<>{prec_nodes} format per line\
                        or  T<>{task}<>{command}<>{prec_nodes}<>{transformation}   ",
        required=True,
    )

    parser.add_argument(
        "-p",
        "--pgraph",
        type=str,
        help="Path to provenance dataset (superdataset)",
        required=True,
    )

    parser.add_argument(
        "-r", "--runs", nargs="+", help="Run number to match", required=True
    )

    args = parser.parse_args()  # pylint: disable = invalid-name

    abspath = args.agraph
    provpath = args.pgraph
    runs = args.runs

    # Match run
    match_run(abspath, provpath, runs)
