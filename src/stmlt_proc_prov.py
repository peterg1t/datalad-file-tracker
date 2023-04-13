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
import uuid
import csv
import time

import cProfile

import graphs
from bokeh.transform import linear_cmap

import utils


profiler = cProfile.Profile()



def run_preparation_worktree(super_ds, run):
    utils.job_prepare(super_ds, run)


def run_cleaning_worktree(super_ds):
    utils.job_clean(super_ds)


def graph_diff_calc(gdb_abs, super_ds, run):
    
    node_mapping = {}
    repo = git.Repo(super_ds)
    tree = repo.heads[run].commit.tree
    output_datasets=[]
    
    for blob in tree.blobs:
        if blob.name == 'tf.csv':
            translation_file_data = blob.data_stream.read().decode('utf-8').split('\n')
 
            for row in translation_file_data[:-1]:
                row_splitted = row.split(',')
                print('row',row,row_splitted, len(row_splitted))
                node_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"
            
            gdb_abs_proc = utils.graph_relabel(gdb_abs,node_mapping)
            print('node_mapping', node_mapping, run)
            print(gdb_abs_proc.graph.nodes())

            gdb_conc = graphs.GraphProvenance(super_ds, run)
            # gplot_concrete = gdb_conc.graph_object_plot()
            # export_png(gplot_concrete, filename=f"/tmp/graph_concrete_{run}.png")
            gdb_abstract, gdb_difference = utils.graph_diff(gdb_abs_proc, gdb_conc)
            # print('run->', run, gdb_difference.graph.nodes, gdb_abstract.graph.nodes(data=True))

            #We now need to get the input file/files for this job so it can be passed to the pending nodes job
            clone_dataset = f"/tmp/test_{run}"

            # clone the repo
            utils.sub_clone_flock(super_ds, clone_dataset, run)
            
            # get all submodules with no data
            utils.sub_get(clone_dataset, True)

            #mark dead here (ephemeral dataset)
            utils.sub_dead_here(clone_dataset)
            

            next_nodes = gdb_difference.next_nodes_run()
            # print('next task->',next_nodes)
            for item in next_nodes:
                output_datasets.extend([os.path.dirname(os.path.relpath(s, super_ds)) for s in gdb_abstract.graph.successors(item) if os.path.exists(os.path.dirname(os.path.join(clone_dataset,os.path.relpath(s, super_ds))))])

            # print('output_datasets->', output_datasets, run)
            for item in output_datasets:
                utils.job_checkout(clone_dataset, item, run)


            status = utils.run_pending_nodes(super_ds, clone_dataset, gdb_abstract, gdb_difference, run)
            # print('status->', run, status)

            if status is not None:
                for item in output_datasets:
                    utils.sub_push_flock(clone_dataset, item, 'origin')
            
 
    return output_datasets
            








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
    
    outputs=[]
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        fs = {executor.submit(graph_diff_calc, gdb_abs, provenance_path, run) for run in runs}
        
        
        for future in futures.as_completed(fs):
            # outputs.extend([x for x in future.result() if x not in outputs])
            outputs.extend(future.result())
            print(f"result: {outputs}")

    # now we perform a git merge and branch delete on origin
    outputs = list(set(outputs))
    for output in outputs:
        print('to_merge',provenance_path, output)
        utils.git_merge(provenance_path, output)
    
    #Saving the current branch
    repo = git.Repo(provenance_path)
    current_branch = repo.active_branch
    utils.branch_save(provenance_path, current_branch)


    # now for every other branch we save the datasets to acknowledge the changes
    for run in runs:
        utils.branch_save(provenance_path, run)

        

    

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
