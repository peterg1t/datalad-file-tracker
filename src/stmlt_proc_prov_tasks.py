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
    attribute_mapping = {}
    repo = git.Repo(super_ds)
    tree = repo.heads[run].commit.tree
    output_datasets=[]
    
    for blob in tree.blobs:
        if blob.name == 'tf.csv':
            translation_file_data = blob.data_stream.read().decode('utf-8').split('\n')
            for row in translation_file_data[:-1]:
                row_splitted = row.split(',')
                attribute_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"
            
            gdb_abs_proc = utils.graph_remap_attributes(gdb_abs, attribute_mapping)
            gdb_conc = graphs.GraphProvenanceTasks(super_ds, run)
            gdb_abstract, gdb_difference = utils.graph_diff_tasks(gdb_abs_proc, gdb_conc)
            
            #We now need to get the input file/files for this job so it can be passed to the pending nodes job
            clone_dataset = f"/tmp/test_{run}"
            print("clone_dataset",clone_dataset)

            # clone the repo
            utils.sub_clone_flock(super_ds, clone_dataset, run)
            print("after clonning")
            
            # get all submodules with no data
            utils.sub_get(clone_dataset, True)

            #mark dead here (ephemeral dataset)
            utils.sub_dead_here(clone_dataset)
            print("after dead here")
            
            next_nodes = gdb_difference.next_nodes_run()
            for item in next_nodes:
                output_datasets.extend([os.path.dirname(os.path.relpath(s, super_ds)) for s in gdb_abstract.graph.successors(item) if os.path.exists(os.path.dirname(os.path.join(clone_dataset,os.path.relpath(s, super_ds))))])

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
        runs (lst): A list of branches (could also contain just one branch)
    """
    node_abstract_list, edge_abstract_list = utils.gcg_processing_tasks(abstract)
    gdb_abs = graphs.GraphBaseTasks(node_abstract_list, edge_abstract_list)

        
    outputs=[]
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        fs = {executor.submit(graph_diff_calc, gdb_abs, provenance_path, run) for run in runs}
        
        
        for future in futures.as_completed(fs):
            outputs.extend(future.result())
    

    # now we perform a git merge and branch delete on origin
    outputs = list(set(outputs))
    print('b4 merging',provenance_path, outputs)
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
                        or  T<>{task}<>{prec_nodes}<>{command}<>{transformation}   ",
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
