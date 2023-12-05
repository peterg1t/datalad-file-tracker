"""
Docstring
"""
import argparse
import cProfile
import os
from concurrent import futures

import git
import networkx as nx

from . import (
    graphs,
    match,
    utilities,
)

profiler = cProfile.Profile()


def run_preparation_worktree(super_ds, run):
    """
    Run preparation tasks for a worktree associated with a superdataset.

    This function initiates preparation tasks for a worktree associated with the
    specified superdataset. It delegates the preparation to the `utilities.job_prepare`
    function, which handles the actual preparation work.

    Args:
        super_ds (str): The path to the superdataset for which the worktree
        is being prepared.
        run (Any): The run-specific information or configuration required
          for the preparation.

    Returns:
        None

    Notes:
        - The `utilities` module must contain a function named `job_prepare` that
          performs the actual preparation work.
    """
    utilities.job_prepare(super_ds, run)


def run_cleaning_worktree(super_ds):
    """_summary_

    Args:
        super_ds (_type_): _description_
    """
    utilities.job_clean(super_ds)


def graph_diff_calc(gdb_abs, super_ds, run):  # pylint: disable=too-many-locals
    """Calculate the graph differences and perform necessary actions based
      on the provided parameters.

    Args:
        gdb_abs (DiGraph): The abstract graph database.
        super_ds (str): The path to the super dataset.
        run (str): The specific run to analyze.

    Returns:
        List[str]: A list of output datasets resulting from the graph differences.
    """
    attribute_mapping = {}
    repo = git.Repo(super_ds)
    tree = repo.heads[run].commit.tree
    output_datasets = []

    for blob in tree.blobs:
        if blob.name == "tf.csv":
            translation_file_data = blob.data_stream.read().decode("utf-8").split("\n")
            for row in translation_file_data[:-1]:
                row_splitted = row.split(",")
                attribute_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"

            gdb_abs_proc = match.graph_id_relabel(gdb_abs, attribute_mapping)
            nodes_provenance, edges_provenance = graphs.prov_scan(super_ds, run)
            gdb_provenance = nx.DiGraph()
            gdb_provenance.add_nodes_from(nodes_provenance)
            gdb_provenance.add_edges_from(edges_provenance)

            gdb_abstract, gdb_difference = match.graph_diff_tasks(
                gdb_abs_proc, gdb_provenance
            )

            # We now need to get the input file/files for this job so it can be passed
            # to the pending nodes job
            clone_dataset = f"/tmp/test_{run}"
            print("clone_dataset", clone_dataset)

            # clone the repo
            utilities.sub_clone_flock(super_ds, clone_dataset, run)
            print("after cloning")

            # get all submodules with no data
            utilities.sub_get(clone_dataset, True)

            # mark dead here (ephemeral dataset)
            utilities.sub_dead_here(clone_dataset)
            print("after dead here")

            next_nodes = gdb_difference.next_nodes_run()
            for item in next_nodes:
                output_datasets.extend(
                    [
                        os.path.dirname(os.path.relpath(s, super_ds))
                        for s in gdb_abstract.graph.successors(item)
                        if os.path.exists(
                            os.path.dirname(
                                os.path.join(
                                    clone_dataset, os.path.relpath(s, super_ds)
                                )
                            )
                        )
                    ]
                )

            for item in output_datasets:
                utilities.job_checkout(clone_dataset, item, run)

            status = utilities.run_pending_nodes(
                super_ds, clone_dataset, gdb_abstract, gdb_difference, run
            )
            # print('status->', run, status)

            if status is not None:
                for item in output_datasets:
                    utilities.sub_push_flock(clone_dataset, item, "origin")

    return output_datasets


def match_run(abstract, provenance_path, branch):
    """This function will match and run pending nodes

    Args:
        abstract (graph): Abstract graph
        provenance_path (graph): Concrete graph
        branch (lst): A list of branches (could also contain just one branch)
    """
    node_abstract_list, edge_abstract_list = graphs.gcg_processing_tasks(abstract)
    gdb_abs = nx.DiGraph()
    gdb_abs.add_nodes_from(node_abstract_list)
    gdb_abs.add_edges_from(edge_abstract_list)

    outputs = []
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        future_results = {
            executor.submit(graph_diff_calc, gdb_abs, provenance_path, run)
            for run in branch
        }

        for future in futures.as_completed(future_results):
            outputs.extend(future.result())

    # Now we perform a git merge and branch delete
    outputs = list(set(outputs))
    print("b4 merging", provenance_path, outputs)
    for output in outputs:
        print("to_merge", provenance_path, output)
        utilities.git_merge(provenance_path, output)

    # Saving the current branch
    repo = git.Repo(provenance_path)
    current_branch = repo.active_branch
    utilities.branch_save(provenance_path, current_branch)

    # Now for every other branch we save the datasets to acknowledge the changes
    for run in branch:
        utilities.branch_save(provenance_path, run)


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
