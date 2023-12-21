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
    """This function will prepare a job

    Args:
        super_ds (str): Path to the superdataset
        run (str): Run name
    """
    utilities.job_prepare(super_ds, run)


def run_cleaning_worktree(super_ds):
    """This function will clear the worktree

    Args:
        super_ds (str): A path to the superdatasets
    """
    utilities.job_clean(super_ds)


def graph_diff_calc(gdb_abs, super_ds, run):  # pylint: disable=too-many-locals
    """This function will calculate the difference between two graphs

    Args:
        gdb_abs (DiGraph): An abstract graph
        super_ds (str): A path to the superdataset
        run (str): A run name (branch)

    Returns:
        DiGraph: A difference tree
    """
    node_mapping = {}
    repo = git.Repo(super_ds)
    tree = repo.heads[run].commit.tree
    output_datasets = []

    for blob in tree.blobs:
        print("blob name", blob.name)
        if blob.name == "tf.csv":
            translation_file_data = (
                blob.data_stream.read().decode("utf-8").split("\n")
            )  # noqa: E501

            for row in translation_file_data[:-1]:
                row_splitted = row.split(",")
                print("row", row, row_splitted, len(row_splitted))
                node_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"

            gdb_abs_proc = match.graph_id_relabel(gdb_abs, node_mapping)

            nodes_provenance, edges_provenance = graphs.prov_scan(super_ds, run)
            gdb_provenance = nx.DiGraph()
            gdb_provenance.add_nodes_from(nodes_provenance)
            gdb_provenance.add_edges_from(edges_provenance)

            gdb_abstract, gdb_difference = match.graph_diff(
                gdb_abs_proc, gdb_provenance
            )

            # We now need to get the input file/files for this job so it can
            # be passed to the pending nodes job
            clone_dataset = f"/tmp/test_{run}"

            # clone the repo
            utilities.sub_clone_flock(super_ds, clone_dataset, run)

            # get all submodules with no data
            utilities.sub_get(clone_dataset, True)

            # mark dead here (ephemeral dataset)
            utilities.sub_dead_here(clone_dataset)

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


def match_run(abstract, provenance_path, all_runs):
    """This function will match and run pending nodes

    Args:
        abstract (graph): Abstract graph
        provenance_path (graph): Concrete graph
        all_runs (lst): A list of branches (could also contain just one branch)
    """
    node_abstract_list, edge_abstract_list = graphs.gcg_processing_tasks(abstract)
    gdb_abs = nx.DiGraph()
    gdb_abs.add_nodes_from(node_abstract_list)
    gdb_abs.add_edges_from(edge_abstract_list)

    outputs = []
    with futures.ProcessPoolExecutor(max_workers=4) as executor:
        future_results = {
            executor.submit(graph_diff_calc, gdb_abs, provenance_path, run)
            for run in all_runs
        }

        for future in futures.as_completed(future_results):
            outputs.extend(future.result())

    # now we perform a git merge and branch delete on origin
    outputs = list(set(outputs))
    print("b4 merging", provenance_path, outputs)
    for output in outputs:
        print("to_merge", provenance_path, output)
        utilities.git_merge(provenance_path, output)

    # Saving the current branch
    repo = git.Repo(provenance_path)
    current_branch = repo.active_branch
    utilities.branch_save(provenance_path, current_branch)

    # now for every other branch we save the datasets to acknowledge
    # the changes
    for run in all_runs:
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
