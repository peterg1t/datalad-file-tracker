"""
Docstring
"""
import argparse
import cProfile
import os
import csv
from concurrent import futures

import git
import networkx as nx

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode

import graphs, match, utilities


profiler = cProfile.Profile()


def add_func(a, b):
    """Function to execute."""
    import time
    time.sleep(20)
    return a + b


def submit_globus_job(src_dataset, dest_dataset, branch):
    """Globus submit endpoint."""
    # tutorial endpoint (Raspberry Pi)
    tutorial_endpoint_id = '3677a8af-0b38-4941-b9e7-1edda0af44d8' 
    # ... then create the executor, ...
    gcc = Client(code_serialization_strategy=CombinedCode())
    with Executor(endpoint_id=tutorial_endpoint_id, client=gcc) as gce:
        # ... then submit for execution, ...
        future = gce.submit(add_func, 5, 10)
        # future = gce.submit(utilities.sub_clone_flock, src_dataset, dest_dataset, branch)

    # ... and finally, wait for the result
    try:
        print("Future result", future.result())
    except Exception as exc:
        print("Globus Compute returned an exception: ", exc)


def scheduler_configuration() -> int:
    """
    Initializes and customizes a background scheduler for task management.

    This function creates a BackgroundScheduler instance with a customized configuration,
    including a SQLAlchemyJobStore as the jobstore, a ThreadPoolExecutor with 8 threads
    as the executor, and specific job defaults.

    Returns:
    str: The current state of the scheduler.

    Notes:
    - The default jobstore is a MemoryJobStore, but in this customization, it is replaced
      with an SQLAlchemyJobStore using an SQLite database at the specified URL.
    - The default executor is a ThreadPoolExecutor, and its thread count is set to 8.
    - Job defaults include "coalesce" set to False and "max_instances" set to 3.
    - The scheduler is started after customization.

    Example:
    ```python
    scheduler_state = initialize_custom_scheduler()
    print(f"The scheduler is initialized with state: {scheduler_state}")
    ```
    """
    # We now start the background scheduler
    # scheduler = BackgroundScheduler()
    # This will get you a BackgroundScheduler with a MemoryJobStore named
    # “default” and a ThreadPoolExecutor named “default” with a default
    # maximum thread count of 10.

    # Lets customize the scheduler a little bit lets keep the default
    # MemoryJobStore but define a ProcessPoolExecutor
    jobstores = {
        "default": SQLAlchemyJobStore(
            url="sqlite:////Users/pemartin/Projects/file-provenance-tracker/src/jobstore.sqlite"  # noqa: E501
        )
    }
    executors = {
        "default": ThreadPoolExecutor(8),
    }
    job_defaults = {"coalesce": False, "max_instances": 3}
    return BackgroundScheduler(
        jobstores=jobstores, executors=executors, job_defaults=job_defaults
    )


def run_pending_nodes_scheduler(
    scheduler_instance, provenance_ds_path, gdb_difference, branch
):  # pylint: disable=too-many-locals
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        gdb_difference (graph): An abstract graph
    """
    inputs_dict = {}
    outputs_dict = {}
    inputs = []

    # we need to use the translation file so the nodes in the difference tree have the
    # file names instead of the abstract names. From the nodes we can extract the
    # list of inputs and outputs for the job that is going to run
    node_mapping = {}
    with open(
        f"{provenance_ds_path}/tf.csv", "r", encoding="utf-8"
    ) as translation_file:
        reader = csv.reader(translation_file)
        for row in reader:
            node_mapping[row[0]] = f"{provenance_ds_path}{row[1]}"

    gdb_difference = match.graph_id_relabel(gdb_difference, node_mapping)

    try:
        next_nodes = match.next_nodes_run(gdb_difference)
        for item in next_nodes:
            for predecessors in gdb_difference.predecessors(item):
                inputs_dict[predecessors] = gdb_difference.nodes[predecessors]
                inputs.append(gdb_difference.nodes[predecessors])

            for successors in gdb_difference.successors(item):
                outputs_dict[successors] = gdb_difference.nodes[successors]

            inputs = list(inputs_dict.keys())
            outputs = list(outputs_dict.keys())
            dataset = utilities.get_git_root(os.path.dirname(inputs[0]))
            command = gdb_difference.nodes[item]["cmd"]
            message = "test"

            scheduler_instance.add_job(
                utilities.job_submit,
                args=[dataset, branch, inputs, outputs, message, command],
            )

    except ValueError as err:  # pylint: disable = bare-except
        print(
            f"No provance graph has been matched to this abstract graph, match one first {err}"  # noqa: E501
        )


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
                # attribute_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"
                attribute_mapping[row_splitted[0]] = f"{row_splitted[1]}"

            gdb_abs_proc = match.graph_remap_command_task(gdb_abs, attribute_mapping)
            gdb_abs_proc = match.graph_id_relabel(gdb_abs_proc, attribute_mapping)
            nodes_provenance, edges_provenance = graphs.prov_scan_task(super_ds, run)
            gdb_provenance = nx.DiGraph()
            gdb_provenance.add_nodes_from(nodes_provenance)
            gdb_provenance.add_edges_from(edges_provenance)

            gdb_difference = match.graph_diff_tasks(
                gdb_abs_proc, gdb_provenance
            )

            list_nodes_run = graphs.start_nodes(gdb_difference)
            print("list_nodes_run", list_nodes_run)

            # We now need to get the input file/files for this job so it can be passed
            # to the pending nodes job
            clone_dataset = f"/tmp/test_{run}"

            super_ds = "/home/peter/Devel/datalad-distribits-v2"
            submit_globus_job(super_ds, clone_dataset, run)
            exit(0)



            # clone the repo
            utilities.sub_clone_flock(super_ds, clone_dataset, run)

            # get all submodules with no data
            utilities.sub_get(clone_dataset, True)

            # mark dead here (ephemeral dataset)
            utilities.sub_dead_here(clone_dataset)

            next_nodes = match.next_nodes_run(gdb_difference)
            for item in next_nodes:
                output_datasets.extend(
                    [
                        os.path.dirname(os.path.relpath(s, super_ds))
                        for s in gdb_abs.successors(item)
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
                super_ds, clone_dataset, gdb_abs, gdb_difference, run
            )

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
    for output in outputs:
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

    #we initialize the scheduler
    scheduler = scheduler_configuration()
    scheduler.start()

    abspath = args.agraph
    provpath = args.pgraph
    runs = args.runs

    # Match run
    match_run(abspath, provpath, runs)
