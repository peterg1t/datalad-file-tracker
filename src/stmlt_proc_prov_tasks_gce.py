"""
Docstring
"""
import argparse
import cProfile
import csv
import os
from concurrent import futures
import logging
from pathlib import Path
from datetime import datetime

import git
import networkx as nx
from bokeh.plotting import show
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode

import graphs
import match
import utilities

profiler = cProfile.Profile()


def scheduler_configuration():
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
    job_defaults = {"coalesce": False,
                    "max_instances": 3}
    return BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone='UTC',
        first_run_time=datetime.now()
    )



def remote_job_submit(
    dataset, branch, inputs, outputs, message, command
):
    """! This function will execute the datalad run command

    Args:
        dataset (str): Path to the dataset
        input (str): Path to input
        output (str): Path to output
        message (str): Commit message
        command (str): Command

    Raises:
        Exception: If error is found
    """
    import os
    import subprocess
    import datalad.api as dl
    outlogs = []
    errlogs = []

    # making the output stage folder
    if os.path.exists(os.path.dirname(outputs[0])):
        pass
    else:
        os.mkdir(os.path.dirname(outputs[0]))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)
    # saving the dataset prior to processing

    dataset = dl.Dataset(dataset)

    dl.save(  # pylint: disable=no-member
        path=dataset.path,
        dataset=dataset.path,
        recursive=True,
    )

    datalad_run_command = f"cd {dataset.path}; git checkout {branch}; datalad run -m '{message}' -d {dataset.path} -i {inputs_proc} -o {outputs_proc} '{command}'"  # noqa: E501

    command_run_output = subprocess.run(
        datalad_run_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = command_run_output.stdout.split("\n")
    errlog = command_run_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    if "error" in errlog:
        raise Exception(
            """Error found in the datalad containers run command,
                check the log for more information on this error."""
            )

    print("logs", outlog, errlog)


def run_pending_nodes_gce_scheduler(
    remote_endpoint_id,
    graph_difference: nx.DiGraph,
    branch_run: str
):  # pylint: disable=too-many-locals
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        graph_difference (graph): A graph of differences (abs-prov)
    """
    inputs = []
    next_nodes = match.next_nodes_run(graph_difference)
    print("NEXT NODES TO RUN", next_nodes, branch_run)
    for item in next_nodes:
        inputs = graph_difference.nodes(data=True)[item]["inputs"]
        outputs = graph_difference.nodes(data=True)[item]["outputs"]
        dataset = utilities.get_git_root(os.path.dirname(inputs[0]))
        command = graph_difference.nodes(data=True)[item]["command"]
        message = "test-remote"

        # we need to rename the inputs with the remote dataset
        remote_dataset = "/home/pemartin/datalad-distribits-remote"
        inputs_remote = [inp.replace(dataset, remote_dataset) for inp in inputs]
        outputs_remote = [out.replace(dataset, remote_dataset) for out in inputs]

        gcc = Client(code_serialization_strategy=CombinedCode())
        with Executor(endpoint_id=remote_endpoint_id, client=gcc) as gce:
            # ... then submit for execution, ...
            future = gce.submit(remote_job_submit,
                                dataset,
                                branch_run,
                                inputs,
                                outputs,
                                message,
                                command)

    # ... and finally, wait for the result
    try:
        print("Future result", future.result())
    except Exception as exc:
        print("Globus Compute returned an exception: ", exc)


def run_pending_nodes_scheduler(
    scheduler_instance,
    graph_difference: nx.DiGraph,
    branch_run: str
):  # pylint: disable=too-many-locals
    """! Given a graph and the list of nodes (and requirements i.e. inputs)
    compute the task with APScheduler

    Args:
        graph_difference (graph): A graph of differences (abs-prov)
    """
    inputs = []
    next_nodes = match.next_nodes_run(graph_difference)
    print("NEXT NODES TO RUN", next_nodes, branch_run)
    for item in next_nodes:
        inputs = graph_difference.nodes(data=True)[item]["inputs"]
        outputs = graph_difference.nodes(data=True)[item]["outputs"]
        dataset = utilities.get_git_root(os.path.dirname(inputs[0]))
        command = graph_difference.nodes(data=True)[item]["command"]
        message = "test"

        scheduler_instance.add_job(
            utilities.job_submit,
            args=[dataset, branch_run, inputs, outputs, message, command],
        )



# def run_preparation_worktree(super_ds, run):
#     """
#     Run preparation tasks for a worktree associated with a superdataset.

#     This function initiates preparation tasks for a worktree associated with the
#     specified superdataset. It delegates the preparation to the `utilities.job_prepare`
#     function, which handles the actual preparation work.

#     Args:
#         super_ds (str): The path to the superdataset for which the worktree
#         is being prepared.
#         run (Any): The run-specific information or configuration required
#           for the preparation.

#     Returns:
#         None

#     Notes:
#         - The `utilities` module must contain a function named `job_prepare` that
#           performs the actual preparation work.
#     """
#     utilities.job_prepare(super_ds, run)


# def run_cleaning_worktree(super_ds):
#     """_summary_

#     Args:
#         super_ds (_type_): _description_
#     """
#     utilities.job_clean(super_ds)


# def graph_diff_calc(gdb_abs, super_ds, run):  # pylint: disable=too-many-locals
#     """Calculate the graph differences and perform necessary actions based
#       on the provided parameters.

#     Args:
#         gdb_abs (DiGraph): The abstract graph database.
#         super_ds (str): The path to the super dataset.
#         run (str): The specific run to analyze.

#     Returns:
#         List[str]: A list of output datasets resulting from the graph differences.
#     """
#     attribute_mapping = {}
#     repo = git.Repo(super_ds)
#     tree = repo.heads[run].commit.tree
#     output_datasets = []

#     for blob in tree.blobs:
#         if blob.name == "tf.csv":
#             translation_file_data = blob.data_stream.read().decode("utf-8").split("\n")
#             for row in translation_file_data[:-1]:
#                 row_splitted = row.split(",")
#                 # attribute_mapping[row_splitted[0]] = f"{super_ds}/{row_splitted[1]}"
#                 attribute_mapping[row_splitted[0]] = f"{row_splitted[1]}"

#             gdb_abs_proc = match.graph_remap_command_task(gdb_abs, attribute_mapping)
#             gdb_abs_proc = match.graph_id_relabel(gdb_abs_proc, attribute_mapping)
#             nodes_provenance, edges_provenance = graphs.prov_scan_task(super_ds, run)
#             gdb_provenance = nx.DiGraph()
#             gdb_provenance.add_nodes_from(nodes_provenance)
#             gdb_provenance.add_edges_from(edges_provenance)

#             gdb_difference = match.graph_diff_tasks(gdb_abs_proc, gdb_provenance)

#             list_nodes_run = graphs.start_nodes(gdb_difference)
#             print("list_nodes_run", list_nodes_run)

#             # We now need to get the input file/files for this job so it can be passed
#             # to the pending nodes job
#             clone_dataset = f"/tmp/test_{run}"

#             super_ds = "/home/peter/Devel/datalad-distribits-v2"
#             submit_globus_job(super_ds, clone_dataset, run)
#             exit(0)

#             # clone the repo
#             utilities.sub_clone_flock(super_ds, clone_dataset, run)

#             # get all submodules with no data
#             utilities.sub_get(clone_dataset, True)

#             # mark dead here (ephemeral dataset)
#             utilities.sub_dead_here(clone_dataset)

#             next_nodes = match.next_nodes_run(gdb_difference)
#             for item in next_nodes:
#                 output_datasets.extend(
#                     [
#                         os.path.dirname(os.path.relpath(s, super_ds))
#                         for s in gdb_abs.successors(item)
#                         if os.path.exists(
#                             os.path.dirname(
#                                 os.path.join(
#                                     clone_dataset, os.path.relpath(s, super_ds)
#                                 )
#                             )
#                         )
#                     ]
#                 )

#             for item in output_datasets:
#                 utilities.job_checkout(clone_dataset, item, run)

#             status = utilities.run_pending_nodes(
#                 super_ds, clone_dataset, gdb_abs, gdb_difference, run
#             )

#             if status is not None:
#                 for item in output_datasets:
#                     utilities.sub_push_flock(clone_dataset, item, "origin")

#     return output_datasets


# def match_run(abstract, provenance_path, branch):
#     """This function will match and run pending nodes

#     Args:
#         abstract (graph): Abstract graph
#         provenance_path (graph): Concrete graph
#         branch (lst): A list of branches (could also contain just one branch)
#     """
#     node_abstract_list, edge_abstract_list = graphs.gcg_processing_tasks(abstract)
#     gdb_abs = nx.DiGraph()
#     gdb_abs.add_nodes_from(node_abstract_list)
#     gdb_abs.add_edges_from(edge_abstract_list)

#     outputs = []
#     with futures.ProcessPoolExecutor(max_workers=4) as executor:
#         future_results = {
#             executor.submit(graph_diff_calc, gdb_abs, provenance_path, run)
#             for run in branch
#         }

#         for future in futures.as_completed(future_results):
#             outputs.extend(future.result())

#     # Now we perform a git merge and branch delete
#     outputs = list(set(outputs))
#     for output in outputs:
#         utilities.git_merge(provenance_path, output)

#     # Saving the current branch
#     repo = git.Repo(provenance_path)
#     current_branch = repo.active_branch
#     utilities.branch_save(provenance_path, current_branch)

#     # Now for every other branch we save the datasets to acknowledge the changes
#     for run in branch:
#         utilities.branch_save(provenance_path, run)


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

    # we initialize the scheduler
    scheduler_instance_jobs = scheduler_configuration()

    # we set the gce id
    gce_remote_endpoint_id = "edcbe7e1-e271-4790-ba81-885f1c038779"

    abspath = Path(args.agraph)
    provpath = Path(args.pgraph)
    runs = args.runs



    # Create abstract graph
    gdb = graphs.create_absract_graph_tasks(abspath)

    # Create provenance graph for every run (orhpan branch)
    for run in runs:

        # For every branch we need to use a translation file
        # in the root folder of the dataset
        node_mapping = {}
        repo = git.Repo(provpath)
        branch = repo.heads[run]
        branch.checkout()
        with open(
            f"{provpath}/tf.csv", "r", encoding="utf-8"
        ) as translation_file:
            reader = csv.reader(translation_file)
            for row in reader:
                node_mapping[row[0]] = f"{provpath}/{row[1]}"

        if utilities.exists_case_sensitive(provpath):
            gdb_provenance = graphs.prov_scan_task(
                provpath, run
            )

        gdb_abstract = match.graph_remap_command_task(gdb, node_mapping)
        gdb_abstract = match.graph_id_relabel(gdb_abstract, node_mapping)
        gdb_difference = match.graph_diff_tasks(gdb_abstract, gdb_provenance)
        # print("ABSTRACT", gdb_abstract.nodes(data=True))
        # print("PROV", gdb_provenance.nodes(data=True))

        if gdb_difference:
            # print("DIFF", gdb_difference.nodes(data=True))
            graph_plot_diff = graphs.graph_object_plot_task(gdb_abstract)

        # run_pending_nodes_scheduler(scheduler_instance_jobs, gdb_difference, run)
        run_pending_nodes_gce_scheduler(gce_remote_endpoint_id, gdb_difference, run)



    # print("Pending jobs", scheduler_instance_jobs.get_jobs())
    # scheduler_instance_jobs.start()
 
    # print("BG scheduler status", scheduler_instance_jobs.running)
    # scheduler_instance_jobs.remove_all_jobs()
    # scheduler_instance_jobs.shutdown()

