"""
Docstring
"""

import argparse
import cProfile
import csv
import os
import time
from concurrent import futures
import logging
from pathlib import Path
from datetime import datetime
from retrying import retry

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
    job_defaults = {"coalesce": False, "max_instances": 3}
    return BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone="UTC",
        first_run_time=datetime.now(),
    )


def remote_job_submit(dataset, brnch, inputs, outputs, message, command):
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
    if os.path.exists(os.path.join(dataset, os.path.dirname(outputs[0]))):
        pass
    else:
        os.mkdir(os.path.join(dataset, os.path.dirname(outputs[0])))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)
    # saving the dataset prior to processing

    dataset = dl.Dataset(dataset)

    dl.save(  # pylint: disable=no-member
        path=dataset.path,
        dataset=dataset.path,
        recursive=True,
    )

    datalad_run_command = f"cd {dataset.path}; git checkout {brnch}; datalad run -m '{message}' -d {dataset.path} -i {inputs_proc} -o {outputs_proc} '{command}'"  # noqa: E501

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

    return ("logs", outlog, errlog)


@retry(wait_fixed=2000, stop_max_attempt_number=7)
def file_sense(dataset: Path, file: Path, branch_run):
    file_size1 = os.path.getsize(file)
    time.sleep(0.5)
    file_size2 = os.path.getsize(file)
    if not file.exists():
        print("File does not exists yet")
        raise IOError("File not present")
    if file_size1 != file_size2:
        print("File still downloading")
        raise IOError("File still downloading")
    # Execute git import
    print(f"File {file} now exists and it is ready to be consumed")
    print(utilities.git_bundle_import(dataset, file, branch_run))


def run_pending_nodes_gce_scheduler(
    remote_endpoint_id, graph_difference: nx.DiGraph, branch_run: str
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
        REMOTE_DIR = "/home/pemartin"
        remote_dataset = f"{REMOTE_DIR}/datalad-distribits-remote"
        SRC_COLLECTION_ID = "05d01160-cb63-11ee-86f4-a14c48059678"
        DST_COLLECTION_ID = "c6ac8e0e-b18f-11ee-b088-4bb870e392e2"
        DST_DIR = "/Users/pemartin/Scripts"
        local_dataset = f"{DST_DIR}/datalad-distribits"

        # We are going to use only relative paths
        inputs_remote = [os.path.relpath(inp, dataset) for inp in inputs]
        outputs_remote = [os.path.relpath(out, dataset) for out in outputs]
        command_remote = command.replace(f"{dataset}/", "")

        gcc = Client(code_serialization_strategy=CombinedCode())
        with Executor(
            endpoint_id=remote_endpoint_id, client=gcc, user_endpoint_config={}
        ) as gce:
            print("ID", gce.endpoint_id)

            # ... then submit for execution, ...
            future_task_compute = gce.submit(
                remote_job_submit,
                remote_dataset,
                branch_run,
                inputs_remote,
                outputs_remote,
                message,
                command_remote,
            )
            try:
                print("Future result", future_task_compute.result())
            except Exception as exc:
                print("Globus Compute returned an exception: ", exc)

    with Executor(
        endpoint_id=remote_endpoint_id, client=gcc, user_endpoint_config={}
    ) as gce:
        future_task_bundle_create = gce.submit(
            utilities.git_bundle_create,
            remote_dataset,
            branch_run,
            REMOTE_DIR,
            len(next_nodes),
        )
        try:
            print("Bundle created", future_task_bundle_create.result())
        except Exception as exc:
            print("Globus Compute returned an exception: ", exc)

        print(
            utilities.globus_transfer(
                SRC_COLLECTION_ID,
                DST_COLLECTION_ID,
                future_task_bundle_create.result()[-1],
                DST_DIR,
                "One file transfer",
            )
        )

        local_path_bundle = Path(
            DST_DIR, os.path.basename(future_task_bundle_create.result()[-1])
        )
        file_sense(Path(local_dataset), local_path_bundle, branch_run)


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
    GCE_REMOTE_ENPOINT_ID = "ba2ebd33-a9e6-4bac-a472-ce239421f414"

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
        with open(f"{provpath}/tf.csv", "r", encoding="utf-8") as translation_file:
            reader = csv.reader(translation_file)
            for row in reader:
                node_mapping[row[0]] = f"{provpath}/{row[1]}"

        if utilities.exists_case_sensitive(provpath):
            gdb_provenance = graphs.prov_scan_task(provpath, run)

        gdb_abstract = match.graph_remap_command_task(gdb, node_mapping)
        gdb_abstract = match.graph_id_relabel(gdb_abstract, node_mapping)
        gdb_difference = match.graph_diff_tasks(gdb_abstract, gdb_provenance)
        # print("ABSTRACT", gdb_abstract.nodes(data=True))
        # print("PROV", gdb_provenance.nodes(data=True))

        if gdb_difference:
            # print("DIFF", gdb_difference.nodes(data=True))
            graph_plot_diff = graphs.graph_object_plot_task(gdb_abstract)

        run_pending_nodes_gce_scheduler(GCE_REMOTE_ENPOINT_ID, gdb_difference, run)
