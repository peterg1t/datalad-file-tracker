"""Docstring."""
import os
import subprocess

import datalad.api as dl

from . import get_superdataset
from match import next_nodes_run


def command_submit(command):
    """This method will submit a command with subprocess

    Args:
        command (str): A command to be submitted

    Returns:
        str, str: Log files
    """
    command_run_output = subprocess.run(
        command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = command_run_output.stdout.split("\n")
    errlog = command_run_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element

    return outlog, errlog


def job_prepare(dataset, branch):
    """
    Retrieve the contents of a DataLad dataset.

    This function performs a 'datalad get' operation on the specified DataLad
    dataset, optionally including subdatasets recursively.

    Parameters:
        source_dataset (str): The path to the DataLad dataset to retrieve.
        recursive (bool, optional): If True, perform a recursive 'datalad get'
            operation including subdatasets. Defaults to False.

    Returns:
        tuple: A tuple containing two lists:
            - A list of strings representing the standard output logs of the
              'datalad get' operation.
            - A list of strings representing the standard error logs.
    """
    outlogs = []
    errlogs = []

    worktree_command = f"cd {dataset}; git worktree add .wt/{branch}_wt {branch}"
    outlog, errlog = command_submit(worktree_command)

    outlogs.append(("dataset->", dataset))
    outlogs.append(outlog)
    errlogs.append(errlog)

    print("logs", outlogs, errlogs)


def job_clean(dataset):
    """
    Clean up a DataLad dataset by removing worktrees and pruning unused ones.

    This function removes the worktree directory and prunes any unused worktrees
    associated with the specified DataLad dataset.

    Args:
        dataset (str): The path to the DataLad dataset to clean.

    Returns:
        None
    """
    outlogs = []
    errlogs = []

    worktree_rm_command = f"cd {dataset}; rm -rf .wt/"
    outlog, errlog = command_submit(worktree_rm_command)
    outlogs.append(outlog)
    errlogs.append(errlog)

    worktree_prune_command = f"cd {dataset}; git worktree prune"
    outlog, errlog = command_submit(worktree_prune_command)
    outlogs.append(outlog)
    errlogs.append(errlog)

    print("logs", outlogs, errlogs)


def run_pending_nodes(original_ds, dataset, gdb_abstract, gdb_difference, branch):
    """Process the next nodes to be run, generating inputs and outputs for a job.

    This method extracts information from the specified graph databases
      (`gdb_difference`
    and `gdb_abstract`) to determine the inputs and outputs for the next nodes to be
    executed. It then constructs paths relative to the dataset and checks the existence
    of the necessary directories. If all conditions are met, it extracts the command
    associated with the nodes and submits a job.

    Args:
        dataset (str): The path to the DataLad dataset.
        branch (str): The branch to which the job belongs.
        gdb_difference: Graph database representing the differences between datasets.
        gdb_abstract: Graph database representing the abstract representation of
          the workflow.
        original_ds (str): The path to the original dataset.

    Returns:
        job_submit_result: The result of the job submission, or None if no job was
          submitted.
    """
    inputs = []
    outputs = []
    # try:
    next_nodes_req = next_nodes_run(gdb_difference)
    print("next_nodes_req", next_nodes_req, "branch->", branch)

    for item in next_nodes_req:
        inputs.extend(
            [
                os.path.join(dataset, os.path.relpath(p, original_ds))
                for p in gdb_abstract.predecessors(item)
            ]
        )
        outputs.extend(
            [
                os.path.join(dataset, os.path.relpath(s, original_ds))
                for s in gdb_abstract.successors(item)
            ]
        )

        if inputs:
            if all(os.path.exists(os.path.dirname(f)) for f in outputs) and all(
                os.path.exists(os.path.dirname(f)) for f in inputs
            ):
                command = gdb_difference.nodes[item]["cmd"]
                message = "test"

                return job_submit(dataset, branch, inputs, outputs, message, command)

    return None


def job_submit(
    dataset, branch, inputs, outputs, message, command
):  # pylint: disable=too-many-arguments
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
    outlogs = []
    errlogs = []
    print("submitting job", inputs, outputs, branch, message, command)

    # making the output stage folder
    if os.path.exists(os.path.dirname(outputs[0])):
        pass
    else:
        os.mkdir(os.path.dirname(outputs[0]))

    inputs_proc = " -i ".join(inputs)
    outputs_proc = " -o ".join(outputs)
    # saving the dataset prior to processing

    superdataset = get_superdataset(dataset=dataset)

    dl.save(  # pylint: disable=no-member
        path=superdataset.path,
        dataset=superdataset.path,
        recursive=True,
    )

    datalad_run_command = f"cd {superdataset.path}; datalad run -m '{message}' -d^ -i {inputs_proc} -o {outputs_proc} '{command}'"  # noqa: E501
    print("command->", datalad_run_command, "branch->", branch)

    outlog, errlog = command_submit(datalad_run_command)
    outlogs.append(outlog)
    errlogs.append(errlog)
    for item in errlogs[0]:
        if "error" in item:
            raise Exception(
                """Error found in the datalad containers run command,
                check the log for more information on this error."""
            )

    print("logs", outlogs, errlogs)
    return outlogs
