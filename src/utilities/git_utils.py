"""This module will contain functions for git operations"""
import ast
import os
import re
import subprocess

import datalad.api as dl
import git


def get_commit_list(commits):
    """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD"""
    return [item for item in commits if "DATALAD RUNCMD" in item.message]


def commit_message_node_extract(commit):
    """
    Extracts a dictionary representing a commit message node
    from a commit object.

    This function parses the commit message of a given commit
    object to extract a dictionary
    representing a commit message node. The commit message is
    expected to contain a JSON-like
    structure enclosed in curly braces ('{}').

    Args:
        commit (Commit): A commit object containing the commit
          message.

    Returns:
        dict: A dictionary representing the commit message node
        extracted from the commit. Returns an empty
        dictionary if no valid commit message node is found.
    """
    return ast.literal_eval(
        re.search(r"(?=\{)(.|\n)*?(?<=\}\n)", commit.message).group(0)
    )


def get_dataset(dataset):
    """! This function will return a Datalad dataset for the given path
    Args:
        dataset (str): _description_
    Returns:
        dset (Dataset): A Datalad dataset
    """
    dset = dl.Dataset(dataset)
    if dset is not None:
        return dset
    raise Exception("""Dataset not valid.""")


def get_superdataset(dataset: str) -> dl.Dataset:
    """! This function will return the superdataset
    Returns:
        sds/dset (Dataset): A datalad superdataset
    """
    dset = dl.Dataset(dataset)
    sds = dset.get_superdataset()

    if sds is not None:  # pylint: disable = no-else-return
        return sds
    else:
        return dset


def get_git_root(path_file):
    """! This function will get the git repo of a file
    Args:
        path_initial_file (str): A path to the initial file
    Returns:
        str: The root of the git repo
    """
    git_repo = git.Repo(path_file, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")

    return git_root


def get_branches(path_dataset):
    """This function will return all the branches of a datalad project except
      for git-annex which is not main nor an orphan branch

    Args:
        path_dataset (str): A path to the dataset

    Returns:
        list: A list of all branches
    """
    repo = git.Repo(path_dataset)
    repo_heads = repo.heads  # or it's alias: r.branches
    repo_heads_names = [h.name for h in repo_heads]
    repo_heads_names.remove("git-annex")
    return repo_heads_names


def sub_clone_flock(source_dataset, path_dataset, branch):
    """This function will clone a subdataset in a location specified by
    path_dataset in the case path_superdataset is specified the
    subdataset is nested under the superdataset. The difference with the
    function that uses the API call is that this function
    also uses a lock to prevent concurrency issues
     @param source_dataset (str): A path to the original dataset
     @param path_dataset (str): A path to the target dataset location
    """
    outlogs = []
    errlogs = []
    clone_command = f"cd {os.path.dirname(path_dataset)} && flock --verbose {source_dataset}/.git/datalad_lock datalad clone {source_dataset} {os.path.basename(path_dataset)} --branch {branch}"  # noqa: E501
    clone_command_output = subprocess.run(
        clone_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = clone_command_output.stdout.split("\n")
    errlog = clone_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    # print('outlogs=',outlogs)
    # print('errlogs=',errlogs)


def sub_get(source_dataset, recursive=False):  # pylint: disable=unused-argument
    """Retrieve the contents of a DataLad dataset.

    This function performs a 'datalad get' operation on the specified DataLad
    dataset, optionally recursively including subdatasets.

    Args:
        source_dataset (str): The path to the DataLad dataset to retrieve.
        recursive (bool, optional): If True, perform a recursive 'datalad get'
            operation including subdatasets. Defaults to False.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing two lists - the first
        list represents the standard output logs of the 'datalad get' operation,
        and the second list represents the standard error logs. Each log entry
        is a string.
    """
    outlogs = []
    errlogs = []
    get_command = f"cd {source_dataset} && datalad get -n -r ."
    get_command_output = subprocess.run(
        get_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = get_command_output.stdout.split("\n")
    errlog = get_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    # print('outlogs=',outlogs)
    # print('errlogs=',errlogs)


def sub_dead_here(source_dataset):
    """This function will mark a subdataset as a throwaway dataset

    Args:
        source_dataset (str): The path to th source subdataset
    """
    outlogs = []
    errlogs = []
    dead_here_command = f"cd {source_dataset} && git submodule foreach --recursive git annex dead here"  # noqa: E501
    dead_here_command_output = subprocess.run(
        dead_here_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = dead_here_command_output.stdout.split("\n")
    errlog = dead_here_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    # print('outlogs=',outlogs)
    # print('errlogs=',errlogs)


def sub_push_flock(
    clone_dataset, ds_output, sibling
):  # pylint: disable=unused-argument
    """This task will perform a push of a dataset with a file
    lock to prevent concurrency issues
    @param source_dataset (str): The source dataset to push
    @param sibling (str): The name of the sibling to push to
    """
    outlogs = []
    errlogs = []

    push_command = f"cd {clone_dataset} && flock --verbose {clone_dataset}/.git/datalad_lock datalad push -d {clone_dataset} --to {sibling} --data anything -f all -r"  # noqa: E501
    print("push->", push_command)
    push_command_output = subprocess.run(
        push_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = push_command_output.stdout.split("\n")
    errlog = push_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    # print('outlogs_push=',outlogs)
    # print('errlogs_push=',errlogs)


def job_checkout(clone_dataset, ds_output, branch):
    """This task will perform a branch checkout
    @param source_dataset (str): The source dataset to push
    @param sibling (str): The name of the sibling to push to
    """
    outlogs = []
    errlogs = []
    dataset = os.path.join(clone_dataset, ds_output)
    print("dataset to checkout", dataset)
    checkout_command = f"cd {dataset} && git -C {dataset} checkout --recurse-submodules -b 'job-{branch}'"  # noqa: E501
    # print('checkout_command->', checkout_command)
    checkout_command_output = subprocess.run(
        checkout_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = checkout_command_output.stdout.split("\n")
    errlog = checkout_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    # print('outlogs_checkout=',outlogs)
    # print('errlogs_checkout=',errlogs)


def git_merge(superdataset, ds_output):
    """This task will perform a push of a dataset with a file lock to
    prevent concurrency issues
    @param source_dataset (str): The source dataset to push
    @param sibling (str): The name of the sibling to push to
    """
    outlogs = []
    errlogs = []
    dataset = f"{superdataset}/{ds_output}"
    merge_command = f"cd {dataset} && git merge -m  'Octopus merge' $(git branch -l | grep 'job-' | tr -d ' ')"  # noqa: E501
    print("merge->", merge_command)
    merge_command_output = subprocess.run(
        merge_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = merge_command_output.stdout.split("\n")
    errlog = merge_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)
    print("outlogs_merge=", outlogs)
    print("errlogs_merge=", errlogs)


def branch_save(dataset, run):
    """This task will perform a push of a dataset with a file lock
    to prevent concurrency issues
    @param source_dataset (str): The source dataset to push
    @param sibling (str): The name of the sibling to push to
    """
    outlogs = []
    errlogs = []

    branch_save_command = f"cd {dataset} && git checkout {run} && datalad save -d^ {dataset} -m 'Updating status for branch {run}' -r"  # noqa: E501
    print("merge->", branch_save_command)
    branch_save_command_output = subprocess.run(
        branch_save_command, shell=True, capture_output=True, text=True, check=False
    )
    outlog = branch_save_command_output.stdout.split("\n")
    errlog = branch_save_command_output.stderr.split("\n")
    outlog.pop()  # drop the empty last element
    errlog.pop()  # drop the empty last element
    outlogs.append(outlog)
    errlogs.append(errlog)


#     print('outlogs_merge=',outlogs)
#     print('errlogs_merge=',errlogs)
