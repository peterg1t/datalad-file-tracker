"""Utilities for graph provenance"""
import os
from datetime import datetime
from pathlib import Path
import datalad.api as dl
import git
import networkx as nx
import datetime as dt
import json
from copy import copy as shallow_copy
from typing import (Any,
                    TypeVar)

import utilities  # pylint: disable=import-error


def prov_scan(
    dataset_path, dataset_branch
):  # pylint: disable=too-many-statements, too-many-locals
    """! This function will return the nodes and edges list
    Args:
        ds_name (str): A path to the dataset (or subdataset)
    Returns:
        graph: A networkx graph
    """
    node_list = []
    edge_list = []
    superdataset = utilities.get_superdataset(dataset_path)
    subdatasets = [dataset_path]
    for subdataset in subdatasets:
        repo = git.Repo(subdataset)
        commits = list(repo.iter_commits(repo.heads[dataset_branch]))
        dl_run_commits = utilities.get_commit_list(commits)
        for commit in dl_run_commits:
            task = {}
            dict_o = utilities.commit_message_node_extract(commit)
            task["dataset"] = superdataset.path
            task["command"] = dict_o["cmd"]
            task["commit"] = commit.hexsha
            task["author"] = commit.author.name
            task["date"] = datetime.utcfromtimestamp(commit.authored_date).strftime(
                "%Y-%m-%d %H:%M:%S"
            )  # noqa: E501
            task["inputs"] = ",".join(sorted(dict_o["inputs"]))
            task["outputs"] = ",".join(sorted(dict_o["outputs"]))

            # inputs_full_path = [
            #     full_path_from_partial(superdataset.path, inp)
            #     for inp in dict_o["inputs"]
            # ]
            # outputs_full_path = [
            #     full_path_from_partial(superdataset.path, out)
            #     for out in dict_o["outputs"]
            # ]
            inputs = dict_o["inputs"]
            outputs = dict_o["outputs"]

            full_task_description = inputs + outputs
            full_task_description.append(dict_o["cmd"])
            task["ID"] = ",".join(sorted(full_task_description))
            if task["inputs"]:
                for input_file in inputs:
                    input_file_full_path = utilities.full_path_from_partial(
                        superdataset.path, input_file
                    )
                    file = {}
                    ds_file = git.Repo(
                        utilities.get_git_root(input_file_full_path)
                    )  # noqa: E501
                    file_status = dl.status(  # pylint: disable=no-member
                        path=input_file_full_path, dataset=ds_file.working_tree_dir
                    )[
                        0
                    ]  # noqa: E501
                    file["dataset"] = subdataset
                    file["path"] = input_file
                    file["commit"] = commit.hexsha
                    file["author"] = commit.author.name
                    file["date"] = datetime.utcfromtimestamp(
                        commit.authored_date
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    file["status"] = file_status["gitshasum"]
                    # file["ID"] = encode(input_file_full_path)
                    file["ID"] = input_file

                    node_list.append((file["path"], file))
                    edge_list.append((file["path"], task["commit"]))
            if task["outputs"]:
                for output_file in outputs:
                    output_file_full_path = utilities.full_path_from_partial(
                        superdataset.path, output_file
                    )
                    file = {}
                    ds_file = git.Repo(
                        utilities.get_git_root(output_file_full_path)
                    )  # noqa: E501
                    file_status = dl.status(  # pylint: disable=no-member
                        path=output_file_full_path, dataset=ds_file.working_tree_dir
                    )[0]
                    file["dataset"] = subdataset
                    file["path"] = output_file
                    file["commit"] = commit.hexsha
                    file["author"] = commit.author.name
                    file["date"] = datetime.utcfromtimestamp(
                        commit.authored_date
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    file["status"] = file_status["gitshasum"]
                    # file["ID"] = encode(output_file_full_path)
                    file["ID"] = output_file

                    node_list.append((file["path"], file))
                    edge_list.append((task["commit"], file["path"]))
            node_list.append((task["commit"], task))

    return node_list, edge_list


ListOrSet = TypeVar("ListOrSet", list, set)

def ensure_iter(s: Any, cls: type[ListOrSet], copy: bool=False, iterate: bool=True) -> ListOrSet:
    """Given not a list, would place it into a list. If None - empty list is returned

    Parameters
    ----------
    s: list or anything
    cls: class
      Which iterable class to ensure
    copy: bool, optional
      If correct iterable is passed, it would generate its shallow copy
    iterate: bool, optional
      If it is not a list, but something iterable (but not a str)
      iterate over it.
    """

    if isinstance(s, cls):
        return s if not copy else shallow_copy(s)
    elif isinstance(s, str):
        return cls((s,))
    elif iterate and hasattr(s, '__iter__'):
        return cls(s)
    elif s is None:
        return cls()
    else:
        return cls((s,))

def ensure_list(s: Any, copy: bool=False, iterate: bool=True) -> list:
    """Given not a list, would place it into a list. If None - empty list is returned

    Parameters
    ----------
    s: list or anything
    copy: bool, optional
      If list is passed, it would generate a shallow copy of the list
    iterate: bool, optional
      If it is not a list, but something iterable (but not a str)
      iterate over it.
    """
    return ensure_iter(s, list, copy=copy, iterate=iterate)


def abs2prov(abstract_graph: nx.DiGraph,
             dataset: Path,
             abstract_branch: str = "abstract"):
    """This function will take an abstract graph and write a provenance entry
    for every node in the dataset, this function will also take a branch name
    to store the provenance. If the input branch does not exists it will create it.

    Args:
        abstract_graph (DiGraph): An abstract graph
    """

    print("Function inputs", abstract_graph.nodes(data=True), abstract_branch, dataset)

    # First we create the branch in the existing dataset if it doesn't exists.
    # If the branch does exists then
    # we will check it out and commit to it.
    print("get_git_root", dataset, utilities.get_git_root(dataset))
    repo = git.Repo(utilities.get_git_root(dataset))
    branches_project = utilities.get_branches(utilities.get_git_root(dataset))

    ds = utilities.get_superdataset(dataset=dataset)
    
    print("branches", branches_project)
    
    if abstract_branch in branches_project:
        print("branch already exist in project")
        branch = repo.heads[abstract_branch]
        branch.checkout()
    else:
        print("branch des not exist in project")
        branch = repo.git.checkout("--orphan", abstract_branch)

    index = repo.index
    author = git.Actor("Pedro Martinez", "pemartin@ucalgaryc.ca")

    # Time objects for commit
    timezone_offset = -7.0  # Mountain Standard Time (UTC−07:00)
    tzinfo = dt.timezone(dt.timedelta(hours=timezone_offset))
    # [DATALAD RUNCMD] test

    # === Do not change lines below ===
    # {
    #  "chain": [],
    #  "cmd": "sh code/t1.sh -i {inputs} -o {outputs}",
    #  "dsid": "3ac42793-053f-4c19-8fc7-5925378e50c3",
    #  "exit": 0,
    #  "extra_inputs": [],
    #  "inputs": [
    #   "outputs/output_t0.txt"
    #  ],
    #  "outputs": [
    #   "outputs/output_t01.txt"
    #  ],
    #  "pwd": ".."
    # }
    # ^^^ Do not change lines above ^^^

    for node in abstract_graph.nodes(data=True):
        print(node)
        # For every node we need to push an empty commit  since there is no
        # file to be added to the dataset

        run_info = {
            'cmd': node[1]['command'],
            # rerun does not handle any prop being None, hence all
            # the `or/else []`
            'chain': [],
        }
    # for all following we need to make sure that the raw
    # specifications, incl. any placeholders make it into
    # the run-record to enable "parametric" re-runs
    # ...except when expansion was requested
        extra_inputs = []
        specs = {
            k: ensure_list(v) for k, v in (('inputs', node[1]['inputs']),
                                           ('extra_inputs', extra_inputs),
                                           ('outputs', node[1]['outputs']))
        }
        # run_info['inputs'] = specs['inputs']
        # run_info['outputs'] = specs['outputs']
        for k, v in specs.items():
            # we don't need to expand globs here as this graph is abstract
            run_info[k] = v

        run_info['pwd'] = os.path.abspath(os.path.dirname(__file__))
        if ds.id:
            run_info["dsid"] = ds.id
        # no extra info is added at this time
        # if extra_info:
        #     run_info.update(extra_info)
    
        record = json.dumps(run_info, indent=1, sort_keys=True, ensure_ascii=False)


        # compose commit message
        msg = f"""\
[DATALAD RUNCMD] {node[1]["message"]}

=== Do not change lines below ===
{record}
^^^ Do not change lines above ^^^
"""

        commit_date = dt.datetime.now(tzinfo)
        index.write()
        index.commit(message=msg,
                     author=author,
                     commit_date=commit_date)
