"""Utilities for graph provenance"""
from datetime import datetime

import datalad.api as dl
import git

import utilities


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
                    ds_file = git.Repo(utilities.get_git_root(input_file_full_path))  # noqa: E501
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




def abs2prov(abstract_graph):
    """This function will take an abstract graph and write 

    Args:
        abstract_graph (_type_): _description_
    """
    pass

