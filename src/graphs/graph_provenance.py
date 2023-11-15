import os
import datalad.api as dl
from datetime import datetime
import git
import utilities

def prov_scan(dataset_path, dataset_branch):
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
            )
            task["inputs"] = ",".join(sorted(dict_o["inputs"]))
            task["outputs"] = ",".join(sorted(dict_o["outputs"]))

            inputs_full_path = [
                utilities.full_path_from_partial(superdataset.path, inp)
                for inp in dict_o["inputs"]
            ]
            outputs_full_path = [
                utilities.full_path_from_partial(superdataset.path, out)
                for out in dict_o["outputs"]
            ]
            full_task_description = inputs_full_path + outputs_full_path
            full_task_description.append(dict_o["cmd"])
            task["ID"] = utilities.encode(",".join(sorted(full_task_description)))
            if task["inputs"]:
                for input_file in inputs_full_path:
                    file = {}
                    ds_file = git.Repo(utilities.get_git_root(input_file))
                    file_status = dl.status(
                        path=input_file, dataset=ds_file.working_tree_dir
                    )[0]
                    file["dataset"] = subdataset
                    file["path"] = input_file
                    file["commit"] = commit.hexsha
                    file["author"] = commit.author.name
                    file["date"] = datetime.utcfromtimestamp(
                        commit.authored_date
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    file["status"] = file_status["gitshasum"]
                    file["ID"] = utilities.encode(file["path"])

                    node_list.append((file["path"], file))
                    edge_list.append((file["path"], task["commit"]))
            if task["outputs"]:
                for output_file in outputs_full_path:
                    file = {}

                    ds_file = git.Repo(utilities.get_git_root(output_file))
                    file_status = dl.status(
                        path=output_file, dataset=ds_file.working_tree_dir
                    )[0]
                    file["dataset"] = subdataset
                    file["path"] = output_file
                    file["commit"] = commit.hexsha
                    file["author"] = commit.author.name
                    file["date"] = datetime.utcfromtimestamp(
                        commit.authored_date
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    file["status"] = file_status["gitshasum"]
                    file["ID"] = utilities.encode(file["path"])

                    node_list.append((file["path"], file))
                    edge_list.append((task["commit"], file["path"]))
            node_list.append((task["commit"], task))

    return node_list, edge_list
