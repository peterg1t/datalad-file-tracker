"""A class for graph provenance"""
import os
import re
import ast
import copy
import glob
import git
import datalad.api as dl
import utils
import graphs
from graphs.graph_base import GraphBase


class GraphProvenance(GraphBase):  # pylint: disable = too-few-public-methods
    """! This class will represent a graph created from provenance

    Returns:
        obj: A provenance graph object
    """

    def __init__(self, ds_name, ds_branch):
        self.dataset = self._get_dataset(ds_name)
        self.superdataset = self._get_superdataset(ds_name)
        self.ds_branch = ds_branch
        self.node_list, self.edge_list = self.prov_scan()
        super().__init__(self.node_list, self.edge_list)

    def _get_commit_list(self, commits):
        """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD"""
        return [item for item in commits if "DATALAD RUNCMD" in item.message]

    def _commit_message_node_extract(self, commit):
        return ast.literal_eval(
            re.search("(?=\{)(.|\n)*?(?<=\}\n)", commit.message).group(0)
        )

    def _get_dataset(self, dataset):
        """! This function will return a Datalad dataset for the given path

        Args:
            dataset (str): _description_

        Returns:
            dset (Dataset): A Datalad dataset
        """
        dset = dl.Dataset(dataset)
        if dset is not None:
            return dset

    def _get_superdataset(self, dataset):
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

    def prov_scan(self):
        """! This function will return the nodes and edges list
        Args:
            ds_name (str): A path to the dataset (or subdataset)

        Returns:
            graph: A networkx graph
        """
        node_list = []
        edge_list = []

        # subdatasets = self.superdataset.subdatasets()
        subdatasets = [self.dataset.path]

        for subdataset in subdatasets:
            repo = git.Repo(subdataset)
            repo.heads[self.ds_branch].checkout()
            branch = repo.active_branch
            commits = list(repo.iter_commits(branch))
            dl_run_commits = self._get_commit_list(commits)

            for commit in dl_run_commits:
                dict_o = self._commit_message_node_extract(commit)

                task = graphs.TaskWorkflow(
                    self.superdataset.path, dict_o["cmd"], commit.hexsha
                )

                if dict_o["inputs"]:
                    task.parent_files = dict_o["inputs"]
                    for input_file in task.parent_files:
                        print(
                            "task.parent_files",
                            task.parent_files,
                            self.superdataset.path,
                        )
                        print(f"/**/*{os.path.basename(input_file)}")
                        input_path = glob.glob(
                            self.superdataset.path
                            + f"/**/*{os.path.basename(input_file)}",
                            recursive=True,
                        )[0]
                        ds_file = git.Repo(os.path.dirname(input_path))
                        file_status = dl.status(
                            path=input_path, dataset=ds_file.working_tree_dir
                        )[0]

                        file = graphs.FileWorkflow(
                            subdataset,
                            input_path,
                            commit.hexsha,
                            file_status["gitshasum"],
                        )
                        file.ID = utils.encode(file.name)
                        file.child_task = dict_o["cmd"]

                        # Creating a shallow copy of the object attribute dictionary
                        dict_file = copy.copy(file.__dict__)
                        dict_file.pop("child_task", None)

                        node_list.append((file.name, dict_file))
                        edge_list.append((file.name, task.commit))

                if dict_o["outputs"]:
                    task.child_files = dict_o["outputs"]
                    for output in task.child_files:
                        output_path = glob.glob(
                            self.superdataset.path + f"/**/*{os.path.basename(output)}",
                            recursive=True,
                        )[0]
                        ds_file = git.Repo(os.path.dirname(output_path))
                        file_status = dl.status(
                            path=output_path, dataset=ds_file.working_tree_dir
                        )[0]

                        file = graphs.FileWorkflow(
                            subdataset,
                            output_path,
                            commit.hexsha,
                            file_status["gitshasum"],
                        )
                        file.ID = utils.encode(file.name)
                        file.parent_task = dict_o["cmd"]

                        dict_file = copy.copy(file.__dict__)
                        dict_file.pop("parent_task", None)

                        node_list.append((file.name, dict_file))
                        edge_list.append((task.commit, file.name))

                node_list.append((task.commit, task.__dict__))

        return node_list, edge_list
