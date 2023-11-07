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
from .graph_base import GraphBase


class GraphProvenanceTasks(GraphBase):  # pylint: disable = too-few-public-methods
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
            commits = list(repo.iter_commits(repo.heads[self.ds_branch]))
            dl_run_commits = self._get_commit_list(commits)

            for commit in dl_run_commits:
                dict_o = self._commit_message_node_extract(commit)

                task = graphs.TaskWorkflow(
                    self.superdataset.path,
                    dict_o["cmd"],
                    commit.hexsha,
                    commit.author.name,
                    commit.authored_date,
                )

                if dict_o["inputs"]:
                    for input_file in dict_o["inputs"]:
                        input_path = glob.glob(
                            self.superdataset.path
                            + f"/**/*{os.path.basename(input_file)}",
                            recursive=True,
                        )[0]
                        task.parentFiles.append(input_path)
                
                if dict_o["outputs"]:
                    for output_file in dict_o["outputs"]:
                        output_path = glob.glob(
                            self.superdataset.path 
                            + f"/**/*{os.path.basename(output_file)}",
                            recursive=True,
                        )[0]
                        task.childFiles.append(output_path)

                node_list.append((task.commit, task.__dict__))

            for idx_node, node1 in enumerate(node_list):
                for node2 in node_list[:idx_node + 1]:
                    diff_set = set(node1[1]["childFiles"]).intersection(set(node2[1]["parentFiles"]))
                    if diff_set:
                        edge_list.append((node1[0], node2[0]))


        return node_list, edge_list
