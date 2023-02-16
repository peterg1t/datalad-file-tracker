import os
import git
import re
import ast
import copy
import glob
import datalad.api as dl
import networkx as nx
import utils
from graphs.taskWorkflow import taskWorkflow
from graphs.fileWorkflow import fileWorkflow


class graph_provenance:

    """! This class will represent a graph created from provenance

    Returns:
        obj: A provenance graph object
    """

    def __init__(self, ds_name):
        self.superdataset = self._get_superdataset(ds_name)
        self.dataset_list = []
        self.node_list = []
        self.edge_list = []
        # self.absGraphID = absGraphID # An abstract graph ID to match with this graph
        self.concrete_graph_ID = 0
        self.graph = self._graph_gen()




    def _gen_graph_ID(self, node_list):
        """Given a graph with a series of nodes compute the ID of the concrete graph"""
        return hash(tuple(node_list))
    



    def _get_commit_list(self, commits):
        """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD"""
        return [item for item in commits if "DATALAD RUNCMD" in item.message]
    



    def _commit_message_node_extract(self, commit):
        return ast.literal_eval(
            re.search("(?=\{)(.|\n)*?(?<=\}\n)", commit.message).group(
                0
            )  # pylint: disable=anomalous-backslash-in-string
        )



    def _get_superdataset(self, ds):
        """! This function will return the superdataset
        Returns:
            sds: A datalad superdataset
        """
        dset = dl.Dataset(ds)
        sds = dset.get_superdataset()
        if sds is not None:
            return sds
        else:
            return dset

    def _graph_gen(self):
        """! This function will return a graph from a dataset input
        Args:
            ds_name (str): A path to the dataset (or subdataset)

        Returns:
            graph: A networkx graph
        """

        self.dataset_list.append(self.superdataset.path)
        subdatasets = self.superdataset.subdatasets()

        for subdataset in subdatasets:
            repo = git.Repo(subdataset["path"])
            commits = list(repo.iter_commits("master"))
            # _get_commit_list(commits, run_commits)
            dl_run_commits = self._get_commit_list(commits)

            for commit in dl_run_commits:
                dict_o = self._commit_message_node_extract(commit)

                task = taskWorkflow(
                    self.superdataset.path, dict_o["cmd"], commit.hexsha
                )
                task.parentFiles = dict_o["inputs"]
                task.childFiles = dict_o["outputs"]

                task.compute_id()

                for input in dict_o["inputs"]:
                    input_path = glob.glob(
                        self.superdataset.path + f"/**/*{os.path.basename(input)}",
                        recursive=True,
                    )[0]
                    ds_file = git.Repo(os.path.dirname(input_path))
                    file_status = dl.status(
                        path=input_path, dataset=ds_file.working_tree_dir
                    )[0]

                    file = fileWorkflow(
                        subdataset, input_path, commit.hexsha, file_status["gitshasum"]
                    )
                    file.ID = utils.encode(file.name)
                    file.childTask = dict_o["cmd"]

                    # Creating a shallow copy of the object attribute dictionary
                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop("childTask", None)

                    self.node_list.append((file.name, dict_file))
                    self.edge_list.append((file.name, task.name))



                for output in dict_o["outputs"]:
                    print("output", output)
                    output_path = glob.glob(
                        self.superdataset.path + f"/**/*{os.path.basename(output)}",
                        recursive=True,
                    )[0]
                    ds_file = git.Repo(os.path.dirname(output_path))
                    file_status = dl.status(
                        path=output_path, dataset=ds_file.working_tree_dir
                    )[0]

                    file = fileWorkflow(
                        subdataset, output_path, commit.hexsha, file_status["gitshasum"]
                    )
                    file.ID = utils.encode(file.name)
                    file.parentTask = dict_o["cmd"]
                    print('file_values', file.name, file.label, file.path)

                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop("parentTask", None)
                    

                    self.node_list.append((file.name, dict_file))
                    self.edge_list.append((task.name, file.name))


                self.node_list.append((task.name, task.__dict__))



        graph = nx.DiGraph()
        graph.add_nodes_from(self.node_list)
        graph.add_edges_from(self.edge_list)

        print('all nodes',graph.nodes(data=True))

        # Once a graph is computed we need to apply the transforms of every task, 
        # if there are any to the neighbours nodes and then recompute all IDs 
        # in preparation for graph matching
        task_nodes = [n for n,v in graph.nodes(data=True) if v['type']=='task']

        

        for node in task_nodes:
            transform = graph.nodes[node]['transform']


            predecesors = list(graph.predecessors(node))
            successors = list(graph.successors(node))


            if (len(predecesors) == len(successors)) and len(transform.rstrip()) != 0:
                for idx,item in enumerate(successors):
                    full_path = transform.replace('*',graph.nodes[(predecesors[idx])]['label'])
                    graph.nodes[item]['name'] = full_path
                    graph.nodes[item]['path'] = os.path.dirname(full_path)
                    graph.nodes[item]['label'] = os.path.basename(full_path).split('.')[0]
                    graph.nodes[item]['ID'] = utils.encode(full_path)


            elif len(transform.rstrip()) != 0:
                for idx,item in enumerate(successors):
                    full_path = transform.replace('*',graph.nodes[item]['label'])
                    graph.nodes[item]['name'] = full_path
                    graph.nodes[item]['path'] = os.path.dirname(full_path)
                    graph.nodes[item]['label'] = os.path.basename(full_path).split('.')[0]
                    graph.nodes[item]['ID'] = utils.encode(full_path)


            neighbors = list(nx.all_neighbors(graph,node))
            neighbors_name=[]
            for n in neighbors:
                neighbors_name.append(graph.nodes[n]['name'])


            graph.nodes[node]['ID'] = utils.encode(''.join(sorted(neighbors_name)))

        return graph




    def graph_object_plot(self):
        plot = utils.graph_plot(self.graph)
        return plot
    


    def end_nodes(self):
        end_nodes = [
            x
            for x in self.graph.nodes()
            if self.graph.out_degree(x) == 0 and self.graph.in_degree(x) == 1
        ]
        return end_nodes
