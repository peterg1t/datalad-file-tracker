import os
import git
import re
import ast
import copy
import glob
import datalad.api as dl
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (BoxZoomTool, Quad, Circle, HoverTool, ResetTool, ColumnDataSource, LabelSet, DataRange1d)
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis
import matplotlib.pyplot as plt
from taskWorkflow import taskWorkflow
from fileWorkflow import fileWorkflow



class graphProvDB:
    """This class will represent a graph created from provenance

    Returns:
        _type_: _description_
    """
    def __init__(self, dsname):
        self.superdataset = self._get_superdataset(dsname)
        self.dataset_list = []
        self.NodeList = []
        self.EdgeList = []
        self.graph = self._graph_gen()        


    def _get_commit_list(self,commits):
        """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD 
        """
        return [item for item in commits if 'DATALAD RUNCMD' in item.message]



    def _commit_message_node_extract(self,commit):
        return ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', commit.message).group(0))


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
            dsname (str): A path to the dataset (or subdataset)

        Returns:
            graph: A networkx graph
        """
      

        self.dataset_list.append(self.superdataset.path)
        subdatasets = self.superdataset.subdatasets()

    
        for subdataset in subdatasets:
            repo = git.Repo(subdataset['path'])
            commits = list(repo.iter_commits('master'))
            # _get_commit_list(commits, run_commits)
            dl_run_commits = self._get_commit_list(commits)


            for commit in dl_run_commits:
                dict_o = self._commit_message_node_extract(commit)

                gI=1
                aNI=2
                cGID=3
                task = taskWorkflow(dict_o['cmd'],gI, aNI, cGID, commit.hexsha, commit.hexsha) 

                dict_task = copy.copy(task.__dict__)
                dict_task.pop('childFiles')
                dict_task.pop('parentFiles')
                self.NodeList.append((task.taskID, task.__dict__))


                for input in dict_o['inputs']:
                    task.parentFiles.append(input)

                    input_path = glob.glob(self.superdataset.path+f"/**/*{os.path.basename(input)}", recursive=True)[0]
                    ds_file = git.Repo(os.path.dirname(input_path))
                    file_status = dl.status(path=input_path, dataset=ds_file.working_tree_dir)[0]

                    file = fileWorkflow(input_path,gI, aNI, cGID, commit.hexsha, file_status['gitshasum'])
                    file.childTask=dict_o['cmd']

                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop('childTask', None)

                    self.NodeList.append((file.fileBlob, dict_file))
                    self.EdgeList.append((file.fileBlob,task.taskID))


                for output in dict_o['outputs']:
                    task.childFiles.append(output)

                    output_path = glob.glob(self.superdataset.path+f"/**/*{os.path.basename(output)}", recursive=True)[0]
                    ds_file = git.Repo(os.path.dirname(output_path))
                    file_status = dl.status(path=output_path, dataset=ds_file.working_tree_dir)[0]

                    file = fileWorkflow(output_path, gI, aNI, cGID, commit.hexsha, file_status['gitshasum'])
                    file.parentTask=dict_o['cmd']

                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop('parentTask', None)

                    self.NodeList.append((file.fileBlob, dict_file))
                    self.EdgeList.append((task.taskID,file.fileBlob))


        graph = nx.DiGraph()
        graph.add_nodes_from(self.NodeList)
        graph.add_edges_from(self.EdgeList)


        return graph

    def graph_plot(self):
        # Uncomment to plot the graph
        
        gl = graphviz_layout(self.graph, prog='dot', root=None)
        graph = from_networkx(self.graph, gl)

        plot = figure(title="File provenance tracker",
                  toolbar_location="below", tools = "pan,wheel_zoom")
        plot.axis.visible = False

        plot.x_range = DataRange1d(range_padding=1)
        plot.y_range = DataRange1d(range_padding=1)

        node_hover_tool = HoverTool(tooltips=[("index", "@index"), ("name", "@name")])
        plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())


        graph.node_renderer.glyph = Circle(size=20, fill_color='node_color')
        plot.renderers.append(graph)

        x, y = zip(*graph.layout_provider.graph_layout.values())
        node_labels = nx.get_node_attributes(self.graph, 'basename')

        fn = list(node_labels.values())
        source = ColumnDataSource({'x': x, 'y': y, 'basename': fn})
        labels = LabelSet(x='x', y='y', text='basename', source=source,
                  background_fill_color='white', text_align='center', y_offset=11)
        plot.renderers.append(labels)

    
        return plot


        # this finds the 
        # end_nodes = [x for x in graph.nodes() if graph.out_degree(x)==0 and graph.in_degree(x)==1]
        
