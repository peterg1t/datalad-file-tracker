import cmd
import os
from os import curdir
from pathlib import Path
import glob 
import profile
from tkinter.messagebox import NO
import copy


import git
import re
import ast
from fileinput import filename
from importlib.resources import path
from importlib_metadata import method_cache
from sqlalchemy import between
import streamlit as st



import datalad.api as dl
from datalad_metalad.extractors import runprov
from datalad_metalad import extract
from datalad_metalad.extract import Extract
from datalad_metalad.aggregate import Aggregate
import argparse

import networkx as nx



import utils

import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (BoxZoomTool, Circle, HoverTool, ResetTool, ColumnDataSource, LabelSet, DataRange1d)
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis

import cProfile, pstats




class GraphProvDB:
    """This class will represent a graph created from provenance

    Returns:
        _type_: _description_
    """
    def __init__(self, ds_name):
        self.superdataset = self._get_superdataset(ds_name)
        self.dataset_list = []
        self.node_list = []
        self.edge_list = []
        self.graph = self._graph_gen()        


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
            repo = git.Repo(subdataset['path'])
            branch = repo.active_branch
            commits = list(repo.iter_commits(branch))
            # _get_commit_list(commits, run_commits)
            dl_run_commits = _get_commit_list(commits)


            for commit in dl_run_commits:
                dict_o = _commit_message_node_extract(commit)

                gI=1
                aNI=2
                cGID=3
                task = taskWorkflow(dict_o['cmd'],gI, aNI, cGID, commit.hexsha, commit.hexsha) 

                dict_task = copy.copy(task.__dict__)
                dict_task.pop('childFiles')
                dict_task.pop('parentFiles')
                self.node_list.append((task.taskID, task.__dict__))


                for input in dict_o['inputs']:
                    task.parentFiles.append(input)

                    input_path = glob.glob(self.superdataset.path+f"/**/*{os.path.basename(input)}", recursive=True)[0]
                    ds_file = git.Repo(os.path.dirname(input_path))
                    file_status = dl.status(path=input_path, dataset=ds_file.working_tree_dir)[0]

                    file = fileWorkflow(input_path,gI, aNI, cGID, commit.hexsha, file_status['gitshasum'])
                    file.childTask=dict_o['cmd']

                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop('childTask', None)

                    self.node_list.append((file.fileBlob, dict_file))
                    self.edge_list.append((file.fileBlob,task.taskID))


                for output in dict_o['outputs']:
                    task.childFiles.append(output)

                    output_path = glob.glob(self.superdataset.path+f"/**/*{os.path.basename(output)}", recursive=True)[0]
                    ds_file = git.Repo(os.path.dirname(output_path))
                    file_status = dl.status(path=output_path, dataset=ds_file.working_tree_dir)[0]

                    file = fileWorkflow(output_path, gI, aNI, cGID, commit.hexsha, file_status['gitshasum'])
                    file.parentTask=dict_o['cmd']

                    dict_file = copy.copy(file.__dict__)
                    dict_file.pop('parentTask', None)

                    self.node_list.append((file.fileBlob, dict_file))
                    self.edge_list.append((task.taskID,file.fileBlob))


        graph = nx.DiGraph()
        graph.add_nodes_from(self.node_list)
        graph.add_edges_from(self.edge_list)


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
        





class nodeWorkflow:
    """ Base class of a node in the provenance trail (can be task or file)
    """
    def __init__(self, graphInstanceID, abstractNodeIndex, concreteGraphID, commit):
        """ Init method of the class

        Args:
            graphInstanceID (str): The graph instance
            abstractNodeIndex (str): _description_
            concreteGraphID (str): _description_
            commit (str): _description_
        """
        self.graphInstanceID = graphInstanceID
        self.abstractNodeIndex = abstractNodeIndex
        self.concreteGraphID = concreteGraphID
        self.commit = commit
    


class fileWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """
    def __init__(self, name, graphInstanceID, abstractNodeIndex, concreteGraphID, commit, fileBlob):
        super().__init__(graphInstanceID, abstractNodeIndex, concreteGraphID, commit)
        self.name = name
        self.basename = os.path.basename(name)
        self.fileBlob = fileBlob
        self.parentTask=[]
        self.childTask=[]
        self.node_color = 'red'
        
    

class taskWorkflow(nodeWorkflow):
    """_summary_

    Args:
        nodeWorkflow (_type_): _description_
    """
    def __init__(self, name, graphInstanceID, abstractNodeIndex, concreteGraphID, commit, taskID):
        super().__init__(graphInstanceID, abstractNodeIndex, concreteGraphID, commit)
        self.name = name
        self.basename = name
        self.taskID = taskID
        self.parentFiles=[]
        self.childFiles=[]
        self.node_color = 'green'









profiler = cProfile.Profile()

st.write("""
Welcome to file provenance tracker!
""")




def _get_commit_list(commits):
    """! This function will append to run_cmd_commits if there is a DATALAD RUNCMD 
    """
    return [item for item in commits if 'DATALAD RUNCMD' in item.message]



def _commit_message_node_extract(commit):
    return ast.literal_eval(re.search('(?=\{)(.|\n)*?(?<=\}\n)', commit.message).group(0))
   


def git_log_parse(ds_name, a_option):
    """! This function will generate the graph of the entire project
    Args:
        ds_name (str): An absolute path to the filename
        a_option (str): An analysis mode for the node calculation 
    """
    gdb = GraphProvDB(ds_name)
    plot_db = gdb.graph_plot()
    st.bokeh_chart(plot_db, use_container_width=True)
        














if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--dspath", help="Path to dataset")
    parser.add_argument("-a", "--analysis", help="Analysis to apply to nodes", choices=['Centrality','Betweeness'])
    
    args = parser.parse_args()  # pylint: disable = invalid-name

    if args.dspath and args.analysis:
        dataset_name = args.dspath
        analysis_type = args.analysis
    else: 
        print("Not all command line arguments were used as input, results might be wrong")
        dataset_name = st.text_input('Input the dataset to track')
        with st.sidebar:
            analysis_type = st.selectbox('Analysis mode', ['None', 'Degree Centrality', 'Betweeness Centrality'])



    # Sreamlit UI implementation
    

    if dataset_name:
        git_log_parse(dataset_name, analysis_type)