import os
import git
import re
import ast
import copy
import glob
import base64
import datalad.api as dl
import networkx as nx
import toposort
from utils import graph_plot
from utils import encode
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (BoxZoomTool, Quad, Circle, HoverTool, ResetTool, ColumnDataSource, LabelSet, DataRange1d)
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis
import matplotlib.pyplot as plt
from taskWorkflow import taskWorkflow
from fileWorkflow import fileWorkflow



class graphAbs:
    """This class will represent a graph created from provenance

    Returns:
        abstract graph: An abstract graph
    """
    def __init__(self, file_inputs, commands, file_outputs):
        self.inputs = file_inputs
        self.commands = commands
        self.outputs = file_outputs
        self.dataset_list = []
        self.NodeList = []
        self.EdgeList = []
        self.status = []
        # self.absGraphID = absGraphID # An abstract graph ID to match with this graph
        self.conGraphID = 0
        self.graph = self._graph_gen()        

    def _gen_graph_ID(self, node_list):
        """Given a graph with a series of nodes compute the ID of the concrete graph
        """
        return hash(tuple(node_list))




    def _graph_gen(self):
        """! This function will return a graph from a dataset input
        Args:
            dsname (str): A path to the dataset (or subdataset)

        Returns:
            graph: A networkx graph
        """
        # Generate file ID

        for idx, inp_list in enumerate(self.inputs):
            # For every input list we create an edge from file to task and a node for the file input
            for item in inp_list.split(','):
                if item:
                    self.NodeList.append((item, {'name': item, 'type': 'file', 'node_color': 'red', 'id': encode(item)}))
                    self.EdgeList.append((item, self.commands[idx]))


        for idx, out_list in enumerate(self.outputs):
            # For every input list we create an edge from file to task and a node for the file output
            for item in out_list.split(','):
                if item:
                    self.NodeList.append((item, {'name': item, 'type': 'file', 'node_color': 'red', 'id': encode(item)}))
                    self.EdgeList.append((self.commands[idx], item))


        # Generate task ID
        for idx, task in enumerate(self.commands):
            if task:
                taskID=f"{self.inputs[idx]}<>{task}<>{self.outputs[idx]}"
                self.NodeList.append((task, {'name': task, 'type': 'task', 'node_color': 'green', 'id': encode(taskID)}))



        #If nodes have been created by the user
        
        graph = nx.DiGraph()
        graph.add_nodes_from(self.NodeList)
        graph.add_edges_from(self.EdgeList)
        
        return graph




    def graph_ObjPlot(self):
        return graph_plot(self.graph)

        # this finds the 
        # end_nodes = [x for x in graph.nodes() if graph.out_degree(x)==0 and graph.in_degree(x)==1]
        
