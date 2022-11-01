import os
from datetime import datetime
import itertools
import streamlit as st
import graphviz as graphviz
import pandas as pd
import utils

from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (BoxZoomTool, Circle, HoverTool, ResetTool, ColumnDataSource, LabelSet, DataRange1d)
from bokeh.transform import linear_cmap
from bokeh.palettes import Viridis
import matplotlib.pyplot as plt
import networkx as nx




class PlotNotes:
    """ This is a builder class for the graphs
    """
    def __init__(self, trackline, mode):
        """_summary_

        Args:
            trackline (_type_): _description_
            mode (_type_): _description_
        """
        self._trackline = trackline
        self._mode = mode
        self._graph=None


    def _loc_duplicate(self, value_list):
        """_summary_

        Args:
            value_list (_type_): _description_

        Returns:
            _type_: _description_
        """
        for i in range(len(value_list)):
            if value_list[i]==value_list[i+1]:
                return i+1

    def _create_network(self):
        """This function will calculate and return a graph

        Returns:
            graph: A networkx digraph
        """
        G = nx.DiGraph()
        k = 0
        for note in self._trackline:
            if self._mode == 'Bidirectional':
                indx_dupl = self._loc_duplicate([item.filename for item in self._trackline])
                if k == indx_dupl:
                    G.add_node(note.filename, date = datetime.fromtimestamp(note.date).strftime("%d/%m/%Y, %H:%M:%S"), author = str(note.author), commit = note.commit, dataset = note.dataset, \
                    message = note.message, relative = note.relative, summary = note.summary, colour = 'green')
                else:
                    G.add_node(note.filename, date = datetime.fromtimestamp(note.date).strftime("%d/%m/%Y, %H:%M:%S"), author = str(note.author), commit = note.commit, dataset = note.dataset, \
                    message = note.message, relative = note.relative, summary = note.summary, colour = 'red')

            else:
                if k == 0:
                    G.add_node(note.filename, date = datetime.fromtimestamp(note.date).strftime("%d/%m/%Y, %H:%M:%S"), author = str(note.author), commit = note.commit, dataset = note.dataset, \
                    message = note.message, relative = note.relative, summary = note.summary, colour = 'green')
                else:
                    G.add_node(note.filename, date = datetime.fromtimestamp(note.date).strftime("%d/%m/%Y, %H:%M:%S"), author = str(note.author), commit = note.commit, dataset = note.dataset, \
                    message = note.message, relative = note.relative, summary = note.summary, colour = 'red')
            
            k = k + 1


        if self._mode == 'Reverse':
            for item in self._trackline:
                for relative in item.relative:
                    G.add_edge(relative, item.filename) 

        elif self._mode == 'Forward':       
            for item in self._trackline:
                for relative in item.relative:
                    G.add_edge(item.filename, relative)

        elif self._mode == 'Bidirectional':
            indx_dupl = self._loc_duplicate([item.filename for item in self._trackline])
            
            for item in self._trackline[:indx_dupl]:
                for relative in item.relative:
                    G.add_edge(relative, item.filename)
            for item in self._trackline[indx_dupl:]:
                for relative in item.relative:
                    G.add_edge(item.filename, relative)
            

        
        return G








    def plot_bokeh(self):
        plot = figure(title="File provenance tracker",
              toolbar_location="below", tools = "pan,wheel_zoom")

        BG = self._create_network()
        betweeness_centr = utils.calc_betw_centrl(BG)
        nx.set_node_attributes(BG, betweeness_centr, name='bc')
        

        gl = graphviz_layout(BG, prog='dot', root=None)
        graph = from_networkx(BG, gl)
        plot.axis.visible = False
        
        
        plot.x_range = DataRange1d(range_padding=0.2)
        plot.y_range = DataRange1d(range_padding=0.2)



        node_hover_tool = HoverTool(tooltips=[("index", "@index"), ("date", "@date"), ("message", "@message"), ("bc", "@bc")])
        plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())
        
        fc = linear_cmap('bc', 'Spectral8', min(list(betweeness_centr.values())), max(list(betweeness_centr.values())))
        graph.node_renderer.glyph = Circle(size=20, fill_color=fc)
        # graph.node_renderer.glyph = Circle(size=20, fill_color='colour')

        #ploting the names of the files as labels        
        plot.renderers.append(graph)
        x, y = zip(*graph.layout_provider.graph_layout.values())
        node_labels = nx.get_node_attributes(BG, 'date')
        # colour_node = nx.get_node_attributes(BG, 'colour')
        fn = list(node_labels.keys())

        
        
        fn_mod = [os.path.basename(f) for f in fn]
                
        source = ColumnDataSource({'x': x, 'y': y, 'name': fn_mod})
        labels = LabelSet(x='x', y='y', text='name', source=source,
                  background_fill_color='white', text_align='center', y_offset=11)

        plot.renderers.append(labels)


        return st.bokeh_chart(plot, use_container_width=True)






    def plot_notes(self):
        """ This function will generate the graphviz plots

        Returns:
            _type_: _description_
        """
        # self._graph = graphviz.Digraph(node_attr={'shape': 'record'})
        # self._graph.attr(rankdir='TB')  
        self.plot_bokeh()






    # def plot_one_direction(self, trackline):
    #     if self._option == 'Process':
    #         # this section create the nodes of the graph
    #         for index, item in enumerate(trackline):
    #             if index == 0:
    #                 self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
    #             else:
    #                 self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
    #         #this section create the links on the graph between a file and its parents
    #         for index, item in enumerate(trackline[:-1]):
    #             for rel in item.relative:
    #                 for j in range(index, len(trackline)):
    #                     if rel == trackline[j].filename:
    #                         if self._mode == 'Reverse':
    #                             self._graph.edge(trackline[j].commit, item.commit)
    #                         elif self._mode == 'Forward':
    #                             self._graph.edge(item.commit, trackline[j].commit)

    #     elif self._option == 'Simple':
    #         # This section creates the nodes and links between the files (nodes of the tree)
    #         for index, item in enumerate(trackline):
    #             if index == 0:
    #                 self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="green")
    #             else:
    #                 self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="red")
    #             for rel in item.relative:
    #                 self._graph.node(rel, f"{rel}")
    #                 if self._mode == 'Reverse':
    #                     self._graph.edge(rel,item.filename)
    #                 elif self._mode == 'Forward':
    #                     self._graph.edge(item.filename,rel)

    #     now = datetime.now()
    #     time_stamp = datetime.timestamp(now)
    #     self._graph.render(f"/tmp/{time_stamp}",format='png')
    #     return st.graphviz_chart(self._graph,use_container_width=True)


    # def plot_two_direction(self, trackline):
    #     if self._option == 'Process':
    #         # this section create the nodes of the graph
    #         for index, item in enumerate(trackline):
    #             if index == 0:
    #                 self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
    #             else:
    #                 self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
    #         #this section create the links on the graph between a file and its parents
    #         for index, item in enumerate(trackline[:-1]):
    #             for rel in item.relative:
    #                 for j in range(index, len(trackline)):
    #                     if rel == trackline[j].filename:
    #                         if self._mode == 'Reverse':
    #                             self._graph.edge(trackline[j].commit, item.commit)
    #                         elif self._mode == 'Forward':
    #                             self._graph.edge(item.commit, trackline[j].commit)

    #     elif self._option == 'Simple':
    #         # This section creates the nodes and links between the files (nodes of the tree)
    #         for index, item in enumerate(itertools.islice(trackline,len(trackline)//2)):
    #             self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
    #             for rel in item.relative:
    #                 self._graph.node(rel, f"{rel}")
    #                 # print(f"{rel}")
    #                 self._graph.edge(item.filename,rel)

    #         for index, item in enumerate(itertools.islice(trackline,len(trackline)//2, len(trackline), 1)):
    #             self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
    #             for rel in item.relative:
    #                 self._graph.node(rel, f"{rel}")
    #                 # print(f"{rel}")
    #                 self._graph.edge(item.filename,rel)

    #     now = datetime.now()
    #     time_stamp = datetime.timestamp(now)
    #     self._graph.render(f"/tmp/{time_stamp}",format='png')
    #     return st.graphviz_chart(self._graph,use_container_width=True)



    
    # def plot_notes(self):
    #     """ This function will generate the graphviz plots

    #     Returns:
    #         _type_: _description_
    #     """
    #     self._graph = graphviz.Digraph(node_attr={'shape': 'record'})
    #     self._graph.attr(rankdir='TB')  

    #     if self._mode == 'Bidirectional':
    #         self.plot_two_direction(self._trackline)
    #     else:
    #         self.plot_one_direction(self._trackline)