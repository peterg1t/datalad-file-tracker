from datetime import datetime
import itertools
import streamlit as st
import graphviz as graphviz


class PlotNotes:
    """ This is a builder class for the graphs
    """
    def __init__(self, trackline, mode, option):
        self._trackline = trackline
        self._mode = mode
        self._option = option
        self._graph=None

    def plot_one_direction(self, trackline):
        if self._option == 'Process':
            # this section create the nodes of the graph
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
                else:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
            #this section create the links on the graph between a file and its parents
            for index, item in enumerate(trackline[:-1]):
                for rel in item.relative:
                    for j in range(index, len(trackline)):
                        if rel == trackline[j].filename:
                            if self._mode == 'Reverse':
                                self._graph.edge(trackline[j].commit, item.commit)
                            elif self._mode == 'Forward':
                                self._graph.edge(item.commit, trackline[j].commit)

        elif self._option == 'Simple':
            # This section creates the nodes and links between the files (nodes of the tree)
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="green")
                else:
                    self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}",color="red")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    if self._mode == 'Reverse':
                        self._graph.edge(rel,item.filename)
                    elif self._mode == 'Forward':
                        self._graph.edge(item.filename,rel)

        now = datetime.now()
        time_stamp = datetime.timestamp(now)
        self._graph.render(f"/tmp/{time_stamp}",format='png')
        return st.graphviz_chart(self._graph,use_container_width=True)


    def plot_two_direction(self, trackline):
        if self._option == 'Process':
            # this section create the nodes of the graph
            for index, item in enumerate(trackline):
                if index == 0:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}", color="green")
                else:
                    self._graph.node(item.commit, f"file={item.filename}|{{ commit={item.commit}|author={item.author}|date={datetime.fromtimestamp(item.date)}|parent(s)={item.relative}|summary={item.summary} }}",color="red")
            
            #this section create the links on the graph between a file and its parents
            for index, item in enumerate(trackline[:-1]):
                for rel in item.relative:
                    for j in range(index, len(trackline)):
                        if rel == trackline[j].filename:
                            if self._mode == 'Reverse':
                                self._graph.edge(trackline[j].commit, item.commit)
                            elif self._mode == 'Forward':
                                self._graph.edge(item.commit, trackline[j].commit)

        elif self._option == 'Simple':
            # This section creates the nodes and links between the files (nodes of the tree)
            for index, item in enumerate(itertools.islice(trackline,len(trackline)//2)):
                self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    # print(f"{rel}")
                    self._graph.edge(item.filename,rel)

            for index, item in enumerate(itertools.islice(trackline,len(trackline)//2, len(trackline), 1)):
                self._graph.node(item.filename, f"{{ commit message={item.summary.replace('[DATALAD RUNCMD]','')}|file={item.filename} }}")
                for rel in item.relative:
                    self._graph.node(rel, f"{rel}")
                    # print(f"{rel}")
                    self._graph.edge(item.filename,rel)

        now = datetime.now()
        time_stamp = datetime.timestamp(now)
        self._graph.render(f"/tmp/{time_stamp}",format='png')
        return st.graphviz_chart(self._graph,use_container_width=True)



    
    def plot_notes(self):
        """ This function will generate the graphviz plots

        Returns:
            _type_: _description_
        """
        self._graph = graphviz.Digraph(node_attr={'shape': 'record'})
        self._graph.attr(rankdir='TB')  

        if self._mode == 'Bidirectional':
            self.plot_two_direction(self._trackline)
        else:
            self.plot_one_direction(self._trackline)