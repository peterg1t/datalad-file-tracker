import os
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from bokeh.plotting import from_networkx, figure
from bokeh.models import (
    BoxZoomTool,
    Circle,
    HoverTool,
    ResetTool,
    ColumnDataSource,
    LabelSet,
    DataRange1d,
)
import utils


class GraphBase:
    """This class will represent a graph created from provenance

    Returns:
        abstract graph: An abstract graph
    """

    def __init__(self, node_list, edge_list):
        self.node_list = node_list
        self.edge_list = edge_list
        self.status = []
        # self.absGraphID = absGraphID # An abstract graph ID
        #  to match with this graph
        self.concrete_graph_ID = 0
        self.graph = self._graph_gen()

    def _gen_graph_ID(self, node_list):
        """Given a graph with a series of nodes compute the
          ID of the concrete graph

        Args:
            node_list (list): A list of nodes

        Returns:
            int: A hash computed from the tuple of the node
              list (inmutable)
        """
        return hash(tuple(node_list))

    def _graph_gen(self):
        """! This function will return a graph from a dataset input
        Args:
            ds_name (str): A path to the dataset (or subdataset)

        Returns:
            graph: A networkx graph
        """
        graph = nx.DiGraph()
        graph.add_nodes_from(self.node_list)
        graph.add_edges_from(self.edge_list)

        # print('nodes',graph.nodes(data=True))

        # Once a graph is computed we need to apply the transforms of every task,
        # if there are any to the neighbours nodes and then recompute all IDs
        # in preparation for graph matching

        task_nodes = [n for n, v in graph.nodes(data=True) if v["type"] == "task"]

        for node in task_nodes:
            transform = graph.nodes[node]["transform"]

            predecesors = list(graph.predecessors(node))
            successors = list(graph.successors(node))

            if (len(predecesors) == len(successors)) and len(transform.rstrip()) != 0:
                for idx, item in enumerate(successors):
                    full_path = transform.replace(
                        "*", graph.nodes[(predecesors[idx])]["label"]
                    )
                    graph.nodes[item]["name"] = full_path
                    graph.nodes[item]["path"] = os.path.dirname(full_path)
                    graph.nodes[item]["label"] = os.path.basename(full_path).split(".")[
                        0
                    ]
                    graph.nodes[item]["ID"] = utils.encode(full_path)

            elif len(transform.rstrip()) != 0:
                for idx, item in enumerate(successors):
                    full_path = transform.replace("*", graph.nodes[item]["label"])
                    graph.nodes[item]["name"] = full_path
                    graph.nodes[item]["path"] = os.path.dirname(full_path)
                    graph.nodes[item]["label"] = os.path.basename(full_path).split(".")[
                        0
                    ]
                    graph.nodes[item]["ID"] = utils.encode(full_path)

            neighbors = list(nx.all_neighbors(graph, node))
            full_task_description = []
            for n in neighbors:
                full_task_description.append(graph.nodes[n]["name"])

            command = graph.nodes[node]["cmd"]
            full_task_description.append(command)

            graph.nodes[node]["ID"] = utils.encode(
                ",".join(sorted(full_task_description))
            )

            # mapping = {item:os.path.basename(full_path)}
            # graph = nx.relabel_nodes(graph, mapping)

        return graph

    def _graph_export(self, filename):
        """This function will write the graph to the path specified
          by filename

        Args:
            filename (string): A path to the saved GML graph
        """
        nx.write_gml(self.graph, filename)

    def graph_object_plot(self, fc="node_color"):
        """! Utility to generate a plot for a networkx graph

        Args:
            graph_nx (graph): A networkx graph

        Returns:
            plot: A graphviz figure to be plotted with bokeh
        """
        graph_layout = graphviz_layout(
            self.graph, prog="dot", root=None, args="-Gnodesep=1000 -Grankdir=TB"
        )
        graph = from_networkx(self.graph, graph_layout)

        plot = figure(
            title="File provenance tracker",
            toolbar_location="below",
            tools="pan,wheel_zoom",
        )
        plot.axis.visible = False

        plot.x_range = DataRange1d(range_padding=0.5)
        plot.y_range = DataRange1d(range_padding=0.5)

        node_hover_tool = HoverTool(
            tooltips=[
                ("index", "@index"),
                ("name", "@name"),
                ("label", "@label"),
                ("status", "@status"),
                ("author", "@author"),
                ("ID", "@ID"),
            ]
        )
        plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())

        graph.node_renderer.glyph = Circle(size=20, fill_color=fc)
        plot.renderers.append(graph)

        x_coord, y_coord = zip(*graph.layout_provider.graph_layout.values())
        node_labels = nx.get_node_attributes(self.graph, "name")

        node_names = list(node_labels.values())
        source = ColumnDataSource({"x": x_coord, "y": y_coord, "name": node_names})
        labels = LabelSet(
            x="x",
            y="y",
            text="name",
            source=source,
            background_fill_color="white",
            text_align="center",
            y_offset=11,
        )

        plot.renderers.append(labels)

        return plot

    def end_nodes(self):
        """This function return the last node(s) in a tree

        Returns:
            list: A list of ending nodes
        """
        end_nodes = [
            x
            for x in self.graph.nodes()
            if self.graph.out_degree(x) == 0 and self.graph.in_degree(x) == 1
        ]
        return end_nodes

    def next_nodes_run(self):
        """This function return the first node(s) in a tree or in the
        case of a diff graph the next node scheduled to run

        Returns:
            list: A list of starting nodes
        """
        next_nodes_run = [
            x
            for x in self.graph.nodes()
            if int(self.graph.out_degree(x)) >= 1 and int(self.graph.in_degree(x)) == 0
        ]

        return next_nodes_run
