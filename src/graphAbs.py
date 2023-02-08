import networkx as nx
from utils import graph_plot
from utils import encode


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
        self.node_list = []
        self.edge_list = []
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
        # Generate task ID and node list for the tasks
        taskID_list = []
        for idx, task in enumerate(self.commands):
            if task:
                inputs_sorted = ",".join(sorted(self.inputs[idx].split(",")))
                outputs_sorted = ",".join(sorted(self.outputs[idx].split(",")))

                taskID = f"{inputs_sorted}<>{task}<>{outputs_sorted}"
                taskID_list.append(taskID)
                self.node_list.append(
                    (
                        encode(taskID),
                        {
                            "name": encode(task),
                            "type": "task",
                            "node_color": "grey",
                            "status": "pending",
                            "literal_name": task,
                        },
                    )
                )

        for idx, inp_list in enumerate(self.inputs):
            # For every input list we create an edge from file to 
            # task and a node for the file input
            for item in inp_list.split(","):
                if item:
                    self.node_list.append(
                        (
                            encode(item),
                            {
                                "name": encode(item),
                                "type": "file",
                                "node_color": "grey",
                                "status": "pending",
                                "literal_name": item,
                            },
                        )
                    )
                    self.edge_list.append((encode(item), encode(taskID_list[idx])))

        for idx, out_list in enumerate(self.outputs):
            # For every input list we create an edge from file to 
            # task and a node for the file output
            for item in out_list.split(","):
                if item:
                    self.node_list.append(
                        (
                            encode(item),
                            {
                                "name": encode(item),
                                "type": "file",
                                "node_color": "grey",
                                "status": "pending",
                                "literal_name": item,
                            },
                        )
                    )
                    self.edge_list.append((encode(taskID_list[idx]), encode(item)))

        graph = nx.DiGraph()
        graph.add_nodes_from(self.node_list)
        graph.add_edges_from(self.edge_list)

        return graph

    def _graph_export(self, filename):
        """This function will write the graph to the path specified
          by filename

        Args:
            filename (string): A path to the saved GML graph
        """
        nx.write_gml(self.graph, filename)

    def graph_object_plot(self):
        """This function will return a plot of the networkx graph that
          can be plotted with bokeh or plotly

        Returns:
            plot: A plot of the networkx graph
        """
        return graph_plot(self.graph)

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

    def start_nodes(self):
        """This function return the first node(s) in a tree or in the 
        case of a diff graph the next node scheduled to run

        Returns:
            list: A list of starting nodes
        """
        start_nodes = [
            x
            for x in self.graph.nodes()
            if self.graph.out_degree(x) == 1 and self.graph.in_degree(x) == 0
        ]
        return start_nodes
