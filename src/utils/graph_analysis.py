"""! Graph Analisys Module
This module allows the user to print the different network attributes as well as execute several operations with graphs
"""

import copy
import networkx as nx
import utils


def calc_betw_centrl(graph):
    """! This function will calculate the betweenness
    centrality. https://en.wikipedia.org/wiki/Betweenness_centrality

    Args:
        graph (graph (Networkx)): A netwrokx graph

    Returns:
        node attribute: betweenness centrality
    """
    return nx.betweenness_centrality(graph)


def deg_centrl(graph):
    """! This function will return the degree centrality

    Args:
        graph (graph (Networkx)): A netwrokx graph

    Returns:
        node attribute: degree centrality
    """
    return nx.degree_centrality(graph)


def eigen_centrl(graph):
    """! This function will return the eigen vector centrality

    Args:
        graph (graph (Networkx)): A netwrokx graph

    Returns:
        node attribute: degree centrality
    """
    return nx.eigenvector_centrality_numpy(graph)


def close_centrl(graph):
    """! This function will return the eigen vector centrality

    Args:
        graph (graph (Networkx)): A netwrokx graph

    Returns:
        node attribute: degree centrality
    """
    return nx.closeness_centrality(graph)


def graph_diff(abstract, provenance):
    """! Calculate the difference of the abstract and provenance graphs

    Args:
        abstract (graph): An abstract graph
        provenance (graph): A concrete or provenance graph

    Returns:
        graphs: An updated abstract graph with completed nodes for plotting and a graph containing the difference between the nodes. (abstract-concrete)
    """
    abs_graph_id = list(nx.get_node_attributes(abstract.graph, "ID").values())    
    prov_graph_id = list(nx.get_node_attributes(provenance.graph, "ID").values())
    
    difference = copy.deepcopy(abstract)
    nodes_update = [
        n for n, v in abstract.graph.nodes(data=True) if v["ID"] in prov_graph_id
    ]

    for node in nodes_update:
        nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
        if abstract.graph.nodes()[node]["type"] == "task":
            nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
        elif abstract.graph.nodes()[node]["type"] == "file":
            nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")

    
    difference.graph.remove_nodes_from(
        n for n, v in abstract.graph.nodes(data=True) if v["status"] == "complete"
    )

    # In the difference graph the start_nodes is the list of nodes that can be
    # started (these should usually be a task)
    return abstract, difference


def graph_diff_tasks(abstract, provenance):
    """! Calculate the difference of the abstract and provenance graphs

    Args:
        abstract (graph): An abstract graph
        provenance (graph): A concrete or provenance graph

    Returns:
        graphs: An updated abstract graph with completed nodes for plotting and a graph containing the difference between the nodes. (abstract-concrete)
    """
    abs_graph_id = list(nx.get_node_attributes(abstract.graph, "ID").values())    
    prov_graph_id = list(nx.get_node_attributes(provenance.graph, "ID").values())
    
    difference = copy.deepcopy(abstract)

    print("a",abstract.graph.nodes(data=True))
    print("p",provenance.graph.nodes(data=True))
    # print("difference", difference.graph.nodes(data=True))
    # nodes_update = [
    #     n for n, v in abstract.graph.nodes(data=True) if v["ID"] in prov_graph_id
    # ]

    # for node in nodes_update:
    #     nx.set_node_attributes(abstract.graph, {node: "complete"}, "status")
    #     if abstract.graph.nodes()[node]["type"] == "task":
    #         nx.set_node_attributes(abstract.graph, {node: "green"}, "node_color")
    #     elif abstract.graph.nodes()[node]["type"] == "file":
    #         nx.set_node_attributes(abstract.graph, {node: "red"}, "node_color")

    
    # difference.graph.remove_nodes_from(
    #     n for n, v in abstract.graph.nodes(data=True) if v["status"] == "complete"
    # )

    # In the difference graph the start_nodes is the list of nodes that can be
    # started (these should usually be a task)
    return abstract, difference



def graph_relabel(graph, nmap):
    """This function will relabel the ID on the graphs

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping
    """
    graph2remap = copy.deepcopy(graph)
    graph2remap.graph = nx.relabel_nodes(graph2remap.graph, nmap)
    for node, attrs in graph2remap.graph.nodes(data=True):
        if attrs["type"] == "file":
            attrs["ID"] = utils.encode(node)
    
    for node, attrs in graph2remap.graph.nodes(data=True):
        if attrs["type"]=='task':
            full_task_description = list(nx.all_neighbors(graph2remap.graph, node))
            full_task_description.append(attrs["cmd"])
            attrs["ID"] = utils.encode(
                ",".join(sorted(full_task_description))
            )

    return graph2remap

def graph_remap_attributes(graph, nmap):
    """This function will relabel the ID on the file nodes of the graph

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping
    """
    graph2remap = copy.deepcopy(graph)

    for node, attrs in graph2remap.graph.nodes(data=True):
        inputs_mapped = []
        outputs_mapped = []
        for inp in attrs["inputs"]:
            if inp in nmap:
                inputs_mapped.append(nmap[inp])
        
        for out in attrs["outputs"]:
            if out in nmap:
                outputs_mapped.append(nmap[out])
        
        graph2remap.graph.nodes[node]["inputs"] = inputs_mapped
        graph2remap.graph.nodes[node]["outputs"] = outputs_mapped

        full_task_description = inputs_mapped + outputs_mapped
        full_task_description.append(attrs["command"])
        graph2remap.graph.nodes[node]["ID"] = utils.encode(
            ",".join(sorted(full_task_description))
        )
    
    return graph2remap
