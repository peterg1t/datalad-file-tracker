"""! Graph Analisys Module
This module allows the user to print the different network attributes as well as execute several operations with graphs
"""
from pathlib import Path
import copy
import networkx as nx
import utils


class FileHandleNotFound(Exception):
    """Exception when file handle is not found."""


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

    print("a", abstract.graph.nodes(data=True))
    print("p", provenance.graph.nodes(data=True))
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
        if attrs["type"] == "task":
            full_task_description = list(nx.all_neighbors(graph2remap.graph, node))
            full_task_description.append(attrs["cmd"])
            attrs["ID"] = utils.encode(",".join(sorted(full_task_description)))

    return graph2remap


def _file_handles_for_node(
    node: dict, file_handles: dict[str, Path]
) -> dict[str, Path]:
    """Return input and output file handles with their file paths for a node."""
    node_handles = node["inputs"] + node["outputs"]
    for handle in node_handles:
        if handle not in file_handles:
            raise FileHandleNotFound(f"The file handle, {handle}, was not recognized.")
    return {handle: file_handles[handle] for handle in node_handles}
    # return {
    #     handle: filepath for handle, filepath in file_handles.items()
    #     if handle in node_handles
    # }

def _materialize_files_in_command(command: str, file_handles: dict[str, Path]) -> str:
    """Inject input/output files into their handles in a command."""
    for handle, file in file_handles.items():
        if handle not in command:
            raise FileHandleNotFound(f"The file handle, {handle}, was not recognized.")
        command = command.replace(handle, str(file))
    return command


def graph_remap_attributes(graph, nmap):
    """This function will relabel the ID on the file nodes of the graph

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping
    """
    graph2remap = copy.deepcopy(graph)

    for node, attrs in graph2remap.graph.nodes(data=True):
        print("node", node, attrs)
        node_handles_paths = _file_handles_for_node(attrs, nmap)

        inputs_mapped = {inp: nmap[inp] for inp in attrs["inputs"]}
        outputs_mapped = {out: nmap[out] for out in attrs["outputs"]}
        inputs_paths = [nmap[inp] for inp in attrs["inputs"]]
        output_paths = [nmap[out] for out in attrs["outputs"]]

        graph2remap.graph.nodes[node]["inputs"] = inputs_paths
        graph2remap.graph.nodes[node]["outputs"] = output_paths

        full_task_description = inputs_paths + output_paths
        full_task_description.append(attrs["command"])
        graph2remap.graph.nodes[node]["ID"] = utils.encode(
            ",".join(sorted(full_task_description))
        )
        inputs_mapped.update(outputs_mapped)
        new_command = _materialize_files_in_command(attrs["command"], node_handles_paths)
        graph2remap.graph.nodes[node]["command"] = new_command
        print(graph2remap.graph.nodes(data=True))

    return graph2remap
