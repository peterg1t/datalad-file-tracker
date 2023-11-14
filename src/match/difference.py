from pathlib import Path
import copy
import networkx as nx
from utilities import encode


class FileHandleNotFound(Exception):
    """Exception when file handle is not found."""

def graph_diff(abstract, provenance):
    """! Calculate the difference of the abstract and provenance graphs

    Args:
        abstract (graph): An abstract graph
        provenance (graph): A concrete or provenance graph

    Returns:
        graphs: An updated abstract graph with completed nodes for plotting and a graph containing the difference between the nodes. (abstract-concrete)
    """
    prov_graph_id = list(nx.get_node_attributes(provenance, "ID").values())

    difference = copy.deepcopy(abstract)
    nodes_update = [
        n for n, v in abstract.nodes(data=True) if v["ID"] in prov_graph_id
    ]

    for node in nodes_update:
        nx.set_node_attributes(abstract, {node: "complete"}, "status")
        if abstract.nodes()[node]["type"] == "task":
            nx.set_node_attributes(abstract, {node: "green"}, "node_color")
        elif abstract.nodes()[node]["type"] == "file":
            nx.set_node_attributes(abstract, {node: "red"}, "node_color")

    difference.remove_nodes_from(
        n for n in abstract.nodes() if n in nodes_update
    )

    return abstract, difference



def graph_diff_tasks(abstract, provenance):
    """! Calculate the difference of the abstract and provenance graphs

    Args:
        abstract (graph): An abstract graph
        provenance (graph): A concrete or provenance graph

    Returns:
        graphs: An updated abstract graph with completed nodes for plotting and a graph containing the difference between the nodes. (abstract-concrete)
    """
    prov_graph_id = list(nx.get_node_attributes(provenance, "ID").values())

    difference = copy.deepcopy(abstract)

    nodes_update = [
        n for n, v in abstract.nodes(data=True) if v["ID"] in prov_graph_id
    ]

    nx.set_node_attributes(abstract, "pending", "status")
    nx.set_node_attributes(abstract, "grey", "node_color")

    for node in nodes_update:
        nx.set_node_attributes(abstract, {node: "complete"}, "status")
        nx.set_node_attributes(abstract, {node: "green"}, "node_color")

    difference.remove_nodes_from(
        n for n, v in abstract.nodes(data=True) if v["status"] == "complete"
    )

    # In the difference graph the start_nodes is the list of nodes that can be
    # started (these should usually be a task)
    return abstract, difference


def _neighbour_handles_for_node(graph,
    node: str, file_handles: dict[str, Path]
) -> dict[str, Path]:
    """Return input and output file handles with their file paths for a node."""
    neighbors_inputs = list(graph.predecessors(node))
    neighbors_outputs = list(graph.successors(node))
    
    node_handles = neighbors_inputs + neighbors_outputs
    for handle in node_handles:
        if handle not in file_handles:
            raise FileHandleNotFound(f"The file handle, {handle}, was not recognized.")
    return {handle: file_handles[handle] for handle in node_handles}


def _file_handles_for_node(
    node: dict, file_handles: dict[str, Path]
) -> dict[str, Path]:
    """Return input and output file handles with their file paths for a node."""
    node_handles = node["inputs"] + node["outputs"]
    for handle in node_handles:
        if handle not in file_handles:
            raise FileHandleNotFound(f"The file handle, {handle}, was not recognized.")
    return {handle: file_handles[handle] for handle in node_handles}


def _materialize_files_in_command(command: str, file_handles: dict[str, Path]) -> str:
    """Inject input/output files into their handles in a command."""
    for handle, file in file_handles.items():
        #original but handle could be or not in abstract command
        # if handle not in command:
        #     raise FileHandleNotFound(f"The file handle, {handle}, was not recognized.")
        # command = command.replace(handle, str(file))
        if handle in command:
            command = command.replace(handle, str(file))
    return command


def graph_ID_relabel(graph, nmap):
    """This function will relabel the ID on the graphs

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping dictionary
    """
    graph2remap = copy.deepcopy(graph)
    graph2remap = nx.relabel_nodes(graph2remap, nmap)
    for node, attrs in graph2remap.nodes(data=True):
        if "type" in attrs:
            if attrs["type"] == "file":
                attrs["ID"] = encode(node)
        
            elif attrs["type"] == "task":
                full_task_description = list(nx.all_neighbors(graph2remap,  node))
                full_task_description.append(attrs["cmd"])
                attrs["ID"] = encode(",".join(sorted(full_task_description)))
                # attrs["ID"] = ",".join(sorted(full_task_description))
        else:
            full_task_description = attrs["inputs"] + attrs["outputs"]
            full_task_description.append(attrs["command"])
            attrs["ID"] = encode(",".join(sorted(full_task_description)))

    return graph2remap



def graph_remap_command_task(graph, nmap):
    """This function will relabel the ID on the file nodes of the graph

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping
    """
    graph2remap = copy.deepcopy(graph)

    for node, attrs in graph2remap.nodes(data=True):
        node_handles_paths = _file_handles_for_node(attrs, nmap)

        inputs_mapped = {inp: nmap[inp] for inp in attrs["inputs"]}
        outputs_mapped = {out: nmap[out] for out in attrs["outputs"]}
        inputs_paths = [nmap[inp] for inp in attrs["inputs"]]
        output_paths = [nmap[out] for out in attrs["outputs"]]

        graph2remap.nodes[node]["inputs"] = inputs_paths
        graph2remap.nodes[node]["outputs"] = output_paths

        full_task_description = inputs_paths + output_paths
        full_task_description.append(attrs["command"])
        graph2remap.nodes[node]["ID"] = encode(
            ",".join(sorted(full_task_description))
        )
        inputs_mapped.update(outputs_mapped)
        new_command = _materialize_files_in_command(attrs["command"], node_handles_paths)
        graph2remap.nodes[node]["command"] = new_command

    return graph2remap



def graph_remap_command(graph, nmap):
    """This function will relabel the ID on the file nodes of the graph

    Args:
        graph (graph): A base graph object
        nmap (dict): Node remapping
    """
    graph2remap = copy.deepcopy(graph)

    for node, attrs in graph2remap.nodes(data=True):
        if attrs["type"] == "task":
            node_handles_paths = _neighbour_handles_for_node(graph, node, nmap)
            neighbors_inputs = list(graph.predecessors(node))
            neighbors_outputs = list(graph.successors(node))
            print("neigh inputs",neighbors_inputs, nmap)

            inputs_mapped = {inp: nmap[inp] for inp in neighbors_inputs}
            outputs_mapped = {out: nmap[out] for out in neighbors_outputs}
            inputs_paths = [nmap[inp] for inp in neighbors_inputs]
            output_paths = [nmap[out] for out in neighbors_outputs]
            print("inputs_paths", inputs_paths)

            graph2remap.nodes[node]["inputs"] = inputs_paths
            graph2remap.nodes[node]["outputs"] = output_paths

            full_task_description = inputs_paths + output_paths
            full_task_description.append(attrs["cmd"])
            graph2remap.nodes[node]["ID"] = encode(
                ",".join(sorted(full_task_description))
                )
            inputs_mapped.update(outputs_mapped)
            new_command = _materialize_files_in_command(attrs["cmd"], node_handles_paths)
            graph2remap.nodes[node]["cmd"] = new_command

    return graph2remap




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



def next_nodes_run(graph):
    """This function return the first node(s) in a tree or in the
    case of a diff graph the next node scheduled to run
    Returns:
        list: A list of starting nodes
    """
    next_nodes_run = [
        x
        for x in graph.nodes()
        if int(graph.out_degree(x)) >= 1 and int(graph.in_degree(x)) == 0
    ]
    
    return next_nodes_run