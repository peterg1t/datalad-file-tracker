import os
from . import line_process_task, encode, line_process_file, line_process_task_v2

def process_task_node(line, nodes, edges):
    task, prec_nodes, command, workflow = line_process_task(line)
    nodes.append(
        (
            task,
            {
                "name": task,
                "label": task,
                "path": "",
                "type": "task",
                "cmd": command,
                "status": "pending",
                "node_color": "grey",
                "predecesor": prec_nodes,
                "workflow": workflow,
                "ID": "",
            },
        )
    )
    for node in prec_nodes:
        if node:
            edges.append((node, task))

def process_file_node(line, nodes, edges):
    files, prec_nodes = line_process_file(line)
    for file in files:
        nodes.append(
            (
                os.path.basename(file),
                {
                    "name": file,
                    "label": os.path.basename(file),
                    "path": os.path.dirname(file),
                    "type": "file",
                    "status": "pending",
                    "node_color": "grey",
                    "predecesor": prec_nodes,
                    "ID": encode(file),
                },
            )
        )
        for node in prec_nodes:
            if node:
                edges.append((node, os.path.basename(file)))

def gcg_processing(filename):
    """! This function generate a networkx graph from a file containing an abstract graph

    Args:
        filename (str): Path to the abstract graph

    Returns:
        nodes: A list of nodes
        edges: A list of edges
    """
    nodes = []
    edges = []
    with open(filename, encoding="utf-8") as file_abstract:
        read_data = file_abstract.readlines()
        for item in read_data:
            stage_type = item.split("<>")[0].strip()
            if stage_type == "T":
                process_task_node(item, nodes, edges)
            elif stage_type == "F":
                process_file_node(item, nodes, edges)
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                

    return nodes, edges


def gcg_processing_tasks(filename):
    """! This function generate a networkx graph from a file containing an abstract graph

    Args:
        filename (str): Path to the abstract graph

    Returns:
        nodes: A list of nodes
        edges: A list of edges
    """
    nodes = []
    edges = []
    with open(filename, encoding="utf-8") as file_abstract:
        read_data = file_abstract.readlines()
        for item in read_data:
            task, inputs, outputs, command, pce, subworkflow, message = line_process_task_v2(item)
            nodes.append(
                (
                    task,
                    {
                        "description": task,
                        "command": command,
                        "inputs": inputs,
                        "outputs": outputs,
                        "message": message,
                        "PCE": pce,
                        "subworkflow": subworkflow,
                    },
                )
            )

        for node1 in nodes:
            for node2 in nodes:
                diff_set = set(node1[1]["outputs"]).intersection(set(node2[1]["inputs"]))
                if diff_set:
                    edges.append((node1[0], node2[0]))

    return nodes, edges
