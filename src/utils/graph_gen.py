import os
import utils


def gcg_from_file(filename):
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
                task, prec_nodes, command, workflow = utils.line_process_task(item)
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

            elif stage_type == "F":
                files, prec_nodes = utils.line_process_file(item)
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
                                "ID": utils.encode(file),
                            },
                        )
                    )
                    for node in prec_nodes:
                        if node:
                            edges.append((node, os.path.basename(file)))

    return nodes, edges


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
                task, prec_nodes, command, workflow = utils.line_process_task(item)
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

            elif stage_type == "F":
                files, prec_nodes = utils.line_process_file(item)
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
                                "ID": utils.encode(file),
                            },
                        )
                    )
                    for node in prec_nodes:
                        if node:
                            edges.append((node, os.path.basename(file)))

    return nodes, edges
