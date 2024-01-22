import networkx as nx


def start_nodes(workflow: nx.DiGraph) -> list[str]:
    """Return the starting nodes.

    Args:
        workflow (nx.DiGraph): Workflow graph

    Returns:
        list[str]: Nodes of the workflow with no predecessors
    """
    return [node for node in workflow.nodes if workflow.in_degree(node) == 0]


def end_nodes(workflow: nx.DiGraph) -> list[str]:
    """Return the terminating nodes.

    Args:
        workflow (nx.DiGraph): Workflow graph

    Returns:
        list[str]: Nodes of the workflow with no successors
    """
    return [node for node in workflow.nodes if workflow.out_degree(node) == 0]
