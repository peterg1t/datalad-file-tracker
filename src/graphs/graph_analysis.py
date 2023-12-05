"""! Graph Analysis Module
This module allows the user to print the different network attributes
"""
import networkx as nx


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
