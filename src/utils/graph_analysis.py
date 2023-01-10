import os
import networkx as nx


def calc_betw_centrl(graph):
    return nx.betweenness_centrality(graph)



def deg_centrl(graph):
    return nx.degree_centrality(graph)
