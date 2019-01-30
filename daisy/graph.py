import copy
import networkx as nx


class Graph(nx.Graph):
    '''
    '''

    def __init__(self, graph_data=None):
        super(Graph, self).__init__(incoming_graph_data=graph_data)

    def copy(self):
        '''Return a deep copy of this RAG.'''

        return copy.deepcopy(self)


class DiGraph(nx.DiGraph):
    def __init__(self, graph_data=None):
        super(DiGraph, self).__init__(incoming_graph_data=graph_data)

    def copy(self):
        '''Return a deep copy of this DirectedGraph.'''

        return copy.deepcopy(self)
