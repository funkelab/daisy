from __future__ import absolute_import
from ..graph import Graph

class SharedGraphProvider(object):
    '''Interface for shared graph providers that supports slicing to retrieve
    subgraphs.

    Implementations should support the following interactions::

        # provider is a SharedGraphProvider

        # slicing with ROI to extract a subgraph
        sub_graph = provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

        # sub_graph should inherit from SharedSubGraph

        # write nodes
        sub_graph.write_nodes()

        # write edges
        sub_graph.write_edges()
    '''

    def __getitem__(self, roi):
        raise RuntimeError("not implemented in %s"%self.name())

    def name(self):
        return type(self).__name__

class SharedSubGraph(Graph):

    def write_edges(self, roi=None):
        '''Write edges and their attributes. Restrict the write to the given
        ROI, if given.'''
        raise RuntimeError("not implemented in %s"%self.name())

    def write_nodes(self, roi=None):
        '''Write nodes and their attributes. Restrict the write to the given
        ROI, if given.'''
        raise RuntimeError("not implemented in %s"%self.name())

    def name(self):
        return type(self).__name__
