from __future__ import absolute_import
from ..graph import Graph
from ..roi import Roi
from ..scheduler import run_blockwise
from queue import Empty
import multiprocessing
import numpy as np


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

    def read_blockwise(self, roi, block_size, num_workers):
        '''Read a list of nodes and edges blockwise. This is useful to get a
        representation of very large graphs, where parallel reading of smaller
        blocks might be faster.

        Arguments:

            roi (``daisy.Roi``):

                The ROI to read the graph from.

            block_size (``daisy.Coordinate``):

                The size of each block to read.

            num_workers (``int``):

                The number of processes to use.

        Returns:

            A tuple ``(nodes, edges)`` of dictionaries, each mapping each
            node/edge attribute to a ``ndarray`` with the corresponding values.
        '''

        manager = multiprocessing.Manager()
        block_queue = manager.Queue()
        blocks_done = manager.Event()

        master = multiprocessing.Process(
            target=read_blockwise_master,
            args=(
                self,
                roi,
                block_size,
                num_workers,
                block_queue,
                blocks_done))
        master.start()

        nodes = {}
        edges = {}

        while True:

            last_round = blocks_done.is_set()

            try:
                block_nodes, block_edges = block_queue.get(timeout=0.1)
            except Empty:
                if last_round:
                    break
                else:
                    continue

            for k, v in block_nodes.items():
                if k not in nodes:
                    nodes[k] = []
                nodes[k].append(v)

            for k, v in block_edges.items():
                if k not in edges:
                    edges[k] = []
                edges[k].append(v)

        master.join()

        nodes = {
            k: np.concatenate(v)
            for k, v in nodes.items()
        }
        edges = {
            k: np.concatenate(v)
            for k, v in edges.items()
        }

        return (nodes, edges)

    def __getitem__(self, roi):
        raise RuntimeError("not implemented in %s" % self.name())

    def name(self):
        return type(self).__name__


class SharedSubGraph(Graph):

    def write_edges(self, roi=None):
        '''Write edges and their attributes. Restrict the write to the given
        ROI, if given.'''
        raise RuntimeError("not implemented in %s" % self.name())

    def write_nodes(self, roi=None):
        '''Write nodes and their attributes. Restrict the write to the given
        ROI, if given.'''
        raise RuntimeError("not implemented in %s" % self.name())

    def name(self):
        return type(self).__name__


def read_blockwise_master(
        graph_provider,
        roi,
        block_size,
        num_workers,
        block_queue,
        blocks_done):

    run_blockwise(
        roi,
        read_roi=Roi((0,)*len(block_size), block_size),
        write_roi=Roi((0,)*len(block_size), block_size),
        process_function=lambda b: read_blockwise_worker(
            graph_provider,
            b,
            block_queue),
        fit='shrink',
        num_workers=num_workers)

    blocks_done.set()


def read_blockwise_worker(graph_provider, block, block_queue):

    graph = graph_provider[block.read_roi]

    nodes = {
        'id': []
    }
    edges = {
        'u': [],
        'v': []
    }

    for node, data in graph.nodes(data=True):

        # skip over nodes that are not part of this block (they have been
        # pulled in by edges leaving this block)
        if 'position' not in data:
            continue
        if not block.read_roi.contains(data['position']):
            continue

        nodes['id'].append(node)
        for k, v in data.items():
            if k not in nodes:
                nodes[k] = []
            nodes[k].append(v)

    for u, v, data in graph.edges(data=True):

        edges['u'].append(u)
        edges['v'].append(v)
        for k, v in data.items():
            if k not in edges:
                edges[k] = []
            edges[k].append(v)

    nodes = {
        k: np.array(v)
        for k, v in nodes.items()
    }
    edges = {
        k: np.array(v)
        for k, v in edges.items()
    }

    block_queue.put((nodes, edges))
