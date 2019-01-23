from __future__ import absolute_import
from ..graph import Graph, DiGraph
from ..roi import Roi
from ..scheduler import run_blockwise
from queue import Empty
import multiprocessing
import numpy as np
import logging
import time

logger = logging.getLogger(__name__)


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

        i = 0
        while True:

            last_round = blocks_done.is_set()

            try:
                start = time.time()
                block_nodes, block_edges = block_queue.get(timeout=0.1)
                logger.debug(
                    "Read graph data from block in %.3fs",
                    time.time() - start)
            except Empty:
                if last_round:
                    break
                else:
                    continue

            i += 1
            if i % 100 == 0:
                logger.debug("%d blocks read so far", i)

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
    def get_nodes_in_roi(self):
        '''Only returns nodes in the roi, excluding "dangling" nodes
        introduced by edges that cross the roi boundary'''

        nodes = {}
        for node, data in self.nodes(data=True):
            if 'position' in data:
                nodes[node] = data
        return nodes

    def write_edges(self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        '''Write edges and their attributes. 
        Args:
            roi(`class:Roi`):
                Restrict the write to the given ROI
            
            attributes(`class:list`):
                Only write the given attributes. If None, write all attributes.
            
            fail_if_exists:
                If true, throw error if edge with same u,v already exists in back end.

            fail_if_not_exists: 
                If true, throw error if edge with same u,v does not already exist in back end.

            delete:
                If true, delete edges in ROI in back end that do not exist in subgraph. 

        '''
        raise RuntimeError("not implemented in %s" % self.name())

    def write_nodes(self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        '''Write nodes and their attributes. 
        Args:
            roi(`class:Roi`):
                Restrict the write to the given ROI
            
            attributes(`class:list`):
                Only write the given attributes. If None, write all attributes.
            
            fail_if_exists:
                If true, throw error if node with same id already exists in back end,
                while still performing all other valid writes.

            fail_if_not_exists: 
                If true, throw error if node with same id does not already exist in back end,
                while still performing all other valid writes.

            delete:
                If true, delete nodes in ROI in back end that do not exist in subgraph. 
        
        '''
        raise RuntimeError("not implemented in %s" % self.name())

    def name(self):
        return type(self).__name__


class SharedSubDiGraph(DiGraph):
    def get_nodes_in_roi(self):
        '''Only returns nodes in the roi, excluding "dangling" nodes
        introduced by edges that cross the roi boundary'''

        nodes = {}
        for node, data in self.nodes(data=True):
            if 'position' in data:
                nodes[node] = data
        return nodes

    def write_edges(self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        '''Write edges and their attributes. 
        Args:
            roi(`class:Roi`):
                Restrict the write to the given ROI
            
            attributes(`class:list`):
                Only write the given attributes. If None, write all attributes.
            
            fail_if_exists:
                If true, throw error if edge with same u,v already exists in back end.

            fail_if_not_exists: 
                If true, throw error if edge with same u,v does not already exist in back end.

            delete:
                If true, delete edges in ROI in back end that do not exist in subgraph. 

        '''
        raise RuntimeError("not implemented in %s" % self.name())

    def write_nodes(self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        '''Write nodes and their attributes. 
        Args:
            roi(`class:Roi`):
                Restrict the write to the given ROI
            
            attributes(`class:list`):
                Only write the given attributes. If None, write all attributes.
            
            fail_if_exists:
                If true, throw error if node with same id already exists in back end,
                while still performing all other valid writes.

            fail_if_not_exists: 
                If true, throw error if node with same id does not already exist in back end,
                while still performing all other valid writes.

            delete:
                If true, delete nodes in ROI in back end that do not exist in subgraph. 
        
        '''
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

    start = time.time()
    graph = graph_provider[block.read_roi]
    logger.debug("Read block graph in %.3fs", time.time() - start)

    nodes = {
        'id': []
    }
    edges = {
        'u': [],
        'v': []
    }

    start = time.time()
    for node, data in graph.nodes(data=True):

        # skip over nodes that are not part of this block (they have been
        # pulled in by edges leaving this block and don't have a position
        # attribute)

        if type(graph_provider.position_attribute) == list:
            probe = graph_provider.position_attribute[0]
        else:
            probe = graph_provider.position_attribute
        if probe not in data:
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
    logger.debug("Parsed block graph in %.3fs", time.time() - start)

    start = time.time()
    block_queue.put((nodes, edges))
    logger.debug("Queued graph data in %.3fs", time.time() - start)
