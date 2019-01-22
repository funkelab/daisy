from __future__ import absolute_import
from ..client import Client
from ..graph import Graph
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

        block_queue = multiprocessing.Queue()

        master = multiprocessing.Process(
            target=read_blockwise_master,
            args=(
                self,
                roi,
                block_size,
                num_workers,
                block_queue))
        master.start()

        nodes = {}
        edges = {}

        i = 0
        while True:

            try:
                start = time.time()
                block = block_queue.get(timeout=0.1)

                if block is None:
                    logger.debug(
                        "Found None in queue, returning")
                    break

                block_nodes, block_edges = block

                logger.debug(
                    "Read graph data from queue in %.3fs",
                    time.time() - start)

            except Empty:
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
        block_queue):

    run_blockwise(
        roi,
        read_roi=Roi((0,)*len(block_size), block_size),
        write_roi=Roi((0,)*len(block_size), block_size),
        process_function=lambda: read_blockwise_worker(
            graph_provider,
            block_queue),
        fit='shrink',
        num_workers=num_workers)

    # indicate that there are no more blocks to come
    block_queue.put(None)

    logger.debug("Read block-wise master exiting")


def read_blockwise_worker(graph_provider, block_queue):

    client = Client()

    while True:

        block = client.acquire_block()
        if block is None:
            break

        read_block(graph_provider, block, block_queue)

        client.release_block(block, 0)

    # make sure all changes are flushed before we exit
    block_queue.close()
    block_queue.join_thread()

    logger.debug(
        "Read block-wise worker %d done, all data written to queue",
        client.context.worker_id)


def read_block(graph_provider, block, block_queue):

    start = time.time()
    logger.debug("Reading graph in block %s", block)
    graph = graph_provider[block.read_roi]
    logger.debug(
        "Read graph from graph provider in %.3fs",
        time.time() - start)

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

        nodes['id'].append(np.uint64(node))
        for k, v in data.items():
            if k not in nodes:
                nodes[k] = []
            nodes[k].append(v)

    for u, v, data in graph.edges(data=True):

        edges['u'].append(np.uint64(u))
        edges['v'].append(np.uint64(v))
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
    logger.debug("Parsed graph in %.3fs", time.time() - start)

    start = time.time()
    block_queue.put((nodes, edges))
    logger.debug("Queued graph data in %.3fs", time.time() - start)
