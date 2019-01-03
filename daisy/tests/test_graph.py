from __future__ import absolute_import

import daisy
import logging
import unittest
import random

logger = logging.getLogger(__name__)


class TestGraph(unittest.TestCase):

    def test_graph_io(self):

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            nodes_collection='nodes',
            edges_collection='edges',
            mode='w')
        graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        graph.add_node(2, comment="without position")
        graph.add_node(42, position=(1, 1, 1))
        graph.add_node(23, position=(5, 5, 5), swip='swap')
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap='zip')
        graph.add_edge(42, 23)
        graph.add_edge(57, 23)
        graph.add_edge(2, 42)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            nodes_collection='nodes',
            edges_collection='edges',
            mode='r')
        compare_graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        nodes = sorted(list(graph.nodes()))
        nodes.remove(2)  # node 2 has no position and will not be queried
        compare_nodes = sorted(list(compare_graph.nodes()))

        edges = sorted(list(graph.edges()))
        edges.remove((2, 42))  # node 2 has no position and will not be queried
        compare_edges = sorted(list(compare_graph.edges()))

        self.assertEqual(nodes, compare_nodes)
        self.assertEqual(edges, compare_edges)

    def test_graph_read_blockwise(self):

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            nodes_collection='nodes',
            edges_collection='edges',
            mode='w')

        graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        num_nodes = 1000
        num_edges = 10000

        random.seed(42)

        for i in range(num_nodes):
            graph.add_node(
                i,
                position=(
                    random.randint(0, 9),
                    random.randint(0, 9),
                    random.randint(0, 9)))
        for i in range(num_edges):
            graph.add_edge(
                random.randint(0, num_nodes - 1),
                random.randint(0, num_nodes - 1),
                score=random.random())

        graph.write_nodes()
        graph.write_edges()

        nodes, edges = graph_provider.read_blockwise(
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10)),
            daisy.Coordinate((1, 1, 1)),
            num_workers=40)

        written_nodes = list(range(num_nodes))
        read_nodes = sorted(nodes['id'])

        self.assertEqual(written_nodes, read_nodes)

        for u, v, score in zip(edges['u'], edges['v'], edges['score']):
            self.assertEqual(graph.edges[(u, v)]['score'], score)
