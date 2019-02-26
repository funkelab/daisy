from __future__ import absolute_import

import daisy
import logging
import unittest
import random
import numpy as np

logger = logging.getLogger(__name__)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestGraph(unittest.TestCase):

    def test_graph_io_mongo(self):

        def provider_factory(mode):
            return daisy.persistence.MongoDbGraphProvider(
                'test_daisy_graph',
                nodes_collection='nodes',
                edges_collection='edges',
                mode=mode)

        self.run_test_graph_io(provider_factory)

    def test_graph_io_file(self):

        def provider_factory(mode):
            return daisy.persistence.FileGraphProvider(
                'test_daisy_graph',
                chunk_size=(10, 10, 10),
                nodes_collection='nodes',
                edges_collection='edges',
                mode=mode)

        self.run_test_graph_io(provider_factory)

    def run_test_graph_io(self, provider_factory):

        graph_provider = provider_factory('w')

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

        graph_provider = provider_factory('r')
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

    def test_graph_fail_if_exists(self):

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
        with self.assertRaises(Exception):
            graph.write_nodes(fail_if_exists=True)
        with self.assertRaises(Exception):
            graph.write_edges(fail_if_exists=True)

    def test_graph_fail_if_not_exists(self):

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

        with self.assertRaises(Exception):
            graph.write_nodes(fail_if_not_exists=True)
        with self.assertRaises(Exception):
            graph.write_edges(fail_if_not_exists=True)

    def test_graph_write_attributes(self):

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
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

        graph.write_nodes(attributes=['position', 'swip'])
        graph.write_edges()

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            mode='r')
        compare_graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        nodes = {}
        for node, data in graph.nodes(data=True):
            if node == 2:
                continue
            if 'zap' in data:
                del data['zap']
            data['position'] = list(data['position'])
            nodes[node] = data

        compare_nodes = compare_graph.get_nodes_in_roi()

        self.assertEqual(nodes, compare_nodes)

    def test_graph_write_roi(self):

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
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

        write_roi = daisy.Roi((0, 0, 0), (6, 6, 6))
        graph.write_nodes(roi=write_roi)
        graph.write_edges(roi=write_roi)

        graph_provider = daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            mode='r')
        compare_graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        nodes = sorted(list(graph.nodes()))
        nodes.remove(2)  # node 2 has no position and will not be queried
        nodes.remove(57)  # node 57 is outside of the write_roi
        compare_nodes = sorted(list(compare_graph.get_nodes_in_roi().keys()))
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

        nodes_to_write = list(range(1000))
        nodes_to_write += [np.uint64(-10), np.uint64(-1)]
        num_edges = 10000

        random.seed(42)

        for n in nodes_to_write:
            graph.add_node(
                n,
                position=(
                    random.randint(0, 9),
                    random.randint(0, 9),
                    random.randint(0, 9)))
        for i in range(num_edges):
            ui = random.randint(0, len(nodes_to_write) - 1)
            vi = random.randint(0, len(nodes_to_write) - 1)
            u = nodes_to_write[ui]
            v = nodes_to_write[vi]
            graph.add_edge(
                u, v,
                score=random.random())

        graph.write_nodes()
        graph.write_edges()

        nodes, edges = graph_provider.read_blockwise(
            # read in larger ROI to test handling of empty blocks
            daisy.Roi(
                (0, 0, 0),
                (20, 10, 10)),
            daisy.Coordinate((1, 1, 1)),
            num_workers=40)

        assert nodes['id'].dtype == np.uint64
        assert edges['u'].dtype == np.uint64
        assert edges['v'].dtype == np.uint64

        written_nodes = list(nodes_to_write)
        read_nodes = sorted(nodes['id'])

        self.assertEqual(written_nodes, read_nodes)

        for u, v, score in zip(edges['u'], edges['v'], edges['score']):
            self.assertEqual(graph.edges[(u, v)]['score'], score)
