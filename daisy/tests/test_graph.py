from __future__ import absolute_import

import daisy
import logging
import unittest
import random
import numpy as np

logger = logging.getLogger(__name__)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestGraph(unittest.TestCase):
    def mongo_provider_factory(self, mode):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            mode=mode)

    def file_provider_factory(self, mode):
        return daisy.persistence.FileGraphProvider(
            'test_daisy_graph',
            chunk_size=(10, 10, 10),
            mode=mode)

    # test basic graph io
    def test_graph_io_mongo(self):
        self.run_test_graph_io(self.mongo_provider_factory)

    def test_graph_io_file(self):
        self.run_test_graph_io(self.file_provider_factory)

    # test fail_if_exists flag when writing subgraph
    def test_graph_fail_if_exists_mongo(self):
        self.run_test_graph_fail_if_exists(self.mongo_provider_factory)

    # test fail_if_not_exists flag when writing subgraph
    def test_graph_fail_if_not_exists_mongo(self):
        self.run_test_graph_fail_if_not_exists(self.mongo_provider_factory)

    # test that only specified attributes are written to backend
    def test_graph_write_attributes_mongo(self):
        self.run_test_graph_write_attributes(self.mongo_provider_factory)

    # test that only write nodes inside the write_roi
    def test_graph_write_roi_mongo(self):
        self.run_test_graph_write_roi(self.mongo_provider_factory)

    def test_graph_write_roi_file(self):
        self.run_test_graph_write_roi(self.file_provider_factory)

    # test connected components
    def test_graph_connected_components_mongo(self):
        self.run_test_graph_connected_components(self.mongo_provider_factory)

    # test has_edge
    def test_graph_has_edge_mongo(self):
        self.run_test_graph_has_edge(self.mongo_provider_factory)

    # test read_blockwise function
    def test_graph_read_blockwise_mongo(self):
        self.run_test_graph_read_blockwise(self.mongo_provider_factory)

    def test_graph_read_blockwise_file(self):
        self.run_test_graph_read_blockwise(self.file_provider_factory)

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

    def run_test_graph_fail_if_exists(self, provider_factory):

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
        with self.assertRaises(Exception):
            graph.write_nodes(fail_if_exists=True)
        with self.assertRaises(Exception):
            graph.write_edges(fail_if_exists=True)

    def run_test_graph_fail_if_not_exists(self, provider_factory):

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

        with self.assertRaises(Exception):
            graph.write_nodes(fail_if_not_exists=True)
        with self.assertRaises(Exception):
            graph.write_edges(fail_if_not_exists=True)

    def run_test_graph_write_attributes(self, provider_factory):

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

        graph.write_nodes(attributes=['position', 'swip'])
        graph.write_edges()

        graph_provider = provider_factory('r')
        compare_graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        nodes = []
        for node, data in graph.nodes(data=True):
            if node == 2:
                continue
            if 'zap' in data:
                del data['zap']
            data['position'] = list(data['position'])
            nodes.append((node, data))

        compare_nodes = compare_graph.nodes(data=True)
        compare_nodes = [(node_id, data) for node_id, data in compare_nodes
                         if len(data) > 0]
        self.assertCountEqual(nodes, compare_nodes)

    def run_test_graph_write_roi(self, provider_factory):

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

        write_roi = daisy.Roi((0, 0, 0), (6, 6, 6))
        graph.write_nodes(roi=write_roi)
        graph.write_edges(roi=write_roi)

        graph_provider = provider_factory('r')
        compare_graph = graph_provider[
            daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        ]

        nodes = sorted(list(graph.nodes()))
        nodes.remove(2)  # node 2 has no position and will not be queried
        nodes.remove(57)  # node 57 is outside of the write_roi
        compare_nodes = compare_graph.nodes(data=True)
        compare_nodes = [node_id for node_id, data in compare_nodes
                         if len(data) > 0]
        compare_nodes = sorted(list(compare_nodes))
        edges = sorted(list(graph.edges()))
        edges.remove((2, 42))  # node 2 has no position and will not be queried
        compare_edges = sorted(list(compare_graph.edges()))

        self.assertEqual(nodes, compare_nodes)
        self.assertEqual(edges, compare_edges)

    def run_test_graph_connected_components(self, provider_factory):

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
        graph.add_edge(57, 23)
        graph.add_edge(2, 42)

        components = graph.get_connected_components()
        self.assertEqual(len(components), 2)
        c1, c2 = components
        n1 = sorted(list(c1.nodes()))
        n2 = sorted(list(c2.nodes()))

        compare_n1 = [2, 42]
        compare_n2 = [23, 57]

        if 2 in n2:
            temp = n2
            n2 = n1
            n1 = temp

        self.assertCountEqual(n1, compare_n1)
        self.assertCountEqual(n2, compare_n2)

    def run_test_graph_read_blockwise(self, provider_factory):

        graph_provider = provider_factory('w')

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

    def run_test_graph_has_edge(self, provider_factory):

        graph_provider = provider_factory('w')

        roi = daisy.Roi(
                (0, 0, 0),
                (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, comment="without position")
        graph.add_node(42, position=(1, 1, 1))
        graph.add_node(23, position=(5, 5, 5), swip='swap')
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap='zip')
        graph.add_edge(42, 23)
        graph.add_edge(57, 23)

        write_roi = daisy.Roi((0, 0, 0), (6, 6, 6))
        graph.write_nodes(roi=write_roi)
        graph.write_edges(roi=write_roi)

        self.assertTrue(graph_provider.has_edges(roi))
