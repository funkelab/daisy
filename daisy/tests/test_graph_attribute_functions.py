from __future__ import absolute_import

import daisy
import logging
import unittest
import networkx as nx

logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.DEBUG)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestFilterMongoGraph(unittest.TestCase):

    def get_mongo_graph_provider(
            self, mode):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            directed=True,
            mode=mode)

    def test_graph_filtering(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2, position=(2, 2, 2), selected=True)
        graph.add_node(42, position=(1, 1, 1), selected=False)
        graph.add_node(23, position=(5, 5, 5), selected=True)
        graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), selected=True)
        graph.add_edge(42, 23, selected=False)
        graph.add_edge(57, 23, selected=True)
        graph.add_edge(2, 42, selected=True)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r')

        filtered_nodes = graph_provider.read_nodes(
                roi, attr_filter={'selected': True})
        filtered_node_ids = [node['id'] for node in filtered_nodes]
        expected_node_ids = [2, 23, 57]
        self.assertCountEqual(expected_node_ids, filtered_node_ids)

        filtered_edges = graph_provider.read_edges(
                roi, attr_filter={'selected': True})
        filtered_edge_endpoints = [(edge['u'], edge['v'])
                                   for edge in filtered_edges]
        expected_edge_endpoints = [(57, 23), (2, 42)]
        self.assertCountEqual(expected_edge_endpoints, filtered_edge_endpoints)

        filtered_subgraph = graph_provider.get_graph(
                roi,
                nodes_filter={'selected': True},
                edges_filter={'selected': True})
        nodes_with_position = [node for node, data
                               in filtered_subgraph.nodes(data=True)
                               if 'position' in data]
        self.assertCountEqual(expected_node_ids, nodes_with_position)
        self.assertCountEqual(expected_edge_endpoints,
                              filtered_subgraph.edges())

    def test_graph_filtering_complex(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2,
                       position=(2, 2, 2),
                       selected=True,
                       test='test')
        graph.add_node(42,
                       position=(1, 1, 1),
                       selected=False,
                       test='test2')
        graph.add_node(23,
                       position=(5, 5, 5),
                       selected=True,
                       test='test2')
        graph.add_node(57,
                       position=daisy.Coordinate((7, 7, 7)),
                       selected=True,
                       test='test')

        graph.add_edge(42, 23,
                       selected=False,
                       a=100,
                       b=3)
        graph.add_edge(57, 23,
                       selected=True,
                       a=100,
                       b=2)
        graph.add_edge(2, 42,
                       selected=True,
                       a=101,
                       b=3)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r')

        filtered_nodes = graph_provider.read_nodes(
                roi, attr_filter={'selected': True,
                                  'test': 'test'})
        filtered_node_ids = [node['id'] for node in filtered_nodes]
        expected_node_ids = [2, 57]
        self.assertCountEqual(expected_node_ids, filtered_node_ids)

        filtered_edges = graph_provider.read_edges(
                roi, attr_filter={'selected': True,
                                  'a': 100})
        filtered_edge_endpoints = [(edge['u'], edge['v'])
                                   for edge in filtered_edges]
        expected_edge_endpoints = [(57, 23)]
        self.assertCountEqual(expected_edge_endpoints, filtered_edge_endpoints)

        filtered_subgraph = graph_provider.get_graph(
                roi,
                nodes_filter={'selected': True,
                              'test': 'test'},
                edges_filter={'selected': True,
                              'a': 100})
        nodes_with_position = [node for node, data
                               in filtered_subgraph.nodes(data=True)
                               if 'position' in data]
        self.assertCountEqual(expected_node_ids, nodes_with_position)
        self.assertCountEqual(expected_edge_endpoints,
                              filtered_subgraph.edges())

    def test_graph_read_and_update_specific_attrs(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        graph = graph_provider[roi]

        graph.add_node(2,
                       position=(2, 2, 2),
                       selected=True,
                       test='test')
        graph.add_node(42,
                       position=(1, 1, 1),
                       selected=False,
                       test='test2')
        graph.add_node(23,
                       position=(5, 5, 5),
                       selected=True,
                       test='test2')
        graph.add_node(57,
                       position=daisy.Coordinate((7, 7, 7)),
                       selected=True,
                       test='test')

        graph.add_edge(42, 23,
                       selected=False,
                       a=100,
                       b=3)
        graph.add_edge(57, 23,
                       selected=True,
                       a=100,
                       b=2)
        graph.add_edge(2, 42,
                       selected=True,
                       a=101,
                       b=3)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r+')
        limited_graph = graph_provider.get_graph(
                roi, node_attrs=['selected'], edge_attrs=['c'])

        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertTrue('selected' in data)
            data['selected'] = True

        for u, v, data in limited_graph.edges(data=True):
            self.assertFalse('a' in data)
            self.assertFalse('b' in data)
            nx.set_edge_attributes(limited_graph, 5, 'c')

        limited_graph.update_edge_attrs(attributes=['c'])
        limited_graph.update_node_attrs(attributes=['selected'])

        updated_graph = graph_provider.get_graph(roi)

        for node, data in updated_graph.nodes(data=True):
            self.assertTrue(data['selected'])

        for u, v, data in updated_graph.edges(data=True):
            self.assertEqual(data['c'], 5)

    def test_graph_read_unbounded_roi(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        unbounded_roi = daisy.Roi((None, None, None), (None, None, None))

        graph = graph_provider[roi]

        graph.add_node(2,
                       position=(2, 2, 2),
                       selected=True,
                       test='test')
        graph.add_node(42,
                       position=(1, 1, 1),
                       selected=False,
                       test='test2')
        graph.add_node(23,
                       position=(5, 5, 5),
                       selected=True,
                       test='test2')
        graph.add_node(57,
                       position=daisy.Coordinate((7, 7, 7)),
                       selected=True,
                       test='test')

        graph.add_edge(42, 23,
                       selected=False,
                       a=100,
                       b=3)
        graph.add_edge(57, 23,
                       selected=True,
                       a=100,
                       b=2)
        graph.add_edge(2, 42,
                       selected=True,
                       a=101,
                       b=3)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r+')
        limited_graph = graph_provider.get_graph(
                unbounded_roi, node_attrs=['selected'], edge_attrs=['c'])

        seen = []
        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertTrue('selected' in data)
            data['selected'] = True
            seen.append(node)

        self.assertCountEqual(seen, [2, 42, 23, 57])

    def test_graph_read_dangling_attrs(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        small_roi = daisy.Roi((4, 4, 4), (2, 2, 2))
        graph = graph_provider[roi]

        graph.add_node(2,
                       position=(2, 2, 2),
                       selected=False,
                       test='test')
        graph.add_node(42,
                       position=(1, 1, 1),
                       selected=False,
                       test='test2')
        graph.add_node(23,
                       position=(5, 5, 5),
                       selected=True,
                       test='test2')
        graph.add_node(57,
                       position=daisy.Coordinate((7, 7, 7)),
                       selected=False,
                       test='test')

        graph.add_edge(42, 23,
                       selected=False,
                       a=100,
                       b=3)
        graph.add_edge(57, 23,
                       selected=True,
                       a=100,
                       b=2)
        graph.add_edge(2, 42,
                       selected=True,
                       a=101,
                       b=3)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r+')
        limited_graph = graph_provider.get_graph(
                small_roi,
                node_attrs=['selected'],
                edge_attrs=['c'],
                node_inclusion='dangling',
                edge_inclusion='either')

        seen = []
        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertTrue('selected' in data)
            seen.append(node)
        self.assertCountEqual(seen, [42, 23, 57])

        limited_graph = graph_provider.get_graph(
                small_roi,
                node_attrs=['selected'],
                edge_attrs=['c'],
                node_inclusion='strict',
                edge_inclusion='either')

        seen = []
        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertEqual('selected' in data, node == 23)
            seen.append(node)
        self.assertCountEqual(seen, [42, 23, 57])

    def test_graph_read_targeting_edges(self):
        graph_provider = self.get_mongo_graph_provider('w')
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        small_roi = daisy.Roi((4, 4, 4), (2, 2, 2))
        graph = graph_provider[roi]

        graph.add_node(2,
                       position=(2, 2, 2),
                       selected=False,
                       test='test')
        graph.add_node(42,
                       position=(1, 1, 1),
                       selected=False,
                       test='test2')
        graph.add_node(23,
                       position=(5, 5, 5),
                       selected=True,
                       test='test2')
        graph.add_node(57,
                       position=daisy.Coordinate((7, 7, 7)),
                       selected=False,
                       test='test')

        graph.add_edge(42, 23,
                       selected=False,
                       a=100,
                       b=3)
        graph.add_edge(57, 23,
                       selected=True,
                       a=100,
                       b=2)
        graph.add_edge(2, 42,
                       selected=True,
                       a=101,
                       b=3)

        graph.write_nodes()
        graph.write_edges()

        graph_provider = self.get_mongo_graph_provider('r+')
        limited_graph = graph_provider.get_graph(
                small_roi,
                node_attrs=['selected'],
                edge_attrs=['c'],
                node_inclusion='strict',
                edge_inclusion='either')

        seen = []
        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertEqual('selected' in data, node == 23)
            seen.append(node)
        self.assertCountEqual(seen, [42, 23, 57])

        limited_graph = graph_provider.get_graph(
                small_roi,
                node_attrs=['selected'],
                edge_attrs=['c'],
                node_inclusion='strict',
                edge_inclusion='u')

        seen = []
        for node, data in limited_graph.nodes(data=True):
            self.assertFalse('test' in data)
            self.assertEqual('selected' in data, node == 23)
            seen.append(node)
        self.assertCountEqual(seen, [23])
