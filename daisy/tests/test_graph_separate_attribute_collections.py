from __future__ import absolute_import

import daisy
import logging
import unittest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestFilterMongoGraph(unittest.TestCase):

    def get_mongo_graph_provider(self, mode, node_attributes, edge_attributes):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            directed=True,
            node_attribute_collections=node_attributes,
            edge_attribute_collections=edge_attributes,
            mode=mode)

    def test_graph_separate_collection_simple(self):
        attributes = {'1': ['selected']}
        graph_provider = self.get_mongo_graph_provider(
                'w', attributes, attributes)
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

        graph_provider = self.get_mongo_graph_provider(
                'r', attributes, attributes)
        compare_graph = graph_provider[roi]

        self.assertEqual(True, compare_graph.nodes[2]['selected'])
        self.assertEqual(False, compare_graph.nodes[42]['selected'])
        self.assertEqual(True, compare_graph.edges[2, 42]['selected'])
        self.assertEqual(False, compare_graph.edges[42, 23]['selected'])
