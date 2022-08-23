from __future__ import absolute_import

from copy import deepcopy
import logging
import pymongo
import unittest

import daisy

logger = logging.getLogger(__name__)
daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


class TestMongoGraphProviderJoin(unittest.TestCase):

    def setUp(self):
        db_host = 'localhost'
        self.db_name = 'test_daisy_graph'
        try:
            self.client = pymongo.MongoClient(
                db_host, serverSelectionTimeoutMS=0)
        except pymongo.errors.ServerSelectionTimeoutError:
            self.skipTest("No MongoDB server found")

    def tearDown(self):
        self.delete_db()

    def delete_db(self):
        self.client.drop_database(self.db_name)

    def mongo_provider_factory(self, mode):
        return daisy.persistence.MongoDbGraphProvider(
            self.db_name,
            mode=mode,
            position_attribute=['x', 'y'])

    def test_join_collection(self):
        """Base test case, verify that output nodes contain attributes from
        both nodes collection and join_collection"""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5, "score": 0.11}
        node2 = {"x": 3, "y": 3, "score": 0.99}
        node3 = {"x": 7, "y": 7, "score": 0.55, "v3": 0}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.add_node(66, **node3)
        graph.write_nodes()

        join_collection = self.client[self.db_name]["join_collection"]
        join1 = {"id": 42, "v1": 1337, "v2": 420}
        join2 = {"id": 23, "v1": 314159, "v2": 141421, "v3": 271828}
        join3 = {"id": 66, "v1": 1, "v2": 161803}
        join_collection.insert_many(deepcopy([join1, join2, join3]))

        graph_provider = self.mongo_provider_factory('r')

        out_nodes = graph_provider.read_nodes(
            roi, join_collection="join_collection")
        node1.update(join1)
        node2.update(join2)
        node3.update(join3)

        out_nodes2 = graph_provider.read_nodes(roi)
        out_nodes2 = sorted(out_nodes2, key=lambda i: i['id'])
        out_nodes2[0].update(join2)
        out_nodes2[1].update(join1)
        out_nodes2[2].update(join3)

        in_nodes = [node1, node2, node3]
        self.delete_db()

        self.assertEqual(sorted(out_nodes, key=lambda i: i['id']),
                         sorted(in_nodes, key=lambda i: i['id']))
        self.assertEqual(sorted(out_nodes, key=lambda i: i['id']),
                         out_nodes2)
        self.assertNotIn('_id', out_nodes[0])
        self.assertIn('id', out_nodes[0])

    def test_join_collection_empty(self):
        """Assertion if join_collection is empty or does not exist"""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5}
        node2 = {"x": 3, "y": 3}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.write_nodes()

        _ = self.client[self.db_name]["join_collection"]

        graph_provider = self.mongo_provider_factory('r')

        with self.assertRaises(AssertionError):
            _ = graph_provider.read_nodes(
                roi, join_collection="join_collection")

        self.delete_db()

    def test_join_collection_nonmatching_ids(self):
        """Output should contain all and only the nodes in the nodes
        collection, extra ids in the join_collection should be ignored"""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5}
        node2 = {"x": 3, "y": 3}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.write_nodes()

        join_collection = self.client[self.db_name]["join_collection"]
        join1 = {"id": 42, "v1": 1337, "v2": 420}
        join2 = {"id": 24, "v1": 314159, "v2": 141421}
        join_collection.insert_many(deepcopy([join1, join2]))

        graph_provider = self.mongo_provider_factory('r')

        out_nodes = graph_provider.read_nodes(
            roi, join_collection="join_collection")
        node1.update(join1)
        node2["id"] = 23
        in_nodes = [node1, node2]
        self.delete_db()

        self.assertEqual(sorted(out_nodes, key=lambda i: i['id']),
                         sorted(in_nodes, key=lambda i: i['id']))

    def test_join_collection_read_attrs(self):
        """Verify interplay of read_attrs and join_collection.
        An attribute listed in read_attrs can be in either the node collection
        or the join_collection"""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5, "v1": 1337}
        node2 = {"x": 3, "y": 3, "v1": 314159}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.write_nodes()

        join_collection = self.client[self.db_name]["join_collection"]
        join1 = {"id": 42, "v2": 420}
        join2 = {"id": 23, "v2": 141421}
        join_collection.insert_many(deepcopy([join1, join2]))

        graph_provider = self.mongo_provider_factory('r')

        out_nodes1 = graph_provider.read_nodes(
            roi, join_collection="join_collection",
            read_attrs=['v1', 'v2'])
        out_nodes2 = graph_provider.read_nodes(
            roi, join_collection="join_collection",
            read_attrs=['v1'])
        node1.update(join1)
        node2.update(join2)

        in_nodes1 = deepcopy([node1, node2])
        del node1['v2']
        del node2['v2']
        in_nodes2 = deepcopy([node1, node2])
        self.delete_db()

        self.assertEqual(sorted(out_nodes1, key=lambda i: i['id']),
                         sorted(in_nodes1, key=lambda i: i['id']))
        self.assertEqual(sorted(out_nodes2, key=lambda i: i['id']),
                         sorted(in_nodes2, key=lambda i: i['id']))

    def test_join_collection_attr_filter(self):
        """Verify interplay of attr_filter and join_collection.
        The filter should be applied after the join."""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5, "flag1": True}
        node2 = {"x": 3, "y": 3, "flag1": False}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.write_nodes()

        join_collection = self.client[self.db_name]["join_collection"]
        join1 = {"id": 42, "flag2": True}
        join2 = {"id": 23, "flag2": False}
        join_collection.insert_many(deepcopy([join1, join2]))

        graph_provider = self.mongo_provider_factory('r')

        out_nodes1 = graph_provider.read_nodes(
            roi, join_collection="join_collection",
            attr_filter={"flag1": True})
        out_nodes2 = graph_provider.read_nodes(
            roi, join_collection="join_collection",
            attr_filter={"flag2": False})

        with self.assertRaises(KeyError):
            _ = graph_provider.read_nodes(
                roi, join_collection="join_collection",
                attr_filter={"flag3": False})
        node1.update(join1)
        node2.update(join2)

        in_nodes1 = deepcopy([node1])
        in_nodes2 = deepcopy([node2])
        self.delete_db()

        self.assertEqual(sorted(out_nodes1, key=lambda i: i['id']),
                         sorted(in_nodes1, key=lambda i: i['id']))
        self.assertEqual(sorted(out_nodes2, key=lambda i: i['id']),
                         sorted(in_nodes2, key=lambda i: i['id']))

    def test_join_collection_read_attrs_ignore_nonexisting(self):
        """Attributes that are listed in read_attrs but that do exist in
        neither the node collection nor in join_collection are silently
        ignored"""

        graph_provider = self.mongo_provider_factory('w')

        roi = daisy.Roi((0, 0), (10, 10))
        graph = graph_provider[roi]

        node1 = {"x": 5, "y": 5}
        node2 = {"x": 3, "y": 3}
        graph.add_node(42, **node1)
        graph.add_node(23, **node2)
        graph.write_nodes()

        join_collection = self.client[self.db_name]["join_collection"]
        join1 = {"id": 42, "v1": 1337, "v2": 420}
        join2 = {"id": 23, "v1": 314159, "v2": 141421}
        join_collection.insert_many(deepcopy([join1, join2]))

        graph_provider = self.mongo_provider_factory('r')

        out_nodes = graph_provider.read_nodes(
            roi, join_collection="join_collection",
            read_attrs=['v3'])

        self.delete_db()

        self.assertNotIn('v3', out_nodes[0])
