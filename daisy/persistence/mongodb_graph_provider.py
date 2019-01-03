from __future__ import absolute_import
from ..coordinate import Coordinate
from .shared_graph_provider import SharedGraphProvider, SharedSubGraph
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
import logging

logger = logging.getLogger(__name__)


class MongoDbGraphProvider(SharedGraphProvider):
    '''Provides shared graphs stored in a MongoDB.

    Nodes are assumed to have at least an attribute ``id``. If the have an
    array attribute ``position``, it will be used for geometric slicing (see
    ``__getitem__``).

    Edges are assumed to have at least attributes ``u``, ``v``.

    Arguments:

        db_name (``string``):

            The name of the MongoDB database.

        host (``string``, optional):

            The URL of the MongoDB host.

        mode (``string``, optional):

            One of ``r``, ``r+``, or ``w``. Defaults to ``r+``. ``w`` drops the
            node and edge collection.

        nodes_collection (``string``):
        edges_collection (``string``):

            Names of the nodes and edges collections, should they differ from
            ``nodes`` and ``edges``.
    '''

    def __init__(
            self,
            db_name,
            host=None,
            mode='r+',
            nodes_collection='nodes',
            edges_collection='edges'):

        self.db_name = db_name
        self.host = host
        self.mode = mode
        self.nodes_collection_name = nodes_collection
        self.edges_collection_name = edges_collection
        self.client = None
        self.database = None
        self.nodes = None
        self.edges = None

        try:

            self.__connect()
            self.__open_db()

            if mode == 'w':

                logger.info(
                    "dropping collections %s and %s",
                    self.nodes_collection_name,
                    self.edges_collection_name)

                self.__open_collections()
                self.nodes.drop()
                self.edges.drop()

            collection_names = self.database.list_collection_names()
            if (
                    nodes_collection not in collection_names or
                    edges_collection not in collection_names):
                self.__create_collections()

        finally:

            self.__disconnect()

    def read_nodes(self, roi):
        '''Return a list of nodes within roi.
        '''

        logger.debug("Querying nodes in %s", roi)

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            nodes = self.nodes.find(self.__pos_query(roi), {'_id': False})

        finally:

            self.__disconnect()

        return nodes

    def num_nodes(self, roi):

        assert roi.dims() == 3, \
            "Sorry, MongoDbGraphProvider backend does only 3D"

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            num = self.nodes.count(self.__pos_query(roi))

        finally:

            self.__disconnect()

        return num

    def has_edges(self, roi):

        assert roi.dims() == 3, \
            "Sorry, MongoDbGraphProvider backend does only 3D"

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            node = self.nodes.find_one(self.__pos_query(roi))

            # no nodes -> no edges
            if node is None:
                return False

            edges = self.edges.find(
                {
                    'u': node['id']
                })

        finally:

            self.__disconnect()

        return edges.count() > 0

    def __getitem__(self, roi):

        assert roi.dims() == 3, \
            "Sorry, MongoDbGraphProvider backend does only 3D"

        # get all nodes within roi
        nodes = self.read_nodes(roi)

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            # create a list of nodes and their attributes
            node_list = [
                (n['id'], self.__remove_keys(n, ['id']))
                for n in nodes
            ]
            logger.debug("found %d nodes", len(node_list))
            logger.debug("read nodes: %s", node_list)

            # get all edges that have their u in the selected nodes
            node_ids = list([node[0] for node in node_list])
            logger.debug("looking for edges with u in %s", node_ids)
            edges = self.edges.find(
                {
                    'u': {'$in': node_ids}
                })

            # create a list of edges and their attributes
            edge_list = [
                (e['u'], e['v'], self.__remove_keys(e, ['u', 'v']))
                for e in edges
            ]
            logger.debug("found %d edges", len(edge_list))
            logger.debug("read edges: %s", edge_list)

        finally:

            self.__disconnect()

        # create the subgraph
        graph = MongoDbSubGraph(
            self.db_name,
            roi,
            self.host,
            self.mode,
            self.nodes_collection_name,
            self.edges_collection_name)
        graph.add_nodes_from(node_list)
        graph.add_edges_from(edge_list)

        return graph

    def __remove_keys(self, dictionary, keys):

        for key in keys:
            del dictionary[key]
        return dictionary

    def __connect(self):

        self.client = MongoClient(self.host)

    def __open_db(self):

        self.database = self.client[self.db_name]

    def __open_collections(self):

        self.nodes = self.database[self.nodes_collection_name]
        self.edges = self.database[self.edges_collection_name]

    def __disconnect(self):

        self.nodes = None
        self.edges = None
        self.database = None
        self.client.close()
        self.client = None

    def __create_collections(self):

        self.__open_db()
        self.__open_collections()

        self.nodes.create_index(
            [
                ('position', ASCENDING)
            ],
            name='position')

        self.nodes.create_index(
            [
                ('id', ASCENDING)
            ],
            name='id',
            unique=True)

        self.edges.create_index(
            [
                ('u', ASCENDING),
                ('v', ASCENDING)
            ],
            name='incident',
            unique=True)

    def __pos_query(self, roi):

        begin = roi.get_begin()
        end = roi.get_end()

        return {
            'position.%d' % d: {'$gte': b, '$lt': e}
            for d, (b, e) in enumerate(zip(begin, end))
        }


class MongoDbSubGraph(SharedSubGraph):

    def __init__(
            self,
            db_name,
            roi,
            host=None,
            mode='r+',
            nodes_collection='nodes',
            edges_collection='edges'):

        super(SharedSubGraph, self).__init__()

        self.db_name = db_name
        self.roi = roi
        self.host = host
        self.mode = mode

        self.client = MongoClient(self.host)
        self.database = self.client[db_name]
        self.nodes_collection = self.database[nodes_collection]
        self.edges_collection = self.database[edges_collection]

    def write_edges(self, roi=None):

        if self.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing edges in %s", roi)

        edges = []
        for u, v, data in self.edges(data=True):

            u, v = min(u, v), max(u, v)
            if not self.__contains(roi, u):
                continue

            edge = {
                'u': int(u),
                'v': int(v),
            }
            edge.update(data)
            edges.append(edge)

        if len(edges) == 0:
            return

        try:

            self.edges_collection.insert_many(edges)

        except BulkWriteError as e:

            logger.error(e.details)
            raise

    def write_nodes(self, roi=None):

        if self.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing all nodes")

        nodes = []
        for node_id, data in self.nodes(data=True):

            if not self.__contains(roi, node_id):
                continue

            node = {
                'id': int(node_id)
            }
            node.update(data)
            nodes.append(node)

        if len(nodes) == 0:
            return

        try:

            self.nodes_collection.insert_many(nodes)

        except BulkWriteError as e:

            logger.error(e.details)
            raise

    def __contains(self, roi, node):

        node_data = self.node[node]

        # Some nodes are outside of the originally requested ROI (they have
        # been pulled in by edges leaving the ROI). These nodes have no
        # attributes, so we can't perform an inclusion test. However, we
        # know they are outside of the subgraph ROI, and therefore also
        # outside of 'roi', whatever it is.
        if 'position' not in node_data:
            return False

        return roi.contains(Coordinate(node_data['position']))
