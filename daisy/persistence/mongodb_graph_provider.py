from __future__ import absolute_import
from ..coordinate import Coordinate
from ..roi import Roi
from .shared_graph_provider import\
    SharedGraphProvider, SharedSubGraph
from ..graph import Graph, DiGraph
from pymongo import MongoClient, ASCENDING, ReplaceOne
from pymongo.errors import BulkWriteError, WriteError
import logging

logger = logging.getLogger(__name__)


class MongoDbGraphProvider(SharedGraphProvider):
    '''Provides shared graphs stored in a MongoDB.

    Nodes are assumed to have at least an attribute ``id``. If the have a
    position attribute (set via argument ``position_attribute``, defaults to
    ``position``), it will be used for geometric slicing (see ``__getitem__``).

    Edges are assumed to have at least attributes ``u``, ``v``.

    Arguments:

        db_name (``string``):

            The name of the MongoDB database.

        host (``string``, optional):

            The URL of the MongoDB host.

        mode (``string``, optional):

            One of ``r``, ``r+``, or ``w``. Defaults to ``r+``. ``w`` drops the
            node, edge, and meta collections.

        directed (``bool``):

            True if the graph is directed, false otherwise

        nodes_collection (``string``):
        edges_collection (``string``):
        meta_collection (``string``):

            Names of the nodes, edges. and meta collections, should they differ
            from ``nodes``, ``edges``, and ``meta``.

        position_attribute (``string`` or list of ``string``s, optional):

            The node attribute(s) that contain position information. This will
            be used for slicing subgraphs via ``__getitem__``. If a single
            string, the attribute is assumed to be an array. If a list, each
            entry denotes the position coordinates in order (e.g.,
            `position_z`, `position_y`, `position_x`).
    '''

    def __init__(
            self,
            db_name,
            host=None,
            mode='r+',
            directed=None,
            total_roi=None,
            nodes_collection='nodes',
            edges_collection='edges',
            meta_collection='meta',
            position_attribute='position'):

        self.db_name = db_name
        self.host = host
        self.mode = mode
        self.directed = directed
        self.total_roi = total_roi
        self.nodes_collection_name = nodes_collection
        self.edges_collection_name = edges_collection
        self.meta_collection_name = meta_collection
        self.client = None
        self.database = None
        self.nodes = None
        self.edges = None
        self.meta = None
        self.position_attribute = position_attribute

        try:

            self.__connect()
            self.__open_db()

            if mode == 'w':

                logger.info(
                    "dropping collections %s, %s, and %s",
                    self.nodes_collection_name,
                    self.edges_collection_name,
                    self.meta_collection_name)

                self.__open_collections()
                self.nodes.drop()
                self.edges.drop()
                self.meta.drop()

            collection_names = self.database.list_collection_names()

            if meta_collection in collection_names:
                self.__check_metadata()
            else:
                self.__set_metadata()

            if (
                    nodes_collection not in collection_names or
                    edges_collection not in collection_names):

                self.__create_collections()

        finally:

            self.__disconnect()

    def read_nodes(self, roi):
        '''Return a list of nodes within roi.'''

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
        '''Return the number of nodes in the roi.'''

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            num = self.nodes.count(self.__pos_query(roi))

        finally:

            self.__disconnect()

        return num

    def has_edges(self, roi):
        '''Returns true if there is at least one edge in the roi.'''

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

    def read_edges(self, roi):
        '''Returns a list of edges within roi.'''

        nodes = self.read_nodes(roi)
        node_ids = list([n['id'] for n in nodes])
        logger.debug("found %d nodes", len(node_ids))
        logger.debug("looking for edges with u in %s", node_ids)

        edges = []

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            query_size = 128
            for i in range(0, len(node_ids), query_size):
                edges += list(self.edges.find({
                    'u': {'$in': node_ids[i:i+query_size]}
                }))
            logger.debug("found %d edges", len(edges))
            logger.debug("read edges: %s", edges)

        finally:

            self.__disconnect()

        return edges

    def __getitem__(self, roi):

        # get all nodes within roi
        nodes = self.read_nodes(roi)
        node_list = [
                (n['id'], self.__remove_keys(n, ['id']))
                for n in nodes]
        edges = self.read_edges(roi)
        edge_list = [
                (e['u'], e['v'], self.__remove_keys(e, ['u', 'v']))
                for e in edges]

        if self.directed:
            graph = MongoDbDiSubGraph(
                self.db_name,
                roi,
                self.host,
                self.mode,
                self.nodes_collection_name,
                self.edges_collection_name)
        else:
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
        '''Removes given keys from dictionary.'''

        for key in keys:
            del dictionary[key]
        return dictionary

    def __connect(self):
        '''Connects to Mongo client'''

        self.client = MongoClient(self.host)

    def __open_db(self):
        '''Opens Mongo database'''

        self.database = self.client[self.db_name]

    def __open_collections(self):
        '''Opens the node, edge, and meta collections'''

        self.nodes = self.database[self.nodes_collection_name]
        self.edges = self.database[self.edges_collection_name]
        self.meta = self.database[self.meta_collection_name]

    def __get_metadata(self):
        '''Gets metadata out of the meta collection and returns it
        as a dictionary.'''

        metadata = self.meta.find_one({}, {"_id": False})
        return metadata

    def __disconnect(self):
        '''Closes the mongo client and removes references
        to all collections and databases'''

        self.nodes = None
        self.edges = None
        self.meta = None
        self.database = None
        self.client.close()
        self.client = None

    def __create_collections(self):
        '''Creates the node and edge collections, including indexes'''
        self.__open_db()
        self.__open_collections()

        if type(self.position_attribute) == list:
            self.nodes.create_index(
                [
                    (key, ASCENDING)
                    for key in self.position_attribute
                ],
                name='position')
        else:
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

    def __check_metadata(self):
        '''Checks if the provided metadata matches the existing
        metadata in the meta collection'''

        self.__open_collections()
        metadata = self.__get_metadata()
        if self.directed is not None and metadata['directed'] != self.directed:
            raise ValueError((
                    "Input parameter directed={} does not match"
                    "directed value {} already in stored metadata")
                    .format(self.directed, metadata['directed']))
        if self.total_roi:
            if self.total_roi.get_offset() != metadata['total_roi_offset']:
                raise ValueError((
                    "Input total_roi offset {} does not match"
                    "total_roi offset {} already stored in metadata")
                    .format(
                        self.total_roi.get_offset(),
                        metadata['total_roi_offset']))
            if self.total_roi.get_shape() != metadata['total_roi_shape']:
                raise ValueError((
                    "Input total_roi shape {} does not match"
                    "total_roi shape {} already stored in metadata")
                    .format(
                        self.total_roi.get_shape(),
                        metadata['total_roi_shape']))

    def __set_metadata(self):
        '''Sets the metadata in the meta collection to the provided values'''

        if not self.directed:
            # default is false
            self.directed = False
        if not self.total_roi:
            # default is an unbounded roi
            self.total_roi = Roi((0, 0, 0, 0), (None, None, None, None))

        meta_data = {
                'directed': self.directed,
                'total_roi_offset': self.total_roi.get_offset(),
                'total_roi_shape': self.total_roi.get_shape()
            }

        self.__open_db()
        self.__open_collections()
        self.meta.insert_one(meta_data)

    def __pos_query(self, roi):
        '''Generates a mongo query for position'''

        begin = roi.get_begin()
        end = roi.get_end()

        if type(self.position_attribute) == list:
            return {
                key: {'$gte': b, '$lt': e}
                for key, b, e in zip(self.position_attribute, begin, end)
            }
        else:
            return {
                'position.%d' % d: {'$gte': b, '$lt': e}
                for d, (b, e) in enumerate(zip(begin, end))
            }


class MongoDbSharedSubGraph(SharedSubGraph):

    def __init__(
            self,
            db_name,
            roi,
            host=None,
            mode='r+',
            nodes_collection='nodes',
            edges_collection='edges'):

        super().__init__()

        self.db_name = db_name
        self.roi = roi
        self.host = host
        self.mode = mode

        self.client = MongoClient(self.host)
        self.database = self.client[db_name]
        self.nodes_collection = self.database[nodes_collection]
        self.edges_collection = self.database[edges_collection]

    def write_edges(
            self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        assert not delete, "Delete not implemented"
        assert not(fail_if_exists and fail_if_not_exists),\
            "Cannot have fail_if_exists and fail_if_not_exists simultaneously"

        if self.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing edges in %s", roi)

        edges = []
        for u, v, data in self.edges(data=True):
            if not self.is_directed():
                u, v = min(u, v), max(u, v)
            if not self.__contains(roi, u):
                continue

            edge = {
                'u': int(u),
                'v': int(v),
            }
            if not attributes:
                edge.update(data)
            else:
                for key in data:
                    if key in attributes:
                        edge[key] = data[key]

            edges.append(edge)

        if len(edges) == 0:
            logger.debug("No edges to insert in %s", roi)
            return

        try:
            if fail_if_exists:
                self.edges_collection.insert_many(edges)
            elif fail_if_not_exists:
                result = self.edges_collection.bulk_write([
                    ReplaceOne(
                        {
                            'u': edge['u'],
                            'v': edge['v']
                        },
                        edge,
                        upsert=False
                    )
                    for edge in edges
                ])
                raise WriteError(
                        details="{} nodes did not exist and were not written"
                        .format(len(edges) - result.matched_count))
            else:
                self.edges_collection.bulk_write([
                    ReplaceOne(
                        {
                            'u': edge['u'],
                            'v': edge['v']
                        },
                        edge,
                        upsert=True
                    )
                    for edge in edges
                ])

        except BulkWriteError as e:

            logger.error(e.details)
            raise

    def write_nodes(
            self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False):
        assert not delete, "Delete not implemented"
        assert not(fail_if_exists and fail_if_not_exists),\
            "Cannot have fail_if_exists and fail_if_not_exists simultaneously"

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
            if not attributes:
                node.update(data)
            else:
                for key in data:
                    if key in attributes:
                        node[key] = data[key]
            nodes.append(node)

        if len(nodes) == 0:
            return

        try:
            if fail_if_exists:
                self.nodes_collection.insert_many(nodes)
            elif fail_if_not_exists:
                result = self.nodes_collection.bulk_write([
                    ReplaceOne(
                        {
                            'id': node['id'],
                        },
                        node,
                        upsert=False
                    )
                    for node in nodes
                ])
                if result.matched_count != len(nodes):
                    raise WriteError(
                            details="{} nodes didn't exist and weren't written"
                            .format(len(nodes) - result.matched_count))

            else:
                self.nodes_collection.bulk_write([
                    ReplaceOne(
                        {
                            'id': node['id'],
                        },
                        node,
                        upsert=True
                    )
                    for node in nodes
                ])

        except BulkWriteError as e:

            logger.error(e.details)
            raise

    def __contains(self, roi, node):
        '''Determines if the given node is inside the given roi'''
        node_data = self.node[node]

        # Some nodes are outside of the originally requested ROI (they have
        # been pulled in by edges leaving the ROI). These nodes have no
        # attributes, so we can't perform an inclusion test. However, we
        # know they are outside of the subgraph ROI, and therefore also
        # outside of 'roi', whatever it is.
        if 'position' not in node_data:
            return False

        return roi.contains(Coordinate(node_data['position']))

    def is_directed(self):
        raise RuntimeError("not implemented in %s" % self.name())


class MongoDbSubGraph(MongoDbSharedSubGraph, Graph):
    def __init__(
            self,
            db_name,
            roi,
            host=None,
            mode='r+',
            nodes_collection='nodes',
            edges_collection='edges'):
        # this calls the init function of the MongoDbSharedSubGraph,
        # because left parents come before right parents
        super().__init__(
                db_name,
                roi,
                host=host,
                mode=mode,
                nodes_collection=nodes_collection,
                edges_collection=edges_collection)

    def is_directed(self):
        return False


class MongoDbDiSubGraph(MongoDbSharedSubGraph, DiGraph):
    def __init__(
            self,
            db_name,
            roi,
            host=None,
            mode='r+',
            nodes_collection='nodes',
            edges_collection='edges'):
        # this calls the init function of the MongoDbSharedSubGraph,
        # because left parents come before right parents
        super().__init__(
                db_name,
                roi,
                host=host,
                mode=mode,
                nodes_collection=nodes_collection,
                edges_collection=edges_collection)

    def is_directed(self):
        return True
