from __future__ import absolute_import
from ..coordinate import Coordinate
from ..roi import Roi
from .shared_graph_provider import\
    SharedGraphProvider, SharedSubGraph
from ..graph import Graph, DiGraph
from pymongo import MongoClient, ASCENDING, ReplaceOne
from pymongo.errors import BulkWriteError, WriteError
import logging
import numpy as np
import networkx as nx
import json

logger = logging.getLogger(__name__)


def get_node_attribute_collection(coll_name):
    return 'nodes_' + coll_name


def get_edge_attribute_collection(coll_name):
    return 'edges_' + coll_name


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

            True if the graph is directed, false otherwise. If None, attempts
            to read value from existing database. If not found, defaults to
            false.

        nodes_collection (``string``):
        edges_collection (``string``):
        meta_collection (``string``):

            Names of the nodes, edges. and meta collections, should they differ
            from ``nodes``, ``edges``, and ``meta``.

        endpoint_names (``list`` or ``tuple`` with two elements):

            What keys to use for the start and end of an edge. Default is
            ['u', 'v']

        position_attribute (``string`` or list of ``string``s, optional):

            The node attribute(s) that contain position information. This will
            be used for slicing subgraphs via ``__getitem__``. If a single
            string, the attribute is assumed to be an array. If a list, each
            entry denotes the position coordinates in order (e.g.,
            `position_z`, `position_y`, `position_x`).

        node_attribute_collections (``dict`` from ``string`` to list of
            ``string``s):

            Specifies which node attributes should be stored and read from
            separate collections. Maps from collection name (in practice,
            prepended with 'node_' to prevent name clashes with edges)
            to lists of attributes to be stored in that collection.

        edge_attribute_collections (``dict`` from ``string`` to list of
            ``string``s):

            Specifies which edge attributes should be stored and read from
            separate collections. Maps from collection name (in practice,
            prepended with 'edge_' to prevent name clashes with nodes)
            to lists of attributes to be stored in that collection.
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
            endpoint_names=None,
            meta_collection='meta',
            position_attribute='position',
            node_attribute_collections=None,
            edge_attribute_collections=None):

        self.db_name = db_name
        self.host = host
        self.mode = mode
        self.directed = directed
        self.total_roi = total_roi
        self.nodes_collection_name = nodes_collection
        self.edges_collection_name = edges_collection
        self.endpoint_names = ['u', 'v'] if endpoint_names is None\
            else endpoint_names
        self.meta_collection_name = meta_collection
        self.client = None
        self.database = None
        self.nodes = None
        self.edges = None
        self.meta = None
        self.position_attribute = position_attribute
        self.node_attribute_coll_map = node_attribute_collections\
            if node_attribute_collections else {}
        self.edge_attribute_coll_map = edge_attribute_collections\
            if edge_attribute_collections else {}

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

                if node_attribute_collections:
                    coll_names = [get_node_attribute_collection(coll) for coll
                                  in self.node_attribute_coll_map.keys()]
                    logger.warning("dropping node attribute collections %s"
                                   % coll_names)
                    for name in coll_names:
                        self.database[name].drop()

                if edge_attribute_collections:
                    coll_names = [get_edge_attribute_collection(coll) for coll
                                  in self.edge_attribute_coll_map.keys()]
                    logger.warning("dropping edge attribute collections %s" %
                                   coll_names)
                    for name in coll_names:
                        self.database[name].drop()

            collection_names = self.database.list_collection_names()

            if meta_collection in collection_names:
                metadata = self.__get_metadata()
                if metadata:
                    self.__check_metadata(metadata)
                else:
                    self.__set_metadata()
            else:
                self.__set_metadata()

            if nodes_collection not in collection_names:
                self.__create_node_collection()
            if edges_collection not in collection_names:
                self.__create_edge_collection()

            for coll_name, attributes in self.node_attribute_coll_map.items():
                if coll_name not in collection_names:
                    self.__create_node_attribute_collection(coll_name,
                                                            attributes)
            for coll_name, attributes in self.edge_attribute_coll_map.items():
                if coll_name not in collection_names:
                    self.__create_edge_attribute_collection(coll_name,
                                                            attributes)

        finally:

            self.__disconnect()

    def read_nodes(self, roi, attr_filter=None):
        '''Return a list of nodes within roi.
        Arguments:

            roi (``daisy.Roi``):

                Get nodes that fall within this roi

            attr_filter (``dict``):

                Only return nodes that have attribute=value for
                each attribute value pair in attr_filter.
        '''

        logger.debug("Querying nodes in %s", roi)
        if attr_filter is None:
            attr_filter = {}

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()
            pos_query = self.__pos_query(roi)
            if not self.node_attribute_coll_map:
                query_list = [pos_query]
                for attr, value in attr_filter.items():
                    query_list.append({attr: value})
                nodes = self.nodes.find({'$and': query_list}, {'_id': False})
            else:
                agg_pipeline = []
                agg_pipeline.append({'$match': self.__pos_query(roi)})
                for node_attr_coll, attrs in\
                        self.node_attribute_coll_map.items():
                    coll_name = get_node_attribute_collection(node_attr_coll)
                    join_query = self.__join_node_collections_query(
                            coll_name, attrs)
                    agg_pipeline += join_query
                if attr_filter:
                    query_list = []
                    for attr, value in attr_filter.items():
                        query_list.append({attr: value})
                    filter_query = {'$match': {'$and': query_list}}
                    agg_pipeline.append(filter_query)
                logger.debug("Final query: %s" % agg_pipeline)
                nodes = self.nodes.aggregate(agg_pipeline)
            nodes = list(nodes)

        finally:

            self.__disconnect()

        for node in nodes:
            node['id'] = np.uint64(node['id'])

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
                    self.endpoint_names[0]: int(np.int64(node['id']))
                })

        finally:

            self.__disconnect()

        return edges.count() > 0

    def read_edges(self, roi, nodes=None, attr_filter=None):
        '''Returns a list of edges within roi.
        Arguments:

            roi (``daisy.Roi``):

                Get nodes that fall within this roi

            nodes (``dict``):

                Return edges with sources in this nodes list. If none,
                reads nodes in roi using read_nodes. Dictionary format
                is string attribute -> value, including 'id' as an attribute.

            attr_filter (``dict``):

                Only return nodes that have attribute=value for
                each attribute value pair in attr_filter.

        '''

        if nodes is None:
            nodes = self.read_nodes(roi)
        node_ids = list([int(np.int64(n['id'])) for n in nodes])
        logger.debug("found %d nodes", len(node_ids))
        logger.debug("looking for edges with u in %s", node_ids[:100])

        edges = []
        if attr_filter is None:
            attr_filter = {}

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            # limit query to 1M node IDs (otherwise we might exceed the 16MB
            # BSON document size limit)
            length = len(node_ids)
            query_size = 1000000
            num_chunks = (length - 1)//query_size + 1
            filters = []
            for attr, value in attr_filter.items():
                filters.append({attr: value})
            for i in range(num_chunks):

                i_b = i*query_size
                i_e = min((i + 1)*query_size, len(node_ids))
                assert i_b < len(node_ids)
                endpoint_query = {self.endpoint_names[0]:
                                  {'$in': node_ids[i_b:i_e]}}
                if not self.edge_attribute_coll_map:
                    if attr_filter:
                        filters.append(endpoint_query)
                        query = {'$and': filters}
                    else:
                        query = endpoint_query
                    edges += self.edges.find(query)
                else:
                    agg_pipeline = []
                    agg_pipeline.append({'$match': endpoint_query})
                    for edge_attr_coll, attrs in\
                            self.edge_attribute_coll_map.items():
                        coll_name = get_edge_attribute_collection(
                                edge_attr_coll)
                        join_query = self.__join_edge_collections_query(
                                coll_name, attrs)
                        agg_pipeline += join_query
                    if attr_filter:
                        agg_pipeline.append({"$match": {"$and": filters}})
                    logger.debug("Final query: %s"
                                 % json.dumps(agg_pipeline, indent=2))
                    edges += self.edges.aggregate(agg_pipeline)
            if num_chunks > 0:
                assert i_e == len(node_ids)

            logger.debug("found %d edges", len(edges))
            logger.debug("read edges: %s", edges[:100])

        finally:

            self.__disconnect()

        for edge in edges:
            u, v = self.endpoint_names
            edge[u] = np.uint64(edge[u])
            edge[v] = np.uint64(edge[v])

        return edges

    def __getitem__(self, roi):

        return self.get_graph(roi)

    def get_graph(self, roi, nodes_filter=None, edges_filter=None):
        ''' Return a graph within roi, optionally filtering by
        node and edge attributes.

        Arguments:

            roi (``daisy.Roi``):

                Get nodes and edges whose source is within this roi

            nodes_filter (``dict``):
            edges_filter (``dict``):

                Only return nodes/edges that have attribute=value for
                each attribute value pair in nodes/edges_filter.


        '''
        nodes = self.read_nodes(roi, attr_filter=nodes_filter)
        edges = self.read_edges(roi, nodes=nodes, attr_filter=edges_filter)
        u, v = self.endpoint_names
        node_list = [
                (n['id'], self.__remove_keys(n, ['id']))
                for n in nodes]
        edge_list = [
                (e[u], e[v], self.__remove_keys(e, [u, v]))
                for e in edges]
        if self.directed:
            graph = MongoDbSubDiGraph(
                    self,
                    roi)
        else:
            # create the subgraph
            graph = MongoDbSubGraph(
                    self,
                    roi)
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
        self.__open_collections()
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

    def __create_node_collection(self):
        '''Creates the node collection, including indexes'''
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

    def __create_edge_collection(self):
        '''Creates the edge collection, including indexes'''
        self.__open_db()
        self.__open_collections()

        u, v = self.endpoint_names
        self.edges.create_index(
            [
                (u, ASCENDING),
                (v, ASCENDING)
            ],
            name='incident',
            unique=True)

    def __create_node_attribute_collection(self, coll_name, attributes):
        '''Creates the node attribute collection, including indexes'''
        self.__open_db()
        coll_name = get_node_attribute_collection(coll_name)
        coll = self.database[coll_name]

        coll.create_index(
            [
                ('id', ASCENDING)
            ],
            name='id',
            unique=True)

    def __create_edge_attribute_collection(self, coll_name, attributes):
        '''Creates the edge collection, including indexes'''
        self.__open_db()
        coll_name = get_edge_attribute_collection(coll_name)
        coll = self.database[coll_name]

        u, v = self.endpoint_names
        coll.create_index(
            [
                (u, ASCENDING),
                (v, ASCENDING)
            ],
            name='incident',
            unique=True)

    def __check_metadata(self, metadata):
        '''Checks if the provided metadata matches the existing
        metadata in the meta collection'''

        if self.directed is None:
            assert metadata['directed'] is not None,\
                "Meta collection exists but does not contain "\
                "directed information"
            self.directed = metadata['directed']
        elif metadata['directed'] != self.directed:
            raise ValueError((
                    "Input parameter directed={} does not match"
                    "directed value {} already in stored metadata")
                    .format(self.directed, metadata['directed']))
        if self.total_roi is None:
            if 'total_roi_offset' in metadata\
                    and 'total_roi_shape' in metadata:
                offset = metadata['total_roi_offset']
                shape = metadata['total_roi_shape']
                self.total_roi = Roi(offset, shape)
        else:
            offset = self.total_roi.get_offset()
            if list(offset) != metadata['total_roi_offset']:
                raise ValueError((
                    "Input total_roi offset {} does not match"
                    "total_roi offset {} already stored in metadata")
                    .format(
                        self.total_roi.get_offset(),
                        metadata['total_roi_offset']))
            if list(self.total_roi.get_shape()) != metadata['total_roi_shape']:
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
        meta_data = {'directed': self.directed}

        # if total_roi not specified, don't write it
        if self.total_roi:
            meta_data['total_roi_offset'] = self.total_roi.get_offset()
            meta_data['total_roi_shape'] = self.total_roi.get_shape()
        self.__open_collections()
        # It's possible that another worker has already inserted the metadata -
        # upsert to keep only one document in the collection
        self.meta.replace_one(meta_data, meta_data, upsert=True)

    def __pos_query(self, roi):
        '''Generates a mongo query for position'''

        begin = roi.get_begin()
        end = roi.get_end()

        if type(self.position_attribute) == list:
            assert len(self.position_attribute) == roi.dims(), (
                'Number of position attributes does not match number of '
                'dimensions')

            return {
                key: {'$gte': b, '$lt': e}
                for key, b, e in zip(self.position_attribute, begin, end)
            }
        else:
            return {
                'position.%d' % d: {'$gte': b, '$lt': e}
                for d, (b, e) in enumerate(zip(begin, end))
            }

    def __join_edge_collections_query(self, collection, attributes):
        u = self.endpoint_names[0]
        v = self.endpoint_names[1]
        lookup = {
            '$lookup':
                {'from': collection,
                 'let': {u: '$' + u, v: '$' + v},
                 'pipeline': [
                         {'$match': {'$expr': {'$and': [
                             {'$eq': ['$' + u, '$$' + u]},
                             {'$eq': ['$' + v, '$$' + v]}
                             ]}}}
                         ],
                 'as': collection}
        }
        fields_to_add = {attr: {'$arrayElemAt':
                                ['$' + collection + '.' + attr, 0]}
                         for attr in attributes}
        add_fields = {'$addFields': fields_to_add}
        project = {'$project': {collection: 0}}

        aggregate_list = [lookup, add_fields, project]
        logger.debug(aggregate_list)
        return aggregate_list

    def __join_node_collections_query(self, collection, attributes):
        lookup = {
            '$lookup':
                {'from': collection,
                 'localField': 'id',
                 'foreignField': 'id',
                 'as': collection}
        }
        fields_to_add = {attr: {'$arrayElemAt':
                                ['$' + collection + '.' + attr, 0]}
                         for attr in attributes}
        add_fields = {'$addFields': fields_to_add}
        project = {'$project': {collection: 0}}

        aggregate_list = [lookup, add_fields, project]
        logger.debug(json.dumps(aggregate_list, indent=2))
        return aggregate_list


class MongoDbSharedSubGraph(SharedSubGraph):

    def __init__(
            self,
            graph_provider,
            roi):

        super().__init__()

        self.provider = graph_provider
        self.roi = roi

        self.client = MongoClient(self.provider.host)
        self.database = self.client[self.provider.db_name]
        self.nodes_collection = self.database[
                self.provider.nodes_collection_name]
        self.edges_collection = self.database[
                self.provider.edges_collection_name]

    def write_nodes(
            self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False,
            separate_only=False):
        '''If separate_only, only write attributes in separate collections'''
        assert not delete, "Delete not implemented"
        assert not(fail_if_exists and fail_if_not_exists),\
            "Cannot have fail_if_exists and fail_if_not_exists simultaneously"

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing nodes")

        nodes = []
        collection_to_attrs = {}
        attribute_collections = self.provider.node_attribute_coll_map.keys()
        if attribute_collections is None:
            attribute_collections = []
        for coll in attribute_collections:
            collection_to_attrs[coll] = []

        for node_id, data in self.nodes(data=True):

            if not self.__contains(roi, node_id):
                logger.debug(
                        "Skipping node {} with data {} because not in roi {}"
                        .format(node_id, data, roi))
                continue

            node = {
                'id': int(np.int64(node_id))
            }
            # Create a separate document containing specified attributes
            # for each attribute collection
            for coll in attribute_collections:
                attrs = self.provider.node_attribute_coll_map[coll]
                doc_to_insert = node.copy()
                for attr in attrs:
                    if not attributes or attr in attributes:
                        if attr in data:
                            doc_to_insert[attr] = data[attr]
                            del data[attr]
                if len(doc_to_insert) > len(node):
                    collection_to_attrs[coll].append(doc_to_insert)

            # Add remaining attributes to main collection
            if not attributes:
                node.update(data)
            else:
                for key in data:
                    if key in attributes:
                        node[key] = data[key]
            nodes.append(node)

        if len(nodes) == 0:
            return
        if not separate_only:
            try:
                self.__write(self.nodes_collection, ['id'], nodes,
                             fail_if_exists=fail_if_exists,
                             fail_if_not_exists=fail_if_not_exists,
                             delete=delete)
            except BulkWriteError as e:
                logger.error(e.details)
                raise

        for coll, attrs in collection_to_attrs.items():
            try:
                collection = self.database[get_node_attribute_collection(coll)]
                # Don't pass flags to attribute collections - just upsert
                self.__write(collection, ['id'], attrs)
            except BulkWriteError as e:
                logger.error(e.details)
                raise

    def write_edges(
            self,
            roi=None,
            attributes=None,
            fail_if_exists=False,
            fail_if_not_exists=False,
            delete=False,
            separate_only=False):
        '''If separate_only, only write attributes in separate collections'''
        assert not delete, "Delete not implemented"
        assert not(fail_if_exists and fail_if_not_exists),\
            "Cannot have fail_if_exists and fail_if_not_exists simultaneously"

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing edges in %s", roi)

        edges = []
        collection_to_attrs = {}
        attribute_collections = self.provider.edge_attribute_coll_map.keys()
        if attribute_collections is None:
            attribute_collections = []
        for coll in attribute_collections:
            collection_to_attrs[coll] = []

        u_name, v_name = self.provider.endpoint_names
        for u, v, data in self.edges(data=True):
            if not self.is_directed():
                u, v = min(u, v), max(u, v)
            if not self.__contains(roi, u):
                logger.debug(
                        ("Skipping edge with u {}, v {}," +
                         "and data {} because u not in roi {}")
                        .format(u, v, data, roi))
                continue

            edge = {
                u_name: int(np.int64(u)),
                v_name: int(np.int64(v)),
            }

            # Create a separate document containing specified attributes
            # for each attribute collection
            for coll in attribute_collections:
                attrs = self.provider.edge_attribute_coll_map[coll]
                doc_to_insert = edge.copy()
                for attr in attrs:
                    if not attributes or attr in attributes:
                        if attr in data:
                            doc_to_insert[attr] = data[attr]
                            del data[attr]
                if len(doc_to_insert) > len(edge):
                    collection_to_attrs[coll].append(doc_to_insert)

            # Add remaining attributes to main collection
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

        if not separate_only:
            try:
                self.__write(self.edges_collection, [u_name, v_name], edges,
                             fail_if_exists=fail_if_exists,
                             fail_if_not_exists=fail_if_not_exists,
                             delete=delete)
            except BulkWriteError as e:
                logger.error(e.details)
                raise

        for coll, attrs in collection_to_attrs.items():
            try:
                collection = self.database[get_edge_attribute_collection(coll)]
                # Don't pass flags to attribute collections - just upsert
                self.__write(collection, [u_name, v_name], attrs)
            except BulkWriteError as e:
                logger.error(e.details)
                raise

    def get_connected_components(self):
        '''Returns a list of connected components as networkx (di)graphs'''
        subgraphs = []
        if self.is_directed():
            node_set_generator = nx.weakly_connected_components(self)
        else:
            node_set_generator = nx.connected_components(self)

        for node_set in node_set_generator:
            edge_set = self.edges(node_set, data=True)
            if self.is_directed():
                g = nx.DiGraph()
            else:
                g = nx.Graph()

            g.add_nodes_from([(node, self.nodes[node]) for node in node_set])
            g.add_edges_from(edge_set)
            subgraphs.append(g)

        return subgraphs

    def __write(self, collection, match_fields, docs,
                fail_if_exists=False, fail_if_not_exists=False, delete=False):
        '''Writes documents to provided mongo collection, checking for restricitons.

        Args:

            collection (``pymongo.collection``):

                The collection to write the documents into.

            match_fields (``list`` of ``string``):

                The set of fields to match to be considered the same document.

            docs (``dict`` or ``bson``):

                The documents to insert into the collection

            fail_if_exists, fail_if_not_exists, delete (``bool``):

                see write_nodes or write_edges for explanations of these flags
            '''
        assert not delete, "Delete not implemented"
        match_docs = []
        for doc in docs:
            match_doc = {}
            for field in match_fields:
                match_doc[field] = doc[field]
            match_docs.append(match_doc)

        if fail_if_exists:
            self.__write_fail_if_exists(collection, match_docs, docs)
        elif fail_if_not_exists:
            self.__write_fail_if_not_exists(collection, match_docs, docs)
        else:
            self.__write_no_flags(collection, match_docs, docs)

    def __write_no_flags(self, collection, old_docs, new_docs):
        bulk_query = [ReplaceOne(old, new, upsert=True)
                      for old, new in zip(old_docs, new_docs)]
        collection.bulk_write(bulk_query)

    def __write_fail_if_exists(self, collection, old_docs, new_docs):
        for old in old_docs:
            if collection.find(old):
                raise WriteError(
                        "Found existing doc %s and fail_if_exists set to True."
                        " Aborting write for all docs." % old)
        collection.insert_many(new_docs)

    def __write_fail_if_not_exists(self, collection, old_docs, new_docs):
        for old in old_docs:
            if not collection.find(old):
                raise WriteError(
                        "Did not find existing doc %s and fail_if_not_exists "
                        "set to True. Aborting write for all docs." % old)
        bulk_query = [ReplaceOne(old, new, upsert=False)
                      for old, new in zip(old_docs, new_docs)]
        result = collection.bulk_write(bulk_query)
        assert len(new_docs) == result.matched_count,\
            ("Supposed to replace %s docs, but only replaced %s"
                % (len(new_docs), result.matched_count))

    def __contains(self, roi, node):
        '''Determines if the given node is inside the given roi'''
        node_data = self.node[node]

        # Some nodes are outside of the originally requested ROI (they have
        # been pulled in by edges leaving the ROI). These nodes have no
        # attributes, so we can't perform an inclusion test. However, we
        # know they are outside of the subgraph ROI, and therefore also
        # outside of 'roi', whatever it is.
        coordinate = []
        if type(self.provider.position_attribute) == list:
            for pos_attr in self.provider.position_attribute:
                if pos_attr not in node_data:
                    return False
                coordinate.append(node_data[pos_attr])
        else:
            if self.provider.position_attribute not in node_data:
                return False
            coordinate = node_data[self.provider.position_attribute]
        logger.debug("Checking if coordinate {} is inside roi {}"
                     .format(coordinate, roi))
        return roi.contains(Coordinate(coordinate))

    def is_directed(self):
        raise RuntimeError("not implemented in %s" % self.name())


class MongoDbSubGraph(MongoDbSharedSubGraph, Graph):
    def __init__(
            self,
            graph_provider,
            roi):
        # this calls the init function of the MongoDbSharedSubGraph,
        # because left parents come before right parents
        super().__init__(
                graph_provider,
                roi)

    def is_directed(self):
        return False


class MongoDbSubDiGraph(MongoDbSharedSubGraph, DiGraph):
    def __init__(
            self,
            graph_provider,
            roi):
        # this calls the init function of the MongoDbSharedSubGraph,
        # because left parents come before right parents
        super().__init__(
                graph_provider,
                roi)

    def is_directed(self):
        return True
