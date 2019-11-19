from __future__ import absolute_import
from ..coordinate import Coordinate
from ..roi import Roi
from .shared_graph_provider import\
    SharedGraphProvider, SharedSubGraph
from ..graph import Graph, DiGraph
from pymongo import MongoClient, ASCENDING, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError, WriteError
import logging
import numpy as np
import networkx as nx

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
            position_attribute='position'):

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

        try:

            self.__connect()

            if mode != 'w':
                if self.db_name not in self.client.list_database_names():
                    logger.warn("Opened with read mode %s, but no db with name"
                                "%s found in client at %s"
                                % (mode, self.db_name, self.host))
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

        except Exception as e:

            self.__disconnect()
            raise e

    def __del__(self):

        self.__disconnect()

    def read_nodes(self, roi, attr_filter=None, read_attrs=None):
        '''Return a list of nodes within roi.
        Arguments:

            roi (``daisy.Roi``):

                Get nodes that fall within this roi

            attr_filter (``dict``):

                Only return nodes that have attribute=value for
                each attribute value pair in attr_filter.

            read_attrs (``list`` of ``string``):

                Attributes to return. Others will be ignored
        '''

        logger.debug("Querying nodes in %s", roi)
        if attr_filter is None:
            attr_filter = {}

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()
            pos_query = self.__pos_query(roi)
            query_list = [pos_query]
            for attr, value in attr_filter.items():
                query_list.append({attr: value})
            projection = {'_id': False}
            if read_attrs is not None:
                projection['id'] = True
                if type(self.position_attribute) == list:
                    for a in self.position_attribute:
                        projection[a] = True
                else:
                    projection[self.position_attribute] = True
                for attr in read_attrs:
                    projection[attr] = True
            nodes = self.nodes.find({'$and': query_list}, projection)
            nodes = list(nodes)

        except Exception as e:

            self.__disconnect()
            raise e

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

        except Exception as e:

            self.__disconnect()
            raise e

        return num

    def has_edges(self, roi):
        '''Returns true if there is at least one edge in the roi.'''

        try:

            self.__connect()
            self.__open_db()
            self.__open_collections()

            nodes = list(self.nodes.find(self.__pos_query(roi)))

            # no nodes -> no edges
            if len(nodes) == 0:
                return False

            node_ids = list([int(np.int64(n['id'])) for n in nodes])

            # limit query to 1M node IDs (otherwise we might exceed the 16MB
            # BSON document size limit)
            length = len(node_ids)
            query_size = 1000000
            num_chunks = (length - 1)//query_size + 1

            for i in range(num_chunks):

                i_b = i*query_size
                i_e = min((i + 1)*query_size, len(node_ids))
                assert i_b < len(node_ids)
                query = {self.endpoint_names[0]:
                         {'$in': node_ids[i_b:i_e]}}
                if self.edges.find_one(query) is not None:
                    return True

            if num_chunks > 0:
                assert i_e == len(node_ids)

        except Exception as e:

            self.__disconnect()
            raise e

        return False

    def read_edges(
        self,
        roi,
        nodes=None,
        attr_filter=None,
        read_attrs=None,
        edge_inclusion='u'
    ):
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

            read_attrs (``list`` of ``string``):

                Attributes to return. Others will be ignored

            edge_inclusion ('u', 'v', 'either', or 'both'):

                Include edges with only 'u' node inside roi,
                only 'v' node inside roi, both ends inside roi,
                or either end inside roi. Default is 'u'.

        '''

        if nodes is None:
            nodes = self.read_nodes(roi)
        node_ids = list([int(np.int64(n['id'])) for n in nodes])
        logger.debug("found %d nodes", len(node_ids))
        logger.debug("looking for edges with u in %s", node_ids[:100])

        u, v = self.endpoint_names
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
            query_size = 500000
            num_chunks = (length - 1)//query_size + 1
            filters = []
            for attr, value in attr_filter.items():
                filters.append({attr: value})

            projection = {'_id': False}
            if read_attrs is not None:
                projection[u] = True
                projection[v] = True
                for attr in read_attrs:
                    projection[attr] = True

            for i in range(num_chunks):

                i_b = i*query_size
                i_e = min((i + 1)*query_size, len(node_ids))
                assert i_b < len(node_ids)
                if edge_inclusion == 'u':
                    endpoint_query = {u: {'$in': node_ids[i_b:i_e]}}
                elif edge_inclusion == 'v':
                    endpoint_query = {v: {'$in': node_ids[i_b:i_e]}}
                elif edge_inclusion == 'both':
                    raise NotImplementedError(
                        "Both option for read edges not implemented")
                elif edge_inclusion == 'either':
                    endpoint_query = {
                        "$or": [
                            {u: {"$in": node_ids[i_b:i_e]}},
                            {v: {"$in": node_ids[i_b:i_e]}},
                        ]
                    }
                if attr_filter:
                    query = {'$and': filters + [endpoint_query]}
                else:
                    query = endpoint_query
                edges += self.edges.find(query, projection)

            if num_chunks > 0:
                assert i_e == len(node_ids)

            logger.debug("found %d edges", len(edges))
            logger.debug("first 100 edges read: %s", edges[:100])

            for edge in edges:
                edge[u] = np.uint64(edge[u])
                edge[v] = np.uint64(edge[v])

        except Exception as e:

            self.__disconnect()
            raise e

        return edges

    def __getitem__(self, roi):

        return self.get_graph(roi)

    def get_graph(
            self,
            roi,
            nodes_filter=None,
            edges_filter=None,
            node_attrs=None,
            edge_attrs=None,
            node_inclusion='strict',
            edge_inclusion='u'):
        ''' Return a graph within roi, optionally filtering by
        node and edge attributes.

        Arguments:

            roi (``daisy.Roi``):

                Get nodes and edges whose source is within this roi

            nodes_filter (``dict``):
            edges_filter (``dict``):

                Only return nodes/edges that have attribute=value for
                each attribute value pair in nodes/edges_filter.

            node_attrs (``list`` of ``string``):

                Only return these attributes for nodes. Other
                attributes will be ignored, but id and position attribute(s)
                will always be included. If None (default), return all attrs.

            edge_attrs (``list`` of ``string``):

                Only return these attributes for edges. Other
                attributes will be ignored, but source and target
                will always be included. If None (default), return all attrs.

            node_inclusion ('strict' or 'dangling'):

                If 'strict,' only include dummy nodes (aka ids) for nodes
                outside the roi, that are included by being the other end
                of an edge. If 'dangling,' include attributes for the dangling
                nodes outside the roi. Default is 'strict'.

            edge_inclusion ('u', 'v', 'either', or 'both'):

                Include edges with only 'u' node inside roi,
                only 'v' node inside roi, both ends inside roi,
                or either end inside roi. Default is 'u'.

        '''
        nodes = self.read_nodes(
                roi,
                attr_filter=nodes_filter,
                read_attrs=node_attrs)
        edges = self.read_edges(
                roi,
                nodes=nodes,
                attr_filter=edges_filter,
                read_attrs=edge_attrs,
                edge_inclusion=edge_inclusion)
        if node_inclusion == 'dangling':
            try:
                self.__connect()
                self.__open_db()
                self.__open_collections()
                projection = {'_id': False}
                if node_attrs is not None:
                    projection['id'] = True
                    if type(self.position_attribute) == list:
                        for a in self.position_attribute:
                            projection[a] = True
                    else:
                        projection[self.position_attribute] = True
                    for attr in node_attrs:
                        projection[attr] = True

                node_ids = set([int(np.int64(n['id'])) for n in nodes])
                u, v = self.endpoint_names
                to_fetch = []
                for edge in edges:
                    edge[u] = np.uint64(edge[u])
                    edge[v] = np.uint64(edge[v])
                    if edge[u] not in node_ids:
                        to_fetch.append(int(np.int64(edge[u])))
                    elif edge[v] not in node_ids:
                        to_fetch.append(int(np.int64(edge[v])))

                additional_nodes = list(
                    self.nodes.find({"id": {"$in": to_fetch}}, projection))
                nodes += additional_nodes

            finally:
                self.__disconnect()

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

        if not self.client:
            self.client = MongoClient(self.host)

    def __open_db(self):
        '''Opens Mongo database'''

        if not self.database:
            self.database = self.client[self.db_name]

    def __open_collections(self):
        '''Opens the node, edge, and meta collections'''

        if not self.nodes:
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
        if self.client:
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
                (v, ASCENDING)
            ],
            name='target')
        self.edges.create_index(
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
                key: {
                    k: v
                    for k, v in zip(
                        ["$gte", "$lt"],
                        [
                            b if b is not None else float("-inf"),
                            e if e is not None else float("inf"),
                        ],
                    )
                }
                for key, b, e in zip(self.position_attribute, begin, end)
            }
        else:
            return {
                "position.%d"
                % d: {
                    k: v
                    for k, v in zip(
                        ["$gte", "$lt"],
                        [
                            b if b is not None else float("-inf"),
                            e if e is not None else float("inf"),
                        ],
                    )
                }
                for d, (b, e) in enumerate(zip(begin, end))
            }


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
            delete=False):
        assert not delete, "Delete not implemented"
        assert not(fail_if_exists and fail_if_not_exists),\
            "Cannot have fail_if_exists and fail_if_not_exists simultaneously"

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing nodes")

        nodes = []

        for node_id, data in self.nodes(data=True):

            if not self.__contains(roi, node_id):
                logger.debug(
                        "Skipping node {} with data {} because not in roi {}"
                        .format(node_id, data, roi))
                continue

            node = {
                'id': int(np.int64(node_id))
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
            self.__write(self.nodes_collection, ['id'], nodes,
                         fail_if_exists=fail_if_exists,
                         fail_if_not_exists=fail_if_not_exists,
                         delete=delete)
        except BulkWriteError as e:
            logger.error(e.details)
            raise

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

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Writing edges in %s", roi)

        edges = []

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
            self.__write(self.edges_collection, [u_name, v_name], edges,
                         fail_if_exists=fail_if_exists,
                         fail_if_not_exists=fail_if_not_exists,
                         delete=delete)
        except BulkWriteError as e:
            logger.error(e.details)
            raise

    def update_node_attrs(
            self,
            roi=None,
            attributes=None):

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Updating node attributes")

        updates = []

        for node_id, data in self.nodes(data=True):

            if not self.__contains(roi, node_id):
                logger.debug(
                        "Skipping node {} with data {} because not in roi {}"
                        .format(node_id, data, roi))
                continue

            _filter = {
                'id': int(np.int64(node_id))
            }

            if not attributes:
                update = {'$set': data}
            else:
                update = {}
                for key in data:
                    if key in attributes:
                        update[key] = data[key]
                if not update:
                    logger.info("Skipping node %s with data %s"
                                " - no attributes to update"
                                % (node_id, data))
                    continue
                update = {'$set': update}

            updates.append(UpdateOne(_filter, update))

        if len(updates) == 0:
            return

        try:
            self.nodes_collection.bulk_write(updates, ordered=False)
        except BulkWriteError as e:
            logger.error(e.details)
            raise

    def update_edge_attrs(
            self,
            roi=None,
            attributes=None):

        if self.provider.mode == 'r':
            raise RuntimeError("Trying to write to read-only DB")

        if roi is None:
            roi = self.roi

        logger.debug("Updating edge attributes")

        updates = []

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

            _filter = {
                u_name: int(np.int64(u)),
                v_name: int(np.int64(v)),
            }

            if not attributes:
                update = {'$set': data}
            else:
                update = {}
                for key in data:
                    if key in attributes:
                        update[key] = data[key]
                if not update:
                    logger.info("Skipping edge %s -> %s with data %s"
                                "- no attributes to update"
                                % (u, v, data))
                    continue
                update = {'$set': update}

            updates.append(UpdateOne(_filter, update))

        if len(updates) == 0:
            logger.info("No updates in roi %s" % roi)
            return
        try:
            self.edges_collection.bulk_write(updates, ordered=False)
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
        collection.bulk_write(bulk_query, ordered=False)

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
        result = collection.bulk_write(bulk_query, ordered=False)
        assert len(new_docs) == result.matched_count,\
            ("Supposed to replace %s docs, but only replaced %s"
                % (len(new_docs), result.matched_count))

    def __contains(self, roi, node):
        '''Determines if the given node is inside the given roi'''
        node_data = self.nodes[node]

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
