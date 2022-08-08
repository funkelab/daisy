from .conftest import MONGO_AVAILABLE

import daisy

import pymongo
import numpy as np
import pytest

import logging
import random

logger = logging.getLogger(__name__)
# daisy.scheduler._NO_SPAWN_STATUS_THREAD = True


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_io(provider_factory):

    graph_provider = provider_factory("w")

    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    graph.write_nodes()
    graph.write_edges()

    graph_provider = provider_factory("r")
    compare_graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    nodes = sorted(list(graph.nodes()))
    nodes.remove(2)  # node 2 has no position and will not be queried
    compare_nodes = sorted(list(compare_graph.nodes()))

    edges = sorted(list(graph.edges()))
    edges.remove((2, 42))  # node 2 has no position and will not be queried
    compare_edges = sorted(list(compare_graph.edges()))

    assert nodes == compare_nodes
    assert edges == compare_edges


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_fail_if_exists(provider_factory):

    graph_provider = provider_factory("w")
    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    graph.write_nodes()
    graph.write_edges()
    with pytest.raises(Exception):
        graph.write_nodes(fail_if_exists=True)
    with pytest.raises(Exception):
        graph.write_edges(fail_if_exists=True)


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_fail_if_not_exists(provider_factory):

    graph_provider = provider_factory("w")
    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    with pytest.raises(Exception):
        graph.write_nodes(fail_if_not_exists=True)
    with pytest.raises(Exception):
        graph.write_edges(fail_if_not_exists=True)


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_write_attributes(provider_factory):

    graph_provider = provider_factory("w")
    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    graph.write_nodes(attributes=["position", "swip"])
    graph.write_edges()

    graph_provider = provider_factory("r")
    compare_graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    nodes = []
    for node, data in graph.nodes(data=True):
        if node == 2:
            continue
        if "zap" in data:
            del data["zap"]
        data["position"] = list(data["position"])
        nodes.append((node, data))

    compare_nodes = compare_graph.nodes(data=True)
    compare_nodes = [
        (node_id, data) for node_id, data in compare_nodes if len(data) > 0
    ]
    assert nodes == compare_nodes


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_write_roi(provider_factory):

    graph_provider = provider_factory("w")
    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    write_roi = daisy.Roi((0, 0, 0), (6, 6, 6))
    graph.write_nodes(roi=write_roi)
    graph.write_edges(roi=write_roi)

    graph_provider = provider_factory("r")
    compare_graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    nodes = sorted(list(graph.nodes()))
    nodes.remove(2)  # node 2 has no position and will not be queried
    nodes.remove(57)  # node 57 is outside of the write_roi
    compare_nodes = compare_graph.nodes(data=True)
    compare_nodes = [node_id for node_id, data in compare_nodes if len(data) > 0]
    compare_nodes = sorted(list(compare_nodes))
    edges = sorted(list(graph.edges()))
    edges.remove((2, 42))  # node 2 has no position and will not be queried
    compare_edges = sorted(list(compare_graph.edges()))

    assert nodes == compare_nodes
    assert edges == compare_edges


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_connected_components(provider_factory):

    graph_provider = provider_factory("w")
    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(57, 23)
    graph.add_edge(2, 42)

    components = graph.get_connected_components()
    assert len(components) == 2
    c1, c2 = components
    n1 = sorted(list(c1.nodes()))
    n2 = sorted(list(c2.nodes()))

    compare_n1 = [2, 42]
    compare_n2 = [23, 57]

    if 2 in n2:
        temp = n2
        n2 = n1
        n1 = temp

    assert n1 == compare_n1
    assert n2 == compare_n2


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_read_blockwise(provider_factory):
    # I think its the multiprocessing of read_blockwise
    # Seems to hang forever

    graph_provider = provider_factory("w")

    graph = graph_provider[daisy.Roi((0, 0, 0), (10, 10, 10))]

    nodes_to_write = list(range(1000))
    nodes_to_write += [np.uint64(-10), np.uint64(-1)]
    num_edges = 10000

    random.seed(42)

    for n in nodes_to_write:
        graph.add_node(
            n,
            position=(random.randint(0, 9), random.randint(0, 9), random.randint(0, 9)),
        )
    for i in range(num_edges):
        ui = random.randint(0, len(nodes_to_write) - 1)
        vi = random.randint(0, len(nodes_to_write) - 1)
        u = nodes_to_write[ui]
        v = nodes_to_write[vi]
        graph.add_edge(u, v, score=random.random())

    graph.write_nodes()
    graph.write_edges()

    nodes, edges = graph_provider.read_blockwise(
        # read in larger ROI to test handling of empty blocks
        daisy.Roi((0, 0, 0), (20, 10, 10)),
        daisy.Coordinate((1, 1, 1)),
        num_workers=40,
    )

    assert nodes["id"].dtype == np.uint64
    assert edges["u"].dtype == np.uint64
    assert edges["v"].dtype == np.uint64

    written_nodes = list(nodes_to_write)
    read_nodes = sorted(nodes["id"])

    assert written_nodes == read_nodes

    for u, v, score in zip(edges["u"], edges["v"], edges["score"]):
        assert graph == edges[(u, v)]["score"], score


@pytest.mark.parametrize(
    "storage",
    [
        "file",
        pytest.param(
            "mongodb",
            marks=pytest.mark.skipif(
                not MONGO_AVAILABLE, reason="Mongodb not available"
            ),
        ),
    ],
)
def test_graph_has_edge(provider_factory):

    graph_provider = provider_factory("w")

    roi = daisy.Roi((0, 0, 0), (10, 10, 10))
    graph = graph_provider[roi]

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip="swap")
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap="zip")
    graph.add_edge(42, 23)
    graph.add_edge(57, 23)

    write_roi = daisy.Roi((0, 0, 0), (6, 6, 6))
    graph.write_nodes(roi=write_roi)
    graph.write_edges(roi=write_roi)

    assert graph_provider.has_edges(roi)
