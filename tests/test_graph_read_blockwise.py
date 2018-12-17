import daisy
import logging
import random
import time
import numpy as np

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    graph_provider = daisy.persistence.MongoDbGraphProvider(
        'test_daisy_graph',
        '10.40.4.51',
        nodes_collection='nodes',
        edges_collection='edges',
        mode='w')

    graph = graph_provider[
        daisy.Roi(
            (0, 0, 0),
            (10, 10, 10))
    ]

    num_nodes = 100000
    num_edges = 1000000

    random.seed(42)

    for i in range(num_nodes):
        graph.add_node(
            i,
            position=(
                random.randint(0, 9),
                random.randint(0, 9),
                random.randint(0, 9)))
    for i in range(num_edges):
        graph.add_edge(
            random.randint(0, num_nodes - 1),
            random.randint(0, num_nodes - 1),
            score=random.random())

    start = time.time()
    graph.write_nodes()
    graph.write_edges()
    print("Wrote graph in %.3fs"%(time.time() - start))

    start = time.time()
    nodes, edges = graph_provider.read_blockwise(
        daisy.Roi(
            (0, 0, 0),
            (10, 10, 10)),
        daisy.Coordinate((1, 1, 1)),
        num_workers=40)
    print("Read graph in %.3fs"%(time.time() - start))

    written_nodes = list(range(num_nodes))
    read_nodes = sorted(nodes['id'])

    print("Checking consistency...")

    assert np.equal(written_nodes, read_nodes).all()

    for u, v, score in zip(edges['u'], edges['v'], edges['score']):
        assert graph.edges[(u, v)]['score'] == score
