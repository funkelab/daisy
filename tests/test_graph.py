import daisy
import logging
import random
import time

logging.basicConfig(level=logging.DEBUG)

def test_graph():

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

    graph.add_node(2, comment="without position")
    graph.add_node(42, position=(1, 1, 1))
    graph.add_node(23, position=(5, 5, 5), swip='swap')
    graph.add_node(57, position=daisy.Coordinate((7, 7, 7)), zap='zip')
    graph.add_edge(42, 23)

    for i in range(10000):
        graph.add_node(i + 100, position=(
            random.randint(0, 10),
            random.randint(0, 10),
            random.randint(0, 10)))

    start = time.time()
    graph.write_nodes()
    graph.write_edges()
    print("Wrote graph in %.3fs"%(time.time() - start))

    start = time.time()
    graph = graph_provider[
        daisy.Roi(
            (0, 0, 0),
            (10, 10, 10))
    ]
    print("Read graph in %.3fs"%(time.time() - start))

if __name__ == "__main__":
    test_graph()
