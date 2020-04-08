import daisy
import unittest


class TestMetaCollection(unittest.TestCase):

    def get_mongo_graph_provider(self, mode, directed, total_roi):
        return daisy.persistence.MongoDbGraphProvider(
            'test_daisy_graph',
            directed=directed,
            total_roi=total_roi,
            mode=mode)

    def test_graph_read_meta_values(self):
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        self.get_mongo_graph_provider(
                'w', True, roi)

        graph_provider = self.get_mongo_graph_provider(
                'r', None, None)

        self.assertEqual(True, graph_provider.directed)
        self.assertEqual(roi, graph_provider.total_roi)

    def test_graph_default_meta_values(self):
        provider = self.get_mongo_graph_provider(
                'w', None, None)
        self.assertEqual(False, provider.directed)
        self.assertIsNone(provider.total_roi)

        graph_provider = self.get_mongo_graph_provider(
                'r', None, None)

        self.assertEqual(False, graph_provider.directed)
        self.assertIsNone(graph_provider.total_roi)

    def test_graph_nonmatching_meta_values(self):
        roi = daisy.Roi((0, 0, 0),
                        (10, 10, 10))
        roi2 = daisy.Roi((1, 0, 0),
                         (10, 10, 10))
        self.get_mongo_graph_provider(
                'w', True, None)

        with self.assertRaises(ValueError):
            self.get_mongo_graph_provider(
                'r', False, None)

        self.get_mongo_graph_provider(
                'w', None, roi)

        with self.assertRaises(ValueError):
            self.get_mongo_graph_provider(
                'r', None, roi2)
