from datamart.materializers.wikitables_materializer import WikitablesMaterializer
import unittest, os
import pandas as pd
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestWikitablesMaterializer(unittest.TestCase):
    def setUp(self):
        self.wikitables_materializer = WikitablesMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            "materialization": {
                "arguments": {
                    "lang": "en",
                    "url": "https://en.wikipedia.org/wiki/Albedo",
                    "xpath": "/html[1]/body[1]/div[3]/div[3]/div[4]/div[1]/table[2]"
                }
            }
        }

        result = self.wikitables_materializer.get(metadata=mock_metadata).to_dict(orient="records")
        sample_result = pd.read_csv(os.path.join(resources_path, 'wikitables_sample.csv'), dtype=str)
        sample_result = sample_result.fillna('').to_dict(orient="records")

        self.assertEqual(result, sample_result)
