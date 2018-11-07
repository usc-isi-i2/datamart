from datamart.materializers.wikidata_materializer import WikidataMaterializer
import unittest, json, os
import pandas as pd
from datamart.utils import Utils


resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestWikidataMaterializer(unittest.TestCase):
    def setUp(self):
        self.wikidata_materializer = WikidataMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            "materialization": {
                "arguments": {
                    "property": "P2672"
                }
            }
        }

        result = self.wikidata_materializer.get(metadata=mock_metadata).to_dict(orient="records")
        sample_result = pd.read_csv(os.path.join(resources_path, 'P2672_SOATO_ID.csv'), dtype=str)
        sample_result = sample_result.fillna('').to_dict(orient="records")

        self.assertEqual(result, sample_result)
