from datamart.materializers.eia_gov_materializer import EIAGovMaterializer
import unittest, os
import pandas as pd
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestWikidataMaterializer(unittest.TestCase):
    def setUp(self):
        self.eia_gov_materializer = EIAGovMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            "url": "http://api.eia.gov/series/?series_id=PET.RBRTE.D&api_key=5c444b278ff431a31037ba48808c3144&out=json"
        }

        constraints = {
            'start': '20190110',
            'end': '20190120'
        }
        result = self.eia_gov_materializer.get(metadata=mock_metadata, constraints=constraints)

        expected_result = pd.read_csv('{}/eia_gov_sample.csv'.format(resources_path))
        self.assertEqual(len(result), len(expected_result))
