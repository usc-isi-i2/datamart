from datamart.materializers.tradingeconomics_materializer import TradingEconomicsMaterializer
import unittest, os
import pandas as pd
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestWikidataMaterializer(unittest.TestCase):
    def setUp(self):
        self.tradingeconomics_materializer = TradingEconomicsMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            'title': "GDP",
            "url": "https://api.tradingeconomics.com/historical/country/all/indicator/gdp%20growth%20annual?c=guest:guest&format=csv",
            "materialization": {
                "arguments": {
                }
            }
        }
        fake_constrains = {
            "date_range": {
                "start": None,
                "end": None
            },
            "named_entity": {0: ["Albania", "Argentina", "Angola"]}
        }

        result = self.tradingeconomics_materializer.get(metadata=mock_metadata, constrains=fake_constrains).to_dict(
            orient="records")
        sample_result = pd.read_csv(os.path.join(resources_path, 'tradingeconomics_gdp.csv'), encoding='utf-16')
        sample_result = sample_result.fillna('').to_dict(orient="records")

        self.assertEqual(result, sample_result)
