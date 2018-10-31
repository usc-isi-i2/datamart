from datamart.materializers.fao_materializer import FaoMaterializer
import unittest, json, os
import pandas as pd
from datamart.utils import Utils
import json

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestFaoMaterializer(unittest.TestCase):
    def setUp(self):
        self.fao_materializer = FaoMaterializer()

    @Utils.test_print
    def test_get(self):
        fake_metadata = dict()
        fake_metadata['materialization'] = {}
        fake_constrains = {
            "date_range": {
                "start": "2016-09-23",
                "end": "2016-09-23"
            },
            "locations": ["United States of America", "China"]
        }
        result = self.fao_materializer.get(metadata=fake_metadata, constrains=fake_constrains).to_dict(
            orient="records")
        sample_result = json.load(open(os.path.join(resources_path, 'fao_get_test.json'), "r"))
        self.assertEqual(result, sample_result)
