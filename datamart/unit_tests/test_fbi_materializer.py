import unittest
import json
import pandas as pd
import os

from datamart.materializers.fbi_materializer import FbiMaterializer
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")

class TestFbiMaterializer(unittest.TestCase):
    def setUp(self):
        self.materializer = FbiMaterializer()
        self.sample_result_file = os.path.join(resources_path,"CIUS_2009_table_8.csv")

    @Utils.test_print
    def test_get(self):
        metadata = {
           "materialization": {
                "arguments": {
                    "url": "https://www2.fbi.gov/ucr/cius2009/data/documents/09tbl08.xls"
                }
            }
        }

        result = self.materializer.get(metadata)
        sample_result = pd.read_csv(self.sample_result_file)
        self.assertTrue(result.equals(sample_result))