import unittest
from pandas.io.json import json_normalize
import os
import pandas as pd

from datamart.materializers.parsers.json_parser import JSONParser
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")

class TestJSONParser(unittest.TestCase):
    def setUp(self):
        self.parser = JSONParser()
        
    @Utils.test_print
    def test_get_all(self):
        url = 'https://zenodo.org/api/files/9c5ec017-f845-403d-ad5f-24fc93fb9bea/SXS:BBH:0029/Lev4/metadata.json'
        
        result = self.parser.get_all(url)

        self.assertTrue(len(result)==1)


        df = result[0]
        # write to csv and read it back due to issues with pd.equals()
        df.to_csv("tmp.csv",index=None)
        df = pd.read_csv("tmp.csv")
        sample_result = pd.read_csv(os.path.join(resources_path,"json_parser_test.csv"))
        self.assertTrue(df.equals(sample_result))

        os.remove("tmp.csv")