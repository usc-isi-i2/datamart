import unittest
import pandas as pd
import json
import os
from datamart.profilers.two_ravens_profiler import TwoRavensProfiler
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestTwoRavensProfiler(unittest.TestCase):

    @Utils.test_print
    def test_two_ravens_profiler(self):
        data = pd.DataFrame({
            'Name': ['Tom', 'Jack', 'Steve'],
            'Age': [28, 34, 29],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23"]
        })
        meta = Utils.generate_metadata_from_dataframe(data)
        res = TwoRavensProfiler().profile(data, meta)
        expected_file = os.path.join(resources_path, "two_ravens.json")
        with open(expected_file) as f:
            expected = json.load(f)
        self.assertEqual(res, expected)
