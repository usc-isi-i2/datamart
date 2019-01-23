import unittest
import pandas as pd
import os

from datamart.materializers.parsers.excel_parser import ExcelParser
from datamart.utilities.utils import Utils

resources_path = os.path.join(os.path.dirname(__file__), "./resources")

class TestFbiMaterializer(unittest.TestCase):
    def setUp(self):
        self.parser = ExcelParser()
        self.sample_result_file_1 = os.path.join(resources_path,"excel_parser_test_1.csv")
        self.metadata_1 = {}
        self.sample_result_file_2 = os.path.join(resources_path,"excel_parser_test_2.csv")
        self.metadata_2 = {0: ['DARPA FY2010 Contract List']}
        self.sample_result_file_3 = os.path.join(resources_path,"excel_parser_test_3.csv")
        self.metadata_3 = {0: ['DARPA FY2015 Contract List']}

    @Utils.test_print
    def test_get(self):
        url = 'https://zenodo.org/api/files/6423a920-8815-40c3-8849-67b7e2d95a72/SETA%20Support%20DARPA%20FY2010%20and%20FY2015%20Database.xlsx'
        results = self.parser.parse(url)

        num_sheets = len(results)
        self.assertEqual(num_sheets, 3)

        sample_result_1 = pd.read_csv(self.sample_result_file_1)
        self.assertTrue(results[0]["df"].equals(sample_result_1))
        self.assertEqual(results[0]["metadata"], self.metadata_1)

        sample_result_2 = pd.read_csv(self.sample_result_file_2)
        self.assertTrue(results[1]["df"].equals(sample_result_2))
        self.assertEqual(results[1]["metadata"], self.metadata_2)

        sample_result_3 = pd.read_csv(self.sample_result_file_3)
        self.assertTrue(results[2]["df"].equals(sample_result_3))
        self.assertEqual(results[2]["metadata"], self.metadata_3)