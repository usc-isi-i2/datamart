from datamart import bulk_generate_metadata, generate_metadata
from datamart.utilities.utils import Utils
import unittest
import json


class TestUrlUpload(unittest.TestCase):

    def setUp(self):
        self.test_index = 'datamart_tmp'

    @Utils.test_print
    def test_generate_metadata_csv(self):
        url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportStaffSample.csv"
        description = {
            "title": "Sample CSV File for Importing Staff Directory App Information",
            "materialization_arguments": {
                "url": url
            }
        }
        res = generate_metadata(description=description, es_index=self.test_index)
        with open('datamart/unit_tests/resources/url_upload_csv.json') as f:
            expected = json.load(f)

        self.assertEqual(len(res), 1)
        self.assertIsInstance(res[0].get('datamart_id'), int)
        del res[0]['datamart_id']

        variables = res[0].get('variables')
        self.assertNotEqual(variables, None)

        for v in variables:
            self.assertIsInstance(v.get('datamart_id'), int)
            del v['datamart_id']

        self.assertEqual(res[0], expected)

    @Utils.test_print
    def test_bulk_generate_metadata(self):
        url = 'https://sample-videos.com/download-sample-xls.php'
        res = bulk_generate_metadata(url, es_index=self.test_index)
        self.assertEqual(len(res), 6)
        rows = [10, 100, 1000, 5000, 10000, 50000]
        cols = [10, 10, 10, 10, 10, 9]
        for i in range(6):
            self.assertEqual(len(res[i]), 1)
            res_title = res[i][0].get('title')
            expected_title = 'Sample Spreadsheet %d rows(Sample-spreadsheet-file)' % rows[i]
            self.assertEqual(res_title, expected_title)
            variables = res[i][0].get('variables')
            self.assertEqual(len(variables), cols[i])


