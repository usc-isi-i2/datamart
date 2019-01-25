from datamart import bulk_generate_metadata, generate_metadata
from datamart.utilities.utils import Utils
import unittest
import json


class TestUrlUpload(unittest.TestCase):

    @Utils.test_print
    def test_generate_metadata(self):
        url = "http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportStaffSample.csv"
        description = {
            "title": "Sample CSV File for Importing Staff Directory App Information",
            "materialization_arguments": {
                "url": url
            }
        }
        res = generate_metadata(description=description, es_index='datamart_tmp')
        with open('datamart/unit_tests/resources/url_upload_1.json') as f:
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
        pass

