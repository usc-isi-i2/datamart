import unittest
import os
import json
from datamart.index_builder import IndexBuilder
from datamart.utilities.utils import Utils


class TestUserIndex(unittest.TestCase):
    @Utils.test_print
    def test_index_csv(self):
        with open(os.path.join(os.path.dirname(__file__), "resources/user_index_schema.json"), "r") as f:
            description = json.load(f)
        ib = IndexBuilder()
        res = ib.indexing(description_path=description, es_index='datamart_tmp', query_data_for_indexing=True)
        _id = res.get('datamart_id')
        self.assertNotEqual(_id, None)
        expected = {
            'datamart_id': _id,
            'title': 'A short description of the dataset',
            'description': 'A long description of the dataset',
            'implicit_variables': [
                {
                    'name': 'year',
                    'value': '2007',
                    'semantic_type': [
                        'http://schema.org/Integer',
                        'https://metadata.datadrivendiscovery.org/types/Time'
                    ]
                }
            ],
            'materialization':
                {
                    'python_path': 'general_materializer',
                    'arguments': {
                        'url': 'http://data.iowadot.gov/datasets/a59edde607864c64a68a10ec142797d3_0.csv',
                        'file_type': 'csv'
                    }
                },
            'variables': []
        }
        self.assertEqual(res, expected)
