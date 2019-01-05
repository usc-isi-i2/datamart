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
            'description': 'https://cerc.blackboard.com/Page/1189',
            'implicit_variables': [{
                'name': 'year',
                'value': '2007',
                'semantic_type': ['http://schema.org/Integer', 'https://metadata.datadrivendiscovery.org/types/Time']
            }],
            'materialization': {
                'python_path': 'general_materializer',
                'arguments': {
                    'url': 'http://insight.dev.schoolwires.com/HelpAssets/C2Assets/C2Files/C2ImportFamRelSample.csv',
                    'file_type': 'csv'
                }
            },
            'variables': [
                {
                    'datamart_id': _id + 1,
                    'semantic_type': [],
                    'name': 'Parent Identifier',
                    'description': 'column name: Parent Identifier, dtype: int64',
                    'temporal_coverage': {
                        'start': '1001-01-04T00:00:00',
                        'end': '1010-01-04T00:00:00'
                    }
                },
                {
                    'datamart_id': _id + 2,
                    'semantic_type': [],
                    'name': 'Student Identifier',
                    'description': 'column name: Student Identifier, dtype: int64',
                    'temporal_coverage': {
                        'start': '1002-01-04T00:00:00',
                        'end': '1020-01-04T00:00:00'
                    }
                }
            ],
            'keywords': ['Parent Identifier', 'Student Identifier']
        }
        print(res)
        self.assertEqual(res, expected)
