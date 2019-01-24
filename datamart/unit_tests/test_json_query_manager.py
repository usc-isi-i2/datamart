from datamart.utilities.utils import Utils
from datamart.es_managers.json_query_manager import JSONQueryManager
import unittest, json


class TestJSONQueryManager(unittest.TestCase):
    def setUp(self):
        self.qm = JSONQueryManager(es_host="dsbox02.isi.edu", es_port=9200, es_index='datamart_all')

    @Utils.test_print
    def test_dataset_about(self):
        query = {
            "dataset": {
                "about": "PG12"
            }
        }
        parsed = self.qm.parse_json_query(query)
        expected = {"query": {"bool": {"must": [
            {"bool": {"should": [
                {"query_string": {"query": "PG12"}},
                {"nested": {"path": "variables", "query": {"query_string": {"query": "PG12"}}}}
            ]}}
        ]}}}
        self.assertEqual(json.loads(parsed), expected)

    @Utils.test_print
    def test_dataset_arr_str(self):
        query = {
            "dataset": {
                "name": ["WIKIDATA_PROP_PROPERTY"],
                "description": ["property", "constraint"],
                "keywords": ["category"],
                "url": ["www.wikidata.org", "Property:P2302"]
            }
        }
        parsed = self.qm.parse_json_query(query)
        expected = {"query": {"bool": {"must": [
            {"bool": {"must": [
                {"bool": {"should": [
                    {"match_phrase": {"title": "WIKIDATA_PROP_PROPERTY"}}
                ], "minimum_should_match": 1}},
                {"bool": {"should": [
                    {"match_phrase": {"description": "property"}},
                    {"match_phrase": {"description": "constraint"}}
                ], "minimum_should_match": 1}},
                {"bool": {"should": [{"match_phrase": {"keywords": "category"}}], "minimum_should_match": 1}},
                {"bool": {"should": [
                    {"match_phrase": {"url": "www.wikidata.org"}},
                    {"match_phrase": {"url": "Property:P2302"}}
                ], "minimum_should_match": 1}}
            ]}}
        ]}}}
        self.assertEqual(json.loads(parsed), expected)


