from datamart.utilities.utils import Utils
from datamart.es_managers.json_query_manager import JSONQueryManager
import unittest, json
from io import StringIO
import pandas as pd


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

    @Utils.test_print
    def test_variables(self):
        df = pd.read_csv(StringIO("""date,team
2018-08-11,Arsenal
2018-08-12,West Ham United
2018-08-13,Cardiff City
2018-09-11,Liverpool
2018-10-11,Manchester City
2018-11-11,Newcastle United
2018-12-11,Watford
2019-01-11,Huddersfield Town
2019-02-11,Everton
2019-03-11,AFC Bournemouth
2019-04-11,Crystal Palace
2019-05-05,Wolverhampton Wanderers
"""))
        # variables in "required_variables" should perform "AND"
        query = {
            "dataset": {
                "url": ["https://www.football-data.org"]
            },
            "required_variables": [
                {
                    "type": "temporal_entity",
                    "start": "2018-08-11",
                    "end": "2019-05-05"
                },
                {
                    "type": "dataframe_columns",
                    "index": [1]
                }
            ]
        }
        required = self.qm.search(self.qm.parse_json_query(query, df))
        self.assertEqual(len(required), 1)

        # variables in "desired_variables" should perform "OR"
        query['desired_variables'] = query['required_variables']
        # keep the key to avoid auto generated "required_variables":
        query['required_variables'] = []
        desired = self.qm.search(self.qm.parse_json_query(query, df))
        self.assertEqual(len(desired), 5)

