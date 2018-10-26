from datamart.utils import Utils
from datamart.es_managers.query_manager import QueryManager
import unittest, json, math


class TestQueryManager(unittest.TestCase):

    @Utils.test_print
    def test_match_some_terms_from_array(self):
        query = QueryManager.match_some_terms_from_array(terms=["los angeles", "NEW YORK", "AKBA"],
                                                         key="variables.named_entity",
                                                         minimum_should_match=2)
        expected = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"variables.named_entity": "los angeles"}},
                        {"term": {"variables.named_entity": "new york"}},
                        {"term": {"variables.named_entity": "akba"}}],
                    "minimum_should_match": 2
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_some_terms_from_array_default(self):
        terms = ["los angeles", "NEW YORK", "AKBA", "shanghai", "TOKyo"]
        query = QueryManager.match_some_terms_from_array(terms=terms)
        expected = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"variables.named_entity.keyword": "los angeles"}},
                        {"term": {"variables.named_entity.keyword": "new york"}},
                        {"term": {"variables.named_entity.keyword": "akba"}},
                        {"term": {"variables.named_entity.keyword": "shanghai"}},
                        {"term": {"variables.named_entity.keyword": "tokyo"}}],
                    "minimum_should_match": math.ceil(len(terms) / 2)
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_temporal_coverage(self):
        query = QueryManager.match_temporal_coverage(start="2018-09-23", end="2018-09-30T00:00:00")
        expected = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "variables.temporal_coverage.start": {
                                    "lte": "2018-09-23T00:00:00",
                                    "format": "yyyy-MM-dd'T'HH:mm:ss"
                                }
                            }
                        },
                        {
                            "range": {
                                "variables.temporal_coverage.end": {
                                    "gte": "2018-09-30T00:00:00",
                                    "format": "yyyy-MM-dd'T'HH:mm:ss"
                                }
                            }
                        }
                    ]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_temporal_coverage_invalid(self):
        query = QueryManager.match_temporal_coverage(start="2222s", end="2018-09-30T00:00:00")
        expected = {
            "query": {
                "bool": {
                    "must": [{
                        "range": {
                            "variables.temporal_coverage.end": {
                                "gte": "2018-09-30T00:00:00",
                                "format": "yyyy-MM-dd'T'HH:mm:ss"
                            }
                        }
                    }]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_match_global_datamart_id(self):
        query = QueryManager.match_global_datamart_id(datamart_id=0)
        expected = {
            "query": {
                "bool": {
                    "must": [{"term": {"datamart_id": 0}}]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_variable_datamart_id(self):
        query = QueryManager.match_variable_datamart_id(datamart_id=0)
        expected = {
            "query": {
                "bool": {
                    "must": [{"term": {"variables.datamart_id": 0}}]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_key_value_pairs(self):
        query = QueryManager.match_key_value_pairs(key_value_pairs=[
            ("description", "average"),
            ("title.keyword", "TAVG"),
            ("variables.datamart_id", 0)
        ])
        expected = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"description": "average"}},
                        {"term": {"title.keyword": "TAVG"}},
                        {"term": {"variables.datamart_id": 0}}
                    ]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_key_value_pairs_list(self):
        query = QueryManager.match_key_value_pairs(key_value_pairs=[
            ("description", ["average", "temperature"]),
            ("title.keyword", "TAVG"),
            ("variables.datamart_id", 0)
        ])
        expected = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"description": ["average", "temperature"]}},
                        {"term": {"title.keyword": "TAVG"}},
                        {"term": {"variables.datamart_id": 0}}]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_any(self):
        query = QueryManager.match_any(query_string="los angeles average")
        expected = {
            "query": {
                "query_string": {
                    "query": "los angeles average"
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)

    @Utils.test_print
    def test_match_all(self):
        query = QueryManager.match_all()
        expected = {
            "query": {
                "bool": {
                    "must": [{"match_all": {}}]
                }
            }
        }

        self.assertEqual(json.dumps(expected), query)
