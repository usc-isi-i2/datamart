from datamart.utilities.utils import Utils
from datamart.es_managers.query_manager import QueryManager
import unittest, json


class TestQueryManager(unittest.TestCase):

    @Utils.test_print
    def test_match_some_terms_from_array(self):
        query = QueryManager.match_some_terms_from_variables_array(terms=["los angeles", "NEW YORK", "AKBA"],
                                                                   key="variables.named_entity",
                                                                   minimum_should_match=0.5)

        expected = {
            "nested": {
                "path": "variables",
                "inner_hits": {
                    "_source": [
                        "named_entity"
                    ],
                    "highlight": {
                        "fields": {
                            "variables.named_entity": {
                                "pre_tags": [
                                    ""
                                ],
                                "post_tags": [
                                    ""
                                ],
                                "number_of_fragments": 0
                            }
                        }
                    }
                },
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match_phrase": {
                                    "variables.named_entity": {
                                        "query": "los angeles",
                                        "_name": "los angeles"
                                    }
                                }
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {
                                        "query": "new york",
                                        "_name": "new york"
                                    }
                                }
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {
                                        "query": "akba",
                                        "_name": "akba"
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": 2
                    }
                }
            }
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_some_terms_from_array_default(self):
        terms = ["los angeles", "NEW YORK", "AKBA", "shanghai", "TOKyo"]
        query = QueryManager.match_some_terms_from_variables_array(terms=terms)
        expected = {
            "nested": {
                "path": "variables",
                "inner_hits": {
                    "_source": ["named_entity"],
                    "highlight": {"fields": {"variables.named_entity": {
                        "pre_tags": [""],
                        "post_tags": [""],
                        "number_of_fragments": 0}}}
                },
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match_phrase": {
                                    "variables.named_entity": {"query": "los angeles", "_name": "los angeles"}
                                }
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {"query": "new york", "_name": "new york"}}
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {"query": "akba", "_name": "akba"}}
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {"query": "shanghai", "_name": "shanghai"}}
                            },
                            {
                                "match_phrase": {
                                    "variables.named_entity": {"query": "tokyo", "_name": "tokyo"}}
                            }
                        ],
                        "minimum_should_match": 3
                    }
                }
            }
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_temporal_coverage(self):
        query = QueryManager.match_temporal_coverage(start="2018-09-23", end="2018-09-30T00:00:00")
        expected = {
            "nested": {
                "path": "variables",
                "inner_hits": {"_source": ["temporal_coverage"]},
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
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_temporal_coverage_invalid(self):
        query = QueryManager.match_temporal_coverage(start="2222s", end="2018-09-30T00:00:00")
        expected = {
            "nested": {
                "path": "variables",
                "inner_hits": {"_source": ["temporal_coverage"]},
                "query": {
                    "bool": {
                        "must": [
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
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_global_datamart_id(self):
        query = QueryManager.match_global_datamart_id(datamart_id=0)
        expected = {
            "term": {
                "datamart_id": 0
            }
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_variable_datamart_id(self):
        query = QueryManager.match_variable_datamart_id(datamart_id=0)
        expected = {
            "nested": {
                "path": "variables",
                "inner_hits": {"_source": ["datamart_id"]},
                "query": {"bool": {"must": [{"term": {"variables.datamart_id": 0}}]}}}
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_key_value_pairs(self):
        query = QueryManager.match_key_value_pairs(key_value_pairs=[
            ("description", "average"),
            ("title.keyword", "TAVG"),
            ("datamart_id", 0)
        ])
        expected = {
            "bool": {
                "must": [
                    {"match": {"description": "average"}},
                    {"match": {"title.keyword": "TAVG"}},
                    {"match": {"datamart_id": 0}}
                ]
            }
        }

        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_key_value_pairs_list(self):
        query = QueryManager.match_key_value_pairs(key_value_pairs=[
            ("description", ["average", "temperature"]),
            ("title.keyword", "TAVG"),
            ("datamart_id", 0)
        ])
        expected = {
            "bool": {
                "must": [{"match": {"description": "average"}},
                         {"match": {"description": "temperature"}},
                         {"match": {"title.keyword": "TAVG"}},
                         {"match": {"datamart_id": 0}}
                         ]
            }
        }
        self.assertEqual(expected, query)

    @Utils.test_print
    def test_match_any(self):
        query = QueryManager.match_any(query_string="los angeles average")
        expected = {
            "query_string": {
                "query": "los angeles average"
            }
        }

        self.assertEqual(expected, query)

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

    @Utils.test_print
    def test_form_conjunction_query(self):
        query = QueryManager.form_conjunction_query(
            queries=[
                {'query_string': {'query': 'Budget'}},
                {'term': {'datamart_id': 125980000}},
                {'bool': {'must': [{'match': {'title': 'Government'}}]}},
                {'nested': {
                    'path': 'variables',
                    'inner_hits': {'_source': ['named_entity'],
                                   'highlight': {
                                       'fields': {
                                           'variables.named_entity': {
                                               'pre_tags': [
                                                   ''],
                                               'post_tags': [
                                                   ''],
                                               'number_of_fragments': 0}
                                       }
                                   }
                                   },
                    'query': {'bool': {
                        'should': [
                            {
                                'match_phrase': {
                                    'variables.named_entity': {
                                        'query': 'russia',
                                        '_name': 'russia'
                                    }
                                }
                            },
                            {
                                'match_phrase': {
                                    'variables.named_entity': {
                                        'query': 'saudi arabia',
                                        '_name': 'saudi arabia'}}}
                        ],
                        'minimum_should_match': 1
                    }
                    }
                }
                }
            ]
        )

        expected = {"query": {"bool": {
            "must": [
                {"query_string": {"query": "Budget"}},
                {"term": {"datamart_id": 125980000}},
                {"bool": {"must": [{"match": {"title": "Government"}}]}},
                {"nested": {"path": "variables",
                            "inner_hits": {"_source": ["named_entity"],
                                           "highlight": {
                                               "fields": {
                                                   "variables.named_entity": {
                                                       "pre_tags": [
                                                           ""],
                                                       "post_tags": [
                                                           ""],
                                                       "number_of_fragments": 0}}}},
                            "query": {"bool": {
                                "should": [{
                                    "match_phrase": {
                                        "variables.named_entity": {
                                            "query": "russia",
                                            "_name": "russia"}}},
                                    {
                                        "match_phrase": {
                                            "variables.named_entity": {
                                                "query": "saudi arabia",
                                                "_name": "saudi arabia"}}}],
                                "minimum_should_match": 1}}}}]}}}

        self.assertEqual(json.dumps(expected), query)
