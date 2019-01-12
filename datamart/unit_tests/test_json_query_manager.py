from datamart.utilities.utils import Utils
from datamart.es_managers.json_query_manager import JSONQueryManager
import unittest, json
import pandas as pd


def pprint(obj):
    print(json.dumps(obj, indent=2))


class TestQueryManager(unittest.TestCase):

    @Utils.test_print
    def test_parse_temporal_entity(self):
        temporal_var = {
                "type": "temporal_entity",
                "start": "2018-01-01",
                "end": "2018-01-02",
                "granularity": "day"
            }
        query = JSONQueryManager.parse_a_variable(temporal_var, "required_variables", 0)
        expected = {
          "nested": {
            "path": "variables",
            "inner_hits": {
              "_source": [
                "temporal_coverage"
              ],
              "name": "required_variables.0.temporal_entity"
            },
            "query": {
              "bool": {
                "must": [
                  {
                    "range": {
                      "variables.temporal_coverage.start": {
                        "lte": "2018-01-01T00:00:00",
                        "format": "yyyy-MM-dd'T'HH:mm:ss"
                      }
                    }
                  },
                  {
                    "range": {
                      "variables.temporal_coverage.end": {
                        "gte": "2018-01-02T00:00:00",
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
    def test_parse_geospatial_entity_named_entities(self):
        geospatial_var = {
                "type": "geospatial_entity",
                "named_entities": {
                    "semantic_type": "http://schema.org/State",
                    "items": ["california", "new york", "texas"]
                }
            }
        query = JSONQueryManager.parse_a_variable(geospatial_var, "required_variables", 0)

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
              },
              "name": "required_variables.0.geospatial_entity"
            },
            "query": {
              "bool": {
                "should": [
                  {
                    "match_phrase": {
                      "variables.named_entity": {
                        "query": "california",
                        "_name": "california"
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
                        "query": "texas",
                        "_name": "texas"
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
    def test_parse_dataframe_columns_index(self):
        dataframe_col = {
                "type": "dataframe_columns",
                "index": [0]
            }
        query_by_index = JSONQueryManager.parse_a_variable(dataframe_col, "required_variables", 0, pd.DataFrame({
            "city": ["abu dhabi", "ajman", "dubai", "sharjah"],
            'date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"]
        }))

        dataframe_col = {
                "type": "dataframe_columns",
                "names": ["city"]
            }
        query_by_names = JSONQueryManager.parse_a_variable(dataframe_col, "required_variables", 0, pd.DataFrame({
            "city": ["abu dhabi", "ajman", "dubai", "sharjah"],
            'date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"]
        }))

        expected = {
          "bool": {
            "must": [
              {
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
                    },
                    "name": "required_variables.0.dataframe_columns"
                  },
                  "query": {
                    "bool": {
                      "should": [
                        {
                          "match_phrase": {
                            "variables.named_entity": {
                              "query": "abu dhabi",
                              "_name": "abu dhabi"
                            }
                          }
                        },
                        {
                          "match_phrase": {
                            "variables.named_entity": {
                              "query": "ajman",
                              "_name": "ajman"
                            }
                          }
                        },
                        {
                          "match_phrase": {
                            "variables.named_entity": {
                              "query": "dubai",
                              "_name": "dubai"
                            }
                          }
                        },
                        {
                          "match_phrase": {
                            "variables.named_entity": {
                              "query": "sharjah",
                              "_name": "sharjah"
                            }
                          }
                        }
                      ],
                      "minimum_should_match": 2
                    }
                  }
                }
              }
            ]
          }
        }
        self.assertEqual(expected, query_by_index)
        self.assertEqual(expected, query_by_names)

    @Utils.test_print
    def test_parse_generic_entity(self):
        generic_var = {
                "type": "generic_entity",
                "about": "weather"
            }
        query = JSONQueryManager.parse_a_variable(generic_var, "required_variables", 0)
        expected = {
          "bool": {
            "must": [
              {
                "nested": {
                  "path": "variables",
                  "query": {
                    "query_string": {
                      "query": "weather"
                    }
                  },
                  "inner_hits": {
                    "name": "required_variables.0.generic_entity"
                  }
                }
              }
            ]
          }
        }
        self.assertEqual(expected, query)