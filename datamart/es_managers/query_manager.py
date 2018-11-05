from datamart.es_managers.es_manager import ESManager
from datamart.utils import Utils
import typing
import math
import json
import warnings


class QueryManager(ESManager):

    def __init__(self, es_host: str, es_port: int, es_index: str) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            es_index: elastic search index.
            es_host: es_host.
            es_port: es_port.

        Returns:

        """

        super().__init__(es_host=es_host, es_port=es_port)
        self.es_index = es_index

    def search(self, body: str, size: int = 1000, from_index: int = 0, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Entry point for querying.

        Args:
            body: query body.
            size: query return size.
            from_index: from index.

        Returns:

        """

        result = self.es.search(index=self.es_index, body=body, size=size, from_=from_index, **kwargs)
        if result["hits"]["total"] <= 0:
            print("Nothing found")
            return None
        else:
            return result["hits"]["hits"]

    @classmethod
    def match_some_terms_from_variables_array(cls,
                                              terms: list,
                                              key: str = "variables.named_entity",
                                              minimum_should_match=None
                                              ) -> str:
        """Generate query body for query that matches some terms from an array.

        Args:
            terms: list of terms for matching.
            key: which key to match, by default, matches column's named_entity.
            minimum_should_match: minimum should match terms from the list.

        Returns:
            string of query body
        """

        if not terms:
            warnings.warn("Empty terms list, match all")
            return cls.match_all()

        body = {
            "query": {
                "nested": {
                    "path": key.split(".")[0],
                    "inner_hits": {
                        "_source": [
                            key.split(".")[1]
                        ]
                    },
                    "query": {
                        "bool": {
                            "should": [
                            ],
                            "minimum_should_match": 1
                        }
                    }
                }
            }
        }

        for term in terms:
            body["query"]["nested"]["query"]["bool"]["should"].append(
                {
                    "match": {
                        key: term.lower()
                    }
                }
            )

        if minimum_should_match:
            body["query"]["nested"]["query"]["bool"]["minimum_should_match"] = minimum_should_match
        else:
            body["query"]["nested"]["query"]["bool"]["minimum_should_match"] = math.ceil(len(terms) / 2)

        return json.dumps(body)

    @classmethod
    def match_temporal_coverage(cls, start: str = None, end: str = None) -> str:
        """Generate query body for query by temporal_coverage.

        Args:
            start: dataset should cover date time earlier than the start date.
            end: dataset should cover date time later than the end date.

        Returns:
            string of query body
        """
        start = Utils.date_validate(date_text=start)
        end = Utils.date_validate(date_text=end)
        if not start and not end:
            warnings.warn("Start and end are None, match all")
            return cls.match_all()

        body = {
            "query": {
                "nested": {
                    "path": "variables",
                    "inner_hits": {
                        "_source": [
                            "temporal_coverage"
                        ]
                    },
                    "query": {
                        "bool": {
                            "must": [
                            ]
                        }
                    }
                }
            }
        }

        if start:
            body["query"]["nested"]["query"]["bool"]["must"].append(
                {
                    "range": {
                        "variables.temporal_coverage.start": {
                            "lte": start,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                }
            )

        if end:
            body["query"]["nested"]["query"]["bool"]["must"].append(
                {
                    "range": {
                        "variables.temporal_coverage.end": {
                            "gte": end,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                }
            )

        return json.dumps(body)

    @staticmethod
    def match_global_datamart_id(datamart_id: int) -> str:
        """Generate query body for query by global datamart id.

        Args:
            datamart_id: integer for the datamart id to match.

        Returns:
            string of query body
        """

        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "datamart_id": datamart_id
                            }
                        }
                    ]
                }
            }
        }

        return json.dumps(body)

    @staticmethod
    def match_variable_datamart_id(datamart_id: int) -> str:
        """Generate query body for query by variable datamart id.

        Args:
            datamart_id: integer for the datamart id to match.

        Returns:
            string of query body
        """

        body = {
            "query": {
                "nested": {
                    "path": "variables",
                    "inner_hits": {
                        "_source": [
                            "datamart_id"
                        ]
                    },
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "variables.datamart_id": datamart_id
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        }

        return json.dumps(body)

    @classmethod
    def match_global_key_value_pairs(cls, key_value_pairs: typing.List[tuple]) -> str:
        """Generate query body for query by multiple key value pairs.

        Args:
            key_value_pairs: list of (key, value) tuples.

        Returns:
            string of query body
        """

        if not key_value_pairs:
            warnings.warn("Empty key value pairs list, match all")
            return cls.match_all()

        body = {
            "query": {
                "bool": {
                    "must": [
                    ]
                }
            }
        }

        for key, value in key_value_pairs:
            if isinstance(value, list):
                body["query"]["bool"]["must"].append(
                    {
                        "terms": {
                            key: value
                        }
                    }
                )
            else:
                body["query"]["bool"]["must"].append(
                    {
                        "term": {
                            key: value
                        }
                    }
                )
        return json.dumps(body)

    @staticmethod
    def match_any(query_string: str) -> str:
        """Generate query body for query all fields of doc contains query_string.

        Args:
            query_string: string if query.

        Returns:
            string of query body
        """

        body = {
            "query": {
                "query_string": {
                    "query": query_string
                }
            }
        }
        return json.dumps(body)

    @staticmethod
    def match_all() -> str:
        """Generate query body for query all from es.

        Args:

        Returns:
            string of query body
        """

        return json.dumps(
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_all": {}
                            }
                        ]
                    }
                }
            }
        )
