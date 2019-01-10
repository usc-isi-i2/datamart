from datamart.es_managers.es_manager import ESManager
from datamart.utilities.utils import Utils
import typing
import math
import json
import warnings


class QueryManager(ESManager):

    MINIMUM_SHOULD_MATCH_RATIO = 0.5

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

    def search(self, body: str, size: int = 5000, from_index: int = 0, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Entry point for querying.

        Args:
            body: query body.
            size: query return size.
            from_index: from index.

        Returns:
            match result
        """

        result = self.es.search(index=self.es_index, body=body, size=size, from_=from_index, **kwargs)
        count = result["hits"]["total"]
        if count <= 0:
            print("Nothing found")
            return None
        if count <= size:
            return result["hits"]["hits"]
        return self.scroll_search(body=body, size=size, count=count)

    def scroll_search(self, body: str, size: int, count: int, scroll: str = '1m', **kwargs) -> typing.List[dict]:
        """Scroll search for the case that the result from es is too long.

        Args:
            body: query body.
            size: query return size.
            count: total count of doc hitted.
            scroll: how long a scroll id should remain

        Returns:
            match result
        """

        result = self.es.search(index=self.es_index, body=body, size=size, scroll=scroll, **kwargs)
        ret = result["hits"]["hits"]
        scroll_id = result["_scroll_id"]
        from_index = size
        while from_index <= count:
            result = self.es.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_id = result["_scroll_id"]
            ret += result["hits"]["hits"]
            from_index += size
        return ret

    @classmethod
    def match_some_terms_from_variables_array(cls,
                                              terms: list,
                                              key: str = "variables.named_entity",
                                              minimum_should_match=None
                                              ) -> dict:
        """Generate query body for query that matches some terms from an array.

        Args:
            terms: list of terms for matching.
            key: which key to match, by default, matches column's named_entity.
            minimum_should_match: minimum should match terms from the list.

        Returns:
            dict of query body
        """

        body = {
            "nested": {
                "path": key.split(".")[0],
                "inner_hits": {
                    "_source": [
                        key.split(".")[1]
                    ],
                    "highlight": {
                        "fields": {
                            key: {
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
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
        }

        for term in terms:
            body["nested"]["query"]["bool"]["should"].append(
                {
                    "match_phrase": {
                        key: {
                            "query": term.lower(),
                            "_name": term.lower()
                        }
                    }
                }
            )

        if minimum_should_match:
            body["nested"]["query"]["bool"]["minimum_should_match"] = math.ceil(minimum_should_match * len(terms))
        else:
            body["nested"]["query"]["bool"]["minimum_should_match"] = math.ceil(
                QueryManager.MINIMUM_SHOULD_MATCH_RATIO * len(terms))

        return body

    @classmethod
    def match_temporal_coverage(cls, start: str = None, end: str = None) -> typing.Optional[dict]:
        """Generate query body for query by temporal_coverage.

        Args:
            start: dataset should cover date time earlier than the start date.
            end: dataset should cover date time later than the end date.

        Returns:
            dict of query body
        """

        start = Utils.date_validate(date_text=start) if start else None
        end = Utils.date_validate(date_text=end) if end else None
        if not start and not end:
            warnings.warn("Start and end are valid")
            return None

        body = {
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

        if start:
            body["nested"]["query"]["bool"]["must"].append(
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
            body["nested"]["query"]["bool"]["must"].append(
                {
                    "range": {
                        "variables.temporal_coverage.end": {
                            "gte": end,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                }
            )

        return body

    @staticmethod
    def match_global_datamart_id(datamart_id: int) -> dict:
        """Generate query body for query by global datamart id.

        Args:
            datamart_id: integer for the datamart id to match.

        Returns:
            dict of query body
        """

        body = {
            "term": {
                "datamart_id": datamart_id
            }
        }

        return body

    @staticmethod
    def match_variable_datamart_id(datamart_id: int) -> dict:
        """Generate query body for query by variable datamart id.

        Args:
            datamart_id: integer for the datamart id to match.

        Returns:
            dict of query body
        """

        body = {
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

        return body

    @classmethod
    def match_key_value_pairs(cls, key_value_pairs: typing.List[tuple], disjunctive_array_value=False) -> dict:
        """Generate query body for query by multiple key value pairs.

        Args:
            key_value_pairs: list of (key, value) tuples.
            disjunctive_array_value: bool. if True, when the "value" is an array, use their disjunctive match

        Returns:
            dict of query body
        """

        body = {
            "bool": {
                "must": [
                ]
            }
        }

        nested = {
            "nested": {
                "path": "variables",
                "inner_hits": {
                    "_source": [
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

        for key, value in key_value_pairs:
            if not key.startswith("variables"):
                if isinstance(value, list):
                    if disjunctive_array_value:
                        body["bool"]["must"].append({
                            "dis_max": {
                                "queries": [{
                                    "match": {
                                        key: v
                                    }
                                } for v in value]
                            }
                        })
                    else:
                        for v in value:
                            body["bool"]["must"].append(
                                {
                                    "match": {
                                        key: v
                                    }
                                }
                            )
                else:
                    body["bool"]["must"].append(
                        {
                            "match": {
                                key: value
                            }
                        }
                    )
            else:
                nested["nested"]["inner_hits"]["_source"].append(key.split(".")[1])
                if key.split(".")[1] == "named_entity":
                    match_method = "match_phrase"
                else:
                    match_method = "match"
                if isinstance(value, list):
                    if disjunctive_array_value:
                        nested["nested"]["query"]["bool"]["must"].append({
                            "dis_max": {
                                "queries": [{
                                    "match": {
                                        key: v
                                    }
                                } for v in value]
                            }
                        })
                    else:
                        for v in value:
                            nested["nested"]["query"]["bool"]["must"].append(
                                {
                                    match_method: {
                                        key: v
                                    }
                                }
                            )
                else:
                    nested["nested"]["query"]["bool"]["must"].append(
                        {
                            match_method: {
                                key: value
                            }
                        }
                    )

        if nested["nested"]["query"]["bool"]["must"]:
            body["bool"]["must"].append(nested)
        return body

    @staticmethod
    def match_any(query_string: str) -> dict:
        """Generate query body for query all fields of doc contains query_string.

        Args:
            query_string: string if query.

        Returns:
            dict of query body
        """

        body = {
            "query_string": {
                "query": query_string
            }
        }
        return body

    @staticmethod
    def match_all() -> str:
        """Generate query body for query all from es.

        Args:

        Returns:
            dict of query body
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

    @staticmethod
    def form_conjunction_query(queries: list):
        body = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }
        for query in queries:
            if query:
                body["query"]["bool"]["must"].append(query)

        return json.dumps(body)

    @staticmethod
    def conjunction_query(queries: list) -> dict:
        body = {
            "bool": {
                "must": queries
            }
        }
        return body

    @staticmethod
    def disjunction_query(queries: list) -> dict:
        body = {
            "dis_max": {
                "queries": queries
            }
        }
        return body
