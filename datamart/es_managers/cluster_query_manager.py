from datamart.es_managers.query_manager import QueryManager
from datamart.utilities.utils import Utils
import warnings
import math
import json
import typing


class ClusterQueryManager(QueryManager):

    def __init__(self, es_host: str, es_port: int, es_index: str) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            es_index: elastic search index.
            es_host: es_host.
            es_port: es_port.

        Returns:

        """

        super().__init__(es_host=es_host, es_port=es_port, es_index=es_index)

    @staticmethod
    def match_named_entity(terms: list,
                           minimum_should_match=None
                           ):

        body = {
            "bool": {
                "should": [
                ],
                "minimum_should_match": 1
            }
        }

        for term in terms:
            body["bool"]["should"].append(
                {
                    "match_phrase": {
                        "named_entity": {
                            "query": term.lower(),
                            "_name": term.lower()
                        }
                    }
                }
            )

        if minimum_should_match:
            body["bool"]["minimum_should_match"] = math.ceil(minimum_should_match * len(terms))
        else:
            body["bool"]["minimum_should_match"] = math.ceil(
                ClusterQueryManager.MINIMUM_SHOULD_MATCH_RATIO * len(terms))

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
            "bool": {
                "must": [
                ]
            }
        }

        if start:
            body["bool"]["must"].append(
                {
                    "range": {
                        "temporal_coverage.start": {
                            "lte": start,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                }
            )

        if end:
            body["bool"]["must"].append(
                {
                    "range": {
                        "temporal_coverage.end": {
                            "gte": end,
                            "format": "yyyy-MM-dd'T'HH:mm:ss"
                        }
                    }
                }
            )

        return body
