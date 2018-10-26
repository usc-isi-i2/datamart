from datamart.es_managers.es_manager import ESManager
import typing
import math
import json
import warnings


class QueryManager(ESManager):

    def __init__(self, es_host: str, es_port: int, es_index: str) -> None:
        super().__init__(es_host=es_host, es_port=es_port)
        self.es_index = es_index

    def search(self, body, size=1000, from_index=0, **kwargs) -> typing.Optional[typing.List[dict]]:
        result = self.es.search(index=self.es_index, body=body, size=size, from_=from_index, **kwargs)
        if result["hits"]["total"] <= 0:
            print("Nothing found")
            return None
        else:
            return [doc["_source"] for doc in result["hits"]["hits"]]

    def match_some_terms_from_array(self,
                                    terms: list,
                                    key: str="variables.named_entity.keyword",
                                    minimum_should_match=None
                                    ) -> typing.Optional[typing.List[dict]]:
        if not terms:
            warnings.warn("Empty terms list, match all")
            return self.match_all()

        body = {
            "query": {
                "bool": {
                    "should": [
                    ],
                    "minimum_should_match": 1
                }
            }
        }

        for term in terms:
            body["query"]["bool"]["should"].append(
                {
                    "term": {
                        key: term.lower()
                    }
                }
            )

        if minimum_should_match:
            body["query"]["bool"]["minimum_should_match"] = minimum_should_match
        else:
            body["query"]["bool"]["minimum_should_match"] = math.ceil(len(terms) / 2)

        return self.search(body=json.dumps(body))

    def match_datamart_id(self, datamart_id: int) -> typing.Optional[typing.List[dict]]:
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

        return self.search(body=json.dumps(body))

    def match_key_value_pairs(self, key_value_pairs: typing.List[tuple]) -> typing.Optional[typing.List[dict]]:
        if not key_value_pairs:
            warnings.warn("Empty key value pairs list, match all")
            return self.match_all()

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
        return self.search(body=json.dumps(body))

    def match_all(self):
        return self.search(
            body=json.dumps(
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
        )