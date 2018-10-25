from datamart.es_managers.query_manager import QueryManager
import pandas as pd
import typing
import math


class QuerySystem(object):

    def __init__(self, es_index: str, es_host: str="dsbox02.isi.edu", es_port: int=9200) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            es_index: elastic search index.
            es_host: es_host.
            es_port: es_port.

        Returns:

        """

        self.qm = QueryManager(es_host=es_host, es_port=es_port)
        self.es_index = es_index

    def query_by_column(self, col: pd.Series) -> typing.Optional[typing.List[dict]]:
        """Query by a pandas Dataframe column

        Args:
            col: pandas Dataframe column.

        Returns:
            matching documents
        """

        body = {
            "query": {
                "bool": {
                    "should": [
                    ],
                    "minimum_should_match": 1
                }
            }
        }

        for term in col.unique().tolist():
            body["query"]["bool"]["should"].append(
                {
                    "term": {
                        "variables.named_entity.keyword": term.lower()
                    }
                }
            )

        body["query"]["bool"]["minimum_should_match"] = math.ceil(len(col.unique().tolist())/2)

        result = self.qm.search(index=self.es_index, body=body)
        return result
