from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json


class IndexManager(object):

    def __init__(self, es_host: str, es_port: int):
        """Init method for index manager

        Args:
            es_host: str, Elasticsearch host
            es_port: int, Elasticsearch port

        Returns:

        """

        self.es = Elasticsearch([{'host': es_host, 'port': es_port}])

    def check_exists(self, index: str) -> bool:
        """check if index exist

        Args:
            index: str, Elasticsearch index

        Returns:
            Boolean
        """

        if self.es.indices.exists(index=index):
            return True
        return False

    def create_index(self, **kwargs):
        """create index

        Args:
            kwargs

        Returns:

        """

        self.es.indices.create(**kwargs)

    def delete_index(self, **kwargs):
        """delete index

        Args:
            kwargs

        Returns:

        """

        self.es.indices.delete(**kwargs)

    def create_doc(self, **kwargs):
        """create doc

        Args:
            kwargs

        Returns:

        """

        self.es.create(**kwargs)

    def create_doc_bulk(self, file: str, index: str):
        """bulk create doc by taking the metadata.out file produced by index builder

        Args:
            file: str, path to metadata.out file
            index: str, elastic search index

        Returns:

        """

        with open(file, "r") as f:
            bulk(self.es, self.make_documents(f, index))

    def current_global_datamart_id(self, **kwargs) -> int:
        """get current_global_datamart_id from the count of doc in es index

        Args:
            kwargs

        Returns:
            integer
        """

        max_idx_query = json.dumps({
            "aggs": {
                "max_id": {
                    "max": {
                        "field": "datamart_id"
                    }
                }
            },
            "size": 0
        })
        result = self.es.search(index=kwargs["index"], body=max_idx_query)
        return result["aggregations"]["max_id"]["value"] if result["aggregations"]["max_id"][
            "value"] else 0

    @staticmethod
    def make_documents(f, index: str):
        """make documents for bulk load to es

        Args:
            f: file io
            index: es index

        Returns:

        """

        while True:
            line = f.readline()
            if not line:
                break
            idx = int(line.strip())
            line = f.readline()
            doc = {
                '_op_type': 'create',
                '_index': index,
                '_type': "document",
                '_source': line.strip(),
                '_id': idx,
            }
            yield doc
