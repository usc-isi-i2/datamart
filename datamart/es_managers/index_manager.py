from elasticsearch.helpers import bulk
import json
import typing
from datamart.es_managers.es_manager import ESManager


class IndexManager(ESManager):

    def __init__(self, es_host: str = "dsbox02.isi.edu", es_port: int = 9200) -> None:
        """Init method for index manager

        Args:
            es_host: str, Elasticsearch host
            es_port: int, Elasticsearch port

        Returns:

        """
        super().__init__(es_host=es_host, es_port=es_port)

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

    def create_index(self, **kwargs) -> None:
        """create index

        Args:
            kwargs

        Returns:

        """

        self.es.indices.create(**kwargs)

    def delete_index(self, **kwargs) -> None:
        """delete index

        Args:
            kwargs

        Returns:

        """

        self.es.indices.delete(**kwargs)

    def create_doc(self, **kwargs) -> None:
        """create doc

        Args:
            kwargs

        Returns:

        """

        self.es.create(**kwargs, ignore=[400, 404])

    def update_doc(self, **kwargs) -> None:
        """create doc

        Args:
            kwargs

        Returns:

        """

        self.es.update(**kwargs)

    def create_doc_bulk(self, file: str, index: str) -> None:
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

        max_idx_query = json.dumps(
            {
                "aggs": {
                    "max_id": {
                        "max": {
                            "field": "datamart_id"
                        }
                    }
                },
                "size": 0
            }
        )
        result = self.es.search(index=kwargs["index"], body=max_idx_query)
        return int(result["aggregations"]["max_id"]["value"]) if result["aggregations"]["max_id"][
            "value"] else 0

    @staticmethod
    def make_documents(f, index: str) -> typing.Iterator[dict]:
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
                '_index': index,
                '_type': "document",
                '_source': line.strip(),
                '_id': idx,
            }
            yield doc
