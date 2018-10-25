from datamart.es_managers.es_manager import ESManager
from datamart.utils import Utils
import typing
from pandas import DataFrame


class QueryManager(ESManager):

    def __init__(self, es_host, es_port):
        super().__init__(es_host=es_host, es_port=es_port)

    def search(self, index, body, size=1000, from_index=0) -> typing.Optional[typing.List[dict]]:
        result = self.es.search(index=index, body=body, size=size, from_=from_index)
        if result["hits"]["total"] <= 0:
            print("Nothing found")
            return None
        else:
            return [doc["_source"] for doc in result["hits"]["hits"]]

    @staticmethod
    def get_dataset(metadata, variables=None, constrains=None) -> typing.Optional[DataFrame]:
        materializer = Utils.load_materializer(metadata["materialization"]["python_path"])
        return materializer.get(metadata=metadata, variables=variables, constrains=constrains)
