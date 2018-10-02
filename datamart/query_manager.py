from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datamart.utils import Utils
import os
import json


class QueryManager(object):

    def __init__(self, es_host="dsbox02.isi.edu", es_port=9200):
        resources_path = os.path.join(os.path.dirname(__file__), "resources")
        self.index_config = json.load(open(os.path.join(resources_path, 'index_info.json'), 'r'))
        if es_host and es_port:
            self.es = Elasticsearch([{'host': es_host, 'port': es_port}])
        else:
            self.es = Elasticsearch([{'host': self.index_config["es_host"], 'port': self.index_config["es_port"]}])

    def create_index(self, **kwargs):
        self.es.indices.create(**kwargs)

    def delete_index(self, **kwargs):
        self.es.indices.delete(**kwargs)

    def create_doc(self, **kwargs):
        self.es.create(**kwargs)

    def create_doc_bulk(self, file, index):
        with open(file, "r") as f:
            bulk(self.es, self.make_documents(f, index))

    def search(self, index, body):
        result = self.es.search(index=index, body=body)
        if result["hits"]["total"] <= 0:
            print("Nothing found")
            return None
        else:
            return [doc["_source"] for doc in result["hits"]["hits"]]

    @staticmethod
    def get_dataset(metadata):
        materializer_module = metadata["materialization"]["python_path"]
        materializer = Utils.load_materializer(materializer_module)
        data = materializer.get(metadata=metadata, variables=None, constrains=None)
        return data

    @staticmethod
    def make_documents(f, index):
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
