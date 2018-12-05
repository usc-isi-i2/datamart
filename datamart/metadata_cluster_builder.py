from datamart.es_managers.index_manager import IndexManager
from datamart.metadata.metadata_cluster import MetadataCluster
from datamart.augment import Augment
import hashlib


class MetadataClusterBuilder(object):

    def __init__(self,
                 metadata_es_index,
                 cluster_es_index: str = "datamart_cluster",
                 es_host: str = "dsbox02.isi.edu",
                 es_port: int = 9200):

        self.im = IndexManager()
        self.metadata_es_index = metadata_es_index
        self.cluster_es_index = cluster_es_index
        self.augment = Augment(es_index=self.metadata_es_index,
                               cluster_es_index=self.cluster_es_index,
                               es_host=es_host,
                               es_port=es_port)

    def index_metadata_cluster(self,
                               metadata_cluster: dict,
                               delete_old_es_index: bool = False
                               ) -> None:
        """Create es index, delete old one if necessary

        Args:
            metadata_cluster
            cluster_es_index: str, es index for this dataset
            delete_old_es_index: bool, boolean if delete original es index if it exist

        Returns:

        """

        if not self.im.check_exists(index=self.cluster_es_index):
            self.im.create_index(index=self.cluster_es_index)
        elif delete_old_es_index:
            self.im.delete_index(index=[self.cluster_es_index])
            self.im.create_index(index=self.cluster_es_index)

        self.im.create_doc(index=self.cluster_es_index,
                           doc_type='_doc',
                           body=metadata_cluster,
                           id=int(hashlib.sha256(metadata_cluster["source"].encode('utf-8')).hexdigest(), 16) % 10 ** 8)

    def get_cluster_docs_by_source(self, source: str):
        return [x["_source"] for x in self.augment.query(key_value_pairs=[
            ("provenance.source", source)
        ])]

    def get_metadata_cluster_by_source(self, source: str):
        metadata_cluster = MetadataCluster.construct_global(docs=self.get_cluster_docs_by_source(source=source)).value
        metadata_cluster["source"] = source
        return metadata_cluster
