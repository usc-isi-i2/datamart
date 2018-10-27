from abc import ABC, abstractmethod
from elasticsearch import Elasticsearch


class ESManager(ABC):
    """Abstract class of ESManager, should be extended for every elasticsearch manager.

    """

    @abstractmethod
    def __init__(self, es_host, es_port) -> None:
        """Init method for index manager

        Args:
            es_host: str, Elasticsearch host
            es_port: int, Elasticsearch port

        Returns:

        """
        self.es = Elasticsearch([{'host': es_host, 'port': es_port}])
