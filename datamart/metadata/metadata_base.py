from abc import ABC, abstractmethod
from typing import Dict


class MetadataBase(ABC):
    """Abstract class of Metadata, should be extended for global and variable metadata.

    """

    @abstractmethod
    def __init__(self) -> None:
        self._metadata = dict()

    @property
    def value(self) -> Dict:
        """Return actual json representation of metadata that will be load to ES

        """
        return self._metadata
