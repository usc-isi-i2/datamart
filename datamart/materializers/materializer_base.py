from abc import ABC, abstractmethod
from pandas import DataFrame
import typing


class MaterializerBase(ABC):
    """Abstract class of Materializer, should be extended for every Materializer dealing with different data source.

    """

    @abstractmethod
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[DataFrame]:
        """API for Materialize, every Materializer should implement `get` method, returns a pandas dataframe.

        """
        pass
