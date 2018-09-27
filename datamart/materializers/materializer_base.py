from abc import ABC, abstractmethod
import pandas as pd


class MaterializerBase(ABC):
    """Abstract class of Materializer, should be extended for every Materializer dealing with different data source.

    """

    @abstractmethod
    def get(self, *args, **kwargs) -> pd.DataFrame:
        """API for Materialize, every Materializer should implement `get` methods, returns a pandas dataframe.

        """
        pass
