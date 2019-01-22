from enum import Enum
import pandas as pd
from rltk import Record


class DistributeType(Enum):
    CATEGORICAL = 'categorical'
    TOKEN_CATEGORICAL = 'token_categorical'
    NON_CATEGORICAL = 'non_categorical'


class DataType(Enum):
    DATETIME = 'datetime'
    NUMBER = 'number'
    STRING = 'string'
    OTHER = 'other'


class NumberType(Enum):
    FLOAT = 'float'
    INT = 'int'
    OTHER = 'other'


class StringType(Enum):
    WORD = 'word'
    PHRASE = 'phrase'
    SENTENCE = 'sentence'
    PARAGRAPH = 'paragraph'
    OTHER = 'other'


class DatetimeResolution(Enum):
    YEAR = 'year'
    MONTH = 'month'
    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'
    SECOND = 'second'
    MILLISECOND = 'millisecond'


class FeatureBase(object):
    """
    One or more columns in a dataframe that groups together as a feature for joining(Entity Resolution) .

    Args:
        dfw: DataFrameWrapper - include the pd.dataframe, the metadata, and the columns used as joining features
        indexes: list[int] - indexes of the columns for this feature, whose values will be merged as a single value \
                 for joining
    """
    def __init__(self, df: pd.DataFrame, indexes, metadata, distribute_type, data_type):
        self._df = df
        self._indexes = indexes
        self._metadata = metadata

        self._multi_column = False if len(indexes) == 1 else True
        self._headers = [str(df.iloc[:, i].name) for i in indexes]
        self._name = '|'.join(self._headers)

        self._distribute_type = distribute_type
        self._data_type = data_type

    @property
    def multi_column(self) -> bool:
        return self._multi_column

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def distribute_type(self) -> DistributeType:
        return self._distribute_type

    @property
    def data_type(self) -> DataType:
        return self._data_type

    @property
    def name(self):
        """

        Returns: str - name of the feature, the joining of columns headers by '|', also the rltk record property name

        """
        return self._name

    def value_merge_func(self, record: Record):
        """
        Define the function to map the multiple columns values to a single feature value. Only useful when \
        self.multi_column is True

        Args:
            record: dict - the rltk record raw_object, which is a dict in {header1: value1, header2: value2 ... } \
                    representing a row of the original dataframe

        Returns: merged value as the feature value for joining

        """
        return [getattr(record, header) for header in self._headers]

    def similarity_functions(self):
        """
        Define which similarity functions should be use when compare two values of this feature, the order is \
        representing the priorities of different similarity functions .

        Returns: list of functions

        """
        def default_similarity_function(r1, r2):
            v1 = self.value_merge_func(r1)
            v2 = self.value_merge_func(r2)
            return 1 if v1 == v2 else 0

        return [default_similarity_function]
