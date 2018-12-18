from enum import Enum
from datamart.joiners.dataframe_wrapper import DataFrameWrapper


class DistributeType(Enum):
    CATEGORICAL = 'categorical'
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
    def __init__(self, dfw: DataFrameWrapper, indexes):
        self._dfw = dfw
        self._indexes = indexes
        self._headers = [dfw.columns_headers[i] for i in indexes]

        self._name = '|'.join(self._headers)
        self.multi_column = False if len(indexes) == 1 else True

        # types
        self.distribute_type = self.get_distribute_type()
        self.data_type = self.get_data_type()

    @property
    def merged_name(self):
        """

        Returns: str - name of the feature, the joining of columns headers by '|', also the rltk record property name

        """
        return self._name

    def value_merge_func(self, record: dict):
        """
        Define the function to map the multiple columns values to a single feature value. Only useful when \
        self.multi_column is True

        Args:
            record: dict - the rltk record raw_object, which is a dict in {header1: value1, header2: value2 ... } \
                    representing a row of the original dataframe

        Returns: merged value as the feature value for joining

        """
        return [record[header] for header in self._headers]

    def get_distribute_type(self):
        """TODO
        Look at the value distribution of this feature(make use of the profiler), e.g. check if the feature values \
        are categorical or not .

        Returns: DistributeType

        """
        return DistributeType.CATEGORICAL

    def get_data_type(self):
        """TODO
        Look at the value data type of this feature(make use of the profiler), e.g. check if the feature values are \
        numbers, or strings, or dates .

        Returns: DataType

        """
        return DataType.NUMBER

    def similarity_functions(self):
        """
        Define which similarity functions should be use when compare two values of this feature, the order is \
        representing the priorities of different similarity functions .

        Returns: functions generator

        """
        pass