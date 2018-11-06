import pandas as pd
import dateutil.parser
import typing
from pandas.api.types import is_string_dtype


class Profiler(object):
    def __init__(self):
        pass

    @staticmethod
    def profile_named_entity(column: pd.Series) -> typing.List[str]:
        """Profiling this named entities column, use when this column is marked as a named entities column.

        Args:
            column: pandas Series column.

        Returns:
            list of named entities string
        """

        return column.unique().tolist()

    @staticmethod
    def named_entity_recognize(column: pd.Series) -> typing.Union[typing.List[str], bool]:
        """Ideally run a NER on the column and profile.

        Args:
            column: pandas Series column.

        Returns:
            list of unique named entities strings in this column if contains named entity
            False if not a named entity column
        """

        if is_string_dtype(column):
            return column.unique().tolist()
        return False

    @staticmethod
    def profile_temporal_coverage(column: pd.Series, coverage: dict = None) -> typing.Union[dict, bool]:
        """Profiling this temporal column .

        Args:
            coverage: temporal coverage dict
            column: pandas Series column.

        Returns:
            temporal coverage dict
            False if there is no temporal related element detected
        """

        temporal_count = 0
        min_datetime = None
        max_datetime = None
        if len(column) == 0:
            return {
                "start": None,
                "end": None
            }

        for element in column:
            try:
                if isinstance(element, str):
                    this_datetime = dateutil.parser.parse(element)
                else:
                    if len(str(element)) == 4:
                        this_datetime = dateutil.parser.parse(str(element))
                    else:
                        break
                temporal_count += 1
                if not min_datetime:
                    min_datetime = this_datetime
                min_datetime = min(min_datetime, this_datetime)
                if not max_datetime:
                    max_datetime = this_datetime
                max_datetime = max(max_datetime, this_datetime)
            except:
                pass

        if not min_datetime and not max_datetime:
            return False

        if temporal_count < len(column)//2:
            return False

        if not coverage:
            coverage = {
                "start": None,
                "end": None
            }

        if not coverage['start']:
            coverage['start'] = min_datetime.isoformat()
        if not coverage['end']:
            coverage['end'] = max_datetime.isoformat()
        return coverage

    @staticmethod
    def construct_variable_description(column: pd.Series) -> str:
        """construct description for this variable .

        Args:
            column: pandas Series column.

        Returns:
            string of description
        """

        ret = "column name: {}, dtype: {}".format(
            column.name, column.dtype
        )
        return ret

    @staticmethod
    def construct_global_title(data: pd.DataFrame) -> str:
        """construct title for this dataset .

        Args:
            data: pandas dataframe.

        Returns:
            string of title
        """

        return " ".join(data.columns.tolist())

    @staticmethod
    def construct_global_description(data: pd.DataFrame) -> str:
        """construct description for this dataset .

        Args:
            data: pandas dataframe.

        Returns:
            string of description
        """

        return ", ".join(["{} : {}".format(data.columns[i], data.dtypes[i]) for i in range(data.shape[1])])

    @staticmethod
    def construct_global_keywords(data: pd.DataFrame) -> str:
        """construct keywords for this dataset .

        Args:
            data: pandas dataframe.

        Returns:
            list of keywords
        """

        return data.columns.tolist()
