import pandas as pd
import dateutil.parser
import typing
import warnings


class Profiler(object):
    def __init__(self):
        pass

    @staticmethod
    def profile_named_entity(column: pd.Series) -> typing.List[str]:
        """Profiling this named entities column .

        Args:
            column: pandas Series column.

        Returns:
            list of named entities string
        """

        return column.unique().tolist()

    @staticmethod
    def profile_temporal_coverage(coverage: dict, column: pd.Series) -> dict:
        """Profiling this temporal column .

        Args:
            coverage: temporal coverage dict
            column: pandas Series column.

        Returns:
            temporal coverage dict
        """

        min_datetime = None
        max_datetime = None
        this_datetime = None
        for element in column:
            try:
                this_datetime = dateutil.parser.parse(element)
            except:
                warnings.warn("Cannot parse string as date")
                pass
            if this_datetime:
                if not min_datetime:
                    min_datetime = this_datetime
                min_datetime = min(min_datetime, this_datetime)
                if not max_datetime:
                    max_datetime = this_datetime
                max_datetime = max(max_datetime, this_datetime)

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
