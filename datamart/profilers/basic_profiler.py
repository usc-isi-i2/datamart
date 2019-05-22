import pandas as pd
import dateutil.parser
import typing
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.metadata.global_metadata import GlobalMetadata
from pandas.api.types import is_object_dtype


class BasicProfiler(object):
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
        return column.dropna().unique().astype(str).tolist()

    @staticmethod
    def named_entity_column_recognize(column: pd.Series) -> bool:
        """Ideally run a NER on the column and profile.

        Args:
            column: pandas Series column.

        Returns:
            True if this column is a named entity column
            False if not a named entity column
        """
        """TODO: Write a real NER here maybe"""

        if is_object_dtype(column):
            try:
                pd.to_datetime(column)
                return False
            except:
                all_ = column.dropna()
                nums = pd.to_numeric(all_, errors='coerce').dropna()
                all_ = all_.unique()
                if len(all_) == 0 or len(nums) / len(all_) > 0.5:
                    return False
                return True
        return False

    @staticmethod
    def profile_semantic_type(column: pd.Series) -> typing.List:
        # TODO: we need to check str is text or categorical here
        # when to use "https://metadata.datadrivendiscovery.org/types/CategoricalData"
        semantic_types = ["https://metadata.datadrivendiscovery.org/types/Attribute"]
        if column.dtype.name == "object":
            semantic_types.append("http://schema.org/Text")
        elif "float" in column.dtype.name:
            semantic_types.append("http://schema.org/Float")
        elif "int" in column.dtype.name:
            semantic_types.append("http://schema.org/Integer")
        return semantic_types

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
            # TODO: now pure time will be treat as "today"'s time, and generate a range
            # TODO: improve date/time detection and parse
            try:
                if isinstance(element, str):
                    this_datetime = dateutil.parser.parse(element)
                else:
                    if len(str(element)) == 4 and column.name.lower() == 'year':
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

        if temporal_count < len(column) // 2:
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

        return " ".join([str(_) for _ in data.columns.tolist()])

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

        return data.columns.astype(str).tolist()

    @classmethod
    def basic_profiling_column(cls,
                               description: dict,
                               variable_metadata: VariableMetadata,
                               column: pd.Series
                               ) -> VariableMetadata:
        """Profiling single column for necessary fields of metadata, if data is present .

        Args:
            description: description dict about the column.
            variable_metadata: the original VariableMetadata instance.
            column: the column to profile.

        Returns:
            profiled VariableMetadata instance
        """

        if not variable_metadata.name:
            variable_metadata.name = str(column.name)

        if not variable_metadata.description:
            variable_metadata.description = cls.construct_variable_description(column)

        if variable_metadata.named_entity is None:
            variable_metadata.named_entity = cls.profile_named_entity(column)

        elif variable_metadata.named_entity is False and not description:
            if cls.named_entity_column_recognize(column):
                variable_metadata.named_entity = cls.profile_named_entity(column)

        if variable_metadata.temporal_coverage is not False:
            if not variable_metadata.temporal_coverage['start'] or not variable_metadata.temporal_coverage['end']:
                variable_metadata.temporal_coverage = cls.profile_temporal_coverage(
                    column=column, coverage=variable_metadata.temporal_coverage)
        elif not description:
            temporal_coverage = cls.profile_temporal_coverage(column=column)
            if temporal_coverage:
                variable_metadata.temporal_coverage = temporal_coverage

        if not variable_metadata.semantic_type:
            variable_metadata.semantic_type = cls.profile_semantic_type(column)

        return variable_metadata

    @classmethod
    def basic_profiling_entire(cls, global_metadata: GlobalMetadata, data: pd.DataFrame) -> GlobalMetadata:
        """Profiling entire dataset for necessary fields of metadata, if data is present .

        Args:
            global_metadata: the original GlobalMetadata instance.
            data: dataframe of data.

        Returns:
            profiled GlobalMetadata instance
        """

        if not global_metadata.title:
            global_metadata.title = cls.construct_global_title(data)

        if not global_metadata.description:
            global_metadata.description = cls.construct_global_description(data)

        if not global_metadata.keywords:
            global_metadata.keywords = cls.construct_global_keywords(data)

        return global_metadata
