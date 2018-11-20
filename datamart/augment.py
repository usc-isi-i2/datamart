from datamart.es_managers.query_manager import QueryManager
from datamart.profiler import Profiler
import pandas as pd
import typing
from datamart.utilities.utils import Utils
from datamart.joiners.joiner_base import JoinerPrepare
import warnings
from datetime import datetime


class Augment(object):
    DEFAULT_START_DATE = "1900-01-01T00:00:00"

    def __init__(self, es_index: str, es_host: str = "dsbox02.isi.edu", es_port: int = 9200) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            es_index: elastic search index.
            es_host: es_host.
            es_port: es_port.

        Returns:

        """

        self.qm = QueryManager(es_host=es_host, es_port=es_port, es_index=es_index)
        self.joiners = dict()
        self.profiler = Profiler()

    def query_by_column(self,
                        col: pd.Series,
                        minimum_should_match: int = None,
                        **kwargs
                        ) -> typing.Optional[typing.List[dict]]:
        """Query metadata by a pandas Dataframe column

        Args:
            col: pandas Dataframe column.
            minimum_should_match: An integer ranges from 0 to length of unique value in col.
            Represent the minimum number of terms should match.

        Returns:
            matching docs of metadata
        """

        body = self.qm.match_some_terms_from_variables_array(terms=col.unique().tolist(),
                                                             minimum_should_match=minimum_should_match)
        return self.qm.search(body=body, **kwargs)

    def query_by_named_entities(self,
                                named_entities: list,
                                minimum_should_match: int = None,
                                **kwargs
                                ) -> typing.Optional[typing.List[dict]]:
        """Query metadata by a pandas Dataframe column

        Args:
            named_entities: list of named entities
            minimum_should_match: An integer ranges from 0 to length of named entities list.
            Represent the minimum number of terms should match.

        Returns:
            matching docs of metadata
        """

        body = self.qm.match_some_terms_from_variables_array(terms=named_entities,
                                                             key="variables.named_entity",
                                                             minimum_should_match=minimum_should_match)
        return self.qm.search(body=body, **kwargs)

    def query_by_temporal_coverage(self, start=None, end=None, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Query metadata by a temporal coverage of column

        Args:
            start: dataset should cover date time earlier than the start date.
            end: dataset should cover date time later than the end date.

        Returns:
            matching docs of metadata
        """

        body = self.qm.match_temporal_coverage(start=start, end=end)
        return self.qm.search(body=body, **kwargs)

    def query_by_datamart_id(self, datamart_id: int, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Query metadata by datamart id

        Args:
            datamart_id: int

        Returns:
            matching docs of metadata
        """

        global_body = self.qm.match_global_datamart_id(datamart_id=datamart_id)
        variable_body = self.qm.match_variable_datamart_id(datamart_id=datamart_id)
        return self.qm.search(body=global_body, **kwargs) or self.qm.search(body=variable_body, **kwargs)

    def query_by_key_value_pairs(self,
                                 key_value_pairs: typing.List[tuple],
                                 **kwargs
                                 ) -> typing.Optional[typing.List[dict]]:
        """Query metadata by datamart id

        Args:
            key_value_pairs: list of key value tuple

        Returns:
            matching docs of metadata
        """

        body = self.qm.match_key_value_pairs(key_value_pairs=key_value_pairs)
        return self.qm.search(body=body, **kwargs)

    def query_any_field_with_string(self, query_string, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Query any field of matadata with query_string

        Args:
            key_value_pairs: list of key value tuple

        Returns:
            matching docs of metadata
        """

        body = self.qm.match_any(query_string=query_string)
        return self.qm.search(body=body, **kwargs)

    def query_by_es_query(self, body: str, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Query metadata by an elastic search query

        Args:
            body: query body

        Returns:
            matching docs of metadata
        """
        return self.qm.search(body=body, **kwargs)

    def get_dataset(self,
                    metadata: dict,
                    variables: list = None,
                    constrains: dict = None
                    ) -> typing.Optional[pd.DataFrame]:
        """Get the dataset with materializer.

       Args:
           metadata: metadata dict.
           variables: list of integers
           constrains:

       Returns:
            pandas dataframe
       """

        if not constrains:
            constrains = dict()

        if "date_range" not in constrains:
            constrains["date_range"] = dict()

        if not constrains["date_range"].get("start", None):
            constrains["date_range"]["start"] = Augment.DEFAULT_START_DATE

        if not constrains["date_range"].get("end", None):
            constrains["date_range"]["end"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        df = Utils.materialize(metadata=metadata, constrains=constrains)

        if variables:
            df = df.iloc[:, variables]

        if metadata.get("implicit_variables", None):
            df = self.append_columns_for_implicit_variables(metadata["implicit_variables"], df)

        return df.infer_objects()

    @staticmethod
    def get_metadata_intersection(*metadata_lst) -> list:
        """Get the intersect metadata list.

       Args:
           metadata_lst: all metadata list returned by multiple queries

       Returns:
            list of intersect metadata
       """

        metadata_dict = dict()
        metadata_sets = []
        for lst in metadata_lst:
            this_set = set()
            for x in lst:
                if x["_source"]["datamart_id"] not in metadata_dict:
                    metadata_dict[x["_source"]["datamart_id"]] = x
                this_set.add(x["_source"]["datamart_id"])
            metadata_sets.append(this_set)
        return [metadata_dict[datamart_id] for datamart_id in metadata_sets[0].intersection(*metadata_sets[1:])]

    def join(self,
             left_df: pd.DataFrame,
             right_df: pd.DataFrame,
             left_columns: typing.List[typing.List[int]],
             right_columns: typing.List[typing.List[int]],
             left_metadata: dict = None,
             right_metadata: dict = None,
             joiner: str = "default"
             ) -> typing.Optional[pd.DataFrame]:

        """Join two dataframes based on different joiner.

          Args:
              left_df: pandas Dataframe
              right_df: pandas Dataframe
              left_metadata: metadata of left dataframe
              right_metadata: metadata of right dataframe
              left_columns: list of integers from left df for join
              right_columns: list of integers from right df for join
              joiner: string of joiner, default to be "default"

          Returns:
               Dataframe
          """

        if joiner not in self.joiners:
            self.joiners[joiner] = JoinerPrepare.prepare_joiner(joiner=joiner)

        if not self.joiners[joiner]:
            warnings.warn("No suitable joiner, return original dataframe")
            return left_df

        if not left_metadata:
            # Left df is the user provided one.
            # We will generate metadata just based on the data itself, profiling and so on
            left_metadata = Utils.generate_metadata_from_dataframe(data=left_df)

        left_metadata = self.calculate_dsbox_features(data=left_df, metadata=left_metadata)
        right_metadata = self.calculate_dsbox_features(data=right_df, metadata=right_metadata)

        return self.joiners[joiner].join(left_df=left_df,
                                         right_df=right_df,
                                         left_columns=left_columns,
                                         right_columns=right_columns,
                                         left_metadata=left_metadata,
                                         right_metadata=right_metadata,
                                         )

    @staticmethod
    def append_columns_for_implicit_variables(implicit_variables: typing.List[dict], df: pd.DataFrame) -> pd.DataFrame:
        """Append implicit_variables as new column with same value across all rows of the dataframe

         Args:
             implicit_variables: list of implicit_variables in metadata
             df: Dataframe that implicit_variables will be appended on

         Returns:
              Dataframe with appended implicit_variables columns
         """

        for implicit_variable in implicit_variables:
            df[implicit_variable["name"]] = implicit_variable["value"]
        return df

    def calculate_dsbox_features(self, data: pd.DataFrame, metadata: typing.Union[dict, None]) -> dict:
        """Calculate dsbox features, add to metadata dictionary

         Args:
             data: dataset as a pandas dataframe
             metadata: metadata dict

         Returns:
              updated metadata dict
         """

        if not metadata:
            return metadata
        return self.profiler.dsbox_profiler.profile(inputs=data, metadata=metadata)
