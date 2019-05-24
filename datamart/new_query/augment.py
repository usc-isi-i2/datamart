from datamart.profiler import Profiler
import pandas as pd
import typing
import warnings
import traceback
from datamart.utilities.utils import Utils
from datamart.joiners.joiner_base import JoinerPrepare, JoinerType
from datamart.joiners.join_result import JoinResult
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from itertools import chain


class Augment(object):

    def __init__(self, endpoint: str) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            endpoint: query endpoint address

        Returns:

        """

        self.qm = SPARQLWrapper(endpoint)
        self.qm.setReturnFormat(JSON)
        self.qm.setMethod(POST)
        self.qm.setRequestMethod(URLENCODED)

        self.joiners = dict()
        self.profiler = Profiler()

    def query_by_sparql(self, json_query: dict, dataset: pd.DataFrame=None, **kwargs) -> typing.Optional[typing.List[dict]]:
        """
        Args:
            json_query:
            dataset:
            **kwargs:

        Returns:

        """

        if json_query:
            query_body = self.parse_sparql_query(json_query, dataset)
            if query_body:
                self.qm.setQuery(query_body)
                try:
                    results = self.qm.query()
                except:
                    print("Query failed!")
                    traceback.print_exc()
                    return []
                return results

    def parse_sparql_query(self, json_query, dataset) -> str:
        """
        parse the json query to a spaqrl query format
        :param json_query:
        :param dataset:
        :return: a string indicate the sparql query
        """
        spaqrl_query = ""

        return spaqrl_query


    def query(self,
              col: pd.Series = None,
              minimum_should_match_ratio_for_col: float = None,
              query_string: str = None,
              temporal_coverage_start: str = None,
              temporal_coverage_end: str = None,
              global_datamart_id: int = None,
              variable_datamart_id: int = None,
              key_value_pairs: typing.List[tuple] = None,
              **kwargs
              ) -> typing.Optional[typing.List[dict]]:

        """Query metadata by a pandas Dataframe column

        Args:
            col: pandas Dataframe column.
            minimum_should_match_ratio_for_col: An float ranges from 0 to 1
                indicating the ratio of unique value of the column to be matched
            query_string: string to query any field in metadata
            temporal_coverage_start: start of a temporal coverage
            temporal_coverage_end: end of a temporal coverage
            global_datamart_id: match a global metadata id
            variable_datamart_id: match a variable metadata id
            key_value_pairs: match key value pairs

        Returns:
            matching docs of metadata
        """

        queries = list()

        if query_string:
            queries.append(
                self.qm.match_any(query_string=query_string)
            )

        if temporal_coverage_start or temporal_coverage_end:
            queries.append(
                self.qm.match_temporal_coverage(start=temporal_coverage_start, end=temporal_coverage_end)
            )

        if global_datamart_id:
            queries.append(
                self.qm.match_global_datamart_id(datamart_id=global_datamart_id)
            )

        if variable_datamart_id:
            queries.append(
                self.qm.match_variable_datamart_id(datamart_id=variable_datamart_id)
            )

        if key_value_pairs:
            queries.append(
                self.qm.match_key_value_pairs(key_value_pairs=key_value_pairs)
            )

        if col is not None:
            queries.append(
                self.qm.match_some_terms_from_variables_array(terms=col.unique().tolist(),
                                                              minimum_should_match=minimum_should_match_ratio_for_col)
            )

        if not queries:
            return self._query_all()

        return self.qm.search(body=self.qm.form_conjunction_query(queries), **kwargs)

    def join(self,
             left_df: pd.DataFrame,
             right_df: pd.DataFrame,
             left_columns: typing.List[typing.List[int]],
             right_columns: typing.List[typing.List[int]],
             left_metadata: dict = None,
             right_metadata: dict = None,
             joiner: JoinerType = JoinerType.DEFAULT
             ) -> JoinResult:

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
               JoinResult
          """

        if joiner not in self.joiners:
            self.joiners[joiner] = JoinerPrepare.prepare_joiner(joiner=joiner)

        if not self.joiners[joiner]:
            warnings.warn("No suitable joiner, return original dataframe")
            return JoinResult(left_df, [])

        print(" - start profiling")
        if not (left_metadata and left_metadata.get("variables")):
            # Left df is the user provided one.
            # We will generate metadata just based on the data itself, profiling and so on
            left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=left_metadata)

        if not right_metadata:
            right_metadata = Utils.generate_metadata_from_dataframe(data=right_df)

        # Only profile the joining columns, otherwise it will be too slow:
        left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
                                                       selected_columns=set(chain.from_iterable(left_columns)))

        right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
                                                        selected_columns=set(chain.from_iterable(right_columns)))

        # update with implicit_variable on the user supplied dataset
        if left_metadata.get('implicit_variables'):
            Utils.append_columns_for_implicit_variables_and_add_meta(left_metadata, left_df)

        print(" - start joining tables")
        res = self.joiners[joiner].join(left_df=left_df,
                                         right_df=right_df,
                                         left_columns=left_columns,
                                         right_columns=right_columns,
                                         left_metadata=left_metadata,
                                         right_metadata=right_metadata,
                                         )

        return res
