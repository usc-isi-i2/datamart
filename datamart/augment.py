from datamart.es_managers.query_manager import QueryManager
import pandas as pd
import typing
from datamart.utils import Utils


class Augment(object):

    def __init__(self, es_index: str, es_host: str = "dsbox02.isi.edu", es_port: int = 9200) -> None:
        """Init method of QuerySystem, set up connection to elastic search.

        Args:
            es_index: elastic search index.
            es_host: es_host.
            es_port: es_port.

        Returns:

        """

        self.qm = QueryManager(es_host=es_host, es_port=es_port, es_index=es_index)

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

    @staticmethod
    def get_dataset(metadata: dict, variables: list = None, constrains: dict = None) -> typing.Optional[pd.DataFrame]:
        """Get the dataset with materializer.

       Args:
           metadata: metadata dict.
           variables:
           constrains:

       Returns:
            pandas dataframe
       """

        return Utils.materialize(metadata=metadata, variables=variables, constrains=constrains)

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
