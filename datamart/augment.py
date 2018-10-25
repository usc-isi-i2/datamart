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

    def query_by_column(self, col: pd.Series, minimum_should_match: int = None) -> typing.Optional[typing.List[dict]]:
        """Query metadata by a pandas Dataframe column

        Args:
            col: pandas Dataframe column.
            minimum_should_match: An integer ranges from 0 to length of unique value in col.
            Represent the minimum number of terms should match.

        Returns:
            matching docs of metadata
        """

        return self.qm.match_some_terms_from_array(terms=col.unique().tolist(),
                                                   minimum_should_match=minimum_should_match)

    def query_by_named_entities(self,
                                named_entities: list,
                                minimum_should_match: int = None
                                ) -> typing.Optional[typing.List[dict]]:
        """Query metadata by a pandas Dataframe column

        Args:
            named_entities: list of named entities
            minimum_should_match: An integer ranges from 0 to length of named entities list.
            Represent the minimum number of terms should match.

        Returns:
            matching docs of metadata
        """

        return self.qm.match_some_terms_from_array(
            terms=named_entities,
            key="variables.named_entity.keyword",
            minimum_should_match=minimum_should_match)

    def query_by_datamart_id(self, datamart_id: int) -> typing.Optional[typing.List[dict]]:
        """Query metadata by datamart id

        Args:
            datamart_id: int

        Returns:
            matching docs of metadata
        """

        return self.qm.match_datamart_id(datamart_id=datamart_id)

    def query_by_key_value_pairs(self, key_value_pairs: typing.List[tuple]) -> typing.Optional[typing.List[dict]]:
        """Query metadata by datamart id

        Args:
            key_value_pairs: list of key value tuple

        Returns:
            matching docs of metadata
        """

        return self.qm.match_key_value_pairs(key_value_pairs=key_value_pairs)

    def query_by_es_query(self, body, **kwargs) -> typing.Optional[typing.List[dict]]:
        """Query metadata by an elastic search query

        Args:
            body: query body
            kwargs: key value args

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
