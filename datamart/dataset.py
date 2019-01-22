from datamart.utilities.utils import Utils
from pandas import DataFrame
from datamart.es_managers.json_query_manager import JSONQueryManager
import typing


class Dataset:
    """
    Represents a retrieved dataset from datamart, in a search query.
    Contains the meta info and the way to materialize the dataset.
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API
    """
    def __init__(self, es_raw_object, original_data, query_json):
        self.__es_raw_object = es_raw_object
        self._metadata = es_raw_object['_source']
        self._score = es_raw_object['_score']
        self._id = es_raw_object['_id']
        self._matched_cols = []
        self._inner_hits = es_raw_object.get('inner_hits', {})

        self._original_data = original_data
        self._query_json = query_json

        try:
            self.auto_set_match()
        except Exception as e:
            print(str(e))

    def materialize(self) -> typing.Optional[DataFrame]:
        return Utils.materialize(metadata=self.metadata)

    def download(self, destination: str) -> str:
        """
        Materializes the dataset on disk.

        Args:
            destination: the file path where the user would like to save the concrete dataset

        Returns:

        """
        data = self.materialize()
        try:
            data.to_csv(destination, index=False)
            return "Success"
        except Exception as e:
            return "Failed: %s" % str(e)

    @property
    def id(self):
        """
        es _id

        Returns:

        """
        return self._id

    @property
    def inner_hits(self):
        """
        es _id

        Returns:

        """
        return self._inner_hits

    @property
    def _es_raw_object(self):
        """
        es _id

        Returns:

        """
        return self.__es_raw_object

    @property
    def metadata(self):
        """
        Contains the metadata for the dataset, in D3M dataset schema.

        Returns:

        """
        return self._metadata

    @property
    def score(self):
        """
        Floating-point value measuring how well this dataset matches the query parameters. Higher is better.

        Returns:

        """
        return self._score

    @property
    def match(self):
        """
        (TODO better name?)
        Metadata indicating which column of this dataset matches which requested column from the query. \
        This explains why this dataset matches the query, and can be used for joining.

        Returns:

        """
        return None

    @property
    def matched_cols(self):
        """
        (TODO better name?)
        Metadata indicating which column of this dataset matches which requested column from the query. \
        This explains why this dataset matches the query, and can be used for joining.

        Returns:

        """
        return self._matched_cols

    @property
    def original_data(self):
        return self._original_data

    @property
    def query_json(self):
        return self._query_json

    def set_match(self, left_cols, right_cols):
        if len(left_cols) == len(right_cols):
            self._matched_cols = (left_cols, right_cols)

    def auto_set_match(self):
        used = set()
        left = []
        right = []
        for key_path, outer_hits in self.inner_hits.items():
            vars_type, index, ent_type = key_path.split('.')
            if vars_type != 'required_variables':
                continue
            left_index = []
            right_index = []
            index = int(index)
            if ent_type == JSONQueryManager.DATAFRAME_COLUMNS:
                if self.query_json[vars_type][index].get('index'):
                    left_index = self.query_json[vars_type][index].get('index')
                elif self.query_json[vars_type][index].get('names'):
                    left_index = [self.original_data.columns.tolist().index(idx)
                                  for idx in self.query_json[vars_type][index].get('names')]

                inner_hits = outer_hits.get('hits', {})
                hits_list = inner_hits.get('hits')
                if hits_list:
                    for hit in hits_list:
                        offset = hit['_nested']['offset']
                        if offset not in used:
                            right_index.append(offset)
                            used.add(offset)
                            break

            if left_index and right_index:
                left.append(left_index)
                right.append(right_index)

        if left and right:
            self._matched_cols = (left, right)






