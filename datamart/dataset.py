from datamart.utilities.utils import Utils


class Dataset:
    """
    Represents a retrieved dataset from datamart, in a search query.
    Contains the meta info and the way to materialize the dataset.
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API
    """
    def __init__(self, es_raw_object):
        self.__es_raw_object = es_raw_object
        self._metadata = es_raw_object['_source']
        self._score = es_raw_object['_score']
        self._id = es_raw_object['_id']
        self._matched_cols = []

    def materialize(self):
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

    def set_match(self, left_cols, right_cols):
        if len(left_cols) == len(right_cols):
            self._matched_cols = (left_cols, right_cols)
