"""
Abstract class definition for D3MDataMart, DatamartSearchResult and D3MJoinSpec
"""

class D3MDataMart(object):
    self.url = DEFAULT_URL

    def search(self, query=None, supplied_data=None, timeout=None, limit: int=20) -> typing.List[DatamartSearchResult]:
        """
        :param url:
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout:

        :return: list of search results of Dataset
        """
        pass

class DatamartSearchResult:
    def download(self, supplied_data) -> d3m.container.DataFrame:
        """
        download the dataFrame and corresponding metadata information of search result
        everytime call download, the DataFrame will have the exact same columns in same order
        """
        pass

    def augment(self, supplied_data):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        pass

    def get_score(self) -> float:
        pass

    def get_metadata(self) -> dict:
        pass

    def get_join_hints(self, supplied_data=None) -> typing.List[D3MJoinSpec]:
        """
        If supplied_data not given, will try to find the corresponding joing spec in supplied_data given from search
        """
        pass

    @classmethod
    def construct(cls, serialization:str) -> DatamartSearchResult:
        """
        Take into the serilized input and reconsctruct a "DatamartSearchResult"
        """
        pass

    def serialize(self) -> str:
        pass

class D3MJoinSpec:
    def __init__(self, left_columns:typing.List[typing.List[int]], right_columns:typing.List[typing.List[int]]):
        self.left_columns = left_columns
        self.right_columns = right_columns
        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns
        """
        For example:
        left_columns = [[1,2],[3]]
        right_columns = [[1],[2]]
        In this example, we have 2 join pairs
        column 1 and 2 in left dataFrame can join to the column 1 in right dataFrame
        column 3 in left dataFrame can join to the column 2 in right dataFrame
        left_dataFrame_column_names = ["id","city","state","country"]
        right_dataFrame_column_names = ["d3mIndex","city-state","country"]
        """