import pandas as pd


class ParseResult(object):
    def __init__(self, df: pd.DataFrame, index: int=None, name: str=None, metadata: dict=None):
        self._dataframe = df
        self._index = index
        self._name = name
        self._metadata = metadata

    @property
    def dataframe(self):
        return self._dataframe

    @property
    def index(self):
        return self._index

    @property
    def name(self):
        return self._name

    @property
    def metadata(self):
        return self._metadata
