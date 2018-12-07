
import pandas as pd
import typing
import rltk
from rltk.io.reader.dataframe_reader import DataFrameReader


class DataFrameWrapper(object):
    """
    Wrap useful info of a dataset together: dataframe, metadata, columns for joining;
    Dynamically implement the rltk Record subclass;
    Init the columns for joining to FeatureBase subclasses, to generate proper similarity functions .

    TODO: link to FeatureBase
    """
    def __init__(self, pd_dataframe: pd.DataFrame, columns: typing.List[typing.List[int]], metadata: dict):
        self._pd_dataframe = pd_dataframe
        self._columns = columns
        self._metadata = metadata

        self._all_headers = list(self._pd_dataframe.columns.values)
        self._columns_headers = [[self._all_headers[_] for _ in idxs] for idxs in self.columns]
        self._merged_columns = ['|'.join(map(str, cols)) for cols in self._columns]
        self._merged_columns_headers = ['|'.join(cols) for cols in self._columns_headers]

        self._record_class = None
        self._rltk_dataset = None
        self.init_record()

    @property
    def pd_dataframe(self):
        return self._pd_dataframe

    @property
    def columns(self):
        return self._columns

    @property
    def metadata(self):
        return self._metadata

    @property
    def columns_headers(self):
        return self._columns_headers

    @property
    def merged_columns_headers(self):
        return self._merged_columns_headers

    @property
    def rltk_dataset(self):
        return self._rltk_dataset

    def init_record(self):
        properties = {'id':  property(lambda _self: str(_self.raw_object['dataframe_default_index']))}
        for i in range(len(self.columns_headers)):
            headers = self.columns_headers[i]
            merged_header = self._merged_columns_headers[i]
            properties[merged_header] = property(lambda _self: ''.join([_self.raw_object[header] for header in headers]))
        self._record_class = type('SubClass', (rltk.Record,), properties)
        self._rltk_dataset = rltk.Dataset(reader=DataFrameReader(self._pd_dataframe, True), record_class=self._record_class)