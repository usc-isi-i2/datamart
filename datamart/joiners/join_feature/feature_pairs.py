from datamart.joiners.join_feature.feature_factory import *
import typing
import rltk
from rltk.io.reader.dataframe_reader import DataFrameReader


class FeaturePairs:
    def __init__(self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        left_columns: typing.List[typing.List[int]],
        right_columns: typing.List[typing.List[int]],
        left_metadata: dict,
        right_metadata: dict,
    ):
        l1 = len(left_columns)
        l2 = len(right_columns)
        if not (l1 == l2 and l1 and l2):
            # TODO: throw error or warning
            return

        self._length = l1

        self._left_df = left_df
        self._right_df = right_df
        self._left_columns = left_columns
        self._right_columns = right_columns
        self._left_metadata = left_metadata
        self._right_metadata = right_metadata

        self._left_rltk_dataset = self.init_rltk_dataset(left_df, left_columns)
        self._right_rltk_dataset = self.init_rltk_dataset(right_df, right_columns)

    def __len__(self):
        return self._length

    def __iter__(self):
        return self.__next__()

    def __next__(self) -> (FeatureBase, FeatureBase):
        for i in range(self._length):
            yield FeatureFactory.create(self._left_df, self._left_columns[i], self._left_metadata),\
                  FeatureFactory.create(self._right_df, self._right_columns[i], self._right_metadata)

    def init_rltk_dataset(self, df, columns):
        all_headers = list(df.columns.values)
        properties = {'id':  property(lambda _self: str(_self.raw_object['dataframe_default_index']))}

        for i in range(self._length):
            headers = [all_headers[_] for _ in columns[i]]
            merged_header = merge_headers(headers)
            properties[merged_header] = property(lambda _self: ''.join([_self.raw_object[header] for header in headers]))
        record_class = type('SubClass', (rltk.Record,), properties)
        rltk_dataset = rltk.Dataset(reader=DataFrameReader(df, True), record_class=record_class)
        return rltk_dataset

