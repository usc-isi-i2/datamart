import pandas as pd
import typing
from datamart.joiners.joiner_base import JoinerBase
import rltk
from rltk.io.reader.dataframe_reader import DataFrameReader
from rltk.similarity.levenshtein import levenshtein_similarity


"""
TODO
Implement RLTK joiner
"""


class DataFrameWrapper(object):
    def __init__(self, pd_dataframe, columns, metadata):
        self._pd_dataframe = pd_dataframe
        self._columns = columns
        self._metadata = metadata
        self._all_headers = list(self._pd_dataframe.columns.values)
        self._columns_headers = [[self._all_headers[_] for _ in idxs] for idxs in self.columns]
        self._merged_columns = ['|'.join(cols) for cols in self._columns]
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

    @@property
    def rltk_dataset(self):
        return self._rltk_dataset

    def init_record(self):
        properties = {}
        for i in range(len(self.columns)):
            merged_header = self._merged_columns_headers[i]
            properties[merged_header] = property(lambda _self: [getattr(self._record_class, self.columns_headers[i][j]) for j in self.columns[i]])
        self._record_class = type('SubClass', (rltk.Record,), properties)
        self._rltk_dataset = rltk.Dataset(reader=DataFrameReader(self._pd_dataframe), record_class=self._record_class)


class RLTKJoiner(JoinerBase):

    def __init__(self):
        pass

    def join(self,
             left_df: pd.DataFrame,
             right_df: pd.DataFrame,
             left_columns: typing.List[typing.List[int]],
             right_columns: typing.List[typing.List[int]],
             left_metadata: dict,
             right_metadata: dict,
             ) -> pd.DataFrame:
        attr_cnt = min(len(left_columns), len(right_columns))

        # step 1 : transform columns
        """
        1. merge columns if multi-multi relation, mark as "merged" so we use set-based functions only
        2. pull out the mapped columns and form to new datasets with same order to support
        """
        left = DataFrameWrapper(left_df, left_columns, left_metadata)
        right = DataFrameWrapper(right_df, right_columns, right_metadata)

        # step 2 : analyze target columns - get ranked similarity functions for each columns
        """
        1. Numbers: dsbox_profiled.ratio_of_numeric_values > 0.95 ?
          a. categorical: many duplicates, relative small/close value sets - EQUAL TO
          b. continuous: few duplicates, large and open value sets - LOW PERCENTAGE DIFF
        2. String:
          a. short(words and phrases): Levenshtein ...
          b. long phrases and sentences: Jaro-wrinkler ...
          c. long sentences and paragraphs: TFIDF ...
        3. Date time - convert to python datetime
          a. same resolution: same
          b. left < right: duplicate right
          c. left > right: aggregate right
        """


        # step 3 : check if 1-1, 1-n, n-1, or m-n relations,
        # based on the analyze in step 2 we can basically know if it is one-to-one relation

        print(left_df)
        print(right_df)

        print(left_columns)
        print(right_columns)

        pass
