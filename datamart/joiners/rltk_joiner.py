import pandas as pd
import typing
from datamart.joiners.joiner_base import JoinerBase
from datamart.joiners.dataframe_wrapper import DataFrameWrapper
import rltk
from rltk.similarity.levenshtein import levenshtein_similarity


"""
TODO
Implement RLTK joiner
"""


def get_feature_pairs(dfw1: DataFrameWrapper, dfw2: DataFrameWrapper):
    l1 = len(dfw1.merged_columns_headers)
    l2 = len(dfw2.merged_columns_headers)
    if l1 == l2:
        return [(dfw1.merged_columns_headers[i], dfw2.merged_columns_headers[i]) for i in range(l1)]


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
        # print(left_metadata)

        # step 1 : transform columns
        """
        1. merge columns if multi-multi relation, mark as "merged" so we use set-based functions only
        2. pull out the mapped columns and form to new datasets with same order to support
        """

        left = DataFrameWrapper(left_df, left_columns, left_metadata)
        right = DataFrameWrapper(right_df, right_columns, right_metadata)

        pairs = rltk.get_record_pairs(left.rltk_dataset, right.rltk_dataset)
        headers = get_feature_pairs(left, right)
        for r1, r2 in pairs:
            for h1, h2 in headers:
                print(h1, h2)
                print('levenshtein_similarity:', levenshtein_similarity(getattr(r1, h1), getattr(r2, h2)))


        # step 2 : analyze target columns - get ranked similarity functions for each columns
        """
        see https://paper.dropbox.com/doc/ER-for-Datamart--ASlKtpR4ceGaj~6cN4Q7EWoSAQ-tRug6oRX6g5Ko5jzaeynT 
        """


        # step 3 : check if 1-1, 1-n, n-1, or m-n relations,
        # based on the analyze in step 2 we can basically know if it is one-to-one relation
        print(left_df)
        print(right_df)

        print(left_columns)
        print(right_columns)

        return left_df
