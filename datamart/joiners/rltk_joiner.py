import pandas as pd
import typing
from datamart.joiners.joiner_base import JoinerBase
from datamart.joiners.join_feature.feature_pairs import FeaturePairs
import rltk
import json
from munkres import Munkres

"""
TODO
Implement RLTK joiner
"""


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

        fp = FeaturePairs(left_df, right_df, left_columns, right_columns, left_metadata, right_metadata)
        record_pairs = rltk.get_record_pairs(fp.left_rltk_dataset, fp.right_rltk_dataset)
        sim = [[0 for __ in range(len(right_df))] for _ in range(len(left_df))]

        for r1, r2 in record_pairs:
            similarities = []
            for f1, f2 in fp.pairs:
                v1 = f1.value_merge_func(r1)
                v2 = f2.value_merge_func(r2)
                # print(v1, v2, type(f1), type(f2))
                for similarity_func in f1.similarity_functions():
                    similarity = similarity_func(v1, v2)
                    similarities.append(similarity)
                    # print(f1.name, f2.name, v1, v2, similarity, similarity_func, type(f1))
                    # TODO: now only consider the first similarity function for now
                    break
                # print(v1, v2, similarities)
            sim[int(r1.id)][int(r2.id)] = sum(similarities)/len(similarities) if similarities else 0

        matched_rows = self.simple_best_match(sim)
        res = self.one_to_one_concat(matched_rows, left_df, right_df, right_columns)

        # step 2 : analyze target columns - get ranked similarity functions for each columns
        """
        see https://paper.dropbox.com/doc/ER-for-Datamart--ASlKtpR4ceGaj~6cN4Q7EWoSAQ-tRug6oRX6g5Ko5jzaeynT
        """


        # step 3 : check if 1-1, 1-n, n-1, or m-n relations,
        # based on the analyze in step 2 we can basically know if it is one-to-one relation

        return res

    def one_to_one_concat(self, matched_rows, left_df, right_df, right_columns):
        right_remain = self.get_remain_list(right_df, right_columns)
        to_join = pd.DataFrame([right_df.iloc[i, right_remain] for i in matched_rows], index=range(len(matched_rows)))
        res = pd.concat([left_df, to_join], axis=1)
        return res

    def munkrus_match(self, sim: typing.List[typing.List[float]]):
        pass

    @staticmethod
    def simple_best_match(sim: typing.List[typing.List[float]], threshold=0.5):
        res = []
        for idx, v in enumerate(sim):
            max_val = threshold
            max_idx = None
            for idx_, v_ in enumerate(v):
                if v_ >= max_val:
                    max_idx = idx_
                    max_val = v_
            res.append(max_idx)
        return res

    @staticmethod
    def simple_best_matches(sim: typing.List[typing.List[float]], threshold=0.8):
        res = []
        for idx, v in enumerate(sim):
            cur = []
            for idx_, v_ in enumerate(v):
                if v_ >= threshold:
                    cur.append(idx_)
            res.append(cur)
        return res

    @staticmethod
    def get_remain_list(df: pd.DataFrame, columns_2d: typing.List[typing.List[int]]):
        all_columns = list(range(df.shape[1]))
        columns_1d = [item for sublist in columns_2d for item in sublist]
        remianing = [_ for _ in all_columns if _ not in columns_1d]
        return remianing



