import pandas as pd
import typing
from datamart.joiners.joiner_base import JoinerBase
from datamart.joiners.join_feature.feature_pairs import FeaturePairs
import rltk


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
        sim = {}

        for r1, r2 in record_pairs:
            if r1.id not in sim:
                sim[r1.id] = {}
            similarities = []
            for f1, f2 in fp.pairs:
                v1 = f1.value_merge_func(r1)
                v2 = f2.value_merge_func(r2)
                for similarity_func in f1.similarity_functions():
                    similarity = similarity_func(v1, v2)
                    similarities.append(similarity)
                    # print(f1.name, f2.name, v1, v2, similarity, similarity_func, type(f1))
                    # TODO: now only consider the first similarity function for now
                    break
            sim[r1.id][r2.id] = sum(similarities)/len(similarities) if similarities else 0

        print(self.simple_best_match(sim))




        # left = DataFrameWrapper(left_df, left_columns, left_metadata)
        # right = DataFrameWrapper(right_df, right_columns, right_metadata)
        #
        # pairs = rltk.get_record_pairs(left.rltk_dataset, right.rltk_dataset)
        # headers = get_feature_pairs(left, right)
        # for r1, r2 in pairs:
        #     for h1, h2 in headers:
        #         print(h1, h2)
        #         print('levenshtein_similarity:', levenshtein_similarity(getattr(r1, h1), getattr(r2, h2)))


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

    def simple_best_match(self, sim: dict):
        res = []
        for k, v in sim.items():
            max_sim_r2 = max(v, key=v.get)
            res.append(max_sim_r2)
        return res


