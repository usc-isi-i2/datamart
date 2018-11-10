from datamart.augment import Augment
from datamart.utils import Utils
import pandas as pd
import json

"""
This is not well implemented.
Join is difficult, we are not able to fully automate it currently, implement some simple join 
"""


class JoinDatasets(object):

    def __init__(self, es_index="datamart"):
        self.augument = Augment(es_index=es_index)

    def default_join(self, request):

        # print(request.form, request.files)
        query_data = json.loads(request.form['data'])
        selected_metadata = query_data["selected_metadata"]

        old_df = pd.read_csv(request.files['file']).infer_objects()

        offset_and_matched_queries = Utils.get_offset_and_matched_queries_from_variable_metadata(
            metadata=selected_metadata)

        if not offset_and_matched_queries:
            return old_df.to_csv()

        if "constrains" in query_data:
            try:
                constrains = query_data["constrains"]
            except:
                constrains = None
        else:
            constrains = {}

        constrains["named_entity"] = {}
        for offset, matched_queries in offset_and_matched_queries:
            constrains["named_entity"][offset] = matched_queries

        new_df = self.augument.get_dataset(
            metadata=selected_metadata["_source"],
            constrains=constrains
        )

        df = self.augument.join(
            left_df=old_df,
            right_df=new_df,
            left_columns=[int(x) for x in query_data["old_df_column_ids"]],
            right_columns=[offset for offset, _ in offset_and_matched_queries]
        )

        return df.to_csv()
