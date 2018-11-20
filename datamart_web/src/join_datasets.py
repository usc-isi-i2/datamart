from datamart.augment import Augment
from datamart.utilities.utils import Utils
import json

"""
This is not well implemented.
Join is difficult, we are not able to fully automate it currently, implement some simple join 
"""


class JoinDatasets(object):

    def __init__(self, es_index="datamart"):
        self.augument = Augment(es_index=es_index)

    def default_join(self, request, old_df):

        # print(request.form, request.files)
        query_data = json.loads(request.form['data'])
        selected_metadata = query_data["selected_metadata"]
        columns_mapping = query_data["columns_mapping"]

        # Get which string in which column got matched, use it to construct constrains
        offset_and_matched_queries = Utils.get_offset_and_matched_queries_from_variable_metadata(
            metadata=selected_metadata)

        if not offset_and_matched_queries:
            return json.dumps({
                "message": "Default join should perform after default search using default search result"
            })

        if "constrains" in query_data:
            try:
                constrains = query_data["constrains"]
            except:
                constrains = {}
        else:
            constrains = {}

        constrains["named_entity"] = {}

        for offset, matched_queries in offset_and_matched_queries:
            constrains["named_entity"][offset] = matched_queries

        try:
            new_df = self.augument.get_dataset(
                metadata=selected_metadata["_source"],
                constrains=constrains
            )
        except:
            return json.dumps({
                "message": "Failed to join, not getting complementary dataset"
            })

        try:
            df = self.augument.join(
                left_df=old_df,
                right_df=new_df,
                left_columns=[x["old_cols"] for x in columns_mapping],
                right_columns=[x["new_cols"] for x in columns_mapping],
                left_metadata=None,
                right_metadata=selected_metadata["_source"],
                joiner="default"
            )
        except:
            return json.dumps({
                "message": "Failed to join, con not join two dataframes"
            })

        return df.to_csv()
