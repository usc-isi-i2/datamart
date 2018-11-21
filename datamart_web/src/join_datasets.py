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

        left_metadata = Utils.generate_metadata_from_dataframe(data=old_df)

        query_data = json.loads(request.form['data'])
        selected_metadata = query_data["selected_metadata"]
        columns_mapping = query_data["columns_mapping"]

        matches = self.augument.get_inner_hits_info(hitted_es_result=selected_metadata)

        if not matches:
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

        for matched in matches:
            constrains["named_entity"][matched["offset"]] = matched["highlight"]["variables.named_entity"]

        # get temporal coverage from provided dataframe
        if left_metadata.get("variables", []):
            for variable in left_metadata["variables"]:
                if variable.get("temporal_coverage") and variable["temporal_coverage"].get("start") and variable["temporal_coverage"].get("end"):
                    constrains["date_range"] = {
                      "start": variable["temporal_coverage"]["start"],
                      "end": variable["temporal_coverage"]["end"]
                    }
                    break

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
                left_metadata=left_metadata,
                right_metadata=selected_metadata["_source"],
                joiner="default"
            )
        except:
            return json.dumps({
                "message": "Failed to join, con not join two dataframes"
            })

        return df.to_csv()
