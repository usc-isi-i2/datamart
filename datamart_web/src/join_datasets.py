from datamart.augment import Augment
from datamart.utils import Utils
import pandas as pd
import json


class JoinDatasets(object):

    SEMANTIC_TYPES_MAPPING = {
        "https://metadata.datadrivendiscovery.org/types/Location": "locations"
    }

    def __init__(self, es_index="datamart"):
        self.augument = Augment(es_index=es_index)

    def default_join(self, request):

        selected_metadata = json.load(request.files["selected_metadata"])

        highlight_match = Utils.get_highlight_match_from_metadata(metadata=selected_metadata,
                                                                  fields=["variables.named_entity"])

        offset, matched_queries = Utils.get_matched_queries_and_offset_from_variable_metadata(
            metadata=selected_metadata)

        if "constrains" in request.args:
            try:
                constrains = json.loads(request.args["constrains"].encode("utf-8"))
            except:
                constrains = None
        else:
            constrains = {}

        old_df = pd.read_csv(request.files['file']).infer_objects()

        constrains["named_entity"] = highlight_match["variables.named_entity"]

        new_df = self.augument.get_dataset(
            metadata=selected_metadata["_source"],
            constrains=constrains
        )

        df = pd.merge(left=old_df,
                      right=new_df,
                      left_on=old_df.columns[int(request.args["old_df_column_id"])],
                      right_on=new_df.columns[offset],
                      how='outer')

        return df.to_csv()
