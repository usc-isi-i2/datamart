from datamart.augment import Augment
from datamart.utilities.utils import Utils


class SearchMetadata(object):
    MAX_MATCH = 10

    def __init__(self, es_index="datamart_all"):
        self.augment = Augment(es_index=es_index)

    def default_search_by_csv(self, request, old_df):

        query_string = request.args.get("query_string", None)
        minimum_should_match = request.args.get(
            "minimum_should_match_for_column") if "minimum_should_match_for_column" in request.args else None

        ret = {
            "message": "Created Dataframe and finding datasets for augmenting",
            "result": []
        }

        for idx in range(old_df.shape[1]):
            if Utils.is_column_able_to_query(old_df.iloc[:, idx]):
                this_column_result = self.augment.query(
                    col=old_df.iloc[:, idx],
                    minimum_should_match_ratio_for_col=minimum_should_match,
                    query_string=query_string
                )
                if this_column_result:
                    ret["result"].append({
                        "column_idx": idx,
                        "datasets_metadata": this_column_result[:10]
                    })
        return ret
