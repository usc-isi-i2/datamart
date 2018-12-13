from datamart.materializers.wikidata_spo_materializer import WikidataSPOMaterializer
import unittest, os
import pandas as pd
from pandas.util.testing import assert_frame_equal
from datamart.utilities.utils import Utils
from pprint import pprint
import numpy as np

resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestWikidataSPOMaterializer(unittest.TestCase):
    def setUp(self):
        self.wikidata_materializer = WikidataSPOMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            "materialization": {
                "arguments": {
                    "property": "P2672"
                }
            }
        }

        result = self.wikidata_materializer.get(metadata=mock_metadata)
            # .to_dict(orient="records")
        # sort by row
        result2 = pd.DataFrame(np.sort(result.values, axis=0), index=result.index)
        # print(result2)
        sample_result = pd.read_csv(os.path.join(resources_path, 'P2672_SOATO_ID.csv'), dtype=str)
        sample_result = sample_result[["source", "subject_label", "category", "prop_value", "value_label"]]
        sample_result = sample_result.fillna('')
        # sort by row
        sample_result2 = pd.DataFrame(np.sort(sample_result.values, axis=0), index=sample_result.index)

        assert_frame_equal(sample_result2, result2)
