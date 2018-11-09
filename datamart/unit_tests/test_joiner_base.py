import unittest
from datamart.joiners.joiner_base import DefaultJoiner, JoinerType, JoinerPrepare
import pandas as pd
from pandas.testing import assert_frame_equal
from datamart.utils import Utils


class TestJoinerBase(unittest.TestCase):
    def setUp(self):
        self.joiner = DefaultJoiner()

    @Utils.test_print
    def test_joiner_prepare(self):
        joiner = JoinerPrepare.prepare_joiner(joiner="default")
        self.assertIsInstance(joiner, DefaultJoiner)

        joiner = JoinerPrepare.prepare_joiner(joiner="none")
        self.assertEqual(joiner, None)

    @Utils.test_print
    def test_default_joiner(self):
        joiner = JoinerPrepare.prepare_joiner(joiner="default")
        left_df = pd.DataFrame(data={
            'city': ["los angeles", "New york", "Shanghai", "SAFDA", "manchester"],
            'country': ["US", "US", "China", "fwfb", "UK"],
        })

        right_df = pd.DataFrame(data={
            'a_city': ["los angeles", "New york", "Shanghai", "SAFDA", "manchester"],
            'extra': [1, 2, 3, 4, 5],
            'z_country': ["US", "US", "China", "fwfb", "UK"]
        })

        expected = pd.DataFrame(data={
            'city': ["los angeles", "New york", "Shanghai", "SAFDA", "manchester"],
            'extra': [1, 2, 3, 4, 5],
            'country': ["US", "US", "China", "fwfb", "UK"]
        })
        assert_frame_equal(joiner.join(left_df=left_df, right_df=right_df, left_columns=[0, 1], right_columns=[0, 2]),
                           expected)
