from datamart.utilities.utils import Utils
import unittest
from datamart.augment import Augment
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal


class TestAugment(unittest.TestCase):

    def setUp(self):
        self.augment = Augment(es_index="fake")
        self.assertDataframeEqual = assert_frame_equal

        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"]
        }
        self.df = pd.DataFrame(data).infer_objects()

    @Utils.test_print
    def test_joiner(self):
        data = {
            'Age': [28, 34, 29, 42],
            'Date_x': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"],
            'Name_x': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Date_y': ["2018-10-05", "2014-02-23", np.nan, np.nan],
            'Name_y': ['Tom', 'Jack', np.nan, np.nan]
        }
        expected = pd.DataFrame(data, columns=data.keys())

        self.assertDataframeEqual(self.augment.join(
            left_df=self.df,
            right_df=self.df.iloc[:2, :],
            left_columns=[[0]],
            right_columns=[[0]],
            joiner="default"
        ).df, expected)
