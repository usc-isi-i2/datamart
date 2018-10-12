import unittest
import pandas as pd
from datamart.profiler import Profiler
from datamart.utils import Utils


class TestProfiler(unittest.TestCase):
    def setUp(self):
        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23T00:10:00", "2023213"]
        }
        self.df = pd.DataFrame(data)
        self.profiler = Profiler()

    @Utils.test_print
    def test_construct_global_keywords(self):
        self.assertListEqual(self.profiler.construct_global_keywords(self.df), ['Age', 'Date', 'Name'])

    @Utils.test_print
    def test_construct_global_description(self):
        self.assertEqual(
            self.profiler.construct_global_description(self.df),
            "Age : int64, Date : object, Name : object"
        )

    @Utils.test_print
    def test_construct_global_title(self):
        self.assertEqual(self.profiler.construct_global_title(self.df), "Age Date Name")

    @Utils.test_print
    def test_construct_variable_description(self):
        lst = ["column name: Age, dtype: int64", "column name: Date, dtype: object", "column name: Name, dtype: object"]
        for i in range(self.df.shape[1]):
            self.assertEqual(self.profiler.construct_variable_description(self.df.iloc[:, i]), lst[i])

    @Utils.test_print
    def test_profile_temporal_coverage(self):
        date_col = self.df.iloc[:, 1]
        self.assertEqual(self.profiler.profile_temporal_coverage(
            coverage={
                "start": None,
                "end": None
            },
            column=date_col
        ), {
            "start": "2014-02-23T00:00:00",
            "end": "2020-09-23T00:10:00"
        })

        self.assertEqual(self.profiler.profile_temporal_coverage(
            coverage={
                "start": "2010-02-23T00:00:00",
                "end": None
            },
            column=date_col
        ), {
            "start": "2010-02-23T00:00:00",
            "end": "2020-09-23T00:10:00"
        })

        self.assertEqual(self.profiler.profile_temporal_coverage(
            coverage={
                "start": None,
                "end": "2022-09-23T00:10:00"
            },
            column=date_col
        ), {
            "start": "2014-02-23T00:00:00",
            "end": "2022-09-23T00:10:00"
        })

    @Utils.test_print
    def test_profile_named_entity(self):
        named_entity_col = self.df.iloc[:, 2]
        self.assertListEqual(self.profiler.profile_named_entity(named_entity_col), ['Tom', 'Jack', 'Steve', 'Ricky'])
