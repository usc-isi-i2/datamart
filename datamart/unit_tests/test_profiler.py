import unittest
import pandas as pd
from datamart.profilers.basic_profiler import BasicProfiler
from datamart.profilers.dsbox_profiler import DSboxProfiler
from datamart.utils import Utils


class TestProfiler(unittest.TestCase):
    def setUp(self):
        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23T00:10:00", "2023213"]
        }
        self.df = pd.DataFrame(data)
        self.profiler = BasicProfiler

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
    def test_profile_temporal_without_coverage(self):
        self.assertEqual(self.profiler.profile_temporal_coverage(
            column=self.df.iloc[:, 0]),
            False
        )

        self.assertEqual(self.profiler.profile_temporal_coverage(
            column=self.df.iloc[:, 1]),
            {
                "start": "2014-02-23T00:00:00",
                "end": "2020-09-23T00:10:00"
            }
        )

    @Utils.test_print
    def test_profile_named_entity(self):
        named_entity_col = self.df.iloc[:, 2]
        self.assertListEqual(self.profiler.profile_named_entity(named_entity_col), ['Tom', 'Jack', 'Steve', 'Ricky'])

    @Utils.test_print
    def test_dsbox_profiler(self):
        self.fake_matadata = {"variables": []}
        for i in range(self.df.shape[1]):
            self.fake_matadata["variables"].append({})
        dsbox_profiler = DSboxProfiler()
        metadata = dsbox_profiler.profile(inputs=self.df, metadata=self.fake_matadata)
        expected = {
            'variables': [
                {
                    'dsbox_profiled': {
                        'ratio_of_numeric_values': 1.0,
                        'number_of_outlier_numeric_values': 0
                    }
                },
                {
                    'dsbox_profiled': {
                        'ratio_of_numeric_values': 0.25,
                        'number_std': 0,
                        'number_of_outlier_numeric_values': 0,
                        'most_common_tokens': [{'name': '2014-02-23', 'count': 1},
                                               {'name': '2018-10-05', 'count': 1},
                                               {'name': '2020-09-23T00:10:00', 'count': 1},
                                               {'name': '2023213', 'count': 1}],
                        'number_of_tokens_containing_numeric_char': 4,
                        'ratio_of_tokens_containing_numeric_char': 1.0,
                        'number_of_values_containing_numeric_char': 4,
                        'ratio_of_values_containing_numeric_char': 1.0}},
                {
                    'dsbox_profiled': {
                        'most_common_tokens': [{'name': 'Jack', 'count': 1},
                                               {'name': 'Ricky', 'count': 1},
                                               {'name': 'Steve', 'count': 1},
                                               {'name': 'Tom', 'count': 1}]
                    }
                }
            ]
        }
        self.assertEqual(metadata, expected)
