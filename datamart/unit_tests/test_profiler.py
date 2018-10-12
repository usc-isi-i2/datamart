import unittest
import pandas as pd
from datamart.profiler import Profiler
from termcolor import colored


class TestProfiler(unittest.TestCase):
    def setUp(self):
        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23T00:10:00", "2023213"]
        }
        self.df = pd.DataFrame(data)
        self.profiler = Profiler()

    def test_construct_global_keywords(self):
        print("[Test]{}/test_construct_global_keywords".format(self.__class__.__name__))
        self.assertListEqual(self.profiler.construct_global_keywords(self.df), ['Age', 'Date', 'Name'])
        print(colored('.Done', 'red'))

    def test_construct_global_description(self):
        print("[Test]{}/test_construct_global_description".format(self.__class__.__name__))
        self.assertEqual(
            self.profiler.construct_global_description(self.df),
            "Age : int64, Date : object, Name : object"
        )
        print(colored('.Done', 'red'))

    def test_construct_global_title(self):
        print("[Test]{}/test_construct_global_title".format(self.__class__.__name__))
        self.assertEqual(self.profiler.construct_global_title(self.df), "Age Date Name")
        print(colored('.Done', 'red'))

    def test_construct_variable_description(self):
        print("[Test]{}/test_construct_variable_description".format(self.__class__.__name__))
        lst = ["column name: Age, dtype: int64", "column name: Date, dtype: object", "column name: Name, dtype: object"]
        for i in range(self.df.shape[1]):
            self.assertEqual(self.profiler.construct_variable_description(self.df.iloc[:, i]), lst[i])
        print(colored('.Done', 'red'))

    def test_profile_temporal_coverage(self):
        print("[Test]{}/test_profile_temporal_coverage".format(self.__class__.__name__))
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
        print(colored('.Done', 'red'))

    def test_profile_named_entity(self):
        print("[Test]{}/test_profile_named_entity".format(self.__class__.__name__))
        named_entity_col = self.df.iloc[:, 2]
        self.assertListEqual(self.profiler.profile_named_entity(named_entity_col), ['Tom', 'Jack', 'Steve', 'Ricky'])
        print(colored('.Done', 'red'))
