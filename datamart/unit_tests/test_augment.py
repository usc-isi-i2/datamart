from datamart.utilities.utils import Utils
import unittest
from datamart.augment import Augment
import pandas as pd
import numpy as np
import os
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
    def test_get_dataset(self):
        fake_matadata = {
            "materialization": {
                "python_path": "noaa_materializer",
                "arguments": {
                    "type": "TAVG"
                }
            }
        }

        fake_constrains = {
            "named_entity": {
                2: ["new york", "sdasds"]
            },
            "date_range": {
                "start": "2018-09-23T00:00:00",
                "end": "2018-09-30T00:00:00"
            }
        }

        df = self.augment.get_dataset(
            metadata=fake_matadata,
            variables=[0, 2, 3],
            constrains=fake_constrains
        )

        ground_truth = pd.read_csv(os.path.join(os.path.dirname(__file__), "./resources", "test_augment.csv"))
        self.assertDataframeEqual(ground_truth, df)

    @Utils.test_print
    def test_get_metadata_intersection(self):
        metadata_lst1 = [
            {"_source": {"datamart_id": 0}},
            {"_source": {"datamart_id": 1}},
            {"_source": {"datamart_id": 2}}
        ]

        metadata_lst2 = [
            {"_source": {"datamart_id": 0}},
            {"_source": {"datamart_id": 2}},
            {"_source": {"datamart_id": 3}}
        ]

        metadata_lst3 = [
            {"_source": {"datamart_id": 0}},
            {"_source": {"datamart_id": 3}}
        ]

        expect = [{'_source': {'datamart_id': 0}}]
        self.assertListEqual(self.augment.get_metadata_intersection(metadata_lst1, metadata_lst2, metadata_lst3),
                             expect)

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
        ), expected)

    @Utils.test_print
    def test_append_columns_for_implicit_variables(self):
        implicit_variables = [
            {
                "name": "indicator",
                "value": "born"
            },
            {
                "name": "city",
                "value": "shanghai"
            }
        ]

        data = {
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"],
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'indicator': ["born", "born", "born", "born"],
            'city': ['shanghai', 'shanghai', 'shanghai', 'shanghai']
        }
        expected = pd.DataFrame(data, columns=data.keys())

        self.assertDataframeEqual(self.augment.append_columns_for_implicit_variables(
            implicit_variables=implicit_variables,
            df=self.df
        ), expected)

    @Utils.test_print
    def test_calculate_dsbox_features(self):
        expected = {
            'variables': [
                {'dsbox_profiled': {'ratio_of_numeric_values': 1.0, 'number_of_outlier_numeric_values': 0}},
                {'dsbox_profiled': {'most_common_tokens': [
                    {'name': '2014-02-23', 'count': 1},
                    {'name': '2018-10-05', 'count': 1},
                    {'name': '2020-09-23', 'count': 1},
                    {'name': '2023-02-13', 'count': 1}
                ],
                    'number_of_tokens_containing_numeric_char': 4,
                    'ratio_of_tokens_containing_numeric_char': 1.0,
                    'number_of_values_containing_numeric_char': 4,
                    'ratio_of_values_containing_numeric_char': 1.0}
                },
                {
                    'dsbox_profiled': {'most_common_tokens': [
                        {'name': 'Jack', 'count': 1},
                        {'name': 'Ricky', 'count': 1},
                        {'name': 'Steve', 'count': 1},
                        {'name': 'Tom', 'count': 1}
                    ]}
                }
            ]
        }
        self.assertDictEqual(self.augment.calculate_dsbox_features(
            data=self.df,
            metadata={"variables": [{}, {}, {}]}
        ), expected)

    @Utils.test_print
    def test_generate_metadata_from_dataframe(self):
        expected = {
            'datamart_id': None,
            'materialization': {
                'python_path': 'default_materializer', 'arguments': None
            },
            'variables': [
                {
                    'datamart_id': None,
                    'semantic_type': [],
                    'name': 'Age',
                    'description': 'column name: Age, dtype: int64'
                },
                {
                    'datamart_id': None,
                    'semantic_type': [],
                    'name': 'Date',
                    'description': 'column name: Date, dtype: object',
                    'temporal_coverage': {'start': '2014-02-23T00:00:00', 'end': '2023-02-13T00:00:00'}
                },
                {
                    'datamart_id': None,
                    'semantic_type': [],
                    'name': 'Name',
                    'description': 'column name: Name, dtype: object',
                    'named_entity': ['Tom', 'Jack', 'Steve', 'Ricky']
                }
            ],
            'title': 'Age Date Name',
            'description': 'Age : int64, Date : object, Name : object',
            'keywords': ['Age', 'Date', 'Name']
        }
        self.assertEqual(self.augment.generate_metadata_from_dataframe(data=self.df), expected)

    @Utils.test_print
    def test_is_column_able_to_query(self):
        self.assertTrue(self.augment.is_column_able_to_query(col=self.df['Name']))
        self.assertFalse(self.augment.is_column_able_to_query(col=self.df['Date']))
        self.assertFalse(self.augment.is_column_able_to_query(col=self.df['Age']))

    @Utils.test_print
    def test_get_inner_hits_info(self):
        fake_es_result = {
            "inner_hits": {
                "variables": {
                    "hits": {
                        "hits": [
                            {
                                "_nested": {
                                    "field": "variables",
                                    "offset": 2
                                },
                                "highlight": {
                                    "variables.named_entity": [
                                        "new york"
                                    ]
                                },
                                "matched_queries": [
                                    "new york"
                                ]
                            },
                            {
                                "_nested": {
                                    "field": "variables",
                                    "offset": 1
                                },
                                "highlight": {
                                    "variables.named_entity": [
                                        "united states"
                                    ]
                                },
                                "matched_queries": [
                                    "nunited states of american"
                                ]
                            }
                        ]
                    }
                }
            }
        }

        expected = [
            {
                'offset': 2,
                'matched_queries': ['new york'],
                'highlight': {'variables.named_entity': ['new york']}
            },
            {
                'offset': 1,
                'matched_queries': ['nunited states of american'],
                'highlight': {'variables.named_entity': ['united states']}
            }
        ]
        self.assertListEqual(self.augment.get_inner_hits_info(hitted_es_result=fake_es_result), expected)
