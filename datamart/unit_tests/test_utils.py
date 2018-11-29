from datamart.utilities.utils import Utils
import unittest, os, json
from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.noaa_materializer import NoaaMaterializer
import pandas as pd
from pandas.util.testing import assert_frame_equal


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.materializers_path = os.path.join(os.path.dirname(__file__), "../materializers")
        self.resources_path = os.path.join(os.path.dirname(__file__), "resources")
        self.dataframe_equal = assert_frame_equal

        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"]
        }
        self.df = pd.DataFrame(data).infer_objects()

    @Utils.test_print
    def test_validate_schema(self):
        with open(os.path.join(os.path.dirname(__file__), "resources/sample_schema.json"), "r") as f:
            description = json.load(f)
        self.assertEqual(Utils.validate_schema(description["description"]), True)

    @Utils.test_print
    def test_date_validate(self):
        self.assertEqual(Utils.date_validate("2018-10-10"), "2018-10-10T00:00:00")

    @Utils.test_print
    def test_temporal_coverage_validate(self):
        coverage = {}
        self.assertEqual(Utils.temporal_coverage_validate(coverage), {"start": None, "end": None})
        coverage = {"start": None}
        self.assertEqual(Utils.temporal_coverage_validate(coverage), {"start": None, "end": None})
        coverage = {"end": None}
        self.assertEqual(Utils.temporal_coverage_validate(coverage), {"start": None, "end": None})
        coverage = {"start": "2018-09-23T00:00:00", "end": "2018-10-10"}
        self.assertEqual(Utils.temporal_coverage_validate(coverage),
                         {'end': '2018-10-10T00:00:00', 'start': '2018-09-23T00:00:00'})
        coverage = {"start": "2018-00", "end": "2018-10-10"}
        self.assertEqual(Utils.temporal_coverage_validate(coverage),
                         {'end': '2018-10-10T00:00:00', 'start': None})

    @Utils.test_print
    def test_load_materializer(self):
        materializer = Utils.load_materializer("noaa_materializer")
        self.assertEqual(issubclass(type(materializer), MaterializerBase), True)
        self.assertIn(type(materializer).__name__, NoaaMaterializer.__name__)

    @Utils.test_print
    def test_materialize(self):
        fake_metadata = {
            "materialization": {
                "python_path": "noaa_materializer",
                "arguments": {
                    "type": 'PRCP'
                }
            }
        }
        fake_constrains = {
            "date_range": {
                "start": "2016-09-23",
                "end": "2016-09-23"
            },
            "named_entity": {2: ["los angeles"]}
        }
        result = Utils.materialize(metadata=fake_metadata, constrains=fake_constrains).infer_objects()
        expepcted = pd.read_csv(os.path.join(os.path.dirname(__file__), "resources/noaa_result.csv"))
        self.dataframe_equal(result, expepcted)

    @Utils.test_print
    def test_is_column_able_to_query(self):
        self.assertTrue(Utils.is_column_able_to_query(col=self.df['Name']))
        self.assertFalse(Utils.is_column_able_to_query(col=self.df['Date']))
        self.assertFalse(Utils.is_column_able_to_query(col=self.df['Age']))

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
                                    "united states of american"
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
                'matched_queries': ['united states of american'],
                'highlight': {'variables.named_entity': ['united states']}
            }
        ]
        self.assertListEqual(Utils.get_inner_hits_info(hitted_es_result=fake_es_result), expected)

    @Utils.test_print
    def test_get_named_entity_constrain_from_inner_hits(self):
        expected = {2: ['new york'], 1: ['united states']}
        self.assertDictEqual(Utils.get_named_entity_constrain_from_inner_hits(matches=[
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
        ]), expected)

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
        self.assertListEqual(Utils.get_metadata_intersection(metadata_lst1, metadata_lst2, metadata_lst3),
                             expect)

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

        self.dataframe_equal(Utils.append_columns_for_implicit_variables(
            implicit_variables=implicit_variables,
            df=self.df
        ), expected)

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

        df = Utils.get_dataset(
            metadata=fake_matadata,
            variables=[0, 2, 3],
            constrains=fake_constrains
        )

        ground_truth = pd.read_csv(os.path.join(os.path.dirname(__file__), "./resources", "test_augment.csv"))
        self.dataframe_equal(ground_truth, df)

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
        self.assertDictEqual(Utils.calculate_dsbox_features(
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
        self.assertEqual(Utils.generate_metadata_from_dataframe(data=self.df), expected)
