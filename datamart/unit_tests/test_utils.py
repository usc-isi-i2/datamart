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
    def test_generate_metadata_from_dataframe(self):
        data = {
            'Name': ['Tom', 'Jack', 'Steve', 'Ricky'],
            'Age': [28, 34, 29, 42],
            'Date': ["2018-10-05", "2014-02-23", "2020-09-23", "2023-02-13"]
        }
        df = pd.DataFrame(data)
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
        self.assertEqual(Utils.generate_metadata_from_dataframe(data=df), expected)
