from datamart.utils import Utils
import unittest, os, json
from datamart.materializers.materializer_base import MaterializerBase


class TestUtils(unittest.TestCase):

    def test_validate_schema(self):
        description = json.load(open(os.path.join(os.path.dirname(__file__), "resources/trading_economic.json"), "r"))
        self.assertEqual(Utils.validate_schema(description["description"]), True)

    def test_date_validate(self):
        self.assertEqual(Utils.date_validate("2018-10-10"), "2018-10-10")
        self.assertEqual(Utils.date_validate("2018"), None)

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

    def test_load_materializer(self):
        materializer = Utils.load_materializer("noaa_materializer")
        self.assertEqual(issubclass(type(materializer), MaterializerBase), True)
