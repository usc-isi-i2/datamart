from datamart.utils import Utils
import unittest, os, json
from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.noaa_materializer import NoaaMaterializer
from termcolor import colored


class TestUtils(unittest.TestCase):

    def test_validate_schema(self):
        print("[Test]{}/test_validate_schema".format(self.__class__.__name__))
        description = json.load(open(os.path.join(os.path.dirname(__file__), "resources/trading_economic.json"), "r"))
        self.assertEqual(Utils.validate_schema(description["description"]), True)
        print(colored('.Done', 'red'))

    def test_date_validate(self):
        print("[Test]{}/test_date_validate".format(self.__class__.__name__))
        self.assertEqual(Utils.date_validate("2018-10-10"), "2018-10-10")
        self.assertEqual(Utils.date_validate("2018"), None)
        print(colored('.Done', 'red'))

    def test_temporal_coverage_validate(self):
        print("[Test]{}/test_temporal_coverage_validate".format(self.__class__.__name__))
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
        print(colored('.Done', 'red'))

    def test_load_materializer(self):
        print("[Test]{}/test_load_materializer".format(self.__class__.__name__))
        materializer = Utils.load_materializer("noaa_materializer")
        self.assertEqual(issubclass(type(materializer), MaterializerBase), True)
        self.assertIn(type(materializer).__name__, NoaaMaterializer.__name__)
        print(colored('.Done', 'red'))
