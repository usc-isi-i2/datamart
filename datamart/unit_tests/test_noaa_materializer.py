from datamart.materializers.noaa_materializer import NoaaMaterializer
import unittest, json, os
import pandas as pd
from datamart.utils import Utils


resources_path = os.path.join(os.path.dirname(__file__), "./resources")


class TestNoaaMaterializer(unittest.TestCase):
    def setUp(self):
        self.noaa_materializer = NoaaMaterializer()

    @Utils.test_print
    def test_header(self):
        self.assertEqual(self.noaa_materializer.headers, None)

    @Utils.test_print
    def test_get(self):
        fake_metadata = {
            "materialization": {
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
            "locations": ["los angeles"]
        }
        result = self.noaa_materializer.get(metadata=fake_metadata, constrains=fake_constrains).to_dict(
            orient="records")
        sample_result = json.load(open(os.path.join(resources_path, 'noaa_get_test.json'), "r"))
        self.assertEqual(result, sample_result)
        fake_metadata_for_no_return = {
            "materialization": {
                "arguments": {
                    "type": 'ACMC'
                }
            }
        }
        null = self.noaa_materializer.get(metadata=fake_metadata_for_no_return, constrains=fake_constrains).to_dict(
            orient="records")
        self.assertEqual(null, [])

    @Utils.test_print
    def test_next(self):
        fake_true_data = {
            "metadata": {
                "resultset": {
                    "limit": "1000",
                    "offset": "1",
                    "count": "2000"
                }
            }
        }
        fake_false_data = {
            "metadata": {
                "resultset": {
                    "limit": "1000",
                    "offset": "1001",
                    "count": "2000"
                }
            }
        }
        self.assertEqual(NoaaMaterializer.next(fake_true_data), True)
        self.assertEqual(NoaaMaterializer.next(fake_false_data), False)

    @Utils.test_print
    def test_add_result(self):
        fake_result = pd.DataFrame(columns=['date', 'stationid', 'city', 'PRCP'])
        fake_data = {
            "results": [{
                "date": "2018-09-23T00:00:00",
                "station": "GHCND:USW00023174",
                "value": 0
            }, {
                "date": "2018-09-23T00:00:00",
                "station": "GHCND:USW00093134",
                "value": 0
            }]
        }
        NoaaMaterializer.add_result(fake_result, fake_data, 'los angeles')
        excepted = [
            {
                "date": "2018-09-23T00:00:00",
                "stationid": "GHCND:USW00023174",
                "city": "los angeles",
                "PRCP": 0
            }, {
                "date": "2018-09-23T00:00:00",
                "stationid": "GHCND:USW00093134",
                "city": "los angeles",
                "PRCP": 0
            }
        ]
        self.assertEqual(fake_result.to_dict(orient="records"), excepted)
