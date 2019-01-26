# TODO: COMMAND OUT BECAUSE NOAA IS SHUNDOWN RECENTLY
# from datamart.materializers.noaa_materializer import NoaaMaterializer, DEFAULT_TOKEN
# import unittest, json, os
# import pandas as pd
# from datamart.utilities.utils import Utils
# from pandas.util.testing import assert_frame_equal
#
# resources_path = os.path.join(os.path.dirname(__file__), "./resources")
#
#
# class TestNoaaMaterializer(unittest.TestCase):
#     def setUp(self):
#         self.noaa_materializer = NoaaMaterializer()
#
#     @Utils.test_print
#     def test_header(self):
#         self.assertEqual(self.noaa_materializer.headers, {"token": DEFAULT_TOKEN})
#
#     @Utils.test_print
#     def test_get(self):
#         fake_metadata = {
#             "materialization": {
#                 "arguments": {
#                     "type": 'PRCP'
#                 }
#             }
#         }
#         fake_constrains = {
#             "date_range": {
#                 "start": "2016-09-23",
#                 "end": "2016-09-23"
#             },
#             "named_entity": {2: ["los angeles"]}
#         }
#         result = self.noaa_materializer.get(metadata=fake_metadata, constrains=fake_constrains).to_dict(
#             orient="records")
#         expected = [{'date': '2016-09-23T00:00:00', 'stationid': 'GHCND:US1CALA0001', 'city': 'los angeles', 'PRCP': 0}]
#         self.assertEqual(result, expected)
#         fake_metadata_for_no_return = {
#             "materialization": {
#                 "arguments": {
#                     "type": 'ACMC'
#                 }
#             }
#         }
#         null = self.noaa_materializer.get(metadata=fake_metadata_for_no_return, constrains=fake_constrains).to_dict(
#             orient="records")
#         self.assertEqual(null, [])
#
#     @Utils.test_print
#     def test_get_more_than_one_year(self):
#         fake_metadata = {
#             "materialization": {
#                 "arguments": {
#                     "type": 'PRCP'
#                 }
#             }
#         }
#         fake_constrains = {
#             "date_range": {
#                 "start": "2015-09-20",
#                 "end": "2016-09-23"
#             },
#             "named_entity": {2: ["los angeles"]}
#         }
#         result = self.noaa_materializer.get(metadata=fake_metadata, constrains=fake_constrains).infer_objects()
#
#         assert_frame_equal(result, pd.read_csv(os.path.join(resources_path, "noaa_more_than_one_year.csv")).infer_objects())
#
#     @Utils.test_print
#     def test_next(self):
#         fake_true_data = {
#             "metadata": {
#                 "resultset": {
#                     "limit": "1000",
#                     "offset": "1",
#                     "count": "2000"
#                 }
#             }
#         }
#         fake_false_data = {
#             "metadata": {
#                 "resultset": {
#                     "limit": "1000",
#                     "offset": "1001",
#                     "count": "2000"
#                 }
#             }
#         }
#         self.assertEqual(NoaaMaterializer.next(fake_true_data), True)
#         self.assertEqual(NoaaMaterializer.next(fake_false_data), False)
#
#     @Utils.test_print
#     def test_add_result(self):
#         fake_result = pd.DataFrame(columns=['date', 'stationid', 'city', 'PRCP'])
#         fake_data = {
#             "results": [{
#                 "date": "2018-09-23T00:00:00",
#                 "station": "GHCND:USW00023174",
#                 "value": 0
#             }, {
#                 "date": "2018-09-23T00:00:00",
#                 "station": "GHCND:USW00093134",
#                 "value": 0
#             }]
#         }
#         NoaaMaterializer.add_result(fake_result, fake_data, 'los angeles')
#         excepted = [
#             {
#                 "date": "2018-09-23T00:00:00",
#                 "stationid": "GHCND:USW00023174",
#                 "city": "los angeles",
#                 "PRCP": 0
#             }, {
#                 "date": "2018-09-23T00:00:00",
#                 "stationid": "GHCND:USW00093134",
#                 "city": "los angeles",
#                 "PRCP": 0
#             }
#         ]
#         self.assertEqual(fake_result.to_dict(orient="records"), excepted)
#
#     @Utils.test_print
#     def test_get_available_station(self):
#         stationid = self.noaa_materializer.get_available_station(
#             location_id="CITY:US060013",
#             data_type="TAVG",
#             dataset_id="GHCND",
#             start_date="2018-09-23T00:00:00",
#             end_date="2018-09-30T00:00:00"
#         )
#
#         self.assertEqual(stationid, "GHCND:USR0000CACT")
