from datamart.utils import Utils
from datamart.index_builder import IndexBuilder
import unittest
import pandas as pd


class TestIndexBuilder(unittest.TestCase):

    def setUp(self):
        self.ib = IndexBuilder()
        self.global_datamart_id = 10000
        self.df_for_global = pd.DataFrame({
            "city": ["abu dhabi", "ajman", "dubai", "sharjah"],
            'date': ["2018-10-05", "2014-02-23", "2020-09-23T00:10:00", "2023213"]
        })

        self.df_for_variable = pd.DataFrame({
            'date': ["2018-10-05", "2014-02-23", "2020-09-23T00:10:00", "2023213"]
        })

    @Utils.test_print
    def test_construct_variable_metadata_with_empty_variable(self):
        variable_metadata = self.ib.construct_variable_metadata(
            description={},
            global_datamart_id=self.global_datamart_id,
            col_offset=0,
            data=self.df_for_variable
        )
        expected = {
            'datamart_id': 10001,
            'semantic_type': [],
            'name': 'date',
            'description': 'column name: date, dtype: object',
            'temporal_coverage': {'start': '2014-02-23T00:00:00', 'end': '2020-09-23T00:10:00'}
        }

        self.assertEqual(variable_metadata.value, expected)

    @Utils.test_print
    def test_construct_variable_metadata_1(self):
        variable_description = {
            "name": "date",
            "description": "the date of data",
            "semantic_type": [
                "https://metadata.datadrivendiscovery.org/types/Time"
            ],
            "temporal_coverage": {
                "start": "1874-10-13",
                "end": "2018-10-01"
            }
        }
        variable_metadata = self.ib.construct_variable_metadata(
            description=variable_description,
            global_datamart_id=self.global_datamart_id,
            col_offset=0
        )
        expected = {
            'datamart_id': 10001,
            'name': 'date',
            'description': 'the date of data',
            'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Time'],
            'temporal_coverage': {
                'start': '1874-10-13T00:00:00',
                'end': '2018-10-01T00:00:00'
            }
        }

        self.assertEqual(variable_metadata.value, expected)

    @Utils.test_print
    def test_construct_variable_metadata_1_with_data(self):
        variable_description = {
            "description": "the date of data",
            "semantic_type": [
                "https://metadata.datadrivendiscovery.org/types/Time"
            ],
            "temporal_coverage": {
                "start": None,
                "end": None
            }
        }
        variable_metadata = self.ib.construct_variable_metadata(
            description=variable_description,
            global_datamart_id=self.global_datamart_id,
            col_offset=0,
            data=self.df_for_variable
        )
        expected = {
            'datamart_id': 10001,
            'name': 'date',
            'description': 'the date of data',
            'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Time'],
            'temporal_coverage': {
                'start': '2014-02-23T00:00:00',
                'end': '2020-09-23T00:10:00'
            }
        }

        self.assertEqual(variable_metadata.value, expected)

    @Utils.test_print
    def test_construct_variable_metadata_2(self):
        variable_description = {
            "name": "city",
            "description": "the city data belongs to",
            "semantic_type": [
                "https://metadata.datadrivendiscovery.org/types/Location"
            ],
            "named_entity": [
                "abu dhabi",
                "ajman",
                "dubai",
                "sharjah",
                "kabul",
                "kandahar",
                "algiers",
                "annaba",
                "batna"
            ]
        }
        variable_metadata = self.ib.construct_variable_metadata(
            description=variable_description,
            global_datamart_id=self.global_datamart_id,
            col_offset=0
        )
        expected = {
            'datamart_id': 10001,
            'name': 'city',
            'description': 'the city data belongs to',
            'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Location'],
            'named_entity': ['abu dhabi', 'ajman', 'dubai', 'sharjah', 'kabul', 'kandahar', 'algiers', 'annaba',
                             'batna']
        }

        self.assertEqual(variable_metadata.value, expected)

    @Utils.test_print
    def test_construct_variable_metadata_2_with_data(self):
        data = {
            "city": [
                "abu dhabi",
                "ajman",
                "dubai",
                "sharjah",
                "kabul",
                "kandahar",
                "algiers",
                "annaba",
                "batna"
            ]
        }
        df = pd.DataFrame(data)
        variable_description = {
            "name": "city",
            "semantic_type": [
                "https://metadata.datadrivendiscovery.org/types/Location"
            ],
            "named_entity": None
        }
        variable_metadata = self.ib.construct_variable_metadata(
            description=variable_description,
            global_datamart_id=self.global_datamart_id,
            col_offset=0,
            data=df
        )
        expected = {
            'datamart_id': 10001,
            'name': 'city',
            'description': 'column name: city, dtype: object',
            'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Location'],
            'named_entity': ['abu dhabi', 'ajman', 'dubai', 'sharjah', 'kabul', 'kandahar', 'algiers', 'annaba',
                             'batna']
        }

        self.assertEqual(variable_metadata.value, expected)

    @Utils.test_print
    def test_construct_global_metadata(self):
        self.ib.current_global_index = 10000
        description = {
            "title": "TAVG",
            "description": "Average temperature (tenths of degrees C)[Note that TAVG from source 'S' corresponds to an average for the period ending at 2400 UTC rather than local midnight]",
            "url": "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt",
            "keywords": [
                "Average Temperature."
            ],
            "provenance": "noaa.org",
            "materialization": {
                "python_path": "noaa_materializer",
                "arguments": {
                    "type": "TAVG"
                }
            },
            "variables": [
                {
                    "name": "date",
                    "description": "the date of data",
                    "semantic_type": [
                        "https://metadata.datadrivendiscovery.org/types/Time"
                    ],
                    "temporal_coverage": {
                        "start": "1874-10-13",
                        "end": "2018-10-01"
                    }
                },
                {
                    "name": "city",
                    "description": "the city data belongs to",
                    "semantic_type": [
                        "https://metadata.datadrivendiscovery.org/types/Location"
                    ],
                    "named_entity": [
                        "abu dhabi",
                        "ajman",
                        "dubai",
                        "sharjah"
                    ]
                }
            ],
            "date_updated": "2018-09-28"
        }
        global_metadata = self.ib.construct_global_metadata(
            description=description
        )
        expected = {
            'datamart_id': 20000,
            'title': 'TAVG',
            'description': "Average temperature (tenths of degrees C)[Note that TAVG from source 'S' corresponds to an average for the period ending at 2400 UTC rather than local midnight]",
            'url': 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt',
            'keywords': ['Average Temperature.'],
            'date_updated': '2018-09-28T00:00:00',
            'provenance': 'noaa.org',
            'materialization': {
                'python_path': 'noaa_materializer',
                'arguments': {'type': 'TAVG'}
            },
            'variables': [
                {
                    'datamart_id': 20001,
                    'name': 'date',
                    'description': 'the date of data',
                    'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Time'],
                    'temporal_coverage': {'start': '1874-10-13T00:00:00', 'end': '2018-10-01T00:00:00'}
                },
                {
                    'datamart_id': 20002,
                    'name': 'city',
                    'description': 'the city data belongs to',
                    'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Location'],
                    'named_entity': ['abu dhabi', 'ajman', 'dubai', 'sharjah']
                }
            ]
        }

        self.assertEqual(global_metadata.value, expected)

    @Utils.test_print
    def test_construct_global_metadata_with_data(self):
        self.ib.current_global_index = 10000
        description = {
            "url": "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt",
            "keywords": [
                "Average Temperature."
            ],
            "provenance": "noaa.org",
            "materialization": {
                "python_path": "noaa_materializer",
                "arguments": {
                    "type": "TAVG"
                }
            },
            "variables": [
                {
                    "name": "city",
                    "description": "the city data belongs to",
                    "semantic_type": [
                        "https://metadata.datadrivendiscovery.org/types/Location"
                    ],
                    "named_entity": None
                },
                {
                    "name": "date",
                    "description": "the date of data",
                    "semantic_type": [
                        "https://metadata.datadrivendiscovery.org/types/Time"
                    ],
                    "temporal_coverage": None
                }
            ],
            "date_updated": "2018-09-28"
        }
        global_metadata = self.ib.construct_global_metadata(
            description=description,
            data=self.df_for_global
        )

        expected = {
            'datamart_id': 20000,
            'title': 'city date',
            'description': 'city : object, date : object',
            'url': 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt',
            'keywords': ['Average Temperature.'],
            'date_updated': '2018-09-28T00:00:00',
            'provenance': 'noaa.org',
            'materialization': {'python_path': 'noaa_materializer', 'arguments': {'type': 'TAVG'}},
            'variables': [
                {
                    'datamart_id': 20001,
                    'name': 'city',
                    'description': 'the city data belongs to',
                    'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Location'],
                    'named_entity': ['abu dhabi', 'ajman', 'dubai', 'sharjah']
                },
                {
                    'datamart_id': 20002,
                    'name': 'date',
                    'description': 'the date of data',
                    'semantic_type': ['https://metadata.datadrivendiscovery.org/types/Time'],
                    'temporal_coverage': {'start': '2014-02-23T00:00:00', 'end': '2020-09-23T00:10:00'}
                }
            ]
        }

        self.assertEqual(global_metadata.value, expected)

    @Utils.test_print
    def test_construct_global_metadata_with_basic_fields(self):
        self.ib.current_global_index = 10000
        description = {
            "materialization": {
                "python_path": "noaa_materializer"
            }
        }
        global_metadata = self.ib.construct_global_metadata(
            description=description,
            data=self.df_for_global
        )

        expected = {
            'datamart_id': 20000,
            'materialization': {'python_path': 'noaa_materializer', 'arguments': None},
            'variables': [
                {
                    'datamart_id': 20001,
                    'semantic_type': [],
                    'name': 'city',
                    'description': 'column name: city, dtype: object'
                },
                {
                    'datamart_id': 20002,
                    'semantic_type': [],
                    'name': 'date',
                    'description': 'column name: date, dtype: object',
                    'temporal_coverage': {'start': '2014-02-23T00:00:00', 'end': '2020-09-23T00:10:00'}
                }
            ],
            'title': 'city date',
            'description': 'city : object, date : object',
            'keywords': ['city', 'date']
        }

        self.assertEqual(global_metadata.value, expected)
