from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests
import typing
import os

DEFAULT_LOCATIONS = [
    "new york",
    "los angeles",
    "chicago",
    "Houston",
    "Philadelphia"
]
DEFAULT_DATE_RANGE = 7
DEFAULT_DATA_TYPE = "TAVG"
DEFAULT_DATASET_ID = "GHCND"
DEFAULT_TOKEN = "QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS"

LIMIT_NUMBER = 1000
OFFSET_INTERVAL = 8


class NoaaMaterializer(MaterializerBase):
    """NoaaMaterializer class extended from  Materializer class

    """

    def __init__(self):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self)
        self.headers = None
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        with open(os.path.join(resources_path, 'city_id_map.csv'), 'r') as csv_file:
            reader = csv.reader(csv_file)
            self.city_to_id_map = dict(reader)

    def get(self,
            metadata: dict = None,
            variables: typing.List[int] = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        if not constrains:
            constrains = dict()

        materialization_arguments = metadata["materialization"].get("arguments", {})

        self.headers = {"token" : constrains.get("token", DEFAULT_TOKEN)}

        date_range = constrains.get("date_range", {})
        locations = constrains.get("named_entity", DEFAULT_LOCATIONS)
        data_type = materialization_arguments.get("type", DEFAULT_DATA_TYPE)
        dataset_id = constrains.get("dataset_id", DEFAULT_DATASET_ID)

        return self.fetch_data(date_range=date_range, locations=locations, data_type=data_type, dataset_id=dataset_id)

    def fetch_data(self, date_range: dict, locations: list, data_type: str, dataset_id: str):
        """ fetch data using Noaa api and return the dataset
            the date of the data is in the range of date range
            location is a list of city name

        Args:
            date_range: data range constrain.(format: %Y-%m-%d)
            locations: list of string of location
            data_type: string of data type for the query
            dataset_id


        Returns:
             result: A pd.DataFrame;
             An example:
                                        date          stationid           city TAVG
                    0    2018-09-23T00:00:00  GHCND:USR0000CACT    los angeles  233
                    1    2018-09-23T00:00:00  GHCND:USR0000CBEV    los angeles  206
        """
        result = pd.DataFrame(columns=['date', 'stationid', 'city', data_type])
        start_date = date_range.get("start",
                                    (datetime.datetime.now() - datetime.timedelta(days=DEFAULT_DATE_RANGE)).strftime(
                                        '%Y-%m-%d'))
        end_date = date_range.get("end", datetime.datetime.today().strftime('%Y-%m-%d'))
        for location in locations:
            location_id = self.city_to_id_map.get(location.lower(), None)
            if location_id is None:
                continue

            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}' \
                  '&datatypeid={data_type}&locationid={location_id}&startdate={start_date}' \
                  '&enddate={end_date}&limit={limit_number}&offset=1'.format(
                    dataset_id=dataset_id,
                    data_type=data_type,
                    location_id=location_id,
                    start_date=start_date,
                    end_date=end_date,
                    limit_number=LIMIT_NUMBER
                  )

            response = requests.get(api, headers=self.headers)
            data = response.json()
            self.add_result(result, data, location)
            while self.next(data):
                index = api.rfind('&')
                next_offset = int(api[index + OFFSET_INTERVAL:]) + LIMIT_NUMBER
                api = api[:index + OFFSET_INTERVAL] + str(next_offset)
                response = requests.get(api, headers=self.headers)
                data = response.json()
                self.add_result(result, data, location)
        return result

    @staticmethod
    def next(data):
        """ check whether it needs next query(get next 1000 data)

        """
        if 'metadata' not in data:
            return
        resultset = data['metadata']['resultset']
        num = float(resultset['limit']) + float(resultset['offset']) - 1
        if num < float(resultset['count']):
            return True
        return False

    @staticmethod
    def add_result(result, data, location):
        """ get the useful data from raw data and add them in the return dataframe

        """
        if 'results' not in data:
            return
        for row in data['results']:
            result.loc[len(result)] = [row['date'], row['station'], location, row['value']]
