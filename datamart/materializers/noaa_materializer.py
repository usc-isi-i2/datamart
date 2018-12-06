from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import datetime
from dateutil import parser
import csv
import requests
import typing
import os

DEFAULT_DATE_RANGE = 7
DEFAULT_DATA_TYPE = "TAVG"
DEFAULT_DATASET_ID = "GHCND"
DEFAULT_TOKEN = "QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS"

LIMIT_NUMBER = 1000
OFFSET_INTERVAL = 8
CITY_COLUMN_INDEX = 2


class NoaaMaterializer(MaterializerBase):
    """NoaaMaterializer class extended from  Materializer class

    """

    def __init__(self, **kwargs):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self, **kwargs)
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        self.headers = {"token": DEFAULT_TOKEN}
        with open(os.path.join(resources_path, 'city_id_map.csv'), 'r') as csv_file:
            reader = csv.reader(csv_file)
            self.city_to_id_map = dict(reader)

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                constrains: include some constrains like date_range, location and so on
        """
        if not constrains:
            constrains = dict()

        materialization_arguments = metadata["materialization"].get("arguments", {})

        if "token" in constrains:
            self.headers = {"token": constrains["token"]}

        date_range = constrains.get("date_range", {})

        locations = constrains.get("named_entity", {}).get(CITY_COLUMN_INDEX, self.city_to_id_map.keys())

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

            stationid = self.get_available_station(location_id, data_type, dataset_id, start_date, end_date)
            if not stationid:
                return result

            start_parsed_date = parser.parse(start_date)
            end_parsed_date = parser.parse(end_date)

            while start_parsed_date.replace(year=start_parsed_date.year + 1) < end_parsed_date:
                start_parsed_date.replace(year=start_parsed_date.year + 1).isoformat(),
                self.add_rows(dataset_id=dataset_id,
                              data_type=data_type,
                              location=location,
                              location_id=location_id,
                              stationid=stationid,
                              start_date=start_parsed_date.isoformat(),
                              end_date=start_parsed_date.replace(year=start_parsed_date.year + 1).isoformat(),
                              result=result)

                start_parsed_date = start_parsed_date.replace(year=start_parsed_date.year + 1)

            self.add_rows(dataset_id=dataset_id,
                          data_type=data_type,
                          location=location,
                          location_id=location_id,
                          stationid=stationid,
                          start_date=start_parsed_date.isoformat(),
                          end_date=end_parsed_date.isoformat(),
                          result=result)

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

    def get_available_station(self, location_id, data_type, dataset_id, start_date, end_date):
        api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}' \
              '&datatypeid={data_type}&locationid={location_id}&startdate={start_date}' \
              '&enddate={end_date}&limit={limit_number}&offset=1'.format(
            dataset_id=dataset_id,
            data_type=data_type,
            location_id=location_id,
            start_date=start_date,
            end_date=start_date,
            limit_number=10
        )
        response = requests.get(api, headers=self.headers)
        data = response.json()
        start_stationid = [x["station"] for x in data["results"]] if "results" in data else None

        api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}' \
              '&datatypeid={data_type}&locationid={location_id}&startdate={start_date}' \
              '&enddate={end_date}&limit={limit_number}&offset=1'.format(
            dataset_id=dataset_id,
            data_type=data_type,
            location_id=location_id,
            start_date=end_date,
            end_date=end_date,
            limit_number=10
        )
        response = requests.get(api, headers=self.headers)
        data = response.json()
        end_stationid = [x["station"] for x in data["results"]] if "results" in data else None

        if not start_stationid and not end_stationid:
            return None

        if start_stationid and end_stationid and set(start_stationid).intersection(end_stationid):
            for x in end_stationid:
                if x in start_stationid:
                    return x

        if end_stationid:
            return end_stationid[0]

        return start_stationid[0]

    def add_rows(self, dataset_id, data_type, location, location_id, stationid, start_date, end_date, result):
        api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}' \
              '&datatypeid={data_type}&locationid={location_id}&stationid={stationid}&startdate={start_date}' \
              '&enddate={end_date}&limit={limit_number}&offset=1'.format(
            dataset_id=dataset_id,
            data_type=data_type,
            location_id=location_id,
            stationid=stationid,
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
