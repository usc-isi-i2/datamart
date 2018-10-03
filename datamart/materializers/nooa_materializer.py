from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests
import typing
import os


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

    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = {}) -> pd.DataFrame:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        materialization_arguments = metadata["materialization"].get("arguments", {})
        if "token" in materialization_arguments:
            self.headers = {"token": "%s" % materialization_arguments["token"]}
        else:
            self.headers = {"token": "%s" % "QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS"}
        date_range = constrains.get("date_range", {})
        locations = constrains.get("locations", ['los angeles'])
        data_type = materialization_arguments.get("type", 'TAVG')
        dataset_id = constrains.get("dataset_id", "GHCND")
        return self.fetch_data(date_range=date_range, locations=locations, data_type=data_type, dataset_id=dataset_id)

    def fetch_data(self, date_range: dict = None, locations: list = ['los angeles'], data_type: str = 'TAVG',
                   dataset_id: str = 'GHCND'):
        """ fetch data using Noaa api and return the dataset
            the date of the data is in the range of date range
            location is a list of city name

        Args:
            date_range: data range constrain.(format: %Y-%m-%d)
            location: list of string of location
            datatype: string of data type for the query


        Returns:
             result: A pd.DataFrame;
             An example:
                                        date          stationid           city TAVG
                    0    2018-09-23T00:00:00  GHCND:USR0000CACT    los angeles  233
                    1    2018-09-23T00:00:00  GHCND:USR0000CBEV    los angeles  206
        """
        result = pd.DataFrame(columns=['date', 'stationid', 'city', data_type])
        start_date = date_range.get("start_date",
                                    (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d'))
        end_date = date_range.get("end_date", datetime.datetime.today().strftime('%Y-%m-%d'))
        for location in locations:
            location_id = self.city_to_id_map.get(location, None)
            if location_id is None:
                continue
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=' + dataset_id + \
                  '&datatypeid=' + data_type + '&locationid=' + location_id + \
                  '&startdate=' + start_date + '&enddate=' + end_date + '&limit=1000&offset=1'
            response = requests.get(api, headers=self.headers)
            data = response.json()
            self.add_result(result, data, location)
            while self.next(data):
                index = api.rfind('&')
                next_offset = int(api[index + 8:]) + 1000
                api = api[:index + 8] + str(next_offset)
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
