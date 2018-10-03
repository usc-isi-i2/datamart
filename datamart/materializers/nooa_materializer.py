from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests
import typing
import os


class NoaaMaterializer(MaterializerBase):

    def __init__(self):
        MaterializerBase.__init__(self)
        self.headers = None
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        with open(os.path.join(resources_path, 'city_id_map.csv'), 'r') as csv_file:
            reader = csv.reader(csv_file)
            self.city_to_id_map = dict(reader)

    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame:
        materialization_arguments = metadata["materialization"].get("arguments", {})
        if "token" in materialization_arguments:
            self.headers = {"token": "%s" % materialization_arguments["token"]}
        else:
            self.headers = {"token": "%s" % "QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS"}
        datatype = materialization_arguments.get("type", None)
        return self.fetch_data(datatype=datatype)

    def fetch_data(self, data_range=None, location: str = 'los angeles', datatype: str = 'TAVG'):
        """ fetch data using Noaa api and return the dataset
            the date of the data is in the range of data range
            location is the city name

        Args:
            data_range: data range constrain.
            location: string of location
            datatype: string of data type for the query


        Returns:

        """
        result = pd.DataFrame(columns=['date', 'stationid', 'city', datatype])
        if data_range is None:
            startdate = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            enddate = datetime.datetime.today().strftime('%Y-%m-%d')
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TAVG&locationid=CITY:US060013&startdate=' \
                  + startdate + "&enddate=" + enddate
            response = requests.get(api, headers=self.headers)
            data = response.json()
            self.add_result(result, data, location)
        else:
            datatypeid = ''
            locationid = ''
            if len(data_range) == 1:
                data_range.append(datetime.datetime.today().strftime('%Y-%m-%d'))
            if location.startswith('zip') is False:
                locationid += '&locationid=' + self.city_to_id_map[location.lower()]
            else:
                locationid += location
            datatypeid += '&datatypeid='
            datatypeid += datatype + ','
            startdate = '&startdate=' + data_range[0]
            enddate = '&enddate=' + data_range[1]
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND' + datatypeid + locationid + \
                  startdate + enddate + '&limit=1000&offset=1'
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
        resultset = data['metadata']['resultset']
        num = float(resultset['limit']) + float(resultset['offset']) - 1
        if num < float(resultset['count']):
            return True
        return False

    @staticmethod
    def add_result(result, data, location):
        """ get the useful data from raw data and add them in the return dataframe

        """
        for row in data['results']:
            result.loc[len(result)] = [row['date'], row['station'], location, row['value']]
