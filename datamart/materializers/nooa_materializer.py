from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests


class NoaaMaterializer(MaterializerBase):

    def __init__(self):
        MaterializerBase.__init__(self)
        self.default_token = 'QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS'
        self.headers = { "token": "%s"% (self.default_token) }
        with open('../utilities/city_id_map.csv', 'r') as csv_file:
            reader = csv.reader(csv_file)
            self.city_to_id_map = dict(reader)

    def get(self, *args, **kwargs) -> pd.DataFrame:
        my_type = kwargs.get("materialization_component", {}).get("type", None)
        res = self.fetch_data(datatype=my_type)
        return res

    ''' fetch data using Noaa api and return the dataset
        the date of the data is in the range of data range
        location is the city name

    '''
    def fetch_data(self, data_range=None, location='los angeles', datatype='TAVG'):
        result = pd.DataFrame(columns=['date', 'stationid', 'city', datatype])
        if data_range is None:
            startdate = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            enddate = datetime.datetime.today().strftime('%Y-%m-%d')
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TAVG&locationid=CITY:US060013&startdate=' + startdate + "&enddate=" + enddate
            response = requests.get(api, headers=self.headers)
            data = response.json()
            self.add_result(result, data, location)
        else:
            datatypeid = ''
            locationid = ''
            if(len(data_range) == 1):
                data_range.append(datetime.datetime.today().strftime('%Y-%m-%d'))
            if(location.startswith('zip') is False):
                locationid += '&locationid=' + self.city_to_id_map[location.lower()]
            else:
                locationid += location
            datatypeid += '&datatypeid='
            datatypeid += datatype + ','
            startdate = '&startdate=' + data_range[0]
            enddate = '&enddate=' + data_range[1]
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND' + datatypeid + locationid + startdate + enddate + '&limit=1000&offset=1'
            response = requests.get(api, headers=self.headers)
            data = response.json()
            self.add_result(result, data, location)
            while self.next(data):
                index = api.rfind('&')
                next_offset = int(api[index+8:]) + 1000
                api = api[:index+8] + str(next_offset)
                response = requests.get(api, headers=self.headers)
                data = response.json()
                self.add_result(result, data, location)
        return result

    ''' check whether it needs next query(get next 1000 data)
    
    '''
    def next(self, data):
        resultset = data['metadata']['resultset']
        num = float(resultset['limit']) + float(resultset['offset']) - 1
        if(num < float(resultset['count'])):
            return True
        return False

    ''' get the useful data from raw data and add them in the return dataframe
    
    '''
    def add_result(self, result, data, location):
        for row in data['results']:
            result.loc[len(result)] = [row['date'], row['station'], location, row['value']]
