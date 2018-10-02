from materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests


class NoaaMaterializer(MaterializerBase):
    def __init__(self):
        MaterializerBase.__init__(self)
        tokens = ['QoCwZxSlvRuUHcKhflbujnBSOFhHvZoS']          
        self.headers = { "token": "%s"% (tokens[0]) }
        with open('../resources/city_id_map.csv', 'r') as csv_file:
            reader = csv.reader(csv_file)
            self.city_to_id_map = dict(reader)
        self.location = ''

    def get(self, *args, **kwargs) -> pd.DataFrame:
        # args is a tuple of positional arguments,
        # because the parameter name has * prepended.
        if args:  # If args is not empty.
            print(args)

        # kwargs is a dictionary of keyword arguments,
        # because the parameter name has ** prepended.
        if kwargs:  # If kwargs is not empty.
            print(kwargs)
        my_type = kwargs.get("materialization_component", {}).get("type", None)
        res = self.fetch_data()
        print(res)
        return res

    def fetch_data(self, data_range=None, location=None, datatypes=[]):
        api = None
        if data_range is None:
            datatypes.append('TAVG')
            startdate = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y-%m-%d')
            self.location='los angeles'
            enddate = datetime.datetime.today().strftime('%Y-%m-%d')
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=TAVG&locationid=CITY:US060013&startdate=' + startdate + "&enddate=" + enddate
            response = requests.get(api, headers=self.headers)
            data = response.json()
            result = []
            self.add_result(result, data)
        else:
            datatypeid = ''
            locationid = ''
            self.location = location
            self.load_map()
            if(len(data_range) == 1):
                data_range.append(datetime.datetime.today().strftime('%Y-%m-%d'))
            if(location.startswith('zip') is False):
                locationid += '&locationid=' + self.city_to_id_map[location.lower()]
            else:
                locationid += location
            if(len(datatypes) > 0):
                datatypeid += '&datatypeid='
                for datatype in datatypes:
                    datatypeid += datatype + ','
                    datatypeid = datatypeid[:-1]
            startdate = '&startdate=' + data_range[0]
            enddate = '&enddate=' + data_range[1]
            api = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND' + datatypeid + locationid + startdate + enddate + '&limit=1000&offset=1'
            response = requests.get(api, headers=self.headers)
            data = response.json()
            result = []
            self.add_result(result, data)
            while(self.next(data)):
                index = api.rfind('&')
                next_offset = int(api[index+8:]) + 1000
                api = api[:index+8] + str(next_offset)
                response = requests.get(api, headers=self.headers)
                data=response.json()
                self.add_result(result, data)
        return self.postprocess(result, datatypes[0])

    def postprocess(self, result, datatype):
        res = pd.DataFrame(columns=['date', 'stationid', 'city', datatype])
        for i, data in enumerate(result):
            time = data.pop(0)
            data.insert(0, time[:-9])
            res.loc[i] = ([data[0], data[2], self.location, data[3]])
        return res

    def next(self, data):
        resultset = data['metadata']['resultset']
        num = float(resultset['limit']) + float(resultset['offset']) - 1
        if(num < float(resultset['count'])):
            return True
        return False
    def add_result(self, result, data):
        for row in data['results']:
            result.append([row['date'], row['datatype'], row['station'], row['value']])        

i = NoaaMaterializer()
i.get()