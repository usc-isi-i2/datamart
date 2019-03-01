from datamart.materializers.materializer_base import MaterializerBase
from pandas import DataFrame
from typing import Optional
import requests
import sys, traceback


class EIAGovMaterializer(MaterializerBase):
    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)

    def get(self, metadata: dict = None, constraints: dict = None) -> Optional[DataFrame]:
        """
        Materializer for data from eia.gov
        :param metadata: json schema
        :param constraints: filters like data range, date format for this api: YYYYmmdd eg: 20190223
        :return:
        """
        start = None
        end = None
        if constraints:
            start = constraints.get('start', None)
            end = constraints.get('end', None)
        try:
            eia_url = metadata['url']
            if start:
                eia_url = '{}&start={}'.format(eia_url, start)
            if end:
                eia_url = '{}&end={}'.format(eia_url, end)
            response = requests.get(eia_url)
            if response.status_code == 200:
                eia_json = response.json()
                if 'series' in eia_json and len(eia_json['series']) > 0:
                    brent_crude_series = eia_json['series'][0]
                    data_list = brent_crude_series['data']
                    records_list = list()
                    for data in data_list:
                        records_list.append(
                            {
                                'series_id': brent_crude_series['series_id'],
                                'series_name': brent_crude_series['name'],
                                'units': brent_crude_series['units'],
                                'frequency': brent_crude_series['f'],
                                'Date': data[0],
                                'Value': data[1]
                            }
                        )
                    return DataFrame(records_list)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join(lines))
        return DataFrame(columns=['series_id', 'series_name', 'units', 'frequency', 'Date', 'Value'])
