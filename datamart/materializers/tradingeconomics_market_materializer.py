from datamart.materializers.materializer_base import MaterializerBase

import pandas as pd
import typing
import sys
import traceback
import datetime


class TradingEconomicsMarketMaterializer(MaterializerBase):
    """TradingEconomicsMaterializer class extended from  Materializer class

    """

    def __init__(self, **kwargs):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self, **kwargs)

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on

                Assuming date is in the format %Y-%m-%d
        """
        if not constrains:
            constrains = dict()
        materialization_arguments = metadata["materialization"].get("arguments", {})
        #https://api.tradingeconomics.com/markets/historical/lb1:com?c=guest:guest&d1=1700-08-01&format=csv
        getUrl = materialization_arguments['arguments']['url']
        key="guest:guest"
        try:
            keyFile = open("tradingeconomics.txt", "r")
            key = keyFile.read()
        except:
            print("file not found")

        urlStrs=getUrl.split("?c=")
        keyStr=urlStrs[1].split('&')
        keyStr[1]=key
        urlStrs[1]='&'.join(keyStr)
        getUrl="?c=".join(urlStrs)

        date_range = constrains.get("date_range", {})
        datestr = ""

        if date_range.get("start", None) and date_range.get("end", None):
            datestr += "d1=" + date_range["start"]
            datestr += '&d2=' + date_range["end"]
        elif date_range.get("start", None):
            datestr += "d1=" + date_range["start"]
            now = datetime.datetime.now()
            datestr += '&d2=' + "{}-{}-{}".format(now.year, now.month, now.day)
        elif date_range.get("end", None):
            datestr += "d1=" + "{}-{}-{}".format("1800", "01", "01")
            datestr += '&d2=' + date_range["end"]
        else:
            now = datetime.datetime.now()
            datestr += "d1=" + "{}-{}-{}".format("1800", "01", "01")
            datestr += '&d2=' + "{}-{}-{}".format(now.year, now.month, now.day)

        path = getUrl.split("&")
        path[1] = datestr
        getUrl = "&".join(path)

        return self.fetch_data(getUrl)

    def fetch_data(self, getUrl):
        """
        Returns:
             result: A pd.DataFrame;
        """
        try:
            data = pd.read_csv(getUrl, encoding='utf-16')
            return data
        except Exception as e:
            print('exception in run', e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join(lines))
