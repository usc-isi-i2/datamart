from datamart.materializers.materializer_base import MaterializerBase

import pandas as pd
import typing
import sys
import traceback
import datetime


class AcledMaterializer(MaterializerBase):
    """AcledMaterializer class extended from  Materializer class

    """


def __init__(self, **kwargs):
    """ initialization and loading the city name to city id map

    """
    MaterializerBase.__init__(self, **kwargs)
    self.key = None


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

    getUrl = metadata['url']
    date_range = constrains.get("date_range", {})
    datestr = ""

    if date_range.get("start", None) and date_range.get("end", None):
        datestr += "event_date=" + date_range["start"]
        datestr += '|' + date_range["end"]
        datestr+= '&event_date_where = BETWEEN'
    elif date_range.get("start", None):
        datestr += "event_date=" + date_range["start"]
        datestr += '|' + date_range["end"]
        datestr += '&event_date_where = BETWEEN'
        now = datetime.datetime.now()
        end="{}-{}-{}".format(now.year, now.month, now.day)
        datestr += '|' + end
        datestr += '&event_date_where = BETWEEN'
    elif date_range.get("end", None):
        datestr += "event_date=" + date_range["end"]
    else:
        now = datetime.datetime.now()
        datestr += "event_date=" + "{}-{}-{}".format(now.year, now.month, now.day)

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