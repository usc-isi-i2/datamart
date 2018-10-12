from datamart.materializers.materializer_base import MaterializerBase

import pandas as pd
import typing
import os
import sys
from config import config
import traceback

import shutil
dig_data_downloader_path = config['dig_data_downloader_path']
sys.path.append(os.path.join(dig_data_downloader_path, 'downloader'))
from file_downloader import FileDownloader

class TradingEconomicsMaterializer(MaterializerBase):
    """TradingEconomicsMaterializer class extended from  Materializer class

    """

    def __init__(self):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self)
        self.key = None


    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        if not constrains:
            constrains = dict()

        materialization_arguments = metadata["materialization"].get("arguments", {})
        getUrl=metadata['url']
        if "key" in constrains:
            self.key = {"key": constrains["key"]}
        else:
            self.headers = {"key": "guest:guest"}
        date_range = constrains.get("date_range", {})
        if "locations" in constrains:
            locations = constrains["locations"]
        else:
            locations = ['all']
        dataset_id = constrains.get("dataset_id", "TE")

        datasetConfig = {
            "where_to_download": {
                "frequency": "quarterly",
                "method": "get",
                "file_type": "csv",
                "template": metadata['url'],
                "replication": {
                    },
                "identifier": metadata['title'].replace(' ','_')
            },
        }

        return self.fetch_data(getUrl,datasetConfig,date_range=date_range, locations=locations, dataset_id=dataset_id)

    def fetch_data(self, getUrl,datasetConfig,date_range: dict = None, locations: list = None,
                   dataset_id: str = 'TE'):
        """

        Args:
            date_range: data range constrain.(format: %Y-%m-%d)
            locations: list of string of location
            data_type: string of data type for the query
            dataset_id


        Returns:
             result: A pd.DataFrame;
        """
        #ignoring constrain for now.
        # #data downloader json
        try:
            dst_dataset_path = os.path.join('tmp/', datasetConfig["where_to_download"]["identifier"])

            if os.path.exists(dst_dataset_path):
                shutil.rmtree(dst_dataset_path)

            # download
            fs = FileDownloader(dst_dataset_path)
            fs.process(datasetConfig, force=True, current_datetime=None)
            filename = datasetConfig["where_to_download"]["identifier"] + "." + datasetConfig["where_to_download"]["file_type"]
            data = pd.read_csv(dst_dataset_path + "/" + filename)
            return data
        except Exception as e:
            print('exception in run', e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join(lines))

