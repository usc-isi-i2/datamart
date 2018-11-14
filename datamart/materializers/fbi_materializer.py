import numpy as np
import pandas as pd
import typing
import requests
import os

from materializer_base import MaterializerBase

class FbiMaterializer(MaterializerBase):
    """
    FBI Materialzier class.
    Currently only supports table 8.
    """

    def __init__(self):
        MaterializerBase.__init__(self)
        self.tmp_file_path = "tmp_fbi_file.xls"
    
    def get(self,
        metadata: dict = None,
        constrains: dict = None
        ) -> typing.Optional[pd.DataFrame]:
        """
        Fetches the data and returns dataframe
        """

        materialization_arguments = metadata["materialization"].get("arguments", {})
        self.url = materialization_arguments.get("url","")

        try:
            self._get_excel_file(self.url)
        except Exception as e:
            print("Exception occured while fetching file. More details: \n{}".format(e))
            return pd.DataFrame()
            
        metadata, start_row, skipfooter = self._parse_metadata()
        df = self._parse_file(start_row, skipfooter)

        os.remove(self.tmp_file_path)

        return df

    def _get_excel_file(self, url):
        """
        Fetch the excel file from the fbi.gov website and save to disk.

        Params:
            - url: (str) Url to fetch excel file from

        Returns:
    
        """
        r = requests.get(url)
        if r.status_code == 200:
            with open(self.tmp_file_path,'wb') as f:
                f.write(r.content)
        else:
            raise Exception('File not found. Status code: {}'.format(r.status_code))

    def _parse_metadata(self):
        """
        Read excel file and parse metadata from it

        Returns:
            - metadata: (dict) metadata extracted from the file
            - start_row: (int) start index of data
            - skipfooter: (int) index of data starting from the end of table

        """
        df = pd.read_excel(self.tmp_file_path,index_col=None, header=None)
        df = df.replace(' ',np.nan)
        df = df.dropna(how='all',axis=1)
        rows, cols = df.shape
        metadata = []
        start_row = 0
        # start from beginning
        # find starting metadata
        for row in range(rows):
            if df.iloc[row].isnull().sum() > cols/2:
                #is metadata
                metadata.append(df.iloc[row][0])
            else:
                start_row = row
                break
        # start from end
        # find footer metadata
        footer = []
        for row in range(rows-1,-1,-1):
            if df.iloc[row].isnull().sum() > cols/2:
                #is metadata
                footer.append(df.iloc[row][0])
            else:
                end_row = row
                break
        metadata.extend(footer[::-1])
        skipfooter = rows-end_row
        return metadata, start_row, skipfooter

    def _parse_file(self, start_row, skipfooter):
        """
        Read excel file and return table data

        Returns:
            - df: (pd.DataFrame) extracted table

        """
        df = pd.read_excel(self.tmp_file_path,header=start_row, index_col=[0,1], skipfooter=skipfooter)
        df = df.replace('\s+',np.nan,regex=True)
        df = df.dropna(how='all',axis=1)
        return df.dropna(how='all')
    
    

        
    