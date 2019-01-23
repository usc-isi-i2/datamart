from numpy import nan
import pandas as pd

from datamart.materializers.parsers.parser_base import *

class ExcelParser(ParserBase):

    @staticmethod
    def encode_url(url):
        return url.replace(" ", "%20")    

    def parse(self, url: str) -> typing.Optional[pd.DataFrame]:
        """
        Retrive excel file from url and return Dataframe and metadata for every sheet

        Params:
            - url: (str)
        
        Returns:
            - dfs: (list) list of dataframes and metadata dicts
        """
        url = self.encode_url(url)
        xl = pd.ExcelFile(url)
        dfs = []
        for sheet in xl.sheet_names:
            meta, start_row, skipfooter = self._parse_metadata(xl, sheet)
            df = xl.parse(header=start_row, skipfooter=skipfooter, sheet_name=sheet)
            dfs.append({
                "sheet_name":sheet,
                "df":df,
                "metadata":meta
            })
        
        return dfs
    
    
    def _parse_metadata(self, xl, sheetname):
        """
        Read excel sheet and parse metadata from it

        Params:
            - xl: (ExcelFile) pandas ExcelFile objects
            - sheetname: (str) name of the sheet to parse

        Returns:
            - metadata: (dict) metadata extracted from the file
            - start_row: (int) start index of data
            - skipfooter: (int) index of data starting from the end of table

        """
        df = xl.parse(sheet_name=sheetname, index_col=None, header=None)
        df = df.replace(' ', nan)
        df = df.dropna(how='all', axis=1)
        rows, cols = df.shape
        metadata = {}
        start_row = 0
        # start from beginning
        # find starting metadata
        for row in range(rows):
            if df.iloc[row].isnull().sum() > cols / 2:
                # is metadata
                item = df.iloc[row].dropna().tolist()
                if len(item) > 0:
                    metadata[row] = item
            else:
                start_row = row
                break
        # start from end
        # find footer metadata
        end_row = rows
        for row in range(rows - 1, -1, -1):
            if df.iloc[row].isnull().sum() > cols / 2:
                # is metadata
                item = df.iloc[row].dropna().tolist()
                if len(item) > 0:
                    metadata[row] = item
            else:

                end_row = row + 1
                break
        skipfooter = rows - end_row
        return metadata, start_row, skipfooter