import pandas as pd
import urllib.request


class ParserBase:
    """
    Parser for GeneralMaterializer. the parsers will take the url for an HTML page, a csv file, a json file,\
    or an Excel file etc, as input, and parse them into pandas DataFrame(s) and return
    """
    def parse(self, url: str) -> pd.DataFrame:
        return pd.DataFrame()

    @staticmethod
    def load_content(url: str) -> bytes:
        return urllib.request.urlopen(url).read()
