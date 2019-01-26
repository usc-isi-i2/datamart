import pandas as pd
import urllib.request
import typing
from datamart.materializers.parsers.parse_result import ParseResult


class ParserBase:
    """
    Parser for GeneralMaterializer. the parsers will take the url for an HTML page, a csv file, a json file,\
    or an Excel file etc, as input, and parse them into pandas DataFrame(s) and return
    """
    def parse(self, url: str) -> typing.List[ParseResult]:
        return [ParseResult(df=res, index=idx) for idx, res in enumerate(self.get_all(url))]

    def get_all(self, url: str) -> typing.List[pd.DataFrame]:
        return []

    def get(self, url: str, index: int=0) -> typing.Optional[pd.DataFrame]:
        all_results = self.get_all(url)
        if all_results and index < len(all_results):
            return all_results[index]

    @staticmethod
    def load_content(url: str) -> bytes:
        return urllib.request.urlopen(url).read()


