from datamart.materializers.parsers.parser_base import ParserBase
import pandas as pd


class ExcelParser(ParserBase):
    def parse(self, url: str) -> pd.DataFrame:
        # TODO: process excel to DataFrame and return
        # content = self.load_content(url)
        pass