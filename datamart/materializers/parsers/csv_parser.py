from datamart.materializers.parsers.parser_base import ParserBase
import pandas as pd


class CSVParser(ParserBase):
    def parse(self, url: str) -> pd.DataFrame:
        return pd.read_csv(url)