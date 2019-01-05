from datamart.materializers.parsers.parser_base import ParserBase
import pandas as pd


class JSONParser(ParserBase):
    def parse(self, url: str) -> pd.DataFrame:
        # TODO: process json to DataFrame and return
        # content = self.load_content(url)
        pass