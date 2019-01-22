from datamart.materializers.parsers.parser_base import *


class CSVParser(ParserBase):
    def parse(self, url: str) -> typing.Optional[pd.DataFrame]:
        return pd.read_csv(url)