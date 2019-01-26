from datamart.materializers.parsers.parser_base import *


class CSVParser(ParserBase):

    def get_all(self, url: str) -> typing.List[pd.DataFrame]:
        return [pd.read_csv(url)]