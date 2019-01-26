from datamart.materializers.parsers.parser_base import *


class JSONParser(ParserBase):

    def get_all(self, url: str) -> typing.List[pd.DataFrame]:
        # TODO: process json to DataFrame and return
        # ONLY RETURN A LIST OF DATAFRAME - PURE DATA
        # content = self.load_content(url)
        pass

    def parse(self, url: str) -> typing.List[ParseResult]:
        # TODO: process json to DataFrame and return
        # RETURN THE PARSE RESULT - WHICH MAY CONTAIN THE INFERENCE OF METADATA
        # content = self.load_content(url)
        pass