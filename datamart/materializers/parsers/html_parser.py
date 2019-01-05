from datamart.materializers.parsers.parser_base import ParserBase
import pandas as pd
# from etk.extractors.table_extractor import TableExtractor


class HTMLParser(ParserBase):
    def parse(self, url: str) -> pd.DataFrame:
        content = self.load_content(url)
        # table_extractor = TableExtractor()
        # res = table_extractor.extract(content.decode('utf-8', 'ignore'))
        # TODO: pass the Extraction to DataFrame
        pass