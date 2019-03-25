from datamart.materializers.parsers.parser_base import *
try:
    from etk.extractors.table_extractor import TableExtractor
except OSError:
    from spacy.cli import download
    download('en_core_web_sm')
    from etk.extractors.table_extractor import TableExtractor


class HTMLParser(ParserBase):

    def get_all(self, url: str) -> typing.List[pd.DataFrame]:
        content = self.load_content(url)
        table_extractor = TableExtractor()
        extractions = table_extractor.extract(content.decode('utf-8', 'ignore'))
        rows = extractions[0].value.get('rows')
        if not rows:
            return []
        cells = [[cell['text'] for cell in row['cells']] for row in extractions[0].value['rows']]
        headers = None
        try:
            if not rows[0]['cells'][0]['cell'].startswith('<td>'):
                headers = cells.pop(0)
        except (IndexError, KeyError, SyntaxError, AttributeError):
            pass
        return [pd.DataFrame(cells, columns=headers)]

