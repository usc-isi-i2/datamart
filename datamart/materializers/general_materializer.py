from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.parsers import CSVParser, ExcelParser, HTMLParser, JSONParser
import pandas as pd
import typing
from datamart.materializers.parsers.parse_result import ParseResult


class GeneralMaterializer(MaterializerBase):
    type2parser = {
        'csv': CSVParser,
        'json': JSONParser,
        'xls': ExcelParser,
        'xlsb': ExcelParser,
        'xlsm': ExcelParser,
        'xlsx': ExcelParser,
        'excel': ExcelParser,
        'asp': HTMLParser,
        'html': HTMLParser
    }

    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)

    @classmethod
    def get_parser_type(cls, url, file_type=None):
        _type = 'none'
        if file_type:
            if file_type.endswith('csv'):
                _type = 'csv'
            elif file_type.endswith('json'):
                _type = 'json'
            elif file_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
                _type = 'xlsx'
        else:
            _type = url.rstrip('/').rsplit('/', 1)[-1].rsplit('.', 1)[-1]
        return _type

    @classmethod
    def can_parse(cls, url, file_type=None):
        _type = cls.get_parser_type(url, file_type)
        if _type in ['csv', 'json', 'xls', 'xlsb', 'xlsm', 'xlsx', 'excel', 'asp', 'html']:
            return True
        return False

    def parse(self,
              metadata: dict = None,
              constrains: dict = None) -> typing.List[ParseResult]:
        """ API for get a dataframe.
            Args:
                metadata: json schema for data_type
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        url = args['url']
        _type = self.get_parser_type(url, args.get('file_type'))
        parser = self.type2parser.get(_type, HTMLParser)()
        res = parser.parse(url)

        return res

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.
            Args:
                metadata: json schema for data_type
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        url = args['url']
        index = args.get('index', 0)
        _type = args.get('file_type') or url.rstrip('/').rsplit('/', 1)[-1].rsplit('.', 1)[-1]
        parser = self.type2parser.get(_type, HTMLParser)()
        res = parser.get(url, index)

        return res
