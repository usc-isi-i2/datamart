from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.parsers import *
import pandas as pd
import typing
from datamart.materializers.parsers.parse_result import ParseResult


class GeneralMaterializer(MaterializerBase):
    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)
        self.type2parser = {
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

    def parse(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.List[ParseResult]:
        """ API for get a dataframe.
            Args:
                metadata: json schema for data_type
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        url = args['url']
        _type = args.get('file_type') or url.rstrip('/').rsplit('/', 1)[-1].rsplit('.', 1)[-1]
        parser = self.type2parser.get(_type, HTMLParser)()
        res = parser.parse(url)

        # TODO: the index builder is designed for one description - one doc, need to change to support a list of tables
        # if isinstance(res, list):
        #     return res[0]
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

        # TODO: the index builder is designed for one description - one doc, need to change to support a list of tables
        # if isinstance(res, list):
        #     return res[0]
        return res
