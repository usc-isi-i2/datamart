from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.wikitables_downloader.wikitables import tables
from typing import Optional
from pandas import DataFrame


class WikitablesMaterializer(MaterializerBase):
    """ WikitablesMaterializer class extended from  Materializer class """

    def __init__(self, **kwargs):
        """ initialization and loading the city name to city id map

        """
        MaterializerBase.__init__(self, **kwargs)

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> Optional[DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        lang = 'en'
        if 'lang' in args:
            lang = args['lang']
        tab = tables(article=args['url'], lang=lang, store_result=False, xpath=args['xpath'])
        return tab.dataframe()
