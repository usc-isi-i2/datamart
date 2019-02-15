from datamart.materializers.materializer_base import MaterializerBase
from tablextract import tables
from typing import Optional
from pandas import DataFrame
from collections import OrderedDict


class WikitablesMaterializer(MaterializerBase):
    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)

    def get(self, metadata: dict = None, constrains: dict = None) -> Optional[DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        tabs = tables(args['url'], xpath_filter=args['xpath'])
        data = OrderedDict()
        if len(tabs):
            tab = tabs[0].record
            data = OrderedDict()
            for col in tab[0].keys():
                data[col] = [r[col] for r in tab]
        return DataFrame(data)