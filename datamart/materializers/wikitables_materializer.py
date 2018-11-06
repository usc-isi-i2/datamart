from datamart.materializers.materializer_base import MaterializerBase
from datamart.materializers.wikitables_materializer.wikitables import tables

class WikitablesMaterializer(MaterializerBase):
    """ WikitablesMaterializer class extended from  Materializer class """

    def get(self,
            metadata: dict = None,
            variables: typing.List[int] = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.

            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        tab = tables(args['url'], store_result=False, xpath=args['xpath'])
        return tab.dataframe()