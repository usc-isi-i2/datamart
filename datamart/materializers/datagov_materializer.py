from datamart.materializers.materializer_base import MaterializerBase
import requests
import io
import pandas as pd
import typing


class datagov_materializer(MaterializerBase):
    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)

    def get(self,
            metadata: dict = None,
            constrains: dict = None
            ) -> typing.Optional[pd.DataFrame]:
        """ API for get a dataframe.
            Args:
                metadata: json schema for data_type
                variables:
                constrains: include some constrains like date_range, location and so on
        """
        args = metadata['materialization']['arguments']
        database = requests.get(args).content
        df = pd.read_csv(io.StringIO(database.decode('utf-8')))

        return df
