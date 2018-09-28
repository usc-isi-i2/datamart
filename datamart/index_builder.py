import json
import os
import pandas as pd
from datamart.metadata.global_metadata import GlobalMetadata
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.materializers.materializer_base import MaterializerBase
import typing
import importlib

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'materializers'))


class IndexBuilder(object):
    def __init__(self):
        """Init method of Profiler.

        """

        self.index_info_path = os.path.join(os.path.dirname(__file__), 'resources/index_info.json')
        self.index_info = json.load(open(self.index_info_path, 'r'))
        self.current_global_index = self.index_info["current_index"]
        self.GLOBAL_INDEX_INTERVAL = 10000

    def indexing(self, description_path: typing.AnyStr, data_path=None, query_data_for_indexing=False):
        """API for the index builder.

        By providing description file, index builder should be able to process it and create metadata json for the
        dataset, create index in our index store

            Args:
                description_path: Path to description json file.
                data_path: Path to data csv file.
                query_data_for_indexing: Bool. If no data is presented, and query_data_for_indexing is False, will only
                    create metadata according to the description json. If query_data_for_indexing is True and no data is
                    presented, will use Materialize to query data for profiling and indexing

        """
        description, data = self.read_data(description_path, data_path)
        if not data and query_data_for_indexing:
            try:
                materializer_module = description["materialization"]
            except:
                raise ValueError("No materialization method found")
            materializer = self.load_materializer(materializer_module)
            data = materializer.get()

        metadata = self.construct_global_metadata(description=description, data=data)
        metadata_json = metadata.value

    @staticmethod
    def read_data(description_path: typing.AnyStr, data_path: typing.AnyStr = None) -> typing.Tuple:
        """Read dataset description json and dataset if present.

        Args:
            description_path: Path to description json file.
            data_path: Path to data csv file.

        Returns:
            Tuple of (description json, dataframe of data)
        """

        description = json.load(open(description_path, 'r'))
        if data_path:
            data = pd.read_csv(open(data_path), 'r')
        else:
            data = None
        return description, data

    @staticmethod
    def load_materializer(materializer_module: typing.AnyStr) -> MaterializerBase:
        """Given the python path to the materializer_module, return a materializer instance.

        Args:
            materializer_module: Path to materializer_module file.

        Returns:
            materializer instance
        """

        module = importlib.import_module(materializer_module)
        md = module.__dict__
        lst = [
            md[c] for c in md if (
                isinstance(md[c], type) and
                issubclass(md[c], MaterializerBase
                           ) and
                md[c].__module__ == module.__name__)
        ]
        try:
            materializer_class = lst[0]
        except:
            raise ValueError("No materializer class found in {}".format(
                os.path.join(os.path.dirname(__file__), 'materializers', materializer_module)))

        materializer = materializer_class()
        return materializer

    def construct_global_metadata(self, description: typing.Dict, data=None) -> GlobalMetadata:
        """Construct global metadata.

        Args:
            description: description dict.
            data: dataframe of data.

        Returns:
            GlobalMetadata instance
        """

        self.current_global_index += self.GLOBAL_INDEX_INTERVAL

        global_metadata = GlobalMetadata(description, datamart_id=self.current_global_index)
        for col_offset, variable_description in enumerate(description["variables"]):
            variable_metadata = self.construct_variable_metadata(variable_description,
                                                                 col_offset=col_offset,
                                                                 data=data)
            global_metadata.add_variable_metadata(variable_metadata)

        if data is not None:
            global_metadata = self.profiling_entire(global_metadata, data)

        return global_metadata

    def construct_variable_metadata(self, description: typing.Dict, col_offset: int,
                                    data: pd.DataFrame = None) -> VariableMetadata:

        """Construct variable metadata.

        Args:
            description: description dict.
            col_offset: integer, the column index.
            data: dataframe of data.

        Returns:
            VariableMetadata instance
        """

        variable_metadata = VariableMetadata(description, datamart_id=col_offset+self.current_global_index)

        if data is not None:
            variable_metadata = self.profiling_column(variable_metadata, data.iloc[:, col_offset])

        return variable_metadata

    def profiling_column(self, variable_metadata: VariableMetadata, column: pd.Series) -> VariableMetadata:
        """Profiling single column for necessary fields of metadata, if data is present .

        Args:
            variable_metadata: the original VariableMetadata instance.
            column: the column to profile.

        Returns:
            VariableMetadata instance
        """

        if variable_metadata.named_entity is True:
            variable_metadata.named_entity = self.profile_named_entity(column)

        return variable_metadata

    def profiling_entire(self, global_metadata: GlobalMetadata, data: pd.DataFrame) -> GlobalMetadata:
        """Profiling entire dataset for necessary fields of metadata, if data is present .

        Args:
            global_metadata: the original GlobalMetadata instance.
            data: dataframe of data.

        Returns:
            GlobalMetadata instance
        """
        return global_metadata

    @staticmethod
    def profile_named_entity(column: pd.Series):
        return column.tolist()
