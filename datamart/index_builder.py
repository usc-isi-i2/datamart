import json
import os
import pandas as pd
from datamart.metadata.global_metadata import GlobalMetadata
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.utils import Utils
import typing


class IndexBuilder(object):
    def __init__(self):
        """Init method of IndexBuilder.

        """

        self.resources_path = os.path.join(os.path.dirname(__file__), "resources")
        self.index_config = json.load(open(os.path.join(self.resources_path, 'index_info.json'), 'r'))
        self.current_global_index = self.index_config["current_index"]
        self.GLOBAL_INDEX_INTERVAL = 10000

    def indexing(self,
                 description_path: str,
                 data_path: str = None,
                 query_data_for_indexing: bool = False,
                 save_to_file: str = None
                 ) -> dict:
        """API for the index builder.

        By providing description file, index builder should be able to process it and create metadata json for the
        dataset, create index in our index store

        Args:
            description_path: Path to description json file.
            data_path: Path to data csv file.
            query_data_for_indexing: Bool. If no data is presented, and query_data_for_indexing is False, will only
                create metadata according to the description json. If query_data_for_indexing is True and no data is
                presented, will use Materialize to query data for profiling and indexing
            save_to_file: str, a path to the json line file

        Returns:
            metadata dictionary

        """

        description, data = self.read_data(description_path, data_path)
        if not data and query_data_for_indexing:
            try:
                materializer_module = description["materialization"]["python_path"]
            except:
                raise ValueError("No materialization method found")
            materializer = Utils.load_materializer(materializer_module)
            data = materializer.get(metadata=description)
        metadata = self.construct_global_metadata(description=description, data=data)
        if save_to_file:
            self.save_data(save_to_file=save_to_file, metadata=metadata)
        return metadata.value

    @staticmethod
    def read_data(description_path: str, data_path: str = None) -> typing.Tuple[dict, pd.DataFrame]:
        """Read dataset description json and dataset if present.

        Args:
            description_path: Path to description json file.
            data_path: Path to data csv file.

        Returns:
            Tuple of (description json, dataframe of data)
        """

        description = json.load(open(description_path, 'r'))
        Utils.validate_schema(description)
        if data_path:
            data = pd.read_csv(open(data_path), 'r')
        else:
            data = None
        return description, data

    @staticmethod
    def save_data(save_to_file: str, metadata: GlobalMetadata):
        """Save metadata json to file.

        Args:
            save_to_file: Path of the saving file.
            metadata: metadata instance.

        Returns:
            save to file with 2 lines for each metadata, first line is id, second line is metadata json
        """

        with open(save_to_file, 'a+') as out:
            out.write(str(metadata.datamart_id))
            out.write("\n")
            out.write(json.dumps(metadata.value))
            out.write("\n")

    def save_index_config(self):
        """Save index config file.

        Args:

        Returns:
            save to file with updated current_index in es
        """
        self.index_config["current_index"] = self.current_global_index
        with open(os.path.join(self.resources_path, 'index_info.json'), 'w') as f:
            json.dump(self.index_config, f, indent=2)

    def construct_global_metadata(self, description: dict, data: pd.DataFrame = None) -> GlobalMetadata:
        """Construct global metadata.

        Args:
            description: description dict.
            data: dataframe of data.

        Returns:
            GlobalMetadata instance
        """

        self.current_global_index += self.GLOBAL_INDEX_INTERVAL

        global_metadata = GlobalMetadata.construct_global(description, datamart_id=self.current_global_index)
        for col_offset, variable_description in enumerate(description["variables"]):
            variable_metadata = self.construct_variable_metadata(variable_description,
                                                                 col_offset=col_offset,
                                                                 data=data)
            global_metadata.add_variable_metadata(variable_metadata)

        if data is not None:
            global_metadata = self.profiling_entire(global_metadata, data)

        return global_metadata

    def construct_variable_metadata(self,
                                    description: dict,
                                    col_offset: int,
                                    data: pd.DataFrame = None
                                    ) -> VariableMetadata:

        """Construct variable metadata.

        Args:
            description: description dict.
            col_offset: integer, the column index.
            data: dataframe of data.

        Returns:
            VariableMetadata instance
        """

        variable_metadata = VariableMetadata.construct_variable(description,
                                                                datamart_id=col_offset + self.current_global_index + 1)

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
        if variable_metadata.named_entity is None:
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
        return column.unique().tolist()
