import json
import os
import pandas as pd
import warnings
from datamart.metadata.global_metadata import GlobalMetadata
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.index_manager import IndexManager
from datamart.utils import Utils
from datamart.profiler import Profiler
import typing

GLOBAL_INDEX_INTERVAL = 10000


class IndexBuilder(object):
    def __init__(self):
        """Init method of IndexBuilder.

        """

        self.resources_path = os.path.join(os.path.dirname(__file__), "resources")
        self.index_config = json.load(open(os.path.join(self.resources_path, 'index_info.json'), 'r'))
        self.current_global_index = None
        self.GLOBAL_INDEX_INTERVAL = GLOBAL_INDEX_INTERVAL
        self.profiler = Profiler
        self.im = IndexManager(es_host=self.index_config["es_host"], es_port=self.index_config["es_port"])

    def indexing(self,
                 description_path: str,
                 es_index: str,
                 data_path: str = None,
                 query_data_for_indexing: bool = False,
                 save_to_file: str = None,
                 save_to_file_mode: str = "a+",
                 delete_old_es_index: bool = False
                 ) -> dict:
        """API for the index builder.

        By providing description file, index builder should be able to process it and create metadata json for the
        dataset, create index in our index store

        Args:
            description_path: Path to description json file.
            es_index: str, es index for this dataset
            data_path: Path to data csv file.
            query_data_for_indexing: Bool. If no data is presented, and query_data_for_indexing is False, will only
                create metadata according to the description json. If query_data_for_indexing is True and no data is
                presented, will use Materialize to query data for profiling and indexing
            save_to_file: str, a path to the json line file
            save_to_file_mode: str, mode for saving, default "a+"
            delete_old_es_index: bool, boolean if delete original es index if it exist

        Returns:
            metadata dictionary

        """

        self.check_es_index(es_index=es_index, delete_old_es_index=delete_old_es_index)

        if not self.current_global_index or delete_old_es_index:
            self.current_global_index = self.im.current_global_datamart_id(index=es_index)

        description, data = self.read_data(description_path, data_path)
        if not data and query_data_for_indexing:
            try:
                materializer_module = description["materialization"]["python_path"]
                materializer = Utils.load_materializer(materializer_module)
                data = materializer.get(metadata=description)
            except:
                warnings.warn("Materialization Failed, index based on schema json only")
        metadata = self.construct_global_metadata(description=description, data=data)
        Utils.validate_schema(metadata.value)

        if save_to_file:
            self.save_data(save_to_file=save_to_file, save_mode=save_to_file_mode, metadata=metadata)

        self.im.create_doc(index=es_index, doc_type='document', body=metadata.value, id=metadata.value['datamart_id'])

        return metadata.value

    def bulk_indexing(self,
                      description_dir: str,
                      es_index: str,
                      data_dir: str = None,
                      query_data_for_indexing: bool = False,
                      save_to_file: str = None,
                      save_to_file_mode: str = "a+",
                      delete_old_es_index: bool = False
                      ):
        """Bulk indexing many dataset by providing a path

        Args:
            description_dir: dir of description json files.
            es_index: str, es index for this dataset
            data_dir: dir of data csv files.
            query_data_for_indexing: Bool. If no data is presented, and query_data_for_indexing is False, will only
                create metadata according to the description json. If query_data_for_indexing is True and no data is
                presented, will use Materialize to query data for profiling and indexing
            save_to_file: str, a path to the json line file
            save_to_file_mode: str, mode for saving, default "a+"
            delete_old_es_index: bool, boolean if delete original es index if it exist

        Returns:

        """

        self.check_es_index(es_index=es_index, delete_old_es_index=delete_old_es_index)
        for description in os.listdir(description_dir):
            if description.endswith('.json'):
                description_path = os.path.join(description_dir, description)
                data_path = None
                if data_dir:
                    data_path = os.path.join(data_dir, description.replace("_description.json", ".csv"))
                print("==== Creating metadata and indexing for " + description)
                self.indexing(description_path=description_path,
                              es_index=es_index,
                              data_path=data_path,
                              query_data_for_indexing=query_data_for_indexing,
                              save_to_file=save_to_file,
                              save_to_file_mode=save_to_file_mode)

    def check_es_index(self, es_index: str, delete_old_es_index: bool):
        """Check es index, delete or create if necessary

        Args:
            es_index: str, es index for this dataset
            delete_old_es_index: bool, boolean if delete original es index if it exist

        Returns:

        """

        if delete_old_es_index:
            self.im.delete_index(index=[es_index])
            self.im.create_index(index=es_index)
        elif not self.im.check_exists(index=es_index):
            self.im.create_index(index=es_index)

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
    def save_data(save_to_file: str, save_mode: str, metadata: GlobalMetadata):
        """Save metadata json to file.

        Args:
            save_to_file: Path of the saving file.
            save_mode: save mode
            metadata: metadata instance.

        Returns:
            save to file with 2 lines for each metadata, first line is id, second line is metadata json
        """

        with open(save_to_file, mode=save_mode) as out:
            out.write(str(metadata.datamart_id))
            out.write("\n")
            out.write(json.dumps(metadata.value))
            out.write("\n")

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
            profiled VariableMetadata instance
        """

        if not variable_metadata.name:
            variable_metadata.name = column.name

        if not variable_metadata.description:
            variable_metadata.description = self.profiler.construct_variable_description(column)

        if variable_metadata.named_entity is None:
            variable_metadata.named_entity = self.profiler.profile_named_entity(column)

        if variable_metadata.temporal_coverage:
            if variable_metadata.temporal_coverage['start'] or not variable_metadata.temporal_coverage['end']:
                variable_metadata.temporal_coverage = self.profiler.profile_temporal_coverage(
                    variable_metadata.temporal_coverage, column)

        return variable_metadata

    def profiling_entire(self, global_metadata: GlobalMetadata, data: pd.DataFrame) -> GlobalMetadata:
        """Profiling entire dataset for necessary fields of metadata, if data is present .

        Args:
            global_metadata: the original GlobalMetadata instance.
            data: dataframe of data.

        Returns:
            profiled GlobalMetadata instance
        """

        if not global_metadata.title:
            global_metadata.title = self.profiler.construct_global_title(data)

        if not global_metadata.description:
            global_metadata.description = self.profiler.construct_global_description(data)

        if not global_metadata.keywords:
            global_metadata.keywords = self.profiler.construct_global_keywords(data)

        return global_metadata

    def bulk_load_metadata(self,
                           metadata_out_file: str,
                           es_index:str):
        self.im.create_doc_bulk(file=metadata_out_file, index=es_index)
