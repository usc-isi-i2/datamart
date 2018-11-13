import json
import os
import pandas as pd
import warnings
from datamart.metadata.global_metadata import GlobalMetadata
from datamart.metadata.variable_metadata import VariableMetadata
from datamart.es_managers.index_manager import IndexManager
from datamart.utils import Utils
from datamart.profilers.basic_profiler import BasicProfiler
import typing
import traceback

GLOBAL_INDEX_INTERVAL = 10000


class IndexBuilder(object):
    def __init__(self) -> None:
        """Init method of IndexBuilder.

        """

        self.resources_path = os.path.join(os.path.dirname(__file__), "resources")
        with open(os.path.join(self.resources_path, 'index_info.json'), 'r') as index_info_f:
            self.index_config = json.load(index_info_f)
        self.current_global_index = None
        self.GLOBAL_INDEX_INTERVAL = GLOBAL_INDEX_INTERVAL
        self.basic_profiler = BasicProfiler
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

        self._check_es_index(es_index=es_index, delete_old_es_index=delete_old_es_index)

        if not self.current_global_index or delete_old_es_index:
            self.current_global_index = self.im.current_global_datamart_id(index=es_index)

        description, data = self._read_data(description_path, data_path)
        if not data and query_data_for_indexing:
            try:
                data = Utils.materialize(metadata=description).infer_objects()
            except:
                traceback.print_exc()
                warnings.warn("Materialization Failed, index based on schema json only")

        metadata = self.construct_global_metadata(description=description, data=data)
        Utils.validate_schema(metadata.value)

        if save_to_file:
            self._save_data(save_to_file=save_to_file, save_mode=save_to_file_mode, metadata=metadata)

        self.im.create_doc(index=es_index, doc_type='_doc', body=metadata.value, id=metadata.value['datamart_id'])

        return metadata.value

    def updating(self,
                 description_path: str,
                 es_index: str,
                 document_id: int,
                 data_path: str = None,
                 query_data_for_updating: bool = False
                 ) -> dict:

        """Update document in elastic search.

        By providing description file, index builder should be able to process it and create metadata json for the
        dataset, update document in elastic search

        Args:
            description_path: Path to description json file.
            es_index: str, es index for this dataset
            document_id: int, document id of document which need to be updated
            data_path: Path to data csv file.
            query_data_for_updating: Bool. If no data is presented, and query_data_for_updating is False, will only
                create metadata according to the description json. If query_data_for_updating is True and no data is
                presented, will use Materialize to query data for profiling and indexing

        Returns:
            metadata dictionary

        """

        self._check_es_index(es_index=es_index)

        description, data = self._read_data(description_path, data_path)
        if not data and query_data_for_updating:
            try:
                materializer_module = description["materialization"]["python_path"]
                materializer = Utils.load_materializer(materializer_module)
                data = materializer.get(metadata=description)
            except:
                warnings.warn("Materialization Failed, index based on schema json only")

        metadata = self.construct_global_metadata(description=description, data=data, overwrite_datamart_id=document_id)
        Utils.validate_schema(metadata.value)

        self.im.update_doc(index=es_index, doc_type='document', body={"doc": metadata.value},
                           id=metadata.value['datamart_id'])

        return metadata.value

    def bulk_indexing(self,
                      description_dir: str,
                      es_index: str,
                      data_dir: str = None,
                      query_data_for_indexing: bool = False,
                      save_to_file: str = None,
                      save_to_file_mode: str = "a+",
                      delete_old_es_index: bool = False
                      ) -> None:
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

        self._check_es_index(es_index=es_index, delete_old_es_index=delete_old_es_index)
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

    def _check_es_index(self, es_index: str, delete_old_es_index: bool = False) -> None:
        """Check es index, delete or create if necessary

        Args:
            es_index: str, es index for this dataset
            delete_old_es_index: bool, boolean if delete original es index if it exist

        Returns:

        """

        if not self.im.check_exists(index=es_index):
            self.im.create_index(index=es_index)
        elif delete_old_es_index:
            self.im.delete_index(index=[es_index])
            self.im.create_index(index=es_index)

    @staticmethod
    def _read_data(description_path: str, data_path: str = None) -> typing.Tuple[dict, pd.DataFrame]:
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
    def _save_data(save_to_file: str, save_mode: str, metadata: GlobalMetadata) -> None:
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

    def construct_global_metadata(self,
                                  description: dict,
                                  data: pd.DataFrame = None,
                                  overwrite_datamart_id: int = None
                                  ) -> GlobalMetadata:

        """Construct global metadata.

        Args:
            description: description dict.
            data: dataframe of data.
            overwrite_datamart_id: integer id for over writing original one

        Returns:
            GlobalMetadata instance
        """
        if not overwrite_datamart_id:
            self.current_global_index += self.GLOBAL_INDEX_INTERVAL
            datamart_id = self.current_global_index
        else:
            datamart_id = overwrite_datamart_id

        global_metadata = GlobalMetadata.construct_global(description, datamart_id=datamart_id)

        if data is not None:
            global_metadata = self._profiling_entire(global_metadata, data)

        if description.get("variables", []):
            for col_offset, variable_description in enumerate(description["variables"]):
                variable_metadata = self.construct_variable_metadata(description=variable_description,
                                                                     global_datamart_id=datamart_id,
                                                                     col_offset=col_offset,
                                                                     data=data)
                global_metadata.add_variable_metadata(variable_metadata)

        elif data is not None:
            for col_offset in range(data.shape[1]):
                variable_metadata = self.construct_variable_metadata(description={},
                                                                     global_datamart_id=datamart_id,
                                                                     col_offset=col_offset,
                                                                     data=data)
                global_metadata.add_variable_metadata(variable_metadata)

        else:
            warnings.warn(
                "No data to profile for variable metadata. No variable description. Leave empty for variable metadata")

        return global_metadata

    def construct_variable_metadata(self,
                                    description: dict,
                                    global_datamart_id: int,
                                    col_offset: int,
                                    data: pd.DataFrame = None
                                    ) -> VariableMetadata:

        """Construct variable metadata.

        Args:
            description: description dict.
            global_datamart_id: integer of datamart id.
            col_offset: integer, the column index.
            data: dataframe of data.

        Returns:
            VariableMetadata instance
        """

        variable_metadata = VariableMetadata.construct_variable(description,
                                                                datamart_id=col_offset + global_datamart_id + 1)

        if data is not None:
            variable_metadata = self._profiling_column(description, variable_metadata, data.iloc[:, col_offset])

        return variable_metadata

    def _profiling_column(self,
                          description: dict,
                          variable_metadata: VariableMetadata,
                          column: pd.Series
                          ) -> VariableMetadata:
        """Profiling single column for necessary fields of metadata, if data is present .

        Args:
            description: description dict about the column.
            variable_metadata: the original VariableMetadata instance.
            column: the column to profile.

        Returns:
            profiled VariableMetadata instance
        """

        if not variable_metadata.name:
            variable_metadata.name = column.name

        if not variable_metadata.description:
            variable_metadata.description = self.basic_profiler.construct_variable_description(column)

        if variable_metadata.named_entity is None:
            variable_metadata.named_entity = self.basic_profiler.profile_named_entity(column)
        elif variable_metadata.named_entity is False and not description:
            named_entities = self.basic_profiler.named_entity_recognize(column)
            if named_entities:
                variable_metadata.named_entity = named_entities

        if variable_metadata.temporal_coverage is not False:
            if not variable_metadata.temporal_coverage['start'] or not variable_metadata.temporal_coverage['end']:
                variable_metadata.temporal_coverage = self.basic_profiler.profile_temporal_coverage(
                    column=column, coverage=variable_metadata.temporal_coverage)

        elif not description:
            temporal_coverage = self.basic_profiler.profile_temporal_coverage(column=column)
            if temporal_coverage:
                variable_metadata.temporal_coverage = temporal_coverage

        return variable_metadata

    def _profiling_entire(self, global_metadata: GlobalMetadata, data: pd.DataFrame) -> GlobalMetadata:
        """Profiling entire dataset for necessary fields of metadata, if data is present .

        Args:
            global_metadata: the original GlobalMetadata instance.
            data: dataframe of data.

        Returns:
            profiled GlobalMetadata instance
        """

        if not global_metadata.title:
            global_metadata.title = self.basic_profiler.construct_global_title(data)

        if not global_metadata.description:
            global_metadata.description = self.basic_profiler.construct_global_description(data)

        if not global_metadata.keywords:
            global_metadata.keywords = self.basic_profiler.construct_global_keywords(data)

        return global_metadata

    def _bulk_load_metadata(self,
                            metadata_out_file: str,
                            es_index: str
                            ) -> None:
        """Internal method for bulk loading documents to elasticsearch.

        Args:
            metadata_out_file: file of metadata output file produced by index builder
            es_index: str of es index

        Returns:

        """

        self.im.create_doc_bulk(file=metadata_out_file, index=es_index)
