import warnings
import dateutil.parser
from datamart.materializers.materializer_base import MaterializerBase
import importlib
import os
import sys
import json
from jsonschema import validate
from termcolor import colored
import typing
import pandas as pd
import tempfile
from datetime import datetime
from datamart.utilities.timeout import timeout
import re
from datamart.utilities.caching import Cache, EntryState

sys.path.append(os.path.join(os.path.dirname(__file__), '../materializers'))

ES_HOST = 'dsbox02.isi.edu'
ES_PORT = 9200
PRODUCTION_ES_INDEX = 'datamart_all'
TEST_ES_INDEX = 'datamart_tmp'

SEARCH_URL = 'https://isi-datamart.edu'


class Utils:
    INDEX_SCHEMA = json.load(
        open(os.path.join(os.path.join(os.path.dirname(__file__), "../resources"), 'index_schema.json'), 'r'))

    QUERY_SCHEMA = json.load(
        open(os.path.join(os.path.join(os.path.dirname(__file__), "../resources"), 'query_schema.json'), 'r'))

    TMP_FILE_DIR = tempfile.gettempdir()

    DEFAULT_DESCRIPTION = {
        "materialization": {
            "python_path": "default_materializer"
        },
        "variables": []
    }

    MATERIALIZATION_TIME_OUT = 900

    CATEGORICAL_COLUMN_THRESHOLD = 0.2

    DEFAULT_START_DATE = "1900-01-01T00:00:00"

    @staticmethod
    def date_validate(date_text: str) -> typing.Optional[str]:
        """Validate if a string is a valid date.

        Args:
            date_text: date string.

        Returns:
            string of valid date or None
        """

        try:
            this_datetime = dateutil.parser.parse(date_text)
        except ValueError:
            warnings.warn("Incorrect datetime format")
            return None
        return this_datetime.isoformat()

    @classmethod
    def temporal_coverage_validate(cls, coverage: dict) -> dict:
        """Validate if a string is a valid date.

        Args:
            coverage: dict of temporal_coverage.

        Returns:
            dict of temporal_coverage or True
        """
        if not coverage:
            return {
                'start': None,
                'end': None
            }
        if "start" in coverage and coverage["start"]:
            coverage['start'] = cls.date_validate(coverage['start'])
        else:
            coverage['start'] = None
        if "end" in coverage and coverage["end"]:
            coverage['end'] = cls.date_validate(coverage['end'])
        else:
            coverage['end'] = None
        return coverage

    @classmethod
    def load_materializer(cls, materializer_module: str) -> MaterializerBase:
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
            raise ValueError(colored("No materializer class found in {}".format(
                os.path.join(os.path.dirname(__file__), 'materializers', materializer_module))), 'red')

        materializer = materializer_class(tmp_file_dir=cls.TMP_FILE_DIR)
        return materializer

    @classmethod
    @timeout(seconds=MATERIALIZATION_TIME_OUT, error_message="Materialization times out")
    def materialize(cls,
                    metadata: dict,
                    constrains: dict = None) -> typing.Optional[pd.DataFrame]:
        """Get the dataset with materializer.

       Args:
           metadata: metadata dict.
           variables:
           constrains:

       Returns:
            pandas dataframe
       """
        # Get cache instance
        try:
            cache = Cache.get_instance()
        except Exception as e:
            print("ERR: Unable to get cache instance")
            print(e)
            cache = None
        
        if cache:
            key = json.dumps(metadata["materialization"], sort_keys=True) + json.dumps(constrains, sort_keys=True)
            ttl = metadata.get("validity",cache.lifetime_duration)

            # Query cache
            cache_result, reason = cache.get(key, ttl)
        else:
            cache_result = None
            reason = EntryState.ERROR

        # Cache miss
        if cache_result is None:
            materializer = cls.load_materializer(materializer_module=metadata["materialization"]["python_path"])
            df = materializer.get(metadata=metadata, constrains=constrains)

            if isinstance(df, pd.DataFrame):
                if cache is not None:
                    cache.add(key, df) 
                return df
                
            return None
        
        if cache_result is not None:
            # Entry expired - too stale
            if reason == EntryState.EXPIRED:
                # Rematerialize
                materializer = cls.load_materializer(materializer_module=metadata["materialization"]["python_path"])
                df = materializer.get(metadata=metadata, constrains=constrains)
                if isinstance(df, pd.DataFrame):
                    cache.remove(key)
                    cache.add(key, df)
                    return df

                # Else return cached entry
                return cache_result
            
            # Cache hit
            elif reason == EntryState.FOUND:
                return cache_result
        
        return None
        
    @classmethod
    def validate_schema(cls, description: dict) -> bool:
        """Validate dict against json schema.

        Args:
            description: description dict.

        Returns:
            boolean
        """
        try:
            validate(description, cls.INDEX_SCHEMA)
            return True
        except:
            print(colored("[INVALID SCHEMA] title: {}".format(description.get("title"))), 'red')
            raise ValueError("Invalid dataset description json according to index json schema")

    @classmethod
    def validate_query(cls, query: dict) -> bool:
        """Validate dict against json schema.

        Args:
            query: query dict.

        Returns:
            boolean
        """
        try:
            validate(query, cls.QUERY_SCHEMA)
            return True
        except:
            print(colored("[INVALID QUERY] title: {}".format(query.get("title"))), 'red')
            raise ValueError("Invalid query json according to query json schema")

    @staticmethod
    def test_print(func) -> typing.Callable:
        """A decorator for test print.

        """

        def __decorator(self):
            print("[Test]{}/{}".format(self.__class__.__name__, func.__name__))
            func(self)
            print(colored('.Done', 'red'))

        return __decorator

    @classmethod
    def is_categorical_column(cls, col: pd.Series) -> bool:
        """check if column is categorical.

        """

        return col.nunique() / col.size < cls.CATEGORICAL_COLUMN_THRESHOLD

    @staticmethod
    def get_inner_hits_info(hitted_es_result: dict, nested_key: str = "variables") -> typing.Optional[
        typing.List[dict]]:
        """Get offset of nested object got matched,
        which query string is matched and which string in document got matched.

        Args:
            hitted_es_result: hitted result returned by es query
            nested_key: nested_key in the doc, default is variables for out metadata index

        Returns:
            list of dictionary
            offset: offset of nested object in variables
            matched_queries: which query string got matched
            highlight: which string in original doc got matched
        """

        matched_queries_lst = hitted_es_result.get("inner_hits", {}).get(nested_key, {}).get("hits", {}).get("hits",
                                                                                                             [])
        if not matched_queries_lst:
            return None
        return [{
            "offset": matched_queries_lst[idx]["_nested"]["offset"],
            "matched_queries": matched_queries_lst[idx]["matched_queries"],
            "highlight": matched_queries_lst[idx]["highlight"]
        } for idx in range(len(matched_queries_lst))]

    @staticmethod
    def get_named_entity_constrain_from_inner_hits(matches: typing.List[dict]) -> dict:
        """Generate named entity constrain from get_inner_hits_info method result

         Args:
             matches: result returned by get_inner_hits_info method

         Returns:
            dict
         """

        result = dict()
        for matched in matches:
            result[matched["offset"]] = matched["highlight"]["variables.named_entity"]
        return result

    @classmethod
    def is_column_able_to_query(cls, col: pd.Series) -> bool:
        """Determine if a column is able for quering
        Basically means it is a named entity column

         Args:
             col: pandas Series

         Returns:
              boolean
         """
        from datamart.profilers.basic_profiler import BasicProfiler

        return BasicProfiler.named_entity_column_recognize(col)

    @staticmethod
    def append_columns_for_implicit_variables(implicit_variables: typing.List[dict], df: pd.DataFrame) -> pd.DataFrame:
        """Append implicit_variables as new column with same value across all rows of the dataframe

         Args:
             implicit_variables: list of implicit_variables in metadata
             df: Dataframe that implicit_variables will be appended on

         Returns:
              Dataframe with appended implicit_variables columns
         """

        for idx, implicit_variable in enumerate(implicit_variables):
            if implicit_variable.get("value"):
                df[implicit_variable.get("name") or "implicit_variable_%d" % idx] = implicit_variable["value"]
        return df

    @staticmethod
    def append_columns_for_implicit_variables_and_add_meta(meta: dict, df: pd.DataFrame) -> None:
        """Append implicit_variables as new column with same value across all rows of the dataframe

         Args:
             implicit_variables: list of implicit_variables in metadata
             df: Dataframe that implicit_variables will be appended on

         Returns:
              Dataframe with appended implicit_variables columns
         """
        implicit_variables = meta.get("implicit_variables", [])
        for idx, implicit_variable in enumerate(implicit_variables):
            if implicit_variable.get("value"):
                header = implicit_variable.get("name") or "implicit_variable_%d" % idx
                df[header] = implicit_variable["value"]
                if meta.get("variables"):
                    meta["variables"].append({
                        "name": header,
                        "description": header,
                        "named_entity": [implicit_variable["value"]],
                        "semantic_type": implicit_variable.get("semantic_type") or []
                    })

    @staticmethod
    def get_metadata_intersection(*metadata_lst) -> list:
        """Get the intersect metadata list.

       Args:
           metadata_lst: all metadata list returned by multiple queries

       Returns:
            list of intersect metadata
       """

        metadata_dict = dict()
        metadata_sets = []
        for lst in metadata_lst:
            this_set = set()
            for x in lst:
                if x["_source"]["datamart_id"] not in metadata_dict:
                    metadata_dict[x["_source"]["datamart_id"]] = x
                elif "inner_hits" in x:
                    metadata_dict[x["_source"]["datamart_id"]] = x
                this_set.add(x["_source"]["datamart_id"])
            metadata_sets.append(this_set)
        return [metadata_dict[datamart_id] for datamart_id in metadata_sets[0].intersection(*metadata_sets[1:])]

    @classmethod
    def get_dataset(cls,
                    metadata: dict,
                    variables: list = None,
                    constrains: dict = None
                    ) -> typing.Optional[pd.DataFrame]:
        """Get the dataset with materializer.

       Args:
           metadata: metadata dict.
           variables: list of integers
           constrains:

       Returns:
            pandas dataframe
       """

        if not constrains:
            constrains = dict()

        if constrains.get("date_range", None):
            if constrains["date_range"].get("start", None) and not constrains["date_range"].get("end", None):
                constrains["date_range"]["end"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

            if not constrains["date_range"].get("start", None) and constrains["date_range"].get("end", None):
                constrains["date_range"]["start"] = cls.DEFAULT_START_DATE

        df = cls.materialize(metadata=metadata, constrains=constrains)

        if variables:
            df = df.iloc[:, variables]

        if metadata.get("implicit_variables", None):
            df = cls.append_columns_for_implicit_variables(metadata["implicit_variables"], df)

        return df.infer_objects()

    @staticmethod
    def calculate_dsbox_features(data: pd.DataFrame, metadata: typing.Union[dict, None],
                                 selected_columns: typing.Set[int] = None) -> dict:
        """Calculate dsbox features, add to metadata dictionary

         Args:
             data: dataset as a pandas dataframe
             metadata: metadata dict

         Returns:
              updated metadata dict
         """

        from datamart.profilers.dsbox_profiler import DSboxProfiler
        if not metadata:
            return metadata
        return DSboxProfiler().profile(inputs=data, metadata=metadata, selected_columns=selected_columns)

    @classmethod
    def generate_metadata_from_dataframe(cls, data: pd.DataFrame, original_meta: dict=None) -> dict:
        """Generate a default metadata just from the data, without the dataset schema

         Args:
             data: pandas Dataframe

         Returns:
              metadata dict
         """
        from datamart.profilers.basic_profiler import BasicProfiler, VariableMetadata, GlobalMetadata

        global_metadata = GlobalMetadata.construct_global(description=cls.DEFAULT_DESCRIPTION)
        for col_offset in range(data.shape[1]):
            variable_metadata = BasicProfiler.basic_profiling_column(
                description={},
                variable_metadata=VariableMetadata.construct_variable(description={}),
                column=data.iloc[:, col_offset]
            )
            global_metadata.add_variable_metadata(variable_metadata)
        global_metadata = BasicProfiler.basic_profiling_entire(global_metadata=global_metadata,
                                                               data=data)
        if original_meta:
            global_metadata.value.update(original_meta)
        return global_metadata.value

    @staticmethod
    def validate_url(url):
        try:
            regex = re.compile(
                r'^(?:http|ftp)s?://'  # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                r'localhost|'  # localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                r'(?::\d+)?'  # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            return re.match(regex, url)
        except:
            return False