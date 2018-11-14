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
from pandas import DataFrame
import tempfile

sys.path.append(os.path.join(os.path.dirname(__file__), 'materializers'))


class Utils:
    INDEX_SCHEMA = json.load(
        open(os.path.join(os.path.join(os.path.dirname(__file__), "resources"), 'index_schema.json'), 'r'))

    TMP_FILE_DIR = tempfile.gettempdir()

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
            warnings.warn("Incorrect datatime format")
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

    @staticmethod
    def load_materializer(materializer_module: str) -> MaterializerBase:
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

        materializer = materializer_class(tmp_file_dir=Utils.TMP_FILE_DIR)
        return materializer

    @classmethod
    def materialize(cls,
                    metadata: dict,
                    constrains: dict = None) -> typing.Optional[DataFrame]:
        """Get the dataset with materializer.

       Args:
           metadata: metadata dict.
           variables:
           constrains:

       Returns:
            pandas dataframe
       """
        materializer = cls.load_materializer(materializer_module=metadata["materialization"]["python_path"])
        df = materializer.get(metadata=metadata, constrains=constrains)
        if isinstance(df, DataFrame):
            return df
        return None

    @staticmethod
    def validate_schema(description: dict) -> bool:
        """Validate dict against json schema.

        Args:
            description: description dict.

        Returns:
            boolean
        """
        try:
            validate(description, Utils.INDEX_SCHEMA)
            return True
        except:
            print("[INVALID SCHEMA] title: {}".format(description.get("title")))
            raise ValueError("Invalid dataset description json according to index json schema")

    @staticmethod
    def test_print(func) -> typing.Callable:
        def __decorator(self):
            print("[Test]{}/{}".format(self.__class__.__name__, func.__name__))
            func(self)
            print(colored('.Done', 'red'))

        return __decorator

    @staticmethod
    def get_highlight_match_from_metadata(metadata: dict, fields: list) -> dict:
        """Get highlight match, highlight match is the string in some fields of doc which is matched by this query.

        Args:
            metadata: hitted result returned by es query
            fields: list of fields

        Returns:
            boolean
        """

        highlights = metadata.get("highlight", {})
        return {field: highlights[field] for field in fields if field in highlights}

    @staticmethod
    def get_offset_and_matched_queries_from_variable_metadata(metadata: dict) -> typing.Optional[typing.List[tuple]]:
        """Get offset of nested object got matched and which query string in matched.

        Args:
            metadata: hitted result returned by es query

        Returns:
            boolean
        """

        matched_queries_lst = metadata.get("inner_hits", {}).get("variables", {}).get("hits", {}).get("hits", [])
        if not matched_queries_lst:
            return None
        return [(matched_queries_lst[idx]["_nested"]["offset"], matched_queries_lst[idx]["matched_queries"])
                for idx in range(len(matched_queries_lst))]
