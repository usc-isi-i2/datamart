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
from datamart.utilities.timeout import timeout

sys.path.append(os.path.join(os.path.dirname(__file__), '../materializers'))


class Utils:
    INDEX_SCHEMA = json.load(
        open(os.path.join(os.path.join(os.path.dirname(__file__), "../resources"), 'index_schema.json'), 'r'))

    TMP_FILE_DIR = tempfile.gettempdir()

    DEFAULT_DESCRIPTION = {
        "materialization": {
            "python_path": "default_materializer"
        },
        "variables": []
    }

    MATERIALIZATION_TIME_OUT = 300

    CATEGORICAL_COLUMN_THRESHOLD = 0.2

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
        materializer = cls.load_materializer(materializer_module=metadata["materialization"]["python_path"])
        df = materializer.get(metadata=metadata, constrains=constrains)
        if isinstance(df, pd.DataFrame):
            return df
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

    @staticmethod
    def test_print(func) -> typing.Callable:
        """A decorator for test print.

        """

        def __decorator(self):
            print("[Test]{}/{}".format(self.__class__.__name__, func.__name__))
            func(self)
            print(colored('.Done', 'red'))

        return __decorator

    @staticmethod
    def is_categorical_column(col: pd.Series) -> bool:
        """check if column is categorical.

        """

        return col.nunique() / col.size < Utils.CATEGORICAL_COLUMN_THRESHOLD
