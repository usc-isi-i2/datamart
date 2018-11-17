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
import signal
import errno
from functools import wraps

sys.path.append(os.path.join(os.path.dirname(__file__), 'materializers'))
MATERIALIZATION_TIME_OUT = 60


# def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
#     def decorator(func):
#         def _handle_timeout(signum, frame):
#             raise Exception(colored(error_message, 'red'))
#
#         def wrapper(*args, **kwargs):
#             signal.signal(signal.SIGALRM, _handle_timeout)
#             signal.alarm(seconds)
#             try:
#                 result = func(*args, **kwargs)
#             finally:
#                 signal.alarm(0)
#             return result
#
#         return wraps(func)(wrapper)
#
#     return decorator

import sys, threading
class KThread(threading.Thread):
    """A subclass of threading.Thread, with a kill()
    method.
    Come from:
    Kill a thread in Python:
    http://mail.python.org/pipermail/python-list/2004-May/260937.html
    """
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.killed = False
    def start(self):
        """Start the thread."""
        self.__run_backup = self.run
        self.run = self.__run  # Force the Thread to install our trace.
        threading.Thread.start(self)
    def __run(self):
        """Hacked run function, which installs the
        trace."""
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup
    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        else:
            return None
    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
        return self.localtrace
    def kill(self):
        self.killed = True
class Timeout(Exception):
    """function run timeout"""
def timeout(seconds, error_message):
    """超时装饰器，指定超时时间
    若被装饰的方法在指定的时间内未返回，则抛出Timeout异常"""
    def timeout_decorator(func):
        """真正的装饰器"""
        def _new_func(oldfunc, result, oldfunc_args, oldfunc_kwargs):
            result.append(oldfunc(*oldfunc_args, **oldfunc_kwargs))
        def _(*args, **kwargs):
            result = []
            new_kwargs = {  # create new args for _new_func, because we want to get the func return val to result list
                'oldfunc': func,
                'result': result,
                'oldfunc_args': args,
                'oldfunc_kwargs': kwargs
            }
            thd = KThread(target=_new_func, args=(), kwargs=new_kwargs)
            thd.start()
            thd.join(seconds)
            alive = thd.isAlive()
            thd.kill()  # kill the child thread
            if alive:
                # raise Timeout(u'function run too long, timeout %d seconds.' % seconds)
                try:
                    raise Timeout(error_message)
                finally:
                    return u'function run too long, timeout %d seconds.' % seconds
            else:
                return result[0]
        _.__name__ = func.__name__
        _.__doc__ = func.__doc__
        return _
    return timeout_decorator



class Utils:
    INDEX_SCHEMA = json.load(
        open(os.path.join(os.path.join(os.path.dirname(__file__), "resources"), 'index_schema.json'), 'r'))

    TMP_FILE_DIR = tempfile.gettempdir()

    DEFAULT_DESCRIPTION = {
        "materialization": {
            "python_path": "default_materializer"
        },
        "variables": []
    }

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

    @classmethod
    def generate_metadata_from_dataframe(cls, data: DataFrame) -> dict:
        """Generate a default metadata just from the data, without the dataset schema

         Args:
             data: pandas Dataframe

         Returns:
              metadata dict
         """

        from datamart.profiler import Profiler
        from datamart.metadata.global_metadata import GlobalMetadata
        from datamart.metadata.variable_metadata import VariableMetadata

        profiler = Profiler()

        global_metadata = GlobalMetadata.construct_global(description=cls.DEFAULT_DESCRIPTION)
        for col_offset in range(data.shape[1]):
            variable_metadata = profiler.basic_profiler.basic_profiling_column(
                description={},
                variable_metadata=VariableMetadata.construct_variable(description={}),
                column=data.iloc[:, col_offset]
            )
            global_metadata.add_variable_metadata(variable_metadata)
        global_metadata = profiler.basic_profiler.basic_profiling_entire(global_metadata=global_metadata, data=data)

        return global_metadata.value

    @staticmethod
    def materialize_time_out_handler(signum, frame):
        """Materialization times out handler

         Args:
             signum
             frame

         Returns:

         """

        raise Exception(colored("Materialization times out", 'red'))
