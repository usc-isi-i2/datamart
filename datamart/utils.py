import datetime
import warnings
import dateutil.parser
from datamart.materializers.materializer_base import MaterializerBase
import importlib
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'materializers'))


class Utils:
    @staticmethod
    def date_validate(date_text: str):
        """Validate if a string is a valid date.

        Args:
            date_text: date string.

        Returns:
            string of valid date or None
        """

        try:
            datetime.datetime.strptime(date_text, '%Y-%m-%d')
        except ValueError:
            warnings.warn("Incorrect data format, should be YYYY-MM-DD, set to None")
            return None
        return date_text

    @staticmethod
    def temporal_coverage_validate(coverage: dict):
        """Validate if a string is a valid date.

        Args:
            coverage: dict of temporal_coverage.

        Returns:
            dict of temporal_coverage or True
        """
        if coverage.get("need_profile", None) is True:
            return {
                "need_profile": True,
                "start": None,
                "end": None
            }
        elif "start" in coverage or "end" in coverage:
            coverage['need_profile'] = False
            try:
                coverage['start'] = dateutil.parser.parse(coverage['start']).isoformat()
            except:
                warnings.warn("Can not parse start date in temporal coverage")
                coverage['start'] = None
                coverage['need_profile'] = True
            try:
                coverage['end'] = dateutil.parser.parse(coverage['end']).isoformat()
            except:
                warnings.warn("Can not parse end date in temporal coverage")
                coverage['end'] = None
                coverage['need_profile'] = True
            return coverage
        else:
            return {
                "need_profile": False,
                "start": None,
                "end": None
            }

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

        materializer = materializer_class()
        return materializer
