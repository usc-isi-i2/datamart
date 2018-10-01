import datetime
import warnings
import dateutil.parser


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

        if "start" not in coverage or "end" not in coverage:
            warnings.warn("Start time or end time not found in temporal coverage")
            return True
        try:
            coverage['start'] = dateutil.parser.parse(coverage['start']).isoformat()
            coverage['end'] = dateutil.parser.parse(coverage['end']).isoformat()
        except:
            warnings.warn("Can not parse start or end date in temporal coverage, set to True for datamart profiler")
            return True
        return coverage
