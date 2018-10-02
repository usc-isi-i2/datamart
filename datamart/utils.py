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
