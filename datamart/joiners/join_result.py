import typing
import pandas as pd


class JoinResult(object):
    def __init__(self, df: pd.DataFrame or None, matched_rows: typing.List[int]=None):
        self._df = df
        self._matched_rows = matched_rows

    @property
    def df(self):
        return self._df

    @property
    def matched_rows(self):
        return self._matched_rows

    @property
    def cover_ratio(self):
        if self.matched_rows:
            cnt = len([_ for _ in self.matched_rows if _])
            if len(self.matched_rows):
                return cnt/len(self.matched_rows)