from abc import ABC, abstractmethod
import pandas as pd
from enum import Enum
import typing


class JoinerBase(ABC):
    """Abstract class of Joiner, should be extended for other joiners.

    """

    @abstractmethod
    def join(self, **kwargs) -> pd.DataFrame:
        """Implement join method which returns a pandas Dataframe

        """
        pass


class JoinerType(Enum):
    DEFAULT = "default"
    RLTK = "rltk"


class DefaultJoiner(JoinerBase):
    """Default join class.

    """

    @staticmethod
    def join(left_df, right_df, left_columns, right_columns) -> pd.DataFrame:

        if len(left_columns) != len(right_columns):
            raise ValueError("Default join need length of left_columns equals to right_columns")

        right_df = right_df.rename(columns={
            right_df.columns[right_columns[idx]]: left_df.columns[left_columns[idx]] for idx in range(len(left_columns))
        })

        return pd.merge(left=left_df,
                        right=right_df,
                        left_on=[left_df.columns[idx] for idx in left_columns],
                        right_on=[right_df.columns[idx] for idx in right_columns],
                        how='left')


class JoinerPrepare(object):

    @staticmethod
    def prepare_joiner(joiner: str = "default") -> typing.Optional[JoinerBase]:

        """Prepare joiner, lazy evaluation for joiners,
        should be useful because joiner like RLTK may need many dependency packages.

        Args:
            joiner: string of joiner type

        Returns:
            joiner instance

        """

        try:
            JoinerType(joiner)
        except ValueError:
            return None

        if JoinerType(joiner) == JoinerType.RLTK:
            from datamart.joiners.rltk_joiner import RLTKJoiner
            return RLTKJoiner()

        if JoinerType(joiner) == JoinerType.DEFAULT:
            return DefaultJoiner()

        return None
