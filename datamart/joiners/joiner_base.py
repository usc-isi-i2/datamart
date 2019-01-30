from abc import ABC, abstractmethod
import pandas as pd
from enum import Enum
import typing
from datamart.joiners.join_result import JoinResult


class JoinerBase(ABC):
    """Abstract class of Joiner, should be extended for other joiners.

    """

    @abstractmethod
    def join(self, **kwargs) -> JoinResult:
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
    def join(left_df: pd.DataFrame,
             right_df: pd.DataFrame,
             left_columns: typing.List[typing.List[int]],
             right_columns: typing.List[typing.List[int]],
             **kwargs
             ) -> JoinResult:

        left_columns = [x[0] for x in left_columns]
        right_columns = [x[0] for x in right_columns]

        if len(left_columns) != len(right_columns):
            raise ValueError("Default join only perform on 1-1 mapping")

        right_df = right_df.rename(columns={
            right_df.columns[right_columns[idx]]: left_df.columns[left_columns[idx]] for idx in range(len(left_columns))
        })

        df = pd.merge(left=left_df,
                        right=right_df,
                        left_on=[left_df.columns[idx] for idx in left_columns],
                        right_on=[right_df.columns[idx] for idx in right_columns],
                        how='left')

        return JoinResult(df=df)


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
