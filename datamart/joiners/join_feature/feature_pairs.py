from datamart.joiners.join_feature.feature_factory import *
import typing
import rltk
from rltk.io.reader.dataframe_reader import DataFrameReader


class LeftDynamicRecord(rltk.Record):
    def __init__(self, raw_object: dict):
        raw_ = {}
        for k, v in raw_object.items():
            raw_[str(k)] = v
        super().__init__(raw_)
        # TODO: deal with "id" in original dataset, now it will not be in property
        vars(self).update(raw_)

    @property
    def id(self):
        return str(self.raw_object['dataframe_default_index'])


class RightDynamicRecord(LeftDynamicRecord):
    pass


class FeaturePairs:
    def __init__(self,
                 left_df: pd.DataFrame,
                 right_df: pd.DataFrame,
                 left_columns: typing.List[typing.List[int]],
                 right_columns: typing.List[typing.List[int]],
                 left_metadata: dict,
                 right_metadata: dict,
    ):
        l1 = len(left_columns)
        l2 = len(right_columns)
        if not (l1 == l2 and l1 and l2):
            # TODO: throw error or warning
            return

        self._length = l1

        self._left_df = left_df
        self._right_df = right_df
        self._left_columns = left_columns
        self._right_columns = right_columns
        self._left_metadata = left_metadata
        self._right_metadata = right_metadata

        self._left_rltk_dataset = self._init_rltk_dataset(left_df, LeftDynamicRecord)
        self._right_rltk_dataset = self._init_rltk_dataset(right_df, RightDynamicRecord)

        self._pairs = self._init_pairs()

    @property
    def left_rltk_dataset(self):
        return self._left_rltk_dataset

    @property
    def right_rltk_dataset(self):
        return self._right_rltk_dataset

    @property
    def pairs(self):
        return self._pairs

    def get_rltk_block(self) -> typing.Optional[rltk.BlockGenerator]:
        prime_key_l = None
        prime_key_r = None
        for f1, f2 in self.pairs:
            if f1.distribute_type == DistributeType.CATEGORICAL and f1.data_type == DataType.STRING:
                prime_key_l = f1.name
                prime_key_r = f2.name
                break
        if prime_key_l and prime_key_r:
            try:
                bg = rltk.HashBlockGenerator()
                block = bg.generate(
                    bg.block(self.left_rltk_dataset, function_=lambda r: getattr(r, prime_key_l)[0].lower()),
                    bg.block(self.right_rltk_dataset, function_=lambda r: getattr(r, prime_key_r)[0].lower()))
                return block
            except Exception as e:
                print(' - BLOCKING EXCEPTION: %s' % str(e))

    def __len__(self):
        return self._length

    def _init_pairs(self):
        return [(FeatureFactory.create(self._left_df, self._left_columns[i], self._left_metadata),
                 FeatureFactory.create(self._right_df, self._right_columns[i], self._right_metadata))
                for i in range(self._length)]

    @staticmethod
    def _init_rltk_dataset(df, record_class):
        rltk_dataset = rltk.Dataset(reader=DataFrameReader(df, True), record_class=record_class)
        return rltk_dataset

