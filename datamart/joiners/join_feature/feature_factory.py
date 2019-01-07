
from datamart.joiners.join_feature.feature_classes import *
from functools import reduce
import numpy as np


class FeatureFactory:
    subclasses = {
        (DistributeType.CATEGORICAL, DataType.NUMBER): CategoricalNumberFeature,
        (DistributeType.CATEGORICAL, DataType.STRING): CategoricalStringFeature,
        (DistributeType.TOKEN_CATEGORICAL, DataType.STRING): CategoricalTokenFeature,
        (DistributeType.NON_CATEGORICAL, DataType.NUMBER): NonCategoricalNumberFeature,
        (DistributeType.NON_CATEGORICAL, DataType.STRING): NonCategoricalStringFeature
    }

    @classmethod
    def create(cls, df: pd.DataFrame, indexes, df_metadata):
        """
        TODO: dynamically generate subclass of FeatureBase, by profiled info, datatype etc.

        """
        # set default values:
        metadata = cls._get_feature_metadata(df_metadata, indexes)
        data_type = None
        distribute_type = DistributeType.NON_CATEGORICAL

        if len(indexes) > 1:
            distribute_type = DistributeType.TOKEN_CATEGORICAL
            if cls._try_pd_to_datetime(df, indexes):
                data_type = DataType.DATETIME
        else:
            # single column, not datetime
            idx = indexes[0]
            profiles = metadata.get('dsbox_profiled', {})

            if len(df.iloc[:, idx]) // len(df.iloc[:, idx].unique()) >= 1.5:
                distribute_type = DistributeType.CATEGORICAL
            elif profiles:
                most_common_tokens = profiles.get('most_common_tokens')
                if most_common_tokens and cls._get_greater_than(most_common_tokens) >= len(most_common_tokens)//2:
                    distribute_type = DistributeType.TOKEN_CATEGORICAL

            dtype = df.iloc[:, idx].dtype
            if dtype == np.int64 or dtype == np.float64:
                data_type = DataType.NUMBER
            else:
                semantic_types = metadata.get('semantic_type')
                profiles = metadata.get('dsbox_profiled', {})
                data_type = cls._get_data_type_by_semantic_type(semantic_types) \
                            or cls._get_data_type_by_profile(profiles)
                if not data_type and cls._try_pd_to_datetime(df, indexes):
                    data_type = DataType.DATETIME

        return cls.get_instance(df, indexes, metadata, data_type or DataType.STRING, distribute_type)

    @classmethod
    def get_instance(cls, df, indices, metadata, data_type, distribute_type):
        constructor = cls.get_constructor(data_type, distribute_type)
        return constructor(df, indices, metadata, distribute_type, data_type)

    @classmethod
    def get_constructor(cls, data_type, distribute_type=None):
        if data_type == DataType.DATETIME:
            return DatetimeFeature
        return cls.subclasses.get((distribute_type, data_type))

    @staticmethod
    def _get_feature_metadata(metadata, indices):
        return metadata['variables'][indices[0]]

    @staticmethod
    def _get_avg(list_of_dict, key='count'):
        if len(list_of_dict):
            return sum([_.get(key) for _ in list_of_dict])/len(list_of_dict)

    @staticmethod
    def _get_greater_than(list_of_dict, key='count', threshold=2, inclusive=True):
        if inclusive:
            return reduce(lambda x, y: x + 1 if float(y[key]) >= threshold else x, list_of_dict, 0)
        return reduce(lambda x, y: x + 1 if float(y[key]) > threshold else x, list_of_dict, 0)

    @staticmethod
    def _get_data_type_by_semantic_type(semantic_types: list):
        # TODO: it would be better if we have a close set of used semantic_type, \
        # and map them to either STRING, NUMBER or DATETIME
        if semantic_types and len(semantic_types):
            unique_types = set(t.rsplit('/', 1)[-1].lower() for t in semantic_types)
            if 'time' in unique_types or 'date' in unique_types or 'datetime' in unique_types:
                return DataType.DATETIME
            if 'float' in unique_types or 'int' in unique_types or 'number' in unique_types:
                return DataType.NUMBER

    @staticmethod
    def _get_data_type_by_profile(profiles):
        numeric_ratio = profiles.get('ratio_of_numeric_values')
        if numeric_ratio and numeric_ratio >= 0.99:
            return DataType.NUMBER

    @staticmethod
    def _try_pd_to_datetime(df, indices):
        try:
            if len(indices) == 1:
                _ = pd.to_datetime(df.iloc[[0, len(df) - 1], indices[0]])
            else:
                _ = pd.to_datetime(df.iloc[[0, len(df)-1], indices])
            return True
        except ValueError:
            return False


