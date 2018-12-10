
from datamart.joiners.join_feature.feature_classes import *
from functools import reduce
import numpy as np


class FeatureFactory:

    @classmethod
    def create(cls, df: pd.DataFrame, indexes, df_metadata):
        """
        TODO: dynamically generate subclass of FeatureBase, by profiled info, datatype etc.

        """
        multi_column = True if len(indexes) > 1 else False
        if multi_column:
            # TODO: how to deal with multi-columns ?
            idx = indexes[0]
            return CategoricalTokenFeature(df, indexes, df_metadata['variables'][idx], DistributeType.TOKEN_CATEGORICAL, DataType.STRING)
        else:
            idx = indexes[0]

        metadata = df_metadata['variables'][idx]
        data_type = DataType.STRING
        distribute_type = DistributeType.NON_CATEGORICAL

        profiles = metadata.get('dsbox_profiled', {})

        if len(df.iloc[:, idx]) // len(df.iloc[:, idx].unique()) >= 1.5:
            distribute_type = DistributeType.CATEGORICAL
        elif profiles:
            most_common_tokens = profiles.get('most_common_tokens')
            if most_common_tokens and cls._get_greater_than(most_common_tokens) >= len(most_common_tokens)//2:
                distribute_type = DistributeType.TOKEN_CATEGORICAL

        # try to get the data type from the 'semantic_type' in
        semantic_types = metadata.get('semantic_type')
        _type = cls._get_data_type_by_semantic_type(semantic_types)
        if _type:
            data_type = _type
        else:
            dtype = df.iloc[:, idx].dtype
            if dtype == np.int64 or dtype == np.float64:
                data_type = DataType.NUMBER
            else:
                try:
                    if isinstance(pd.to_datetime(df.iloc[0, idx]), pd.Timestamp):
                        data_type = DataType.DATETIME
                    else:
                        data_type = cls._get_data_type_by_profile(profiles) or data_type
                except Exception as e:
                    # print(e)
                    data_type = cls._get_data_type_by_profile(profiles) or data_type

        constructor = cls.get_constructor(distribute_type, data_type)
        return constructor(df, indexes, metadata, distribute_type, data_type)

    @classmethod
    def get_constructor(cls, distribute_type, data_type):
        if data_type == DataType.DATETIME:
            return DatetimeFeature
        subclasses = {
            (DistributeType.CATEGORICAL, DataType.NUMBER): CategoricalNumberFeature,
            (DistributeType.CATEGORICAL, DataType.STRING): CategoricalStringFeature,
            (DistributeType.TOKEN_CATEGORICAL, DataType.STRING): CategoricalTokenFeature,
            (DistributeType.NON_CATEGORICAL, DataType.NUMBER): NonCategoricalNumberFeature,
            (DistributeType.NON_CATEGORICAL, DataType.STRING): NonCategoricalStringFeature,
        }
        return subclasses.get((distribute_type, data_type))

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
            return DataType.STRING

    @staticmethod
    def _get_data_type_by_profile(profiles):
        numeric_ratio = profiles.get('ratio_of_numeric_values')
        if numeric_ratio and numeric_ratio >= 0.99:
            return DataType.NUMBER


