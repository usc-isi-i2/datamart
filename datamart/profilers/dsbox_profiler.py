import numpy as np
import pandas as pd

from datamart.profilers.helpers import feature_compute_hih as fc_hih, feature_compute_lfh as fc_lfh

computable_metafeatures = [
    'ratio_of_values_containing_numeric_char', 'ratio_of_numeric_values',
    'number_of_outlier_numeric_values', 'num_filename', 'number_of_tokens_containing_numeric_char',
    'number_of_numeric_values_equal_-1', 'most_common_numeric_tokens', 'most_common_tokens',
    'ratio_of_distinct_tokens', 'number_of_missing_values',
    'number_of_distinct_tokens_split_by_punctuation', 'number_of_distinct_tokens',
    'ratio_of_missing_values', 'semantic_types', 'number_of_numeric_values_equal_0',
    'number_of_positive_numeric_values', 'most_common_alphanumeric_tokens',
    'numeric_char_density', 'ratio_of_distinct_values', 'number_of_negative_numeric_values',
    'target_values', 'ratio_of_tokens_split_by_punctuation_containing_numeric_char',
    'ratio_of_values_with_leading_spaces', 'number_of_values_with_trailing_spaces',
    'ratio_of_values_with_trailing_spaces', 'number_of_numeric_values_equal_1',
    'natural_language_of_feature', 'most_common_punctuations', 'spearman_correlation_of_features',
    'number_of_values_with_leading_spaces', 'ratio_of_tokens_containing_numeric_char',
    'number_of_tokens_split_by_punctuation_containing_numeric_char', 'number_of_numeric_values',
    'ratio_of_distinct_tokens_split_by_punctuation', 'number_of_values_containing_numeric_char',
    'most_common_tokens_split_by_punctuation', 'number_of_distinct_values',
    'pearson_correlation_of_features']

default_metafeatures = [
    'ratio_of_values_containing_numeric_char', 'ratio_of_numeric_values',
    'number_of_outlier_numeric_values', 'num_filename', 'number_of_tokens_containing_numeric_char']


class DSboxProfiler(object):
    """
    data profiler moduel. Now only supports csv data.

    Parameters:
    ----------
    _punctuation_outlier_weight: a integer
        the coefficient used in outlier detection for punctuation. default is 3

    _numerical_outlier_weight

    _token_delimiter: a string
        delimiter that used to seperate tokens, default is blank space " ".

    _detect_language: boolean
        true: do detect language; false: not detect language

    _topk: a integer

    Attributes:
    ----------
    """

    def __init__(self, compute_features=None) -> None:

        # All other attributes must be private with leading underscore
        self._punctuation_outlier_weight = 3
        self._numerical_outlier_weight = 3
        self._token_delimiter = " "
        self._detect_language = False
        self._topk = 10
        # list of specified features to compute
        self._specified_features = compute_features if compute_features else default_metafeatures

    def produce(self, inputs: pd.DataFrame, metadata: dict) -> dict:
        """Save metadata json to file.

        Args:
            inputs: pandas dataframe
            metadata: dict

        Returns:
            dict
        """

        metadata = self._profile_data(inputs, metadata)

        return metadata

    def _profile_data(self, data: pd.DataFrame, metadata: dict) -> dict:

        """Save metadata json to file.

        Args:
            data: pandas dataframe
            metadata: dict

        Returns:
            dict with dsbox profiled fields
        """

        # STEP 1: data-level calculations
        if "pearson_correlation_of_features" in self._specified_features:
            corr_pearson = data.corr()
            corr_columns = list(corr_pearson.columns)

        if "spearman_correlation_of_features" in self._specified_features:
            corr_spearman = data.corr(method='spearman')
            corr_columns = list(corr_spearman.columns)

        # STEP 2: column-level calculations
        column_counter = -1
        for column_name in data:
            column_counter += 1
            col = data[column_name]
            # dict: map feature name to content
            each_res = dict()

            if "spearman_correlation_of_features" in self._specified_features and column_name in corr_columns:
                stats_sp = corr_spearman[column_name].describe()
                each_res["spearman_correlation_of_features"] = {
                    'min': stats_sp['min'],
                    'max': stats_sp['max'],
                    'mean': stats_sp['mean'],
                    'median': stats_sp['50%'],
                    'std': stats_sp['std']
                }

            if "spearman_correlation_of_features" in self._specified_features and column_name in corr_columns:
                stats_pr = corr_pearson[column_name].describe()
                each_res["pearson_correlation_of_features"] = {
                    'min': stats_pr['min'],
                    'max': stats_pr['max'],
                    'mean': stats_pr['mean'],
                    'median': stats_pr['50%'],
                    'std': stats_pr['std']
                }

            if col.dtype.kind in np.typecodes['AllInteger'] + 'uMmf':
                if "number_of_missing_values" in self._specified_features:
                    each_res["number_of_missing_values"] = pd.isnull(col).sum()
                if "ratio_of_missing_values" in self._specified_features:
                    each_res["ratio_of_missing_values"] = pd.isnull(col).sum() / col.size
                if "number_of_distinct_values" in self._specified_features:
                    each_res["number_of_distinct_values"] = col.nunique()
                if "ratio_of_distinct_values" in self._specified_features:
                    each_res["ratio_of_distinct_values"] = col.nunique() / float(col.size)

            if col.dtype.kind == 'b':
                if "most_common_raw_values" in self._specified_features:
                    fc_hih.compute_common_values(col.dropna().astype(str), each_res, self._topk)

            elif col.dtype.kind in np.typecodes['AllInteger'] + 'uf':
                fc_hih.compute_numerics(col, each_res,
                                        self._specified_features)  # TODO: do the checks inside the function
                if "most_common_raw_values" in self._specified_features:
                    fc_hih.compute_common_values(col.dropna().astype(str), each_res, self._topk)

            else:

                # Need to compute str missing values before fillna
                if "number_of_missing_values" in self._specified_features:
                    each_res["number_of_missing_values"] = pd.isnull(col).sum()
                if "ratio_of_missing_values" in self._specified_features:
                    each_res["ratio_of_missing_values"] = pd.isnull(col).sum() / col.size

                col = col.astype(object).fillna('').astype(str)

                # compute_missing_space Must be put as the first one because it may change the data content,
                # see function def for details
                fc_lfh.compute_missing_space(col, each_res, self._specified_features)
                # fc_lfh.compute_filename(col, each_res)
                fc_lfh.compute_length_distinct(col, each_res, delimiter=self._token_delimiter,
                                               feature_list=self._specified_features)
                if "natural_language_of_feature" in self._specified_features:
                    fc_lfh.compute_lang(col, each_res)
                if "most_common_punctuations" in self._specified_features:
                    fc_lfh.compute_punctuation(col, each_res, weight_outlier=self._punctuation_outlier_weight)

                fc_hih.compute_numerics(col, each_res, self._specified_features)
                if "most_common_numeric_tokens" in self._specified_features:
                    fc_hih.compute_common_numeric_tokens(col, each_res, self._topk)
                if "most_common_alphanumeric_tokens" in self._specified_features:
                    fc_hih.compute_common_alphanumeric_tokens(col, each_res, self._topk)
                if "most_common_raw_values" in self._specified_features:
                    fc_hih.compute_common_values(col, each_res, self._topk)
                fc_hih.compute_common_tokens(col, each_res, self._topk, self._specified_features)
                if "numeric_char_density" in self._specified_features:
                    fc_hih.compute_numeric_density(col, each_res)
                fc_hih.compute_contain_numeric_values(col, each_res, self._specified_features)
                fc_hih.compute_common_tokens_by_puncs(col, each_res, self._topk, self._specified_features)

            # update metadata for a specific column

            metadata["variables"][column_counter]["dsbox_profiled"] = each_res

        return metadata
