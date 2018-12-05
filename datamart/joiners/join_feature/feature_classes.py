from datamart.joiners.join_feature.feature_base import *
from rltk.similarity import *


class CategoricalNumberFeature(FeatureBase):
    pass


class CategoricalStringFeature(FeatureBase):
    pass


class CategoricalTokenFeature(FeatureBase):
    """
    Categorical on tokens of feature values, rather than on feature values, then run set based similarity .

    """
    def similarity_functions(self):
        return [jaccard, cosine]


class NonCategoricalNumberFeature(FeatureBase):
    """
    Non categorical number feature, compare similarity by how different the two values are .

    """
    def __init__(self, df: pd.DataFrame, indexes, metadata, distribute_type, data_type):
        super().__init__(df, indexes, metadata, distribute_type, data_type)
        # TODO: calc the properties below(or copy from profiler info in metadata)
        self._number_type = NumberType.FLOAT
        self._min_value = 0
        self._max_value = 10
        self._sigma = 0.5
        self._max_minus_min = self._max_value - self._min_value

    @property
    def min_value(self):
        return self._min_value

    @property
    def max_value(self):
        return self._max_value

    @property
    def max_minus_min(self):
        return self._max_minus_min

    @property
    def sigma(self):
        return self._sigma

    def value_merge_func(self, record_values: list):
        """
        sum the values
        TODO: how to merge?
        """
        return sum(record_values)

    def similarity_functions(self):
        def similarity_function(left, right):
            # TODO: calc a convert from differences to similarity by the range/variance etc
            diff = abs(left-right)/self.max_minus_min
            return pow((1 - diff), self.sigma)
        return [similarity_function]


class NonCategoricalStringFeature(FeatureBase):
    # TODO: if self.multi_column = True, only use set based similarity
    function_mapping = {
        StringType.WORD: [levenshtein_similarity, jaro_winkler_similarity, ngram_similarity],
        StringType.PHRASE: [hybrid_jaccard_similarity, ngram_similarity],
        StringType.SENTENCE: [hybrid_jaccard_similarity, tf_idf_cosine_similarity],
        StringType.PARAGRAPH: [tf_idf_similarity, hybrid_jaccard_similarity, tf_idf_cosine_similarity],
        StringType.OTHER: []
    }

    def __init__(self, df: pd.DataFrame, indexes, metadata, distribute_type, data_type):
        super().__init__(df, indexes, metadata, distribute_type, data_type)
        # TODO: calc the properties below(or copy from profiler info in metadata)
        self._string_type = StringType.WORD
        # more profiled info needed ...

    def similarity_functions(self):
        return NonCategoricalStringFeature.function_mapping[self._string_type]


class DatetimeFeature(FeatureBase):
    """
    TODO
    """

    def __init__(self, df: pd.DataFrame, indexes, metadata, distribute_type, data_type):
        super().__init__(df, indexes, metadata, distribute_type, data_type)
        self._resolution = DatetimeResolution.HOUR
