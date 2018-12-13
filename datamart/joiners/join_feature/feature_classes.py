from datamart.joiners.join_feature.feature_base import *
from rltk.similarity import *


class StrictMatchFeature(FeatureBase):
    """
    Features that do not need similarity functions; think two values are same only when they are identical .

    """
    def similarity_functions(self):
        yield lambda left, right: 1 if left == right else 0


class SpaceJoinFeature(FeatureBase):
    """
    If the feature is combined by multiple columns, simply join the column values to string by spaces .

    """
    def value_merge_func(self, record: dict):
        return ' '.join(super().value_merge_func(record))


class CategoricalNumberFeature(StrictMatchFeature, SpaceJoinFeature):
    pass


class CategoricalStringFeature(StrictMatchFeature, SpaceJoinFeature):
    pass


class CategoricalTokenFeature(SpaceJoinFeature):
    """
    Categorical on tokens of feature values, rather than on feature values, then run set based similarity .

    """
    def similarity_functions(self):
        for similarity_function in [jaccard, cosine]:
            yield similarity_function


class NonCategoricalNumberFeature(FeatureBase):
    """
    Non categorical number feature, compare similarity by how different the two values are .

    """
    def __init__(self, dfw: DataFrameWrapper, indexes):
        super().__init__(dfw, indexes)
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

    def value_merge_func(self, record: dict):
        """
        sum the values
        TODO: how to merge?
        """
        return sum(super().value_merge_func(record))

    def similarity_functions(self):
        def similarity_function(left, right):
            # TODO: calc a convert from differences to similarity by the range/variance etc
            diff = abs(left-right)/self.max_minus_min
            return pow((1 - diff), self.sigma)
        yield similarity_function


class NonCategoricalStringFeature(SpaceJoinFeature):
    # TODO: if self.multi_column = True, only use set based similarity
    function_mapping = {
        StringType.WORD: [levenshtein_similarity, jaro_winkler_similarity, ngram_similarity],
        StringType.PHRASE: [hybrid_jaccard_similarity, ngram_similarity],
        StringType.SENTENCE: [hybrid_jaccard_similarity, tf_idf_cosine_similarity],
        StringType.PARAGRAPH: [tf_idf_similarity, hybrid_jaccard_similarity, tf_idf_cosine_similarity],
        StringType.OTHER: []
    }

    def __init__(self, dfw: DataFrameWrapper, indexes):
        super().__init__(dfw, indexes)
        # TODO: calc the properties below(or copy from profiler info in metadata)
        self._string_type = StringType.WORD
        # more profiled info needed ...

    def similarity_functions(self):
        for similarity_function in NonCategoricalStringFeature.function_mapping[self._string_type]:
            yield similarity_function


class DatetimeFeature(FeatureBase):
    """
    TODO
    """

    def __init__(self, dfw: DataFrameWrapper, indexes):
        super().__init__(dfw, indexes)
        self._resolution = DatetimeResolution.HOUR

    def value_merge_func(self, record):
        return '\t'.join([record[header] for header in self._headers])

    def similarity_functions(self):
        yield lambda left, right: 1 if left == right else 0