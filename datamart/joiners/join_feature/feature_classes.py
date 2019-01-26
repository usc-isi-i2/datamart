from datamart.joiners.join_feature.feature_base import *
from datamart.joiners.join_feature.wrapped_similarity_functions import *


class CategoricalNumberFeature(FeatureBase):
    pass


class CategoricalStringFeature(FeatureBase):
    def similarity_functions(self):
        return [lambda left, right: 1 if to_lower(left) == to_lower(right) else 0]


class CategoricalTokenFeature(FeatureBase):
    """
    Categorical on tokens of feature values, rather than on feature values, then run set based similarity .

    """

    def value_merge_func(self, record: Record):
        # TODO: if multi-col, compare together or separate?
        tokens = []
        for header in self._headers:
            tokens += getattr(record, header).split()
        return tokens

    def similarity_functions(self):
        return [jaccard_sim, cosine_sim]


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

    def value_merge_func(self, record: Record):
        """
        sum the values
        TODO: how to merge?
        """
        return sum([float(getattr(record, header)) for header in self._headers])

    def similarity_functions(self):
        def similarity_function(left, right):
            # TODO: calc a convert from differences to similarity by the range/variance etc
            diff = abs(left-right)/self.max_minus_min
            return pow((1 - diff), self.sigma)
        return [similarity_function]


class NonCategoricalStringFeature(FeatureBase):
    # TODO: if self.multi_column = True, only use set based similarity
    # TODO: TFIDF in rltk needs more pre-calc info
    function_mapping = {
        StringType.WORD: [levenshtein_sim, jaro_winkler_sim, ngram_sim],
        StringType.PHRASE: [hybrid_jaccard_sim, ngram_sim],
        StringType.SENTENCE: [hybrid_jaccard_sim],
        StringType.PARAGRAPH: [hybrid_jaccard_sim],
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

    units = ['year', 'month', 'day', 'minute', 'second', 'ms', 'us', 'ns']

    def __init__(self, df: pd.DataFrame, indexes, metadata, distribute_type, data_type):
        super().__init__(df, indexes, metadata, distribute_type, data_type)
        # TODO: now suppose the column has same resolution, maybe add a profiler for datetime is better
        # TODO: now not recognize the real resolution, use the explicit resolution
        # e.g. date column with "{date} 00:00" -> real resolution should be "day" but now we treat it as "minute"
        self._resolution = self._get_resolution()
        if self.multi_column:
            self._parsed_series = pd.to_datetime(df.iloc[:, indexes])
        else:
            self._parsed_series = pd.to_datetime(df.iloc[:, indexes[0]])
        self._min = self._parsed_series.min()
        self._max = self._parsed_series.max()
        self._range = self._max - self._min

    def value_merge_func(self, record: Record):
        return self._parsed_series.iloc[int(record.id)]

    def similarity_functions(self):
        def compare_time(x, y):
            delta = x - y
            return 1 - abs(delta/self._range)
        # return [lambda x, y: 1 if to_datetime(x) == to_datetime(y) else 0]
        return [compare_time]

    def _get_resolution(self):
        # datetime_ = pd.to_datetime(self._df.iloc[0, self._indexes])
        return DatetimeResolution.HOUR
