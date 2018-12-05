import unittest
from datamart.joiners.joiner_base import DefaultJoiner, JoinerPrepare
import pandas as pd
from pandas.testing import assert_frame_equal
from datamart.utilities.utils import Utils
from datamart.joiners.join_feature.feature_factory import *
from io import StringIO
from datamart.profilers.dsbox_profiler import DSboxProfiler


class TestJoinFeature(unittest.TestCase):
    def setUp(self):
        raw1 = '\n'.join(['date,city,temp,description,air condition',
                          '12-01-2018,New York,55,rain,3',
                          '12-02-2018,Los Angeles,75,sunny,2',
                          '12-05-2018,Chicago,41,wind,3',
                          '12-06-2018,Chicago,42,cloudy,2',
                          '12-07-2018,Chicago,43,snow,3',
                          '12-08-2018,Los Angeles,72,moggy,2'])
        self.df1 = pd.read_csv(StringIO(raw1))
        raw2 = '\n'.join(['month,day,year,city,wind',
                          '12,01,2018,new york,37.5',
                          '12,02,2018,los angeles,22.1',
                          '12,05,2018,chicago,58.8'])
        self.df2 = pd.read_csv(StringIO(raw2))

    @Utils.test_print
    def test_feature_base(self):
        fb1 = FeatureBase(df=self.df1,
                          indexes=[0],
                          metadata={},
                          distribute_type=DistributeType.NON_CATEGORICAL,
                          data_type=DataType.DATETIME)
        result1 = [fb1.data_type, fb1.distribute_type, fb1.multi_column, fb1.metadata, fb1.name]
        expected1 = [DataType.DATETIME, DistributeType.NON_CATEGORICAL, False, {}, "date"]

        fb2 = FeatureBase(df=self.df2,
                          indexes=[0, 1, 2],
                          metadata={},
                          distribute_type=DistributeType.NON_CATEGORICAL,
                          data_type=DataType.DATETIME)
        result2 = [fb2.data_type, fb2.distribute_type, fb2.multi_column, fb2.metadata, fb2.name]
        expected2 = [DataType.DATETIME, DistributeType.NON_CATEGORICAL, True, {}, "month|day|year"]

        self.assertEqual(result1, expected1)
        self.assertEqual(result2, expected2)

    @Utils.test_print
    def test_feature_factory(self):
        dsbox_profiler = DSboxProfiler()
        meta1 = dsbox_profiler.profile(inputs=self.df1, metadata={
            'variables': [
                {'semantic_type': ['http://schema.org/Date']},
                {}, {}, {}, {}]})
        # meta2 = dsbox_profiler.profile(inputs=self.df2, metadata={
        #     'variables': [
        #         {'semantic_type': ['http://schema.org/Month']},
        #         {'semantic_type': ['http://schema.org/Day']},
        #         {'semantic_type': ['http://schema.org/Year']},
        #         {}, {}]})
        print(meta1)
        indexes_list = [[0], [1], [2], [3], [4]]
        feature_classes = [DatetimeFeature, CategoricalStringFeature, NonCategoricalNumberFeature,
                           NonCategoricalStringFeature, CategoricalNumberFeature]
        for i in range(len(indexes_list)):
            feature = FeatureFactory.create(self.df1, indexes_list[i], meta1)
            self.assertIsInstance(feature, feature_classes[i])


