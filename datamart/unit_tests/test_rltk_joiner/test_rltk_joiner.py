import unittest
from datamart.utilities.utils import Utils
from datamart.joiners.join_feature.feature_pairs import *
from io import StringIO
from datamart.profilers.dsbox_profiler import DSboxProfiler
from datamart.joiners.rltk_joiner import RLTKJoiner


class TestRLTKJoiner(unittest.TestCase):
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

        dsbox_profiler = DSboxProfiler()
        self.meta1 = dsbox_profiler.profile(inputs=self.df1, metadata={
            'variables': [
                {'semantic_type': ['http://schema.org/Date']},
                {}, {}, {}, {}]})
        self.meta2 = dsbox_profiler.profile(inputs=self.df2, metadata={
            'variables': [
                {'semantic_type': ['http://schema.org/Month']},
                {'semantic_type': ['http://schema.org/Day']},
                {'semantic_type': ['http://schema.org/Year']},
                {}, {}]})

        self.args = {
            'left_df': self.df1,
            'right_df': self.df2,
            'left_metadata': self.meta1,
            'right_metadata': self.meta2
        }

        self.rltk_joiner = RLTKJoiner()

    @Utils.test_print
    def test_join_date(self):
        res = self.rltk_joiner.join(
            **self.args,
            left_columns=[[0]],
            right_columns=[[0, 1, 2]]
        )
        # TODO: if date has the same resolution, USE EXACT MATCH
        expected = '''date,city,temp,description,air condition,city,wind
12-01-2018,New York,55,rain,3,new york,37.5
12-02-2018,Los Angeles,75,sunny,2,los angeles,22.1
12-05-2018,Chicago,41,wind,3,chicago,58.8
12-06-2018,Chicago,42,cloudy,2,chicago,58.8
12-07-2018,Chicago,43,snow,3,chicago,58.8
12-08-2018,Los Angeles,72,moggy,2,chicago,58.8
'''

        self.assertEqual(res.df.to_csv(index=False), expected)

    @Utils.test_print
    def test_join_cat_str(self):
        res = self.rltk_joiner.join(
            **self.args,
            left_columns=[[1]],
            right_columns=[[3]]
        )
        expected = '''date,city,temp,description,air condition,month,day,year,wind
12-01-2018,New York,55,rain,3,12,1,2018,37.5
12-02-2018,Los Angeles,75,sunny,2,12,2,2018,22.1
12-05-2018,Chicago,41,wind,3,12,5,2018,58.8
12-06-2018,Chicago,42,cloudy,2,12,5,2018,58.8
12-07-2018,Chicago,43,snow,3,12,5,2018,58.8
12-08-2018,Los Angeles,72,moggy,2,12,2,2018,22.1
'''
        self.assertEqual(res.df.to_csv(index=False), expected)

    @Utils.test_print
    def test_join_date_and_str(self):
        res = self.rltk_joiner.join(
            **self.args,
            left_columns=[[0], [1]],
            right_columns=[[0, 1, 2], [3]]
        )
        expected = '''date,city,temp,description,air condition,wind
12-01-2018,New York,55,rain,3,37.5
12-02-2018,Los Angeles,75,sunny,2,22.1
12-05-2018,Chicago,41,wind,3,58.8
12-06-2018,Chicago,42,cloudy,2,58.8
12-07-2018,Chicago,43,snow,3,58.8
12-08-2018,Los Angeles,72,moggy,2,22.1
'''
        self.assertEqual(res.df.to_csv(index=False), expected)
