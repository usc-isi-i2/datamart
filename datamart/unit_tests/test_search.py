from datamart.utilities.utils import Utils
from datamart import search
import unittest
from io import StringIO
import pandas as pd


class TestJSONQueryManager(unittest.TestCase):
    def setUp(self):
        self.df = pd.read_csv(StringIO("""date,team1,team2
2018-08-11,Arsenal,CD Tondela
2018-08-12,West Ham United,GD Chaves
2018-08-13,Cardiff City,CD Nacional
2018-09-11,Liverpool,CF Os Belenenses
2018-10-11,Manchester City,Portimonense SC
2018-11-11,Newcastle United,Sport Lisboa e Benfica
2018-12-11,Watford,Sporting Clube de Portugal
2019-01-11,Huddersfield Town,CD Feirense
2019-02-11,Everton,CD Feirense
2019-03-11,AFC Bournemouth,CD Feirense
2019-04-11,Crystal Palace,CD Santa Clara
2019-05-05,Wolverhampton Wanderers,Rio Ave FC
"""))

    @staticmethod
    def get_result_ids(query, df=None):
        res = search('isi-datamart', query, df)
        return set(r.id for r in res)

    @Utils.test_print
    def test_no_data(self):
        query = {
            "dataset": {
                "url": ["https://www.football-data.org"]
            }
        }
        res = self.get_result_ids(query)
        self.assertEqual(res, {'127900000', '127890000', '127860000', '127910000', '127840000', '127810000',
                               '127920000', '127850000', '127880000', '127830000', '127870000', '127820000'})

    @Utils.test_print
    def test_required_variables(self):
        # variables in "required_variables" should perform "AND"
        # 1. user defined temporal range
        query = {
            "dataset": {
                "url": ["https://www.football-data.org"]
            },
            "required_variables": [
                {
                    "type": "temporal_entity",
                    "start": "2018-08-11",
                    "end": "2019-05-11"
                }
            ]
        }
        res1 = self.get_result_ids(query)
        self.assertEqual(res1, {'127890000', '127850000', '127820000', '127810000'})

        # 2. user defined temporal range AND a column in supplied data
        query['required_variables'].append({
            "type": "dataframe_columns",
            "index": [1]
        })
        res2 = self.get_result_ids(query, self.df)
        self.assertEqual(res2, {'127890000'})

        # 3. if no required_variables, combine results of using each named_entity column as required:
        query = {
            "dataset": {
                "url": ["https://www.football-data.org"]
            }
        }
        res3 = self.get_result_ids(query, self.df)
        self.assertEqual(res3, {'127890000', '127810000'})

    @Utils.test_print
    def test_desired_variables(self):
        # variables in "desired_variables" should perform "OR"
        # keep the key to avoid auto generated "required_variables"
        query = {
            "dataset": {
                "url": ["https://www.football-data.org"]
            },
            "required_variables": [],
            "desired_variables": [
                {
                    "type": "temporal_entity",
                    "start": "2018-08-11",
                    "end": "2019-05-11"
                },
                {
                    "type": "dataframe_columns",
                    "index": [1]
                }
            ]
        }
        res = self.get_result_ids(query, self.df)
        self.assertEqual(res, {'127850000', '127890000', '127820000', '127810000'})




