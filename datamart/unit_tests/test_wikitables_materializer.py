from datamart.materializers.wikitables_materializer import WikitablesMaterializer
from datamart.utilities.utils import Utils
from os.path import join, dirname
from pandas import read_csv
from unittest import TestCase

resources_path = join(dirname(__file__), './resources')

# This unit test depends on the first table of the Wikipedia page for Albedo.
# It will work as long as such table doesn't change. If that happens,
# wikitables_sample.csv should be updated accordingly.

class TestWikitablesMaterializer(TestCase):
    def setUp(self):
        self.wikitables_materializer = WikitablesMaterializer()

    @Utils.test_print
    def test_get(self):
        mock_metadata = {
            'materialization': {
                'arguments': {
                    'url': 'https://en.wikipedia.org/wiki/Albedo',
                    'xpath': '/html[1]/body[1]/div[3]/div[3]/div[4]/div[1]/table[1]'
                }
            }
        }

        result = self.wikitables_materializer.get(metadata=mock_metadata).to_dict(orient='records')
        sample_result = read_csv(join(resources_path, 'wikitables_sample.csv'), dtype=str)
        sample_result = sample_result.fillna('').to_dict(orient='records')

        self.assertEqual(result, sample_result)
