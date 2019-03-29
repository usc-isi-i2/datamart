import unittest
import json
import os
import pandas as pd
from pandas.testing import assert_frame_equal

from datamart.utilities.caching import Cache, EntryState, CacheConfig
from datamart.utilities.utils import Utils

class TestCaching(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_config = CacheConfig("/tmp/datamart_test_config.json")
        cls.test_config.max_cache_size = 3
        cls.test_config.dataset_dir = "/tmp/datamart_test_cache/"
        cls.test_config.save()

        cls.cache = Cache(test=True, config=cls.test_config)
        cls.cache.add("first", pd.DataFrame(['first']))
        cls.cache.add("second", pd.DataFrame(['second']))
        cls.cache.add("third", pd.DataFrame(['third']))
    
    def setUp(self):
        TestCaching.cache.add("first", pd.DataFrame(['first']))
        TestCaching.cache.add("second", pd.DataFrame(['second']))
        TestCaching.cache.add("third", pd.DataFrame(['third']))

    @Utils.test_print
    def test_get(self):
        pd.DataFrame(['second']).to_csv("tmp.csv", index=None)
        expected = pd.read_csv("tmp.csv")
        got, _ = TestCaching.cache.get("second", None)
        assert_frame_equal(expected, got)
        os.remove("tmp.csv")
    
    @Utils.test_print
    def test_save_get(self):
        newcache = Cache(test=True, config=TestCaching.test_config)
        pd.DataFrame(['second']).to_csv("tmp.csv", index=None)
        expected = pd.read_csv("tmp.csv")
        got, _ = newcache.get("second", None)
        assert_frame_equal(expected, got)
        os.remove("tmp.csv") 
    
    @Utils.test_print
    def test_expired(self):
        result, reason = TestCaching.cache.get("second", 0)
        pd.DataFrame(['second']).to_csv("tmp.csv", index=None)
        expected = pd.read_csv("tmp.csv")
        assert_frame_equal(expected, result)
        self.assertEqual(reason, EntryState.EXPIRED)
        os.remove("tmp.csv") 
    
    @Utils.test_print
    def test_lru(self):
        TestCaching.cache.get("first", None)
        TestCaching.cache.add("fourth", pd.DataFrame(['fourth']))
        result, reason = TestCaching.cache.get("second", None)
        self.assertEqual(result, None)
        self.assertEqual(reason, EntryState.NOT_FOUND)

    @classmethod
    def tearDownClass(cls):
        for f in os.listdir(TestCaching.test_config.dataset_dir):
            os.remove(os.path.join(TestCaching.test_config.dataset_dir,f))
        
