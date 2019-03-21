"""
Contains classes for Cache, EntryState, CacheConfig
"""

import json
import os
import typing
import threading

from time import time
from collections import OrderedDict
from enum import Enum

import pandas as pd

class EntryState(Enum):
    """
    Enum to keep track of different possible states of a cache entry
    """
    EXPIRED = 1
    FOUND = 2
    NOT_FOUND = 3
    ERROR = -1

class CacheConfig():
    """
    Contains configuration data for the cache
    """
    def __init__(self, config_path: str):
        # Default configuration
        self._config = {
            "cache_filename": "cache.json",
            "max_cache_size":10,
            "dataset_dir":"/nfs1/dsbox-repo/datamart/cache",
            "default_validity": 604800
        }

        # Create default location for config file (~/.config/datamart/caching_config.json)
        if config_path is None:
            self._config_path = os.path.join(os.path.expanduser("~"), ".config/datamart/caching_config.json")
        else:
            self._config_path = config_path
        
        # Create folder if not exists
        if not os.path.exists(os.path.dirname(self._config_path)):
            os.makedirs(os.path.dirname(self._config_path))

        # Open file if it exists
        if os.path.exists(self._config_path):
            with open(self._config_path,'r') as f:
                self._config = json.load(f)
        
        # Save config
        self.save()

    @property
    def cache_filename(self):
        name = os.path.join(self.dataset_dir, self._config.get("cache_filename", "cache.json"))
        return name
    
    @cache_filename.setter
    def cache_filename(self, value):
        self._config["cache_filename"] = value

    @property
    def max_cache_size(self):
        return self._config.get("max_cache_size", 10)
    
    @max_cache_size.setter
    def max_cache_size(self, value):
        self._config["max_cache_size"] = value
    
    @property
    def dataset_dir(self):
        return self._config.get("dataset_dir", "/nfs1/dsbox-repo/datamart/cache")
    
    @dataset_dir.setter
    def dataset_dir(self, value):
        self._config["dataset_dir"] = value
    
    @property
    def lifetime_duration(self):
        return self._config.get("default_validity", 7*24*60*60)
    
    @lifetime_duration.setter
    def lifetime_duration(self, value):
        self._config["default_validity"] = value
    
    def save(self):
        with open(self._config_path, 'w+') as f:
            json.dump(self._config, f)
        

class Cache:
    """
    Contains implementation of Cache. Modeled as singleton.
    """
    __instance = None
    __lock = threading.RLock()

    @staticmethod
    def get_instance():
        if Cache.__instance is None:
            Cache.__lock.acquire()
            if Cache.__instance is None:
                Cache()
            Cache.__lock.release()
        return Cache.__instance
    
    def __init__(self, test=False, config=None):
        if test:
            self._init_cache(config)
            return

        if Cache.__instance != None:
            raise Exception("This class is a singleton! Please create instance using get_instance()")
        else:
            self._init_cache()
            Cache.__instance = self
    
    def _init_cache(self, config: CacheConfig = None):
        if config:
            self.config = config
        else:
            self.config = CacheConfig(None)
            self.config.save()

        if not os.path.exists(self.config.dataset_dir):
            os.makedirs(self.config.dataset_dir)
                
        if os.path.exists(self.config.cache_filename):
            with open(self.config.cache_filename, 'r') as f:
                self._cache = json.load(f, object_pairs_hook=OrderedDict)
        else:
            self._cache = OrderedDict()
    
    def _save_cache(self):
        with open(self.config.cache_filename, 'w') as f:
            json.dump(self._cache, f)
    
    def get(self, 
            key: str,
            ttl: int) -> (typing.Optional[pd.DataFrame], EntryState):
        """
        Returns the dataset found in cache

        Args:
            key: cache key
            ttl: time to live
        
        Returns:
            tuple: (df, reason)
        """
        entry = self._cache.get(key, None)

        if ttl is None:
            ttl = self.lifetime_duration

        # if entry is stale (past lifetime duration)
        if entry and (time()-entry["time_added"]) > ttl:
            #print("cache expired")
            return pd.read_csv(entry["path"]), EntryState.EXPIRED

        # if entry exists
        if entry:
            if os.path.exists(entry["path"]):
                #print("cache hit")
                return pd.read_csv(entry["path"]), EntryState.FOUND
            # No file found at entry
            else:
                #print("cache: no file found")
                self.remove(key)
        
        # if entry does not exist
        #print("cache: no entry")
        return None, EntryState.NOT_FOUND
    
    def add(self, key, df):

        if key in self._cache:
            return 
        
        # Get time
        curr_time = time()

        # Save df to csv
        path = self.dataset_path(curr_time)
        df.to_csv(path,index=None)

        entry = {
            "path":path,
            "time_added":curr_time
        }

        if len(self._cache) < self.config.max_cache_size:
            self._cache[key] = entry
            self._save_cache()
        else:
            self._cache_replace(key, entry)  
        
    @property
    def lifetime_duration(self):
        return self.config.lifetime_duration
        
    def remove(self, key):
        """ Removes entry referenced by key """
        popped = self._cache.pop(key, None)
        if os.path.exists(popped["path"]):
            os.remove(popped["path"])
        self._save_cache()
        return popped
    
    def _cache_replace(self, key, entry):
        """ Replaces oldest entry in cache """
        # Remove oldest entry
        old_key, old_entry = self._cache.popitem(last=False)
        if os.path.exists(old_entry["path"]):
            os.remove(old_entry["path"])

        # Save new entry
        self._cache[key] = entry
    
        self._save_cache()
  
    def dataset_path(self, curr_time):
        name = "cached_dataset_{}.csv".format(int(curr_time*1000))
        path = os.path.join(self.config.dataset_dir, name)
        return path


    


