import json
import os
import pandas as pd
import typing
from time import time
from heapq import heappop, heappush

class Cache:
    __instance = None

    @staticmethod
    def get_instance():
        if Cache.__instance is None:
            Cache()
        return Cache.__instance
    
    def __init__(self):
        if Cache.__instance != None:
            raise Exception("This class is a singleton! Please create instance using get_instance()")
        else:
            self._init_cache()
            Cache.__instance = self
    
    def _init_cache(self):
        self._config_path = os.path.join(os.path.expanduser("~"), ".config/datamart/caching_config.json")
        
        # Default config
        self._cache_filename = "/tmp/cache/cache.json"
        self._max_cache_size = 10
        self._dataset_dir = "/tmp/cache/"
        self._lifetime_duration = 7*24*60*60 # 1 week

        if os.path.exists(self._config_path):
            with open(self._config_path,'r') as f:
                config = json.load(f)
            self._cache_filename = config.get("cache_filename", self._cache_filename)
            self._max_cache_size = config.get("max_cache_size", self._max_cache_size)
            self._dataset_dir = config.get("dataset_dir", self._dataset_dir)
            self._lifetime_duration = config.get("default_validity", self._lifetime_duration)
            
        self._queue = []

        if not os.path.exists(self._dataset_dir):
            os.makedirs(self._dataset_dir)

        if os.path.exists(self._cache_filename):
            with open(self._cache_filename, 'r') as f:
                self._cache = json.load(f)
                for key in self._cache:
                    heappush(self._queue, (self._cache[key]["time_added"], key))

        else:
            self._cache = {}
    
    def _save_cache(self):
        with open(self._cache_filename, 'w') as f:
            json.dump(self._cache, f)
    
    def get(self, 
            key: str,
            ttl: int) -> typing.Optional[pd.DataFrame]:
        """
        Returns the dataset found in cache
        """
        entry = self._cache.get(key, None)

        # if entry is stale (past lifetime duration)
        if entry and (time()-entry["time_added"]) > ttl:
            return pd.read_csv(entry["path"]), "expired"

        # if entry exists
        if entry and os.path.exists(entry["path"]):
            return pd.read_csv(entry["path"]), "success"
        
        # if entry does not exist
        return None, "not_found"
    
    def add(self, key, df):

        # Get time
        curr_time = time()

        # Save df to csv
        path = self.dataset_path(curr_time)
        df.to_csv(path,index=None)

        entry = {
            "path":path,
            "time_added":curr_time
        }

        if len(self._cache) < self._max_cache_size:
            self._cache[key] = entry
            heappush(self._queue, (curr_time, key))
            self._save_cache()
        else:
            self._cache_replace(key, entry)  
        
    @property
    def lifetime_duration(self):
        return self._lifetime_duration
        
    def remove(self, key):
        """ Removes entry referenced by key """
        popped = self._cache.pop(key, None)
        self._save_cache()
        return popped

    
    def _cache_replace(self, key, entry):
        """ Replaces oldest entry in cache """
        _, old_key = heappop(self._queue)
        self.remove(old_key)

        self._cache[key] = entry
        heappush(self._queue, (entry["time_added"], key))
    
        self._save_cache()
  
    def dataset_path(self, curr_time):
        name = "cached_dataset_{}.csv".format(int(curr_time*1000))
        path = os.path.join(self._dataset_dir, name)
        return path


    


