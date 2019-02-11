import json
import os
import pandas as pd
from time import time

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
        self._cache_filename = "cache.json"
        self._max_cache_size = 10
        self._dataset_dir = "cache/"
        self._lifetime_duration = 24*60*60 # 24 hours 

        if not os.path.exists(self._dataset_dir):
            os.makedirs(self._dataset_dir)

        if os.path.exists(self._cache_filename):
            with open(self._cache_filename, 'r') as f:
                self._cache = json.load(f)
        else:
            self._cache = {}
    
    def get(self, key):
        entry = self._cache.get(key, None)

        if (time()-entry["time_added"]) > self._lifetime_duration:
            return None, "expired"

        if entry and os.path.exists(entry["path"]):
            return pd.read_csv(entry["path"])
        
        return None, "not_found"
    
    def add(self, key, df):

        # Get time
        curr_time = time()

        # Save df to csv
        path = self.dataset_path(curr_time)
        df.to_csv(path,index=None)

        if len(self._cache) < self._max_cache_size:
            self._cache[key] = {
                "path":path,
                "time_added":curr_time
            }
        else:

    
    def dataset_path(self, curr_time):
        name = "cached_dataset_{}.csv".format(int(curr_time*1000))
        path = os.path.join(self._dataset_dir, name)
        return path


    


