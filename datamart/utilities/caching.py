import json
import os
import pandas as pd
from time import time
from heapq import heapify, heappop, heappush

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
    
    def get(self, key):
        entry = self._cache.get(key, None)

        # if entry is stale (past lifetime duration)
        if entry and (time()-entry["time_added"]) > self._lifetime_duration:
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
        else:
            self._cache_replace(key, entry)
            
    def remove(self, key):
        """ Removes entry referenced by key """
        return self._cache.pop(key, None)
    
    def _cache_replace(self, key, entry):
        """ Replaces oldest entry in cache """
        _, old_key = heappop(self._queue)
        self.remove(old_key)

        self._cache[key] = entry
        heappush(self._queue, (entry["time_added"], key))
  
    def dataset_path(self, curr_time):
        name = "cached_dataset_{}.csv".format(int(curr_time*1000))
        path = os.path.join(self._dataset_dir, name)
        return path


    


