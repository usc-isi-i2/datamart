"""
Contains classes for Cache, EntryState, CacheConfig
"""

import json
import os
import typing
import threading
import uuid

from enum import Enum
from peewee import *
from time import time

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
            "dbname": "cache.db",
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
    def dbname(self):
        name = os.path.join(self.dataset_dir, self._config.get("dbname", "cache.db"))
        return name
    
    @dbname.setter
    def dbname(self, value):
        self._config["dbname"] = value

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

# DB Models START
db_proxy = Proxy()

class BaseModel(Model):
    class Meta:
        database = db_proxy

class CacheEntry(BaseModel):
    key = TextField(primary_key=True)
    file_path = TextField(unique=True)
    time_added = BigIntegerField()
    last_accessed = BigIntegerField()
# DB Models END

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

        if not os.path.exists(self.config.dataset_dir):
            os.makedirs(self.config.dataset_dir)
        
        self.db = SqliteDatabase(self.config.dbname)
        db_proxy.initialize(self.db)    
        self.db.create_tables([CacheEntry])
    
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
        try:
            entry = CacheEntry.get_by_id(key)
        except DoesNotExist:
            entry = None

        if ttl is None:
            ttl = self.lifetime_duration

        # if entry is stale (past lifetime duration)
        if entry and (int(time()*1000)-entry.time_added) > ttl:
            print("cache expired")
            #TODO set last accessed
            return pd.read_csv(entry.file_path), EntryState.EXPIRED

        # if entry exists
        if entry:
            if os.path.exists(entry.file_path):
                print("cache hit")
                CacheEntry.set_by_id(key, {"last_accessed": int(time()*1000)})
                return pd.read_csv(entry.file_path), EntryState.FOUND
            # No file found at entry
            else:
                print("cache: no file found")
                CacheEntry.delete_by_id(key)
        
        # if entry does not exist
        print("cache: no entry")
        return None, EntryState.NOT_FOUND
    
    def add(self, key, df):
        with self.db.atomic() as txn:
            # Get time
            curr_time = int(time()*1000)

            # Save df to csv
            path = self.dataset_path()
            try:
                df.to_csv(path,index=None)
                entry = CacheEntry.create(key=key, file_path=path, time_added=curr_time, last_accessed=curr_time)
                cache_size = CacheEntry.select().count()

                if cache_size > self.config.max_cache_size:
                    last_entry = CacheEntry.select().order_by(CacheEntry.last_accessed).get()
                    last_entry.delete_instance()
                
                entry.save()
            except Exception as e:
                print("ADD Err: {}".format(e))
                if os.path.exists(path):
                    os.remove(path)
                txn.rollback()
                

    def remove(self, key):
        with self.db.atomic() as txn:
            try:
                try:
                    entry = CacheEntry.get_by_id(key)
                except DoesNotExist:
                    entry = None
                if entry:
                    if os.path.exists(entry.file_path):
                        os.remove(entry.file_path)
                    entry.delete_instance()
            except Exception as e:
                print(e)
                txn.rollback()
                
        
    @property
    def lifetime_duration(self):
        return self.config.lifetime_duration
    
    def dataset_path(self):
        name = "cached_dataset_{}.csv".format(uuid.uuid1())
        path = os.path.join(self.config.dataset_dir, name)
        return path


    


