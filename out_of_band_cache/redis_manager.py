import threading

from pkg_resources import get_distribution

from beaker.container import NamespaceManager, Container
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter
from beaker.synchronization import null_synchronizer

import pickle

import redis

class RedisNamespaceManager(NamespaceManager):
    def __init__(self, namespace, host='localhost', port=6379, db=0, password=None,
                 **params):
        NamespaceManager.__init__(self, namespace)
        self.redis_options = dict(host=host, port=port, db=db, password=password)
        self._redis = threading.local()
    
    @property
    def redis(self):
        if not hasattr(self._redis, 'redis'):
            self._redis.redis = redis.Redis(**self.redis_options)
        return self._redis.redis
    
    def get_creation_lock(self, key):
        return null_synchronizer() # No need for creation lock.

    def _format_key(self, key):
        return self.namespace + '_' + key

    def __getitem__(self, key):
        return pickle.loads(self.redis.get(self._format_key(key)))

    def __contains__(self, key):
        value = self.redis.get(self._format_key(key))
        return value is not None

    def has_key(self, key):
        return key in self

    def set_value(self, key, value):
        value = pickle.dumps(value)
        self.redis.set(self._format_key(key), value)

    def __setitem__(self, key, value):
        self.set_value(key, value)
        
    def __delitem__(self, key):
        self.redis.delete(self._format_key(key))

    def do_remove(self):
        self.redis.flushdb()
    
    def keys(self):
        # Fix a bug in redis 1.34.1.
        redis_package = get_distribution('redis')
        if redis_package.version == '1.34.1':
            redis.client.Redis.RESPONSE_CALLBACKS['KEYS'] = lambda r: r
        return redis.keys(self.namespace + '_*')

class MemcachedContainer(Container):
    namespace_class = RedisNamespaceManager
