"""Implements a beaker cache replacement that does out-of-band updates.

When a value expires, the cache will continue to return the expired value until
a replacement can be generated and stored.

Since beaker's models are (? - if they weren't, Pylons couldn't use them, yes?)
inherently thread-safe, this is very convenient."""

import threading
import logging

import beaker.cache
import beaker.middleware
import beaker.container

logger = logging.getLogger(__name__)
if logger.isEnabledFor(logging.DEBUG):
    debug = logger.debug
else:
    def debug(message, *args):
        pass

class CacheMiddleware(beaker.middleware.CacheMiddleware):
    def __init__(self, *args, **kwargs):
        super(CacheMiddleware, self).__init__(*args, **kwargs)
        self.cache_manager = CacheManager(self.cache_manager)

class CacheManager(beaker.cache.CacheManager):
    def __init__(self, beaker_cache_manager):
        self.kwargs = beaker_cache_manager.kwargs
        self.caches = beaker_cache_manager.caches
        self.regions = beaker_cache_manager.regions
    
    def get_cache(self, name, out_of_band=False, **kwargs):
        if not out_of_band:
            return super(CacheManager, self).get_cache(name, **kwargs)
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return self.caches.setdefault(name + str(kw), Cache(name, **kw))
    
    def get_cache_region(self, name, region, out_of_band=False):
        if not out_of_band:
            return super(CacheManager, self).get_cache_region(name, region)
        if region not in self.regions:
            raise BeakerException('Cache region not configured: %s' % region)
        kw = self.regions[region]
        return self.caches.setdefault(name + str(kw), Cache(name, **kw))

class Cache(beaker.cache.Cache):
    def _get_value(self, key, **kw):
        if isinstance(key, unicode):
            key = key.encode('ascii', 'backslashreplace')

        if 'type' in kw:
            return self._legacy_get_value(key, **kw)

        kw.setdefault('expiretime', self.expiretime)
        kw.setdefault('starttime', self.starttime)
        
        return container.Value(key, self.namespace, **kw)

class NewValueInProgressException(Exception):
    """Raised when a request is made for a value we don't have in the cache."""
    pass

class Value(beaker.container.Value):
    """A Value that will still return - but allows querying on - expired keys."""
    
    def get_value(self):
        # Attempt to read the value out of the store.
        self.namespace.acquire_read_lock()
        try:
            has_value = self.has_value()
            if has_value:
                try:
                    value = self._Value__get_value()
                    if not self._is_expired():
                        return value
                except KeyError:
                    # guard against un-mutexed backends raising KeyError
                    pass
                    
            if not self.createfunc:
                raise KeyError(self.key)
        finally:
            self.namespace.release_read_lock()
        
        # No value or expired value; attempt to get the create lock.
        has_createlock = False
        creation_lock = self.namespace.get_creation_lock(self.key)
        if has_value:
            # This branch will either return immediately or get the lock.
            if not creation_lock.acquire(wait=False):
                debug("get_value returning old value while new one is created")
                return value
            else:
                debug("lock_creatfunc (didnt wait)")
        else:
            debug("lock_createfunc (new value)")
            if not creation_lock.acquire(wait=False):
                raise NewValueInProgressException()

        create_thread_spawned = threading.Event()
        try:
            # see if someone created the value already
            self.namespace.acquire_read_lock()
            try:
                has_value = self.has_value()
                if has_value:
                    try:
                        value = self._Value__get_value()
                        if not self._is_expired():
                            return value
                    except KeyError:
                        # guard against un-mutexed backends raising KeyError
                        pass
            finally:
                self.namespace.release_read_lock()

            debug("get_value creating new value")
            # Return the current value and spawn a thread to update it.
            def do_update():
                create_thread_spawned.set()
                v = self.createfunc()
                self.set_value(v)
                creation_lock.release()
            update_thread = threading.Thread(target=do_update)
            update_thread.start()
            create_thread_spawned.wait()
            if has_value:
                return value
            else:
                raise NewValueInProgressException()
        finally:
            if not create_thread_spawned.isSet():
                creation_lock.release()
                debug("released create lock")
