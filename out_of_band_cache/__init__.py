"""Implements a beaker cache replacement that does out-of-band updates.

When a value expires, the cache will continue to return the expired value until
a replacement can be generated and stored.

Since beaker's models are (? - if they weren't, Pylons couldn't use them, yes?)
inherently thread-safe, this is very convenient."""

import threading
import logging
from Queue import Queue, Empty
import sys
import traceback

import beaker.cache
import beaker.middleware
import beaker.container

logger = logging.getLogger(__name__)
if logger.isEnabledFor(logging.DEBUG):
    debug = logger.debug
else:
    def debug(message, *args):
        pass
error = logger.error

class SingleEntryQueue(Queue):
    """Queue implementation that only allows for one item to be in the Queue at a time"""
    
    def _put(self, item):
        if item not in self.queue:
            self.queue.append(item)

            
class Update(object):
    def __init__(self, update_for, job):
        self.update_for = update_for
        self.job = job
    def __hash__(self):
        return hash(self.update_for)
    def __eq__(self, other):
        return self.update_for == other
    def __repr__(self):
        return "<%s.%s instance update for: %r>" % (self.__class__.__module__,
                                                    self.__class__.__name__,
                                                    self.update_for)

class CacheMiddleware(beaker.middleware.CacheMiddleware):
    def __init__(self, *args, **kwargs):
        super(CacheMiddleware, self).__init__(*args, **kwargs)
        self.cache_manager = CacheManager(self.cache_manager)

class CacheManager(beaker.cache.CacheManager):
    def __init__(self, beaker_cache_manager):
        self.kwargs = beaker_cache_manager.kwargs
        self.caches = beaker_cache_manager.caches
        self.regions = beaker_cache_manager.regions
        self.queue = SingleEntryQueue()
        update_thread = threading.Thread(target=update_processor,
                                         kwargs={'queue': self.queue,},)
        update_thread.start()
    
    def get_cache(self, name, out_of_band=False, **kwargs):
        if not out_of_band:
            return super(CacheManager, self).get_cache(name, **kwargs)
        kw = self.kwargs.copy()
        kw.update(kwargs)
        return self.caches.setdefault(name + str(kw), Cache(name, self.queue, **kw))
    
    def get_cache_region(self, name, region, out_of_band=False):
        if not out_of_band:
            return super(CacheManager, self).get_cache_region(name, region)
        if region not in self.regions:
            raise BeakerException('Cache region not configured: %s' % region)
        kw = self.regions[region]
        return self.caches.setdefault(name + str(kw), Cache(name, self.queue, **kw))

class Cache(beaker.cache.Cache):
    def __init__(self, name, queue, **kw):
        super(Cache, self).__init__(name, **kw)
        self.queue = queue
    
    def _get_value(self, key, **kw):
        if isinstance(key, unicode):
            key = key.encode('ascii', 'backslashreplace')

        if 'type' in kw:
            return self._legacy_get_value(key, **kw)

        kw.setdefault('expiretime', self.expiretime)
        kw.setdefault('starttime', self.starttime)
        
        return Value(key, self.namespace, self.queue, **kw)

class NewValueInProgressException(Exception):
    """Raised when a request is made for a value we don't have in the cache."""
    pass

class Value(beaker.container.Value):
    """A Value that will still return - but allows querying on - expired keys."""
    def __init__(self, key, namespace, queue, **kw):
        super(Value, self).__init__(key, namespace, **kw)
        self.queue = queue
    
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
            v = self.createfunc()
            self.set_value(v)
        self.queue.put(Update(self.key, do_update))
        if has_value:
            return value
        else:
            raise NewValueInProgressException()

def update_processor(queue):
    error('Started update processor.')
    while True:
        update = queue.get()
        debug('Running update for %s', update.update_for)
        try:
            update.job()
        except Exception, e:
            error("Exception while loading %s: %r", update.update_for, e)
            debug(''.join(traceback.format_exception(*sys.exc_info())))
