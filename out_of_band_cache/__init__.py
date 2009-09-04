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
from datetime import timedelta
import time

import beaker.cache
import beaker.middleware
import beaker.container

logger = logging.getLogger(__name__)
if logger.isEnabledFor(logging.DEBUG):
    debug = logger.debug
else:
    def debug(message, *args):
        pass

class SingleEntryQueue(Queue):
    """Queue implementation that only allows for one ``item'' to be in the Queue at a time"""
    
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
        for worker in range(int(self.kwargs.get('num_workers', '1'))):
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
    def __init__(self, name, queue, maximum_update_time=None, **kw):
        super(Cache, self).__init__(name, **kw)
        self.queue = queue
        self.maximum_update_time = maximum_update_time
    
    def _get_value(self, key, **kw):
        if isinstance(key, unicode):
            key = key.encode('ascii', 'backslashreplace')

        if 'type' in kw:
            return self._legacy_get_value(key, **kw)

        kw.setdefault('expiretime', self.expiretime)
        kw.setdefault('starttime', self.starttime)
        
        return Value(key, self.namespace, self.queue,
                     maximum_update_time=self.maximum_update_time, **kw)
    
    def entry_age(self, key):
        """The age of an entry as a timedelta."""
        #for server in self.namespace.mc.servers:
            #server.debuglog = lambda str: logger.debug("MemCached: %s\n", str)
        self.namespace.acquire_read_lock()
        try:
            if not self.namespace.has_key(key):
                logger.warning('Namespace %s does not have key: %s', self.namespace.namespace, key)
                return None # Value hasn't been stored yet.
            value = self.namespace[key]
            if len(value) == 3:
                storedtime, expiretime, value = value
            else:
                storedtime, expiretime, in_progress, value = value
            age = timedelta(seconds=int(time.time() - storedtime))
            logger.debug('Namespace %s has key %s with age %s', self.namespace.namespace, key, age)
            return age
        finally:
            self.namespace.release_read_lock()

class NewValueInProgressException(Exception):
    """Raised when a request is made for a value we don't have in the cache."""
    pass

class Value(beaker.container.Value):
    """A Value that will still return - but allows querying on - expired keys."""
    def __init__(self, key, namespace, queue, maximum_update_time=None, **kw):
        super(Value, self).__init__(key, namespace, **kw)
        self.queue = queue
        self.update_in_progress = None
        self.maximum_update_time = maximum_update_time
    
    def get_value(self):
        # Attempt to read the value out of the store.
        self.namespace.acquire_read_lock()
        value = None
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

        debug('Update in Progress: %s, maximum update time: %s',
              self.update_in_progress, self.maximum_update_time)
        if not self.update_in_progress or\
           (self.maximum_update_time and\
            time.time() - self.update_in_progress > self.maximum_update_time):
            debug("get_value creating new value")
            # Return the current value and spawn a thread to update it.
            def do_update():
                self.start_update(value)
                v = self.createfunc()
                self.set_value(v)
            self.queue.put(Update(self.key, do_update))
        if has_value and value is not None:
            return value
        raise NewValueInProgressException()
    
    def _Value__get_value(self):
        """__get_value that supports update_in_progress."""
        value = self.namespace[self.key]
        if len(value) == 4:
            self.storedtime, self.expiretime, self.update_in_progress, value = value
        else:
            self.storedtime, self.expiretime, value = value
        return value
    
    def set_value(self, value):
        """set_value with added in_progress handling."""
        self.namespace.acquire_write_lock()
        try:
            self.storedtime = time.time()
            debug("set_value stored time %r expire time %r", self.storedtime, self.expire_argument)
            self.namespace.set_value(self.key, (self.storedtime, self.expire_argument, None, value))
        except Exception, e:
            print e
            raise
        finally:
            self.namespace.release_write_lock()
    
    def start_update(self, value):
        """Start running an update with some current value."""
        self.namespace.acquire_write_lock()
        try:
            self.update_in_progress = time.time()
            debug("start_update %s stored time %r expire time %r", self.update_in_progress, self.storedtime, self.expire_argument)
            self.namespace.set_value(self.key, (self.storedtime, self.expire_argument, self.update_in_progress, value))
        finally:
            self.namespace.release_write_lock()

def update_processor(queue):
    logger.info('Started update processor.')
    while True:
        update = queue.get()
        debug('Running update for %s', update.update_for)
        try:
            update.job()
            queue.task_done()
        except Exception, e:
            logger.error("Exception while loading %s: %r", update.update_for, e)
            debug(''.join(traceback.format_exception(*sys.exc_info())))
