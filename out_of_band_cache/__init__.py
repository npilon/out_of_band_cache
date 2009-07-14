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

class CacheMiddlewear(beaker.middleware.CacheMiddleware):
    pass

class CacheManager(beaker.cache.CacheManager):
    pass

class Cache(beaker.cache.Cache):
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
            debug("lock_createfunc (waiting)")
            creation_lock.acquire()
            debug("lock_createfunc (waited)")

        create_thread_spawned = threading.Event()
        try:
            # see if someone created the value already
            self.namespace.acquire_read_lock()
            try:
                if self.has_value():
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
            if self.has_value() and self._is_expired():
                # Return the current value and spawn a thread to update it.
                def do_update():
                    create_thread_spawned.set()
                    v = self.createfunc()
                    self.set_value(v)
                    creation_lock.release()
                update_thread = threading.Thread(target=do_update)
                update_thread.start()
                create_thread_spawned.wait()
                return value
            else:
                # Update the value in-line.
                v = self.createfunc()
                self.set_value(v)
                return v
        finally:
            if not create_thread_spawned.isSet():
                creation_lock.release()
                debug("released create lock")
