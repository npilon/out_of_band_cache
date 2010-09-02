Out of band caching for Beaker and Pylons
=========================================

Caches the result of a Pylons controller function using the URL of the request as a key.

    @render_from_cache(my_fail_whale)
    def my_controller(self):
       return expensive_call()

The first time this is called expensive_call function is executed in a newly created thread, and my_fail_whale is called and returned immediately.
