import logging

from decorator import decorator
from pylons import cache, config, request
import pylons
from webhelpers.html import literal

from out_of_band_cache import NewValueInProgressException
from routes import url_for
import routes.util

log = logging.getLogger(__name__)

def render_from_cache(f, rendering_action):
    """decorator. Render f using an out-of-band cache to regenerate the cached
    copy. If no cached copy is available, use rendering_action to display
    something in the meantime. If caching is disabled, just invoke f directly."""
    
    def _render_from_cache(action, self, *args, **kwargs):
        context = dict(
            tmpl_context = self._py_object.tmpl_context,
            app_globals = self._py_object.config['pylons.app_globals'],
            config = self._py_object.config,
            request = self._py_object.request,
            response = self._py_object.response,
            translator = pylons.translator._current_obj(),
            session = pylons.session._current_obj(),
        )
        url = self._py_object.request.url
    
        def createfunc():
            context['url'] = routes.util.URLGenerator(context['config']['routes.map'],
                                                      context['request'].environ)
            headers_copy = {}
            for header, value in context['response'].headers.iteritems():
                headers_copy[header] = value
            for key, value in context.iteritems():
                getattr(pylons, key)._push_object(value)
            
            content = action(self, *args, **kwargs)
            
            cached_headers = {}
            for header, value in context['response'].headers.iteritems():
                if header not in headers_copy or headers_copy[header] != value:
                    cached_headers[header] = value
            log.debug('Headers Copy: %s', headers_copy)
            log.debug('Headers: %s', context['response'].headers)
            log.debug('Cached Headers: %s', cached_headers)
            for key, value in context.iteritems():
                getattr(pylons, key)._pop_object(value)
            return (cached_headers, content)
        
        if context['app_globals'].cache_enabled:
            my_cache = cache.get_cache(
                context['config']['templates.namespace'],
                type=context['config'].get('beaker.cache.type', 'memory'),
                out_of_band=True)
            try:
                headers, content = my_cache.get_value(
                    key=url, createfunc=createfunc, expiretime=60)
                for header, value in headers.iteritems():
                    context['response'].headers[header] = value
                return content
            except NewValueInProgressException:
                context['response'].status = 503
                return rendering_action(*args, **kwargs)
        else:
            return action(self, *args, **kwargs)
    
    return decorator(_render_from_cache, f)