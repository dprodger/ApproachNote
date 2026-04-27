"""
Admin subdomain support.

The admin UI is served at https://admin.approachnote.com (no /admin prefix in
the browser-facing URL). Internally, all admin routes remain registered under
the `/admin` URL prefix on `admin_bp` and `admin_research_bp` — moving every
blueprint to a different prefix would mean dozens of route changes for no
real benefit. Instead this module bridges the two:

- Incoming: a WSGI middleware rewrites PATH_INFO so `admin.approachnote.com/foo`
  routes to the existing `/admin/foo` view. The rewrite is idempotent — if a
  request already carries the `/admin` prefix (e.g. an in-flight fetch from
  page JS that wasn't updated), it's left alone.

- Outgoing: an `after_request` hook strips the leading `/admin` from the
  `Location:` header on 3xx responses, so server-issued redirects produce the
  clean browser-facing URL.

- Templates: `admin_url(internal_path)` is registered as a Jinja global. It
  returns `internal_path` with `/admin` stripped on the admin subdomain, and
  unchanged everywhere else, so href= and action= attributes render correctly
  on whichever host they're being served from.

`is_admin_subdomain()` is the single source of truth for "are we on the admin
host right now?" — used by cookie path selection and redirect rewriting.

Localhost (dev) keeps the original `/admin/*` URL surface; nothing changes
for `python app.py` workflows.
"""

import logging

from flask import request


logger = logging.getLogger(__name__)


ADMIN_HOSTS = frozenset({'admin.approachnote.com'})


def _normalize_host(host: str | None) -> str:
    if not host:
        return ''
    return host.lower().split(':', 1)[0]


def _host_from_environ(environ) -> str:
    """Resolve the client-visible host from a WSGI environ. Honours
    X-Forwarded-Host (set by Render's reverse proxy)."""
    forwarded = environ.get('HTTP_X_FORWARDED_HOST')
    if forwarded:
        host = forwarded.split(',')[0].strip()
    else:
        host = environ.get('HTTP_HOST', '')
    return _normalize_host(host)


def is_admin_subdomain() -> bool:
    """True when the current Flask request is being served from the admin host.
    Safe to call from request context only."""
    try:
        host = _normalize_host(
            request.headers.get('X-Forwarded-Host') or request.host
        )
    except RuntimeError:
        return False
    return host in ADMIN_HOSTS


class AdminSubdomainMiddleware:
    """WSGI middleware that maps `admin.approachnote.com/<path>` onto the
    internal `/admin/<path>` route surface.

    Must be installed AFTER ProxyFix so X-Forwarded-Host has been honoured
    into HTTP_HOST already. Wrap order in app.py: app.wsgi_app = ProxyFix(...)
    then app.wsgi_app = AdminSubdomainMiddleware(app.wsgi_app).
    """

    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        host = _host_from_environ(environ)
        if host in ADMIN_HOSTS:
            path = environ.get('PATH_INFO', '') or '/'
            # Static files keep their /static/... path; only admin-routable
            # paths get the prefix injection.
            if not path.startswith('/static/') and path != '/static':
                if path == '/admin' or path.startswith('/admin/'):
                    # Idempotent: an in-flight fetch like /admin/orphans/123
                    # already maps to the right route, leave it alone.
                    pass
                elif path == '/':
                    environ['PATH_INFO'] = '/admin/'
                else:
                    environ['PATH_INFO'] = '/admin' + path
        return self.wsgi_app(environ, start_response)


def admin_url(internal_path: str) -> str:
    """Convert an internal (`/admin/...`) URL path into the browser-facing
    URL for the current host. Use as a Jinja global in templates:

        <a href="{{ admin_url('/admin/orphans') }}">Orphans</a>

    On admin.approachnote.com → '/orphans'. Elsewhere → '/admin/orphans'.
    """
    if not internal_path:
        return internal_path
    if not is_admin_subdomain():
        return internal_path
    if internal_path == '/admin' or internal_path == '/admin/':
        return '/'
    if internal_path.startswith('/admin/'):
        return internal_path[len('/admin'):]
    return internal_path


def rewrite_location_header(response):
    """Flask `after_request` hook. On the admin subdomain, strip a leading
    `/admin` from the `Location:` header so server-issued redirects (e.g.
    `redirect('/admin/login')`) produce the clean browser-facing URL."""
    if not is_admin_subdomain():
        return response
    loc = response.headers.get('Location')
    if not loc:
        return response
    # Only touch same-origin paths, not absolute URLs to elsewhere.
    if loc.startswith('/admin/') or loc == '/admin' or loc == '/admin/':
        if loc == '/admin' or loc == '/admin/':
            response.headers['Location'] = '/'
        else:
            response.headers['Location'] = loc[len('/admin'):]
    return response


def install(app):
    """Wire up the middleware, after_request hook, and Jinja global on the
    given Flask app. Call once during app construction, after ProxyFix."""
    app.wsgi_app = AdminSubdomainMiddleware(app.wsgi_app)
    app.after_request(rewrite_location_header)
    app.jinja_env.globals['admin_url'] = admin_url
    app.jinja_env.globals['is_admin_subdomain'] = is_admin_subdomain
