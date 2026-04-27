"""
Admin subdomain integration tests.

Cover the host-aware behaviour added in middleware/admin_subdomain.py:

  - admin.approachnote.com/<path> routes to the same view that
    api.approachnote.com (or localhost) serves at /admin/<path>.
  - The path rewrite is idempotent, so an in-flight fetch carrying the
    `/admin` prefix on the admin subdomain still works.
  - Cookies set during login on the admin subdomain are scoped to '/' so
    they're sent on every page on that host.
  - Server-issued redirects (Location: /admin/...) come back to the browser
    without the /admin prefix on the admin subdomain.
  - admin_url() and _safe_next() return host-appropriate forms.
  - /admin/* hits on api.approachnote.com hard-fail (404), per issue #169.

The existing test_admin_auth.py covers the legacy /admin/* surface
(localhost) and we leave those expectations intact.
"""

import pytest


ADMIN_HOST = 'admin.approachnote.com'


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _grant_admin(db, user_id: str, is_admin: bool = True):
    with db.cursor() as cur:
        cur.execute(
            "UPDATE users SET is_admin = %s WHERE id = %s",
            (is_admin, user_id),
        )
    db.commit()


@pytest.fixture
def admin_user(register_user, db):
    body = register_user(
        email="admin@example.com",
        password="correct-horse-battery-staple",
        display_name="Admin",
    )
    _grant_admin(db, body["user"]["id"], True)
    return body


def _on_admin_host(headers=None):
    """Header dict that pins a request to the admin subdomain via the
    X-Forwarded-Host signal Render's proxy sets."""
    out = dict(headers or {})
    out['X-Forwarded-Host'] = ADMIN_HOST
    return out


# ---------------------------------------------------------------------------
# admin_url() unit
# ---------------------------------------------------------------------------

def test_admin_url_strips_prefix_on_admin_host(app):
    from middleware.admin_subdomain import admin_url
    with app.test_request_context('/', headers={'X-Forwarded-Host': ADMIN_HOST}):
        assert admin_url('/admin/orphans') == '/orphans'
        assert admin_url('/admin/') == '/'
        assert admin_url('/admin') == '/'
        assert admin_url('/static/foo.js') == '/static/foo.js'


def test_admin_url_unchanged_off_admin_host(app):
    from middleware.admin_subdomain import admin_url
    with app.test_request_context('/'):
        assert admin_url('/admin/orphans') == '/admin/orphans'
        assert admin_url('/admin') == '/admin'


# ---------------------------------------------------------------------------
# WSGI path rewrite
# ---------------------------------------------------------------------------

def test_admin_root_rewrites_to_admin_login_redirect(client):
    """GET admin.approachnote.com/ on an unauth session should hit the admin
    dashboard route, which 302s to the login page."""
    resp = client.get('/', headers=_on_admin_host({'Accept': 'text/html'}))
    assert resp.status_code == 302
    # Location should be the host-stripped form.
    assert resp.headers['Location'].startswith('/login')
    assert 'next=' in resp.headers['Location']


def test_admin_subdomain_path_routes_to_admin_view(client):
    """GET admin.approachnote.com/orphans should hit /admin/orphans → 302
    to the login page (admin gate, no auth)."""
    resp = client.get('/orphans', headers=_on_admin_host({'Accept': 'text/html'}))
    assert resp.status_code == 302
    assert resp.headers['Location'].startswith('/login')


def test_admin_subdomain_idempotent_on_already_prefixed_path(client):
    """An in-flight fetch like fetch('/admin/orphans') from older JS should
    keep working — middleware leaves an existing /admin prefix alone."""
    resp = client.get('/admin/orphans', headers=_on_admin_host({'Accept': 'text/html'}))
    assert resp.status_code == 302
    # Even when the request came in with /admin/..., we strip the prefix on
    # the way out so the URL bar stays clean.
    assert resp.headers['Location'].startswith('/login')


def test_admin_subdomain_static_paths_are_not_rewritten(client):
    """/static/* must reach Flask's static handler, not /admin/static."""
    resp = client.get('/static/js/admin.js', headers=_on_admin_host())
    assert resp.status_code == 200
    assert b'__adminFetchPatched' in resp.data


# ---------------------------------------------------------------------------
# Hard-fail on the legacy host (issue #169)
# ---------------------------------------------------------------------------

def test_admin_hard_fails_on_api_host(client):
    """api.approachnote.com/admin/* must 404 — the only sanctioned admin
    surface in production is admin.approachnote.com."""
    resp = client.get(
        '/admin/',
        headers={
            'X-Forwarded-Host': 'api.approachnote.com',
            'Accept': 'text/html',
        },
    )
    assert resp.status_code == 404


def test_admin_login_hard_fails_on_api_host(client):
    """Even /admin/login (the gate-exempt entry point) must 404 on the API
    host, otherwise an attacker could phish admins on the wrong domain."""
    resp = client.get(
        '/admin/login',
        headers={'X-Forwarded-Host': 'api.approachnote.com'},
    )
    assert resp.status_code == 404


def test_root_on_admin_host_does_not_serve_landing_page(client):
    """The root path on admin.approachnote.com is the dashboard, not the
    public website's landing page."""
    resp = client.get('/', headers=_on_admin_host({'Accept': 'text/html'}))
    # Expect a redirect to login (admin gate), not a 200 with the website.
    assert resp.status_code == 302
    assert resp.headers['Location'].startswith('/login')


# ---------------------------------------------------------------------------
# Cookie path on the admin subdomain
# ---------------------------------------------------------------------------

def test_admin_login_sets_root_path_cookies_on_admin_host(client, admin_user):
    """After logging in via admin.approachnote.com, the admin_session and
    admin_csrf cookies must be scoped to '/' so they're sent for every URL
    on that host (the browser-facing paths don't carry /admin)."""
    resp = client.post(
        '/login',
        data={
            'email': 'admin@example.com',
            'password': 'correct-horse-battery-staple',
        },
        headers=_on_admin_host(),
    )
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/')

    session = client.get_cookie('admin_session', path='/')
    csrf = client.get_cookie('admin_csrf', path='/')
    assert session is not None, "admin_session cookie must be set with path=/"
    assert csrf is not None, "admin_csrf cookie must be set with path=/"

    # And NOT scoped to /admin — that path won't be visited from a browser.
    assert client.get_cookie('admin_session', path='/admin') is None


def test_admin_csrf_cookie_on_login_page_uses_root_path(client):
    """GET /login on the admin subdomain seeds an admin_csrf cookie. It
    must be scoped to '/' so the form POST carries it."""
    resp = client.get('/login', headers=_on_admin_host())
    assert resp.status_code == 200
    assert client.get_cookie('admin_csrf', path='/') is not None


# ---------------------------------------------------------------------------
# _safe_next host-aware behaviour
# ---------------------------------------------------------------------------

def test_safe_next_accepts_legacy_form_on_admin_host(client, admin_user):
    """A bookmark like ?next=/admin/orphans must still land somewhere
    sensible on the admin subdomain — translated to '/orphans'."""
    resp = client.post(
        '/login?next=/admin/orphans',
        data={
            'email': 'admin@example.com',
            'password': 'correct-horse-battery-staple',
        },
        headers=_on_admin_host(),
    )
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/orphans')


def test_safe_next_accepts_clean_form_on_admin_host(client, admin_user):
    """The clean-URL form (?next=/orphans) is what the admin gate sends,
    and must round-trip cleanly."""
    resp = client.post(
        '/login?next=/orphans',
        data={
            'email': 'admin@example.com',
            'password': 'correct-horse-battery-staple',
        },
        headers=_on_admin_host(),
    )
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/orphans')


def test_safe_next_rejects_open_redirect_on_admin_host(client, admin_user):
    resp = client.post(
        '/login?next=https://evil.example.com/pwn',
        data={
            'email': 'admin@example.com',
            'password': 'correct-horse-battery-staple',
        },
        headers=_on_admin_host(),
    )
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/')
    assert 'evil.example.com' not in resp.headers['Location']


# ---------------------------------------------------------------------------
# Authenticated dashboard works end-to-end on the admin subdomain
# ---------------------------------------------------------------------------

def test_dashboard_renders_on_admin_subdomain_after_login(client, admin_user):
    client.post(
        '/login',
        data={
            'email': 'admin@example.com',
            'password': 'correct-horse-battery-staple',
        },
        headers=_on_admin_host(),
    )
    resp = client.get('/', headers=_on_admin_host({'Accept': 'text/html'}))
    assert resp.status_code == 200
    assert b'Admin Dashboard' in resp.data
    # Rendered hrefs should be the clean form (no /admin prefix).
    assert b'href="/orphans"' in resp.data
    assert b'href="/admin/orphans"' not in resp.data
