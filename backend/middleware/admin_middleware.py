"""
Admin-only gate for /admin/* pages and their JSON sub-endpoints.

The iOS/Mac apps authenticate via `Authorization: Bearer <access_token>`.
Browsers hitting /admin/* use a longer-lived admin-session JWT stored in an
HttpOnly cookie set by /admin/login.

This module exposes `check_admin_or_respond()`, intended to be wired into an
`@app.before_request` in backend/app.py. It covers EVERY URL under /admin/
regardless of which blueprint registered the route (see backend/app.py for
why per-blueprint gating is insufficient).

On authorized requests:
- `g.current_user` is populated.
- `g.admin_auth_source` is 'cookie' or 'bearer'.

On unauthorized requests:
- HTML-preferring requests receive a 302 to /admin/login?next=<path>.
- JSON-preferring requests receive a 401/403 JSON response.

CSRF protection is a double-submit cookie: the admin_csrf cookie (non-HttpOnly,
SameSite=Lax) must match the `X-CSRF-Token` header or `csrf_token` form field
on state-changing methods when the request authenticated via cookie. Bearer-
authenticated requests skip the CSRF check since they can't be mounted from a
victim browser session.
"""

import os
import logging
import secrets
from urllib.parse import quote, urlparse

from flask import request, jsonify, redirect, g, make_response

from core.auth_utils import (
    ADMIN_SESSION_COOKIE,
    ADMIN_CSRF_COOKIE,
    decode_token,
)
from db_utils import get_db_connection
from middleware.admin_subdomain import admin_url, is_admin_subdomain


logger = logging.getLogger(__name__)


# Paths under /admin that do NOT require admin auth. Keep this minimal.
_EXEMPT_PATHS = frozenset({
    '/admin/login',
    '/admin/logout',
})


def _wants_json() -> bool:
    """True when the client is a JSON API consumer rather than a browser.

    Heuristic: any request that explicitly accepts JSON but not HTML, or one
    that sends JSON/form body. XMLHttpRequest is also a signal. Browsers
    loading pages send Accept: text/html,... so they fall through to HTML.
    """
    accept = request.headers.get('Accept', '')
    if 'application/json' in accept and 'text/html' not in accept:
        return True
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return True
    if (request.is_json
            or request.content_type == 'application/json'):
        return True
    return False


def _login_redirect():
    """302 the user to the login page with a sanitised `next` target.

    Internal paths always look like `/admin/...` because the WSGI
    subdomain middleware injects that prefix before routing. We pass the
    internal form through as the `next` value so it round-trips cleanly
    regardless of which host the user came in on; admin_url() then maps
    both the login URL and the next= target to the right browser-facing
    shape.
    """
    target = request.full_path if request.query_string else request.path
    # Strip a trailing '?' that Flask appends when query_string is empty but
    # full_path is used. Keeps the URL clean.
    if target.endswith('?'):
        target = target[:-1]
    path_only = target.split('?', 1)[0]
    if path_only != '/admin' and not path_only.startswith('/admin/'):
        target = '/admin/'
    login_url = admin_url('/admin/login')
    return redirect(f"{login_url}?next={quote(target, safe='/')}", code=302)


def _json_error(status: int, message: str):
    resp = jsonify({'error': message})
    resp.status_code = status
    return resp


def _unauthorized():
    if _wants_json():
        return _json_error(401, 'Authentication required')
    return _login_redirect()


def _forbidden(message: str = 'Admin access required'):
    if _wants_json():
        return _json_error(403, message)
    # A logged-in non-admin still deserves a human page; send them to login.
    return _login_redirect()


def _load_user(user_id: str):
    """Return the user dict if active + unlocked + admin, else None."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, email, display_name, is_active, account_locked, is_admin
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            user = cur.fetchone()

    if not user or not user['is_active'] or user['account_locked']:
        return None
    if not user['is_admin']:
        return None
    return user


def _try_bearer():
    """Return (user, 'bearer') if Authorization header carries a valid admin
    access token, else (None, None). Never raises."""
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return None, None

    parts = auth_header.split(' ')
    if len(parts) != 2 or parts[0] != 'Bearer':
        return None, None

    try:
        payload = decode_token(parts[1])
    except ValueError:
        return None, None

    if payload.get('type') != 'access':
        return None, None

    user_id = payload.get('user_id')
    if not user_id:
        return None, None

    user = _load_user(user_id)
    return (user, 'bearer') if user else (None, None)


def _try_cookie():
    """Return (user, 'cookie') if admin_session cookie is valid, else
    (None, None)."""
    token = request.cookies.get(ADMIN_SESSION_COOKIE)
    if not token:
        return None, None

    try:
        payload = decode_token(token)
    except ValueError:
        return None, None

    if payload.get('type') != 'admin_session':
        return None, None

    user_id = payload.get('user_id')
    if not user_id:
        return None, None

    user = _load_user(user_id)
    return (user, 'cookie') if user else (None, None)


_CSRF_SAFE_METHODS = frozenset({'GET', 'HEAD', 'OPTIONS'})


def _csrf_ok() -> bool:
    """Double-submit-cookie check. Runs only for cookie-auth state-changing
    requests. Compares the admin_csrf cookie against the X-CSRF-Token header
    or csrf_token form field. Constant-time compare."""
    cookie_tok = request.cookies.get(ADMIN_CSRF_COOKIE)
    if not cookie_tok:
        return False
    submitted = (
        request.headers.get('X-CSRF-Token')
        or request.form.get('csrf_token')
    )
    if not submitted:
        return False
    return secrets.compare_digest(cookie_tok, submitted)


def check_admin_or_respond():
    """
    Inspect the current request and either allow it through (return None) or
    short-circuit with a Response.

    Meant to be called from an `@app.before_request` hook for any path under
    /admin/ other than the login/logout endpoints.
    """
    path = request.path

    if path in _EXEMPT_PATHS:
        return None

    # CORS preflights never carry credentials; let them through untouched.
    if request.method == 'OPTIONS':
        return None

    # Prefer bearer (ops scripts) over cookie so a stale cookie doesn't mask a
    # fresh token when both are present.
    user, source = _try_bearer()
    if not user:
        user, source = _try_cookie()

    if not user:
        return _unauthorized()

    # CSRF: double-submit token required for cookie-authenticated mutations.
    if source == 'cookie' and request.method not in _CSRF_SAFE_METHODS:
        if not _csrf_ok():
            logger.warning(
                "admin CSRF check failed: user=%s path=%s method=%s",
                user['id'], path, request.method,
            )
            return _forbidden('CSRF token missing or invalid')

    g.current_user = user
    g.admin_auth_source = source

    logger.info(
        "admin request: user=%s email=%s method=%s path=%s source=%s",
        user['id'], user['email'], request.method, path, source,
    )
    return None


def cookie_secure() -> bool:
    """Whether to mark admin cookies Secure. False in local dev so browsers
    actually accept them over http://localhost tooling."""
    env = os.getenv('FLASK_ENV', '').lower()
    if env == 'development':
        return False
    # request may not be available in some contexts; guard it.
    try:
        host = request.host.split(':')[0].lower()
        if host in ('localhost', '127.0.0.1'):
            return False
    except RuntimeError:
        pass
    return True


def generate_csrf_token() -> str:
    """Fresh 32-byte URL-safe token for the admin_csrf double-submit cookie."""
    return secrets.token_urlsafe(32)
