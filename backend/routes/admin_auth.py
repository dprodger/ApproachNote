"""
Admin login/logout routes.

These endpoints live on `admin_bp` so they share the /admin URL prefix, but
they are listed in `middleware.admin_middleware._EXEMPT_PATHS` so the admin
gate does NOT intercept them.

Login flow: the browser POSTs one of three bodies to /admin/login:

1. form-encoded `email` + `password`
2. JSON `{"id_token": "..."}` for Google Sign-In (web)
3. JSON `{"identity_token": "..."}` for Sign in with Apple JS

The endpoint validates credentials, confirms `users.is_admin = true`, sets
the `admin_session` JWT cookie + the `admin_csrf` double-submit cookie, and
redirects (for form POSTs) or returns JSON with a redirect target (for JSON
POSTs).

Google token acceptance: `verify_oauth2_token` is called with `audience=[
GOOGLE_CLIENT_ID, GOOGLE_CLIENT_ID_WEB]` — the library accepts a list and
matches any member, so we can share the handler between native-app tokens
and web-OAuth tokens.

Apple token acceptance: reuses the same `_apple_jwk_client` and list of
accepted `aud` values as routes/auth.py, meaning the Sign-in-with-Apple
Services ID must be added to the `APPLE_BUNDLE_IDS` env var.
"""

import os
import logging
from urllib.parse import urlparse

from flask import (
    request, jsonify, render_template, redirect, make_response,
)

from db_utils import get_db_connection
from core.auth_utils import (
    verify_password,
    generate_admin_session_token,
    ADMIN_SESSION_COOKIE,
    ADMIN_SESSION_EXPIRY,
    ADMIN_CSRF_COOKIE,
)
from middleware.admin_middleware import cookie_secure, generate_csrf_token
from middleware.admin_subdomain import admin_url, is_admin_subdomain
from rate_limit import limiter, LOGIN_LIMIT

# Google OAuth: accept tokens from both the iOS/Mac client and the web client.
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

# Sign in with Apple: reuse the same JWKS + audience list as routes/auth.py.
import jwt
from jwt import PyJWKClient, InvalidTokenError, PyJWKClientError


logger = logging.getLogger(__name__)

# Registered onto admin_bp by importing this module; the blueprint itself is
# defined in routes/admin.py so the URL prefix stays consistent.
from routes.admin import admin_bp  # noqa: E402


GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_ID_WEB = os.getenv('GOOGLE_CLIENT_ID_WEB')

# The Sign-in-with-Apple Services ID used by the admin login page's AppleID
# JS init. Must be a member of APPLE_BUNDLE_IDS so the resulting id_token
# passes audience validation server-side.
APPLE_SERVICES_ID = os.getenv('APPLE_SERVICES_ID')

APPLE_ISSUER = 'https://appleid.apple.com'
APPLE_JWKS_URL = 'https://appleid.apple.com/auth/keys'
APPLE_BUNDLE_IDS = [
    bid.strip() for bid in os.getenv('APPLE_BUNDLE_IDS', '').split(',') if bid.strip()
]
_apple_jwk_client = PyJWKClient(APPLE_JWKS_URL, cache_keys=True)


def _safe_next(next_param: str | None) -> str:
    """Validate a `next` redirect target and return the browser-facing URL
    for the current host.

    Accepts either form coming in:
      - `/admin/...` (legacy / API host / dev — what the templates emit there)
      - `/...` without the prefix (admin.approachnote.com — what the WSGI
        middleware leaves in the address bar)

    Anything that fails validation collapses to the dashboard root for the
    current host."""
    fallback = admin_url('/admin/')
    if not next_param:
        return fallback
    parsed = urlparse(next_param)
    if parsed.scheme or parsed.netloc:
        return fallback
    path = parsed.path
    # Normalise to the internal representation (`/admin/...`) for validation,
    # then map back out via admin_url for the response.
    if path == '/admin' or path.startswith('/admin/'):
        internal_path = path
    elif is_admin_subdomain() and (path == '/' or path.startswith('/')):
        internal_path = '/admin' if path == '/' else '/admin' + path
    else:
        return fallback
    if internal_path != '/admin' and not internal_path.startswith('/admin/'):
        return fallback
    external = admin_url(internal_path)
    if parsed.query:
        return f"{external}?{parsed.query}"
    return external


def _cookie_path() -> str:
    """The admin cookies are scoped to the path under which the admin UI is
    mounted on the current host:

      - admin.approachnote.com → '/' (everything on this host is admin)
      - api.approachnote.com / localhost → '/admin' (legacy/dev surface)

    The path must match what the BROWSER sees in the URL bar so the cookie
    actually gets sent on subsequent requests. On the admin subdomain the
    WSGI middleware rewrites `/foo` → `/admin/foo` server-side, but the
    browser only ever sees `/foo` — hence path '/'.
    """
    return '/' if is_admin_subdomain() else '/admin'


def _set_admin_cookies(resp, user_id: str):
    """Set the admin_session + admin_csrf cookies on an outgoing response."""
    session_token = generate_admin_session_token(user_id)
    csrf_token = generate_csrf_token()
    secure = cookie_secure()
    max_age = int(ADMIN_SESSION_EXPIRY.total_seconds())
    path = _cookie_path()

    resp.set_cookie(
        ADMIN_SESSION_COOKIE,
        session_token,
        max_age=max_age,
        secure=secure,
        httponly=True,
        samesite='Lax',
        path=path,
    )
    resp.set_cookie(
        ADMIN_CSRF_COOKIE,
        csrf_token,
        max_age=max_age,
        secure=secure,
        httponly=False,  # intentionally readable by JS so fetch() can echo it
        samesite='Lax',
        path=path,
    )
    return resp


def _clear_admin_cookies(resp):
    secure = cookie_secure()
    path = _cookie_path()
    resp.set_cookie(
        ADMIN_SESSION_COOKIE, '', expires=0, max_age=0,
        secure=secure, httponly=True, samesite='Lax', path=path,
    )
    resp.set_cookie(
        ADMIN_CSRF_COOKIE, '', expires=0, max_age=0,
        secure=secure, httponly=False, samesite='Lax', path=path,
    )
    return resp


def _is_admin_active(user: dict) -> bool:
    return bool(user
                and user.get('is_active')
                and not user.get('account_locked')
                and user.get('is_admin'))


def _fetch_user(cur, **where) -> dict | None:
    if 'email' in where:
        cur.execute(
            """
            SELECT id, email, display_name, password_hash, google_id, apple_id,
                   is_active, account_locked, is_admin, failed_login_attempts
            FROM users
            WHERE email = %s
            """,
            (where['email'],),
        )
    elif 'google_id' in where:
        cur.execute(
            """
            SELECT id, email, display_name, password_hash, google_id, apple_id,
                   is_active, account_locked, is_admin, failed_login_attempts
            FROM users
            WHERE google_id = %s OR email = %s
            """,
            (where['google_id'], where.get('email_fallback')),
        )
    elif 'apple_id' in where:
        cur.execute(
            """
            SELECT id, email, display_name, password_hash, google_id, apple_id,
                   is_active, account_locked, is_admin, failed_login_attempts
            FROM users
            WHERE apple_id = %s
            """,
            (where['apple_id'],),
        )
    else:
        return None
    return cur.fetchone()


def _respond_login_failed(wants_json: bool, status: int, message: str):
    """Unified failure response so email/password, Google, and Apple all emit
    the same shape. JSON gets a structured error; form submissions re-render
    the login page with the message."""
    if wants_json:
        return jsonify({'error': message}), status
    resp = make_response(render_template(
        'admin/login.html',
        error=message,
        next_target=_safe_next(request.args.get('next')),
        google_client_id=GOOGLE_CLIENT_ID_WEB,
        apple_services_id=APPLE_SERVICES_ID,
    ), status)
    return _ensure_csrf_cookie(resp)


def _ensure_csrf_cookie(resp):
    """Put a fresh admin_csrf cookie on the response if one isn't already
    present in the request. GET /admin/login uses this so the JS that posts
    Google/Apple tokens has something to echo back."""
    if request.cookies.get(ADMIN_CSRF_COOKIE):
        return resp
    resp.set_cookie(
        ADMIN_CSRF_COOKIE,
        generate_csrf_token(),
        max_age=int(ADMIN_SESSION_EXPIRY.total_seconds()),
        secure=cookie_secure(),
        httponly=False,
        samesite='Lax',
        path=_cookie_path(),
    )
    return resp


# ---------------------------------------------------------------------------
# GET /admin/login — render the form
# ---------------------------------------------------------------------------

@admin_bp.route('/login', methods=['GET'])
def admin_login_page():
    resp = make_response(render_template(
        'admin/login.html',
        next_target=_safe_next(request.args.get('next')),
        google_client_id=GOOGLE_CLIENT_ID_WEB,
        apple_services_id=APPLE_SERVICES_ID,
        error=None,
    ))
    return _ensure_csrf_cookie(resp)


# ---------------------------------------------------------------------------
# POST /admin/login — authenticate
# ---------------------------------------------------------------------------

@admin_bp.route('/login', methods=['POST'])
@limiter.limit(LOGIN_LIMIT)
def admin_login_submit():
    wants_json = request.is_json or request.headers.get('Accept', '').startswith('application/json')

    # Dispatch based on body shape.
    if request.is_json:
        data = request.get_json(silent=True) or {}
        if 'id_token' in data:
            return _login_google(data['id_token'], wants_json=True)
        if 'identity_token' in data:
            return _login_apple(data['identity_token'], wants_json=True)
        if 'email' in data and 'password' in data:
            return _login_password(
                email=data['email'], password=data['password'],
                wants_json=True,
            )
        return _respond_login_failed(True, 400, 'Missing credentials')

    # Form submission.
    email = request.form.get('email')
    password = request.form.get('password')
    if not email or not password:
        return _respond_login_failed(False, 400, 'Email and password required')
    return _login_password(email=email, password=password, wants_json=False)


def _login_password(*, email: str, password: str, wants_json: bool):
    """Email/password path — mirrors the bookkeeping in routes/auth.py:215-238
    (increment failed attempts, lock after 5, reset on success)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                user = _fetch_user(cur, email=email)
                if not user:
                    return _respond_login_failed(wants_json, 401, 'Invalid credentials')

                if not user['is_active']:
                    return _respond_login_failed(wants_json, 401, 'Account is inactive')
                if user['account_locked']:
                    return _respond_login_failed(wants_json, 401, 'Account is locked')

                if not user['password_hash'] or not verify_password(password, user['password_hash']):
                    cur.execute(
                        """
                        UPDATE users
                        SET failed_login_attempts = failed_login_attempts + 1,
                            last_failed_login_at = NOW(),
                            account_locked = CASE
                                WHEN failed_login_attempts >= 4 THEN true
                                ELSE false
                            END
                        WHERE id = %s
                        """,
                        (user['id'],),
                    )
                    conn.commit()
                    return _respond_login_failed(wants_json, 401, 'Invalid credentials')

                if not user['is_admin']:
                    # Don't bump failed_login_attempts — credentials were good.
                    # Just refuse admin access.
                    logger.warning(
                        "Admin login refused: user %s authenticated but is not admin",
                        user['email'],
                    )
                    return _respond_login_failed(wants_json, 403, 'Admin access required')

                cur.execute(
                    """
                    UPDATE users
                    SET failed_login_attempts = 0, last_login_at = NOW()
                    WHERE id = %s
                    """,
                    (user['id'],),
                )
                conn.commit()

                return _issue_session(user, wants_json=wants_json)
    except Exception:
        logger.exception("Admin password login error")
        return _respond_login_failed(wants_json, 500, 'Login failed')


def _login_google(id_token_str: str, *, wants_json: bool):
    if not (GOOGLE_CLIENT_ID or GOOGLE_CLIENT_ID_WEB):
        logger.error("Neither GOOGLE_CLIENT_ID nor GOOGLE_CLIENT_ID_WEB is set")
        return _respond_login_failed(wants_json, 500, 'Google auth not configured')

    # verify_oauth2_token accepts a list for the audience arg.
    audiences = [a for a in (GOOGLE_CLIENT_ID, GOOGLE_CLIENT_ID_WEB) if a]

    try:
        idinfo = google_id_token.verify_oauth2_token(
            id_token_str,
            google_requests.Request(),
            audiences,
        )
    except ValueError as e:
        logger.warning(f"Invalid Google token for admin login: {e}")
        return _respond_login_failed(wants_json, 401, 'Invalid Google token')

    google_id = idinfo['sub']
    email = idinfo.get('email')

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                user = _fetch_user(cur, google_id=google_id, email_fallback=email)
                if not user:
                    return _respond_login_failed(wants_json, 403, 'Admin access required')
                if not _is_admin_active(user):
                    return _respond_login_failed(wants_json, 403, 'Admin access required')
                cur.execute(
                    "UPDATE users SET last_login_at = NOW() WHERE id = %s",
                    (user['id'],),
                )
                conn.commit()
                return _issue_session(user, wants_json=wants_json)
    except Exception:
        logger.exception("Admin Google login error")
        return _respond_login_failed(wants_json, 500, 'Login failed')


def _login_apple(identity_token: str, *, wants_json: bool):
    if not APPLE_BUNDLE_IDS:
        logger.error("APPLE_BUNDLE_IDS not configured")
        return _respond_login_failed(wants_json, 500, 'Apple auth not configured')

    try:
        signing_key = _apple_jwk_client.get_signing_key_from_jwt(identity_token)
        claims = jwt.decode(
            identity_token,
            signing_key.key,
            algorithms=['RS256'],
            audience=APPLE_BUNDLE_IDS,
            issuer=APPLE_ISSUER,
        )
    except (InvalidTokenError, PyJWKClientError) as e:
        logger.warning(f"Invalid Apple token for admin login: {e}")
        return _respond_login_failed(wants_json, 401, 'Invalid Apple token')
    except Exception:
        logger.exception("Apple token verification error")
        return _respond_login_failed(wants_json, 500, 'Login failed')

    apple_id = claims.get('sub')
    if not apple_id:
        return _respond_login_failed(wants_json, 401, 'Invalid Apple token')

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                user = _fetch_user(cur, apple_id=apple_id)
                if not user:
                    return _respond_login_failed(wants_json, 403, 'Admin access required')
                if not _is_admin_active(user):
                    return _respond_login_failed(wants_json, 403, 'Admin access required')
                cur.execute(
                    "UPDATE users SET last_login_at = NOW() WHERE id = %s",
                    (user['id'],),
                )
                conn.commit()
                return _issue_session(user, wants_json=wants_json)
    except Exception:
        logger.exception("Admin Apple login error")
        return _respond_login_failed(wants_json, 500, 'Login failed')


def _issue_session(user: dict, *, wants_json: bool):
    """Build the success response, attach cookies, and redirect/JSON."""
    next_target = _safe_next(request.args.get('next') or request.form.get('next'))

    logger.info(f"admin login: user={user['id']} email={user['email']}")

    if wants_json:
        resp = jsonify({
            'ok': True,
            'next': next_target,
            'user': {
                'id': str(user['id']),
                'email': user['email'],
                'display_name': user['display_name'],
            },
        })
        resp.status_code = 200
    else:
        resp = redirect(next_target, code=302)

    return _set_admin_cookies(resp, user['id'])


# ---------------------------------------------------------------------------
# POST /admin/logout — clear cookies, redirect to login
# ---------------------------------------------------------------------------

@admin_bp.route('/logout', methods=['POST'])
def admin_logout():
    # CSRF is enforced by the gate for cookie-authed requests, but /logout is
    # exempt from the gate. We still require the CSRF header here as a
    # defence against cross-site forced-logout nuisance attacks.
    cookie_tok = request.cookies.get(ADMIN_CSRF_COOKIE)
    submitted = request.headers.get('X-CSRF-Token') or request.form.get('csrf_token')
    if cookie_tok and (not submitted or submitted != cookie_tok):
        return jsonify({'error': 'CSRF token missing or invalid'}), 403

    login_target = admin_url('/admin/login')
    if request.is_json or request.headers.get('Accept', '').startswith('application/json'):
        resp = jsonify({'ok': True, 'next': login_target})
    else:
        resp = redirect(login_target, code=302)
    return _clear_admin_cookies(resp)
