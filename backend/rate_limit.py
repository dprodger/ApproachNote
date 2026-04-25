"""
Rate limiting for authentication and password endpoints.

Provides a module-level ``limiter`` that is initialized against the Flask
app in ``app.py`` and used as a decorator on the endpoints that need it
(see ``routes/auth.py`` and ``routes/password.py``).

Design notes:

- Storage is in-memory by default. The backend runs under gunicorn with
  ``workers = 1`` (see ``gunicorn.conf.py``) so one shared in-memory
  counter is authoritative. If you ever scale to multiple workers or
  multiple instances, set ``RATELIMIT_STORAGE_URI=redis://...`` in the
  environment — Flask-Limiter will pick it up automatically and no code
  change is required.

- Rate limits are identified by real client IP. The backend runs behind
  Render's reverse proxy, so ``request.remote_addr`` by itself reports
  the proxy's IP. ``ProxyFix`` is installed in ``app.py`` with
  ``x_for=1`` to rewrite ``remote_addr`` based on the single trusted
  ``X-Forwarded-For`` entry Render writes. Flask-Limiter's default
  ``get_remote_address`` key function then sees the real client IP.

- Limits can be globally disabled by setting ``RATELIMIT_ENABLED=false``
  in the environment. Used for local development, CI, and load tests
  where tight limits would be noise rather than signal. Rate limiting
  is enabled by default in every other environment.

- The ``/auth/change-password`` endpoint is rate-limited per *user*
  (via the access-token subject) rather than per IP, so that a whole
  office behind one NAT can change passwords independently. All other
  endpoints are limited per IP because the caller is unauthenticated
  at the time of the call.
"""

import os
import logging

from flask import jsonify, request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

logger = logging.getLogger(__name__)


# ============================================================================
# LIMIT STRINGS
# ============================================================================
#
# These are the per-endpoint limit strings, exported so the route
# modules can import them. Flask-Limiter's decorator accepts either a
# literal string or a callable; using module-level constants lets the
# numbers be tweaked from one place without hunting through routes.
#
# Syntax: "<count> per <window>" — e.g. "10 per minute".

LOGIN_LIMIT = "10 per minute"
REGISTER_LIMIT = "5 per hour"
GOOGLE_LOGIN_LIMIT = "10 per minute"
APPLE_LOGIN_LIMIT = "10 per minute"
REFRESH_TOKEN_LIMIT = "30 per minute"
FORGOT_PASSWORD_LIMIT = "3 per hour"
RESET_PASSWORD_LIMIT = "10 per hour"
CHANGE_PASSWORD_LIMIT = "5 per hour"

# POST /api/recordings/batch — shell+hydrate pattern for the song recordings
# list. A typical SongDetailView generates ~5-15 batches per tap as the user
# scrolls through decades; 120/min gives a healthy headroom for that pattern
# while capping sustained abuse at ~2 requests/sec.
BATCH_RECORDINGS_LIMIT = "120 per minute"


# ============================================================================
# KEY FUNCTIONS
# ============================================================================

def _user_id_or_ip_key():
    """
    Key function for endpoints that are rate-limited per authenticated user.

    Decodes the access token from the ``Authorization`` header to extract
    the user id. We can't read ``flask.g.current_user`` here because the
    ``@require_auth`` decorator that populates it runs INSIDE the limiter's
    view-dispatch wrapper — so ``g`` is empty at the moment Flask-Limiter
    calls this function. Decoding the JWT directly sidesteps the ordering
    problem and is cheap (HS256 verify on a ~200-byte token).

    Falls back to client IP if the token is missing, malformed, expired,
    or not an access token. That matches the behavior of unauthenticated
    requests hitting per-IP-keyed endpoints.
    """
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:].strip()
        if token:
            try:
                # Lazy import: avoids a circular dependency at module load
                # time (auth_utils → ... → rate_limit).
                from core.auth_utils import decode_token
                payload = decode_token(token)
                if payload.get('type') == 'access' and payload.get('user_id'):
                    return f"user:{payload['user_id']}"
            except Exception:
                # Any decode failure (expired, bad signature, malformed) —
                # treat as unauthenticated and fall through to IP keying.
                pass
    return get_remote_address()


# ============================================================================
# LIMITER INSTANCE
# ============================================================================
#
# The limiter is created at import time but does NOT have an app attached
# yet. ``init_app()`` is called from ``app.py`` after the Flask app is
# created and after ProxyFix has been installed, so that the key function
# sees the real client IP from the very first request.

_enabled = os.environ.get('RATELIMIT_ENABLED', 'true').lower() != 'false'

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],          # No blanket default — we set limits per-route.
    storage_uri=os.environ.get('RATELIMIT_STORAGE_URI', 'memory://'),
    enabled=_enabled,
    headers_enabled=True,       # Emit X-RateLimit-* headers on each response,
                                # so well-behaved clients can back off voluntarily.
    strategy='fixed-window',    # Simplest and most predictable. Sliding-window
                                # would be overkill for our volume.
)


def init_rate_limiter(app):
    """
    Attach the limiter to the Flask app and install the 429 error handler.

    Must be called AFTER ProxyFix has been applied to the app, so that
    the key function's call to ``get_remote_address()`` returns the real
    client IP rather than the Render proxy's IP.
    """
    limiter.init_app(app)

    @app.errorhandler(429)
    def ratelimit_handler(e):
        # Keep the response shape consistent with the rest of the API
        # (`{"error": "..."}`) and include the limit description so a
        # legitimate client knows why they were throttled. The
        # description from Flask-Limiter looks like "10 per 1 minute";
        # safe to surface.
        logger.warning(
            f"Rate limit exceeded: {request.method} {request.path} "
            f"from {get_remote_address()} — {e.description}"
        )
        return jsonify({
            'error': 'Too many requests. Please slow down and try again.',
            'limit': str(e.description),
        }), 429

    if not _enabled:
        logger.warning(
            "Rate limiting is DISABLED (RATELIMIT_ENABLED=false). "
            "This is only appropriate for local development, CI, or load tests."
        )
    else:
        logger.info(
            "Rate limiting is enabled. Storage: "
            f"{os.environ.get('RATELIMIT_STORAGE_URI', 'memory://')}"
        )
