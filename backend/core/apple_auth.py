"""Sign in with Apple server-to-server helpers.

App Store Review Guideline 5.1.1(v) requires apps that offer "Sign in with
Apple" to revoke the Apple grant when a user deletes their account. To do that
we need an Apple-issued *refresh token*, which we obtain by exchanging the
one-time ``authorization_code`` the client receives at sign-in. At account
deletion we POST that refresh token to Apple's revocation endpoint.

Both operations authenticate with a short-lived ``client_secret`` JWT signed
(ES256) with a Sign in with Apple ``.p8`` private key.

Key configuration
-----------------
Sign in with Apple uses a DIFFERENT Apple key than the Apple Music Feed
importer (which already owns ``APPLE_KEY_ID`` / ``APPLE_PRIVATE_KEY_PATH`` /
``APPLE_TEAM_ID`` for the Media API). To avoid clobbering that, this module
reads its own env vars:

- ``APPLE_SIGNIN_KEY_ID``          – Key ID of the Sign in with Apple key
- ``APPLE_SIGNIN_PRIVATE_KEY_PATH``– Path to the ``.p8`` private key file
- ``APPLE_SIGNIN_TEAM_ID``         – Apple Developer Team ID (falls back to
                                     ``APPLE_TEAM_ID``, which is the same team)

The ``client_id`` for the exchange/revoke is the app's bundle identifier (the
``aud`` claim Apple put on the identity token), e.g. ``com.approachnote.ios`` or
``com.approachnote.mac``. These are the values already configured in
``APPLE_BUNDLE_IDS``.

Everything here is best-effort from the caller's perspective: failures raise
``AppleAuthError`` so the login/delete handlers can log and continue rather
than block the user.
"""

import logging
import os
import time

import jwt

from core.http_client import make_session

logger = logging.getLogger(__name__)

APPLE_TOKEN_URL = 'https://appleid.apple.com/auth/token'
APPLE_REVOKE_URL = 'https://appleid.apple.com/auth/revoke'
APPLE_AUDIENCE = 'https://appleid.apple.com'

# client_secret JWTs may be valid for up to 6 months; we mint a fresh one per
# request with a short lifetime, which is simplest and avoids caching/rotation.
_CLIENT_SECRET_TTL_SECONDS = 5 * 60


class AppleAuthError(Exception):
    """Raised when an Apple token exchange/revoke call cannot be completed."""


def _team_id() -> str | None:
    return os.environ.get('APPLE_SIGNIN_TEAM_ID') or os.environ.get('APPLE_TEAM_ID')


def _key_id() -> str | None:
    return os.environ.get('APPLE_SIGNIN_KEY_ID')


def _private_key_path() -> str | None:
    return os.environ.get('APPLE_SIGNIN_PRIVATE_KEY_PATH')


def is_configured() -> bool:
    """True when all credentials needed to call Apple's token API are present.

    Lets callers skip the exchange/revoke quietly (logging a warning) when the
    deployment hasn't been given Sign in with Apple server credentials yet,
    rather than raising on every Apple login.
    """
    path = _private_key_path()
    return bool(_team_id() and _key_id() and path and os.path.exists(path))


def _load_private_key() -> str:
    path = _private_key_path()
    if not path:
        raise AppleAuthError('APPLE_SIGNIN_PRIVATE_KEY_PATH not set')
    try:
        with open(path, 'r') as f:
            return f.read()
    except OSError as e:
        raise AppleAuthError(f'Failed to read Apple private key: {e}') from e


def generate_client_secret(client_id: str) -> str:
    """Mint a short-lived ES256 ``client_secret`` JWT for ``client_id``.

    ``client_id`` is the app bundle identifier the grant belongs to. The JWT's
    ``sub`` is that client_id and ``aud`` is Apple's token endpoint; Apple ties
    the resulting access to the matching app, so revoking a token requires the
    same client_id it was issued under.
    """
    team_id = _team_id()
    key_id = _key_id()
    if not team_id or not key_id:
        raise AppleAuthError(
            'Apple Sign-In not configured (missing APPLE_SIGNIN_TEAM_ID/'
            'APPLE_TEAM_ID or APPLE_SIGNIN_KEY_ID)'
        )

    now = int(time.time())
    payload = {
        'iss': team_id,
        'iat': now,
        'exp': now + _CLIENT_SECRET_TTL_SECONDS,
        'aud': APPLE_AUDIENCE,
        'sub': client_id,
    }
    headers = {'alg': 'ES256', 'kid': key_id, 'typ': 'JWT'}

    try:
        return jwt.encode(
            payload,
            _load_private_key(),
            algorithm='ES256',
            headers=headers,
        )
    except Exception as e:  # jwt/crypto errors
        raise AppleAuthError(f'Failed to sign Apple client_secret: {e}') from e


def exchange_code_for_refresh_token(authorization_code: str, client_id: str) -> str:
    """Exchange a Sign in with Apple ``authorization_code`` for a refresh token.

    Args:
        authorization_code: The one-time code from
            ``ASAuthorizationAppleIDCredential.authorizationCode``.
        client_id: The bundle ID the code was issued for (the identity token's
            ``aud`` claim).

    Returns:
        Apple's long-lived ``refresh_token``.

    Raises:
        AppleAuthError: on any HTTP/JSON failure or if Apple returns no
            refresh token.
    """
    client_secret = generate_client_secret(client_id)
    data = {
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'client_id': client_id,
        'client_secret': client_secret,
    }

    try:
        # accept_json=False: this is a form POST, not a JSON-bodied request, and
        # Apple replies application/json regardless of our Accept header.
        with make_session(accept_json=False) as session:
            resp = session.post(APPLE_TOKEN_URL, data=data, timeout=10)
    except Exception as e:
        raise AppleAuthError(f'Apple token request failed: {e}') from e

    if resp.status_code != 200:
        raise AppleAuthError(
            f'Apple token exchange returned {resp.status_code}: {resp.text[:300]}'
        )

    try:
        body = resp.json()
    except ValueError as e:
        raise AppleAuthError(f'Apple token response was not JSON: {e}') from e

    refresh_token = body.get('refresh_token')
    if not refresh_token:
        raise AppleAuthError(
            f"Apple token response had no refresh_token (keys: {sorted(body)})"
        )
    return refresh_token


def revoke_refresh_token(refresh_token: str, client_ids: list[str]) -> bool:
    """Revoke an Apple grant given its refresh token.

    The refresh token was issued under one specific ``client_id`` (bundle ID),
    but we don't persist which one. Since there are only a couple of bundle IDs
    (iOS + Mac), we try each until Apple accepts one. Apple returns HTTP 200 on
    a successful revoke (including re-revoking an already-revoked token); a
    mismatched client_id returns 400 ``invalid_client``/``invalid_request``.

    Args:
        refresh_token: The stored ``users.apple_refresh_token``.
        client_ids: Candidate bundle IDs to attempt (typically APPLE_BUNDLE_IDS).

    Returns:
        True if Apple accepted the revocation for some client_id, else False.

    Raises:
        AppleAuthError: only if not configured or no client_ids were provided.
            Per-attempt HTTP failures are caught and logged, returning False, so
            the caller (account deletion) is never blocked.
    """
    if not client_ids:
        raise AppleAuthError('No client_ids provided for Apple revocation')

    last_detail = ''
    for client_id in client_ids:
        try:
            client_secret = generate_client_secret(client_id)
            data = {
                'token': refresh_token,
                'token_type_hint': 'refresh_token',
                'client_id': client_id,
                'client_secret': client_secret,
            }
            with make_session(accept_json=False) as session:
                resp = session.post(APPLE_REVOKE_URL, data=data, timeout=10)
        except Exception as e:
            last_detail = f'{client_id}: request error {e}'
            logger.warning("Apple revoke attempt failed (%s)", last_detail)
            continue

        if resp.status_code == 200:
            logger.info("Apple grant revoked via client_id %s", client_id)
            return True

        last_detail = f'{client_id}: HTTP {resp.status_code} {resp.text[:200]}'
        logger.warning("Apple revoke rejected (%s)", last_detail)

    logger.warning("Apple revoke failed for all client_ids (last: %s)", last_detail)
    return False
