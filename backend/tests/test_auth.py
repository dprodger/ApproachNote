"""
Auth integration tests.

Hits the Flask test client end-to-end (HTTP → handler → real Postgres).
Email sending and rate limiting are stubbed/disabled via autouse fixtures
in conftest.py. External OAuth (Google, Apple) endpoints and the password
reset flow are deliberately out of scope for this stage and tracked in
follow-up issues.
"""

# ----------------------------------------------------------------------------
# /auth/register
# ----------------------------------------------------------------------------

def test_register_creates_user_and_returns_tokens(client, db):
    resp = client.post(
        "/v1/auth/register",
        json={
            "email": "alice@example.com",
            "password": "correct-horse-battery-staple",
            "display_name": "Alice",
        },
    )
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["user"]["email"] == "alice@example.com"
    assert body["user"]["display_name"] == "Alice"
    assert body["user"]["id"]
    assert body["access_token"]
    assert body["refresh_token"]

    # Verify the row landed in the DB.
    with db.cursor() as cur:
        cur.execute("SELECT email, display_name FROM users WHERE email = %s",
                    ("alice@example.com",))
        row = cur.fetchone()
    assert row == ("alice@example.com", "Alice")


def test_register_rejects_short_password(client):
    resp = client.post(
        "/v1/auth/register",
        json={"email": "shorty@example.com", "password": "1234567"},
    )
    assert resp.status_code == 400
    assert "8 characters" in resp.get_json()["error"]


def test_register_rejects_invalid_email(client):
    resp = client.post(
        "/v1/auth/register",
        json={"email": "notanemail", "password": "longenough123"},
    )
    assert resp.status_code == 400
    assert "Invalid email format" in resp.get_json()["error"]


def test_register_rejects_duplicate_email(client, register_user):
    register_user(email="dup@example.com")
    resp = client.post(
        "/v1/auth/register",
        json={"email": "dup@example.com", "password": "another-pass-123"},
    )
    assert resp.status_code == 409
    assert "already registered" in resp.get_json()["error"]


def test_register_succeeds_when_welcome_email_send_fails(client, mocker):
    """Registration must NOT fail if SendGrid raises — email is best-effort."""
    mocker.patch("routes.auth.send_welcome_email",
                 side_effect=RuntimeError("SendGrid is on fire"))
    resp = client.post(
        "/v1/auth/register",
        json={"email": "resilient@example.com", "password": "password1234"},
    )
    assert resp.status_code == 201
    assert resp.get_json()["user"]["email"] == "resilient@example.com"


# ----------------------------------------------------------------------------
# /auth/login
# ----------------------------------------------------------------------------

def test_login_with_correct_password_returns_tokens(client, register_user):
    register_user(email="bob@example.com", password="password1234")
    resp = client.post(
        "/v1/auth/login",
        json={"email": "bob@example.com", "password": "password1234"},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["user"]["email"] == "bob@example.com"
    assert body["access_token"]
    assert body["refresh_token"]


def test_login_with_wrong_password_returns_401_and_increments_failed_attempts(
    client, register_user, db
):
    register_user(email="carol@example.com", password="password1234")
    resp = client.post(
        "/v1/auth/login",
        json={"email": "carol@example.com", "password": "wrong-password"},
    )
    assert resp.status_code == 401
    assert resp.get_json()["error"] == "Invalid credentials"

    with db.cursor() as cur:
        cur.execute(
            "SELECT failed_login_attempts FROM users WHERE email = %s",
            ("carol@example.com",),
        )
        (failed_attempts,) = cur.fetchone()
    assert failed_attempts == 1


def test_login_locks_account_after_repeated_failures(client, register_user):
    """
    The handler updates ``account_locked = (failed_login_attempts >= 4)`` on
    each failed login. So:
      attempts 1-4: 401 invalid credentials, account unlocked
      attempt 5: still 401 invalid credentials, but the row is now locked
      attempt 6: 401 'Account is locked'
    """
    register_user(email="dave@example.com", password="password1234")

    # Five failed attempts. The handler still returns "Invalid credentials"
    # for all of them (the lock flips on attempt 5 but the lock check at
    # the top of the handler ran on attempt 5 BEFORE the update).
    for _ in range(5):
        resp = client.post(
            "/v1/auth/login",
            json={"email": "dave@example.com", "password": "wrong"},
        )
        assert resp.status_code == 401
        assert resp.get_json()["error"] == "Invalid credentials"

    # Sixth attempt sees the row already locked.
    resp = client.post(
        "/v1/auth/login",
        json={"email": "dave@example.com", "password": "wrong"},
    )
    assert resp.status_code == 401
    assert "locked" in resp.get_json()["error"].lower()


# ----------------------------------------------------------------------------
# /auth/refresh-token
# ----------------------------------------------------------------------------

def test_refresh_token_returns_new_access_token(client, auth_headers):
    resp = client.post(
        "/v1/auth/refresh-token",
        json={"refresh_token": auth_headers.refresh_token},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["access_token"]
    assert body["refresh_token"]
    # The handler rotates the refresh token, so it should differ from the old one.
    assert body["refresh_token"] != auth_headers.refresh_token


def test_refresh_token_rejects_garbage_token(client):
    resp = client.post(
        "/v1/auth/refresh-token",
        json={"refresh_token": "this-is-not-a-jwt"},
    )
    assert resp.status_code == 401


def test_refresh_token_rejects_missing_token(client):
    resp = client.post("/v1/auth/refresh-token", json={})
    assert resp.status_code == 400


# ----------------------------------------------------------------------------
# /auth/me
# ----------------------------------------------------------------------------

def test_get_current_user_requires_auth_header(client):
    resp = client.get("/v1/auth/me")
    assert resp.status_code == 401


def test_get_current_user_rejects_malformed_header(client):
    resp = client.get("/v1/auth/me", headers={"Authorization": "NotBearer xyz"})
    assert resp.status_code == 401


def test_get_current_user_returns_user_with_valid_token(client, auth_headers):
    resp = client.get("/v1/auth/me", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["id"] == auth_headers.user["id"]
    assert body["email"] == auth_headers.user["email"]


# ----------------------------------------------------------------------------
# /auth/logout
# ----------------------------------------------------------------------------

def test_logout_revokes_refresh_token(client, auth_headers):
    # Logout, supplying the refresh token.
    resp = client.post(
        "/v1/auth/logout",
        json={"refresh_token": auth_headers.refresh_token},
        headers=auth_headers,
    )
    assert resp.status_code == 200

    # That refresh token should no longer be usable.
    resp2 = client.post(
        "/v1/auth/refresh-token",
        json={"refresh_token": auth_headers.refresh_token},
    )
    assert resp2.status_code == 401


# ----------------------------------------------------------------------------
# /auth/delete-account
# ----------------------------------------------------------------------------

def test_delete_account_requires_auth_header(client):
    resp = client.delete("/v1/auth/delete-account")
    assert resp.status_code == 401


def test_delete_account_removes_user_and_revokes_tokens(client, auth_headers, db):
    user_id = auth_headers.user["id"]

    resp = client.delete("/v1/auth/delete-account", headers=auth_headers)
    assert resp.status_code == 200

    # The user row is gone.
    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        assert cur.fetchone() is None

    # Refresh tokens cascaded away, so the refresh token no longer works.
    resp2 = client.post(
        "/v1/auth/refresh-token",
        json={"refresh_token": auth_headers.refresh_token},
    )
    assert resp2.status_code == 401

    # The access token can't be used to reach a protected endpoint anymore
    # (the user it points at no longer exists).
    resp3 = client.get("/v1/auth/me", headers=auth_headers)
    assert resp3.status_code == 401


# ----------------------------------------------------------------------------
# Sign in with Apple — token revocation wiring (App Store 5.1.1(v))
#
# These exercise the integration between /auth/apple, /auth/delete-account and
# core.apple_auth. Apple's identity-token verification and the server-to-server
# HTTP calls are mocked; the pure crypto/HTTP plumbing is unit-tested in
# test_apple_auth.py.
# ----------------------------------------------------------------------------

def _mock_apple_token(mocker, *, sub, email=None, aud="com.approachnote.ios",
                      email_verified=True):
    """Stub Apple identity-token verification so /auth/apple accepts a fake
    token and returns the given claims."""
    mocker.patch("routes.auth._apple_jwk_client.get_signing_key_from_jwt")
    claims = {"sub": sub, "aud": aud, "email_verified": email_verified}
    if email is not None:
        claims["email"] = email
    mocker.patch("routes.auth.jwt.decode", return_value=claims)
    mocker.patch("routes.auth.APPLE_BUNDLE_IDS",
                 ["com.approachnote.ios", "com.approachnote.mac"])


def test_apple_login_stores_refresh_token_from_authorization_code(client, db, mocker):
    _mock_apple_token(mocker, sub="apple-sub-1", email="applefan@example.com")
    mocker.patch("routes.auth.apple_auth.is_configured", return_value=True)
    exchange = mocker.patch(
        "routes.auth.apple_auth.exchange_code_for_refresh_token",
        return_value="apple-rt-stored",
    )

    resp = client.post(
        "/v1/auth/apple",
        json={
            "identity_token": "fake-token",
            "authorization_code": "one-time-code",
        },
    )
    assert resp.status_code == 200, resp.get_json()

    # The code was exchanged with the bundle ID from the token's aud claim.
    exchange.assert_called_once_with("one-time-code", "com.approachnote.ios")

    # The refresh token landed on the user row.
    with db.cursor() as cur:
        cur.execute(
            "SELECT apple_refresh_token FROM users WHERE apple_id = %s",
            ("apple-sub-1",),
        )
        (stored,) = cur.fetchone()
    assert stored == "apple-rt-stored"


def test_apple_login_without_code_stores_no_refresh_token(client, db, mocker):
    _mock_apple_token(mocker, sub="apple-sub-2", email="nocode@example.com")
    exchange = mocker.patch("routes.auth.apple_auth.exchange_code_for_refresh_token")

    resp = client.post(
        "/v1/auth/apple",
        json={"identity_token": "fake-token"},
    )
    assert resp.status_code == 200, resp.get_json()
    exchange.assert_not_called()

    with db.cursor() as cur:
        cur.execute(
            "SELECT apple_refresh_token FROM users WHERE apple_id = %s",
            ("apple-sub-2",),
        )
        (stored,) = cur.fetchone()
    assert stored is None


def test_apple_login_survives_code_exchange_failure(client, db, mocker):
    """A failed code exchange must not block sign-in; the user is still
    created, just without a stored refresh token."""
    from core import apple_auth

    _mock_apple_token(mocker, sub="apple-sub-3", email="exchangefail@example.com")
    mocker.patch("routes.auth.apple_auth.is_configured", return_value=True)
    mocker.patch(
        "routes.auth.apple_auth.exchange_code_for_refresh_token",
        side_effect=apple_auth.AppleAuthError("Apple said no"),
    )

    resp = client.post(
        "/v1/auth/apple",
        json={"identity_token": "fake-token", "authorization_code": "code"},
    )
    assert resp.status_code == 200, resp.get_json()

    with db.cursor() as cur:
        cur.execute(
            "SELECT apple_refresh_token FROM users WHERE apple_id = %s",
            ("apple-sub-3",),
        )
        (stored,) = cur.fetchone()
    assert stored is None


def test_delete_account_revokes_apple_grant(client, db, auth_headers, mocker):
    user_id = auth_headers.user["id"]

    # Give the user a stored Apple refresh token.
    with db.cursor() as cur:
        cur.execute(
            "UPDATE users SET apple_refresh_token = %s WHERE id = %s",
            ("apple-rt-to-revoke", user_id),
        )
    db.commit()

    mocker.patch("routes.auth.apple_auth.is_configured", return_value=True)
    mocker.patch("routes.auth.APPLE_BUNDLE_IDS",
                 ["com.approachnote.ios", "com.approachnote.mac"])
    revoke = mocker.patch(
        "routes.auth.apple_auth.revoke_refresh_token", return_value=True
    )

    resp = client.delete("/v1/auth/delete-account", headers=auth_headers)
    assert resp.status_code == 200

    revoke.assert_called_once_with(
        "apple-rt-to-revoke", ["com.approachnote.ios", "com.approachnote.mac"]
    )

    # The user is still deleted.
    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        assert cur.fetchone() is None


def test_delete_account_succeeds_when_revoke_fails(client, db, auth_headers, mocker):
    """Revocation is best-effort: a thrown error must not block deletion."""
    user_id = auth_headers.user["id"]

    with db.cursor() as cur:
        cur.execute(
            "UPDATE users SET apple_refresh_token = %s WHERE id = %s",
            ("apple-rt-to-revoke", user_id),
        )
    db.commit()

    mocker.patch("routes.auth.apple_auth.is_configured", return_value=True)
    mocker.patch(
        "routes.auth.apple_auth.revoke_refresh_token",
        side_effect=RuntimeError("Apple unreachable"),
    )

    resp = client.delete("/v1/auth/delete-account", headers=auth_headers)
    assert resp.status_code == 200

    with db.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE id = %s", (user_id,))
        assert cur.fetchone() is None


def test_delete_account_without_apple_token_does_not_revoke(client, auth_headers, mocker):
    """A non-Apple user (no stored refresh token) deletes without calling
    Apple at all."""
    revoke = mocker.patch("routes.auth.apple_auth.revoke_refresh_token")

    resp = client.delete("/v1/auth/delete-account", headers=auth_headers)
    assert resp.status_code == 200
    revoke.assert_not_called()
