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
