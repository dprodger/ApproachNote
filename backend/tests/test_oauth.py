"""
OAuth integration tests for /auth/google and /auth/apple.

These hit the Flask test client end-to-end, but the remote token-verification
calls are stubbed: we never reach Google's tokeninfo endpoint or Apple's JWKS
endpoint. The handlers' DB writes and token-issuance paths run for real.

Stubs applied per test:
- ``routes.auth.google_id_token.verify_oauth2_token`` for /auth/google
- ``routes.auth._apple_jwk_client.get_signing_key_from_jwt`` and
  ``routes.auth.jwt.decode`` for /auth/apple
"""

import pytest
from jwt import InvalidSignatureError, InvalidAudienceError


# ----------------------------------------------------------------------------
# Per-module config: ensure the handlers think OAuth is configured.
#
# ``GOOGLE_CLIENT_ID`` and ``APPLE_BUNDLE_IDS`` are read at import time. The
# real test env doesn't have them, so we monkeypatch the module-level globals
# for every test in this file.
# ----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _configure_oauth(monkeypatch):
    monkeypatch.setattr("routes.auth.GOOGLE_CLIENT_ID", "test-google-client-id")
    monkeypatch.setattr("routes.auth.APPLE_BUNDLE_IDS", ["com.approachnote.test"])


# ----------------------------------------------------------------------------
# /auth/google
# ----------------------------------------------------------------------------

def test_google_login_creates_new_user(client, db, mocker):
    mocker.patch(
        "routes.auth.google_id_token.verify_oauth2_token",
        return_value={
            "sub": "google-sub-12345",
            "email": "newuser@gmail.com",
            "name": "New User",
            "picture": "https://example.com/avatar.png",
        },
    )

    resp = client.post("/v1/auth/google", json={"id_token": "fake-google-token"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["user"]["email"] == "newuser@gmail.com"
    assert body["user"]["display_name"] == "New User"
    assert body["access_token"]
    assert body["refresh_token"]

    with db.cursor() as cur:
        cur.execute(
            "SELECT google_id, email_verified FROM users WHERE email = %s",
            ("newuser@gmail.com",),
        )
        google_id, email_verified = cur.fetchone()
    assert google_id == "google-sub-12345"
    assert email_verified is True


def test_google_login_links_existing_email(client, db, register_user, mocker):
    """A user who already registered with email/password gets google_id linked."""
    register_user(email="link-me@gmail.com", password="password1234")

    mocker.patch(
        "routes.auth.google_id_token.verify_oauth2_token",
        return_value={
            "sub": "google-sub-99999",
            "email": "link-me@gmail.com",
            "name": "Linked User",
            "picture": None,
        },
    )

    resp = client.post("/v1/auth/google", json={"id_token": "fake-google-token"})
    assert resp.status_code == 200

    with db.cursor() as cur:
        cur.execute(
            "SELECT google_id FROM users WHERE email = %s",
            ("link-me@gmail.com",),
        )
        (google_id,) = cur.fetchone()
    assert google_id == "google-sub-99999"


def test_google_login_rejects_invalid_signature(client, mocker):
    """verify_oauth2_token raises ValueError on signature failure → 401."""
    mocker.patch(
        "routes.auth.google_id_token.verify_oauth2_token",
        side_effect=ValueError("Invalid token signature"),
    )

    resp = client.post("/v1/auth/google", json={"id_token": "tampered-token"})
    assert resp.status_code == 401
    assert "Invalid token" in resp.get_json()["error"]


def test_google_login_rejects_wrong_audience(client, mocker):
    """verify_oauth2_token raises ValueError when aud doesn't match → 401."""
    mocker.patch(
        "routes.auth.google_id_token.verify_oauth2_token",
        side_effect=ValueError("Token has wrong audience some-other-client"),
    )

    resp = client.post("/v1/auth/google", json={"id_token": "wrong-aud-token"})
    assert resp.status_code == 401
    assert "Invalid token" in resp.get_json()["error"]


# ----------------------------------------------------------------------------
# /auth/apple
# ----------------------------------------------------------------------------

def _stub_apple_signing_key(mocker):
    """Stub the JWKS lookup so apple_login doesn't hit appleid.apple.com."""
    fake_key = mocker.MagicMock()
    fake_key.key = "fake-public-key"
    mocker.patch(
        "routes.auth._apple_jwk_client.get_signing_key_from_jwt",
        return_value=fake_key,
    )


def test_apple_login_creates_new_user(client, db, mocker):
    _stub_apple_signing_key(mocker)
    mocker.patch(
        "routes.auth.jwt.decode",
        return_value={
            "sub": "apple-sub-abc123",
            "email": "newapple@privaterelay.appleid.com",
            "email_verified": "true",
        },
    )

    resp = client.post(
        "/v1/auth/apple",
        json={
            "identity_token": "fake-apple-token",
            "full_name": "Apple User",
        },
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["user"]["email"] == "newapple@privaterelay.appleid.com"
    assert body["user"]["display_name"] == "Apple User"
    assert body["access_token"]
    assert body["refresh_token"]

    with db.cursor() as cur:
        cur.execute(
            "SELECT apple_id, email_verified FROM users WHERE email = %s",
            ("newapple@privaterelay.appleid.com",),
        )
        apple_id, email_verified = cur.fetchone()
    assert apple_id == "apple-sub-abc123"
    assert email_verified is True


def test_apple_login_links_existing_email(client, db, register_user, mocker):
    register_user(email="apple-link@example.com", password="password1234")

    _stub_apple_signing_key(mocker)
    mocker.patch(
        "routes.auth.jwt.decode",
        return_value={
            "sub": "apple-sub-link-1",
            "email": "apple-link@example.com",
            "email_verified": True,
        },
    )

    resp = client.post(
        "/v1/auth/apple",
        json={"identity_token": "fake-apple-token"},
    )
    assert resp.status_code == 200

    with db.cursor() as cur:
        cur.execute(
            "SELECT apple_id FROM users WHERE email = %s",
            ("apple-link@example.com",),
        )
        (apple_id,) = cur.fetchone()
    assert apple_id == "apple-sub-link-1"


def test_apple_login_rejects_invalid_signature(client, mocker):
    _stub_apple_signing_key(mocker)
    mocker.patch(
        "routes.auth.jwt.decode",
        side_effect=InvalidSignatureError("Signature verification failed"),
    )

    resp = client.post(
        "/v1/auth/apple",
        json={"identity_token": "tampered-apple-token"},
    )
    assert resp.status_code == 401
    assert "Invalid token" in resp.get_json()["error"]


def test_apple_login_rejects_wrong_audience(client, mocker):
    _stub_apple_signing_key(mocker)
    mocker.patch(
        "routes.auth.jwt.decode",
        side_effect=InvalidAudienceError("Audience doesn't match"),
    )

    resp = client.post(
        "/v1/auth/apple",
        json={"identity_token": "wrong-aud-apple-token"},
    )
    assert resp.status_code == 401
    assert "Invalid token" in resp.get_json()["error"]
