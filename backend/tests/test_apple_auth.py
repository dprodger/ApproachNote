"""Unit tests for core.apple_auth (Sign in with Apple server helpers).

These exercise client_secret signing and the token-exchange / revoke HTTP
plumbing in isolation, mocking the outbound session so no real network calls
are made. The end-to-end wiring through /auth/apple and /auth/delete-account
lives in test_auth.py.
"""

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

from core import apple_auth


# ----------------------------------------------------------------------------
# Fixtures: a throwaway P-256 key written as a .p8, plus the matching env.
# ----------------------------------------------------------------------------

@pytest.fixture
def apple_signin_key(tmp_path, monkeypatch):
    """Generate an EC P-256 private key, write it as a .p8, and configure env.

    Returns the PEM public key so tests can verify the signed client_secret.
    """
    private_key = ec.generate_private_key(ec.SECP256R1())
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path = tmp_path / "AuthKey_TEST123.p8"
    key_path.write_bytes(pem)

    monkeypatch.setenv("APPLE_SIGNIN_TEAM_ID", "TEAMID1234")
    monkeypatch.setenv("APPLE_SIGNIN_KEY_ID", "KEYID56789")
    monkeypatch.setenv("APPLE_SIGNIN_PRIVATE_KEY_PATH", str(key_path))

    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return public_pem


def _mock_session(mocker, response):
    """Patch core.apple_auth.make_session to yield a session whose .post()
    returns ``response``. Returns the session mock so callers can assert on
    .post call args."""
    session = mocker.MagicMock()
    session.post.return_value = response
    cm = mocker.MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    mocker.patch("core.apple_auth.make_session", return_value=cm)
    return session


# ----------------------------------------------------------------------------
# is_configured
# ----------------------------------------------------------------------------

def test_is_configured_true_when_all_present(apple_signin_key):
    assert apple_auth.is_configured() is True


def test_is_configured_false_when_key_missing(monkeypatch):
    monkeypatch.delenv("APPLE_SIGNIN_KEY_ID", raising=False)
    monkeypatch.delenv("APPLE_SIGNIN_PRIVATE_KEY_PATH", raising=False)
    assert apple_auth.is_configured() is False


def test_is_configured_false_when_key_file_absent(tmp_path, monkeypatch):
    monkeypatch.setenv("APPLE_SIGNIN_TEAM_ID", "TEAMID1234")
    monkeypatch.setenv("APPLE_SIGNIN_KEY_ID", "KEYID56789")
    monkeypatch.setenv("APPLE_SIGNIN_PRIVATE_KEY_PATH", str(tmp_path / "nope.p8"))
    assert apple_auth.is_configured() is False


def test_team_id_falls_back_to_apple_team_id(tmp_path, monkeypatch):
    key_path = tmp_path / "k.p8"
    key_path.write_bytes(b"unused")
    monkeypatch.delenv("APPLE_SIGNIN_TEAM_ID", raising=False)
    monkeypatch.setenv("APPLE_TEAM_ID", "SHAREDTEAM")
    monkeypatch.setenv("APPLE_SIGNIN_KEY_ID", "KEYID56789")
    monkeypatch.setenv("APPLE_SIGNIN_PRIVATE_KEY_PATH", str(key_path))
    assert apple_auth.is_configured() is True


# ----------------------------------------------------------------------------
# generate_client_secret
# ----------------------------------------------------------------------------

def test_generate_client_secret_is_verifiable_jwt(apple_signin_key):
    client_id = "com.approachnote.ios"
    token = apple_auth.generate_client_secret(client_id)

    # Verify the signature with the matching public key and check the claims
    # Apple requires.
    claims = jwt.decode(
        token,
        apple_signin_key,
        algorithms=["ES256"],
        audience="https://appleid.apple.com",
    )
    assert claims["iss"] == "TEAMID1234"
    assert claims["sub"] == client_id
    assert claims["aud"] == "https://appleid.apple.com"
    assert claims["exp"] > claims["iat"]

    headers = jwt.get_unverified_header(token)
    assert headers["alg"] == "ES256"
    assert headers["kid"] == "KEYID56789"


def test_generate_client_secret_raises_when_unconfigured(monkeypatch):
    monkeypatch.delenv("APPLE_SIGNIN_TEAM_ID", raising=False)
    monkeypatch.delenv("APPLE_TEAM_ID", raising=False)
    monkeypatch.delenv("APPLE_SIGNIN_KEY_ID", raising=False)
    with pytest.raises(apple_auth.AppleAuthError):
        apple_auth.generate_client_secret("com.approachnote.ios")


# ----------------------------------------------------------------------------
# exchange_code_for_refresh_token
# ----------------------------------------------------------------------------

def test_exchange_returns_refresh_token(apple_signin_key, mocker):
    resp = mocker.MagicMock(status_code=200)
    resp.json.return_value = {"refresh_token": "apple-rt-abc", "access_token": "x"}
    session = _mock_session(mocker, resp)

    rt = apple_auth.exchange_code_for_refresh_token("the-code", "com.approachnote.ios")
    assert rt == "apple-rt-abc"

    # Posted form fields are correct.
    _, kwargs = session.post.call_args
    data = kwargs["data"]
    assert data["grant_type"] == "authorization_code"
    assert data["code"] == "the-code"
    assert data["client_id"] == "com.approachnote.ios"
    assert data["client_secret"]  # a signed JWT


def test_exchange_raises_on_non_200(apple_signin_key, mocker):
    resp = mocker.MagicMock(status_code=400, text='{"error":"invalid_grant"}')
    _mock_session(mocker, resp)
    with pytest.raises(apple_auth.AppleAuthError):
        apple_auth.exchange_code_for_refresh_token("bad", "com.approachnote.ios")


def test_exchange_raises_when_no_refresh_token_in_body(apple_signin_key, mocker):
    resp = mocker.MagicMock(status_code=200)
    resp.json.return_value = {"access_token": "only-access"}
    _mock_session(mocker, resp)
    with pytest.raises(apple_auth.AppleAuthError):
        apple_auth.exchange_code_for_refresh_token("code", "com.approachnote.ios")


# ----------------------------------------------------------------------------
# revoke_refresh_token
# ----------------------------------------------------------------------------

def test_revoke_succeeds_on_first_client_id(apple_signin_key, mocker):
    resp = mocker.MagicMock(status_code=200, text="")
    session = _mock_session(mocker, resp)

    ok = apple_auth.revoke_refresh_token(
        "apple-rt", ["com.approachnote.ios", "com.approachnote.mac"]
    )
    assert ok is True
    # Should stop after the first success.
    assert session.post.call_count == 1
    _, kwargs = session.post.call_args
    assert kwargs["data"]["token"] == "apple-rt"
    assert kwargs["data"]["token_type_hint"] == "refresh_token"


def test_revoke_tries_next_client_id_on_rejection(apple_signin_key, mocker):
    reject = mocker.MagicMock(status_code=400, text="invalid_client")
    accept = mocker.MagicMock(status_code=200, text="")
    session = mocker.MagicMock()
    session.post.side_effect = [reject, accept]
    cm = mocker.MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    mocker.patch("core.apple_auth.make_session", return_value=cm)

    ok = apple_auth.revoke_refresh_token(
        "apple-rt", ["com.approachnote.ios", "com.approachnote.mac"]
    )
    assert ok is True
    assert session.post.call_count == 2


def test_revoke_returns_false_when_all_rejected(apple_signin_key, mocker):
    reject = mocker.MagicMock(status_code=400, text="invalid_client")
    _mock_session(mocker, reject)
    ok = apple_auth.revoke_refresh_token("apple-rt", ["com.approachnote.ios"])
    assert ok is False


def test_revoke_swallows_request_errors(apple_signin_key, mocker):
    session = mocker.MagicMock()
    session.post.side_effect = RuntimeError("network down")
    cm = mocker.MagicMock()
    cm.__enter__.return_value = session
    cm.__exit__.return_value = False
    mocker.patch("core.apple_auth.make_session", return_value=cm)

    ok = apple_auth.revoke_refresh_token("apple-rt", ["com.approachnote.ios"])
    assert ok is False


def test_revoke_raises_with_no_client_ids(apple_signin_key):
    with pytest.raises(apple_auth.AppleAuthError):
        apple_auth.revoke_refresh_token("apple-rt", [])
