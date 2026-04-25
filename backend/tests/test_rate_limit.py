"""
Rate-limit smoke tests.

The suite-wide autouse fixture in ``conftest.py`` disables the limiter so
tight per-endpoint limits don't turn into noise across the rest of the
tests. This file overrides that locally: turn the limiter ON for each
test in this module, and reset its in-memory counters between tests so
quotas don't leak.

The limiter's wiring (storage, ``before_request`` hook) is already done
by ``Limiter.init_app`` at app-import time — we just flip the runtime
``enabled`` flag. Doing it that way avoids the
``"setup method 'before_request' can no longer be called"`` error you
hit if you try to re-init after the first request.

Tests use ``X-Forwarded-For`` to control the apparent client IP; ProxyFix
is wired in ``app.py`` with ``x_for=1`` and rewrites ``remote_addr``
before the limiter's ``get_remote_address`` key function sees it.
"""

import pytest

from rate_limit import (
    limiter,
    LOGIN_LIMIT,
    REGISTER_LIMIT,
    FORGOT_PASSWORD_LIMIT,
    CHANGE_PASSWORD_LIMIT,
)


@pytest.fixture(autouse=True)
def _enable_limiter():
    """Override conftest's suite-wide disable for this module's tests."""
    limiter.enabled = True
    limiter.reset()
    yield
    limiter.reset()
    limiter.enabled = False


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def _post(client, path, json_body, ip="1.2.3.4", extra_headers=None):
    """POST with a deterministic X-Forwarded-For so per-IP tests are isolated.

    The Flask test client otherwise sends from 127.0.0.1, which would let
    test ordering leak quota across tests in this module.
    """
    headers = {"X-Forwarded-For": ip}
    if extra_headers:
        headers.update(extra_headers)
    return client.post(path, json=json_body, headers=headers)


def _login_count(limit_string):
    """Parse '10 per minute' → 10. Tests use this so any future bump to the
    limit string only requires updating ``rate_limit.py``, not these tests.
    """
    return int(limit_string.split()[0])


# ----------------------------------------------------------------------------
# /auth/login — 10 per minute, per IP
# ----------------------------------------------------------------------------

def test_login_returns_429_after_quota_exhausted(client, register_user):
    register_user(email="login-bot@example.com", password="password1234")
    cap = _login_count(LOGIN_LIMIT)

    # The first `cap` attempts pass the limiter (and 401 on bad password).
    for _ in range(cap):
        resp = _post(client, "/auth/login",
                     {"email": "login-bot@example.com", "password": "wrong"})
        assert resp.status_code == 401

    # The next one is over the cap.
    resp = _post(client, "/auth/login",
                 {"email": "login-bot@example.com", "password": "wrong"})
    assert resp.status_code == 429
    body = resp.get_json()
    assert "error" in body
    # The 429 handler in rate_limit.py surfaces the limit description.
    assert "limit" in body
    assert "per" in body["limit"]


# ----------------------------------------------------------------------------
# /auth/register — 5 per hour, per IP
# ----------------------------------------------------------------------------

def test_register_returns_429_after_quota_exhausted(client):
    cap = _login_count(REGISTER_LIMIT)

    for i in range(cap):
        resp = _post(client, "/auth/register", {
            "email": f"reg-{i}@example.com",
            "password": "password1234",
        })
        assert resp.status_code == 201, resp.get_json()

    resp = _post(client, "/auth/register", {
        "email": "reg-overflow@example.com",
        "password": "password1234",
    })
    assert resp.status_code == 429
    assert resp.get_json()["error"]


# ----------------------------------------------------------------------------
# /auth/forgot-password — 3 per hour, per IP
# ----------------------------------------------------------------------------

def test_forgot_password_returns_429_after_quota_exhausted(client):
    cap = _login_count(FORGOT_PASSWORD_LIMIT)

    # Use a non-existent email — the handler always returns 200 to prevent
    # enumeration, so all `cap` attempts succeed at the HTTP layer.
    for _ in range(cap):
        resp = _post(client, "/auth/forgot-password",
                     {"email": "ghost@example.com"})
        assert resp.status_code == 200

    resp = _post(client, "/auth/forgot-password",
                 {"email": "ghost@example.com"})
    assert resp.status_code == 429


# ----------------------------------------------------------------------------
# Per-IP isolation — different X-Forwarded-For gets a fresh quota
# ----------------------------------------------------------------------------

def test_login_quota_isolated_per_client_ip(client, register_user):
    register_user(email="iso@example.com", password="password1234")
    cap = _login_count(LOGIN_LIMIT)

    # Burn the quota from IP A.
    for _ in range(cap):
        _post(client, "/auth/login",
              {"email": "iso@example.com", "password": "wrong"},
              ip="1.2.3.4")
    blocked = _post(client, "/auth/login",
                    {"email": "iso@example.com", "password": "wrong"},
                    ip="1.2.3.4")
    assert blocked.status_code == 429

    # IP B should be unaffected — gets 401 (bad password), not 429.
    other = _post(client, "/auth/login",
                  {"email": "iso@example.com", "password": "wrong"},
                  ip="5.6.7.8")
    assert other.status_code == 401


# ----------------------------------------------------------------------------
# /auth/change-password — 5 per hour, keyed per user (not per IP)
# ----------------------------------------------------------------------------

def test_change_password_quota_shared_across_ips_for_same_user(
    client, register_user
):
    """The same user's quota follows them across IPs (per-user keying)."""
    body = register_user(email="alice@example.com", password="password1234")
    cap = _login_count(CHANGE_PASSWORD_LIMIT)
    headers = {"Authorization": f"Bearer {body['access_token']}"}

    # Use a wrong current_password so the handler 401s without actually
    # rotating the password. The limiter increments either way.
    for _ in range(cap):
        resp = _post(
            client, "/auth/change-password",
            {"current_password": "wrong-current", "new_password": "newpass1234"},
            ip="1.2.3.4",
            extra_headers=headers,
        )
        assert resp.status_code == 401

    # Same user, different IP — should still hit the per-user cap.
    resp = _post(
        client, "/auth/change-password",
        {"current_password": "wrong-current", "new_password": "newpass1234"},
        ip="9.9.9.9",
        extra_headers=headers,
    )
    assert resp.status_code == 429


def test_change_password_quota_isolated_per_user_on_same_ip(
    client, register_user
):
    """Two distinct users from the same IP have independent quotas."""
    a = register_user(email="user-a@example.com", password="password1234")
    b = register_user(email="user-b@example.com", password="password1234")
    cap = _login_count(CHANGE_PASSWORD_LIMIT)

    # Burn user A's quota from IP X.
    for _ in range(cap):
        _post(
            client, "/auth/change-password",
            {"current_password": "wrong-current", "new_password": "newpass1234"},
            ip="7.7.7.7",
            extra_headers={"Authorization": f"Bearer {a['access_token']}"},
        )

    # User B from the same IP X should get a fresh quota — 401, not 429.
    resp = _post(
        client, "/auth/change-password",
        {"current_password": "wrong-current", "new_password": "newpass1234"},
        ip="7.7.7.7",
        extra_headers={"Authorization": f"Bearer {b['access_token']}"},
    )
    assert resp.status_code == 401
