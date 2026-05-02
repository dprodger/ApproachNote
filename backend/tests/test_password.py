"""
Password-flow integration tests for /auth/forgot-password,
/auth/reset-password, and /auth/change-password.

The password-reset email is mocked by an autouse fixture in conftest.py
(``core.email_service.send_password_reset_email``), so nothing here hits
SendGrid.
"""

from datetime import datetime, timedelta, timezone


# ----------------------------------------------------------------------------
# /auth/forgot-password
# ----------------------------------------------------------------------------

def test_forgot_password_known_email_inserts_token_and_sends_email(
    client, db, register_user, mocker
):
    register_user(email="forgot-me@example.com")

    # Re-patch at the routes.password import site so we can assert on the call.
    # The autouse stub in conftest patches core.email_service directly; the
    # handler imports the symbol with `from core.email_service import ...`,
    # which means the bound name in routes.password needs its own patch.
    mock_send = mocker.patch(
        "routes.password.send_password_reset_email", return_value=True
    )

    resp = client.post(
        "/v1/auth/forgot-password",
        json={"email": "forgot-me@example.com"},
    )
    assert resp.status_code == 200
    assert "reset link has been sent" in resp.get_json()["message"]

    with db.cursor() as cur:
        cur.execute(
            """
            SELECT prt.token, prt.expires_at, prt.used_at
            FROM password_reset_tokens prt
            JOIN users u ON u.id = prt.user_id
            WHERE u.email = %s
            """,
            ("forgot-me@example.com",),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    token, expires_at, used_at = rows[0]
    assert token  # opaque, just needs to be set
    assert used_at is None
    assert expires_at > datetime.now(timezone.utc)

    mock_send.assert_called_once()
    args, _ = mock_send.call_args
    assert args[0] == "forgot-me@example.com"
    assert args[1] == token


def test_forgot_password_unknown_email_returns_200_without_inserting_token(
    client, db, mocker
):
    """Email enumeration defense: unknown email gets the same 200 response,
    but no token row is created and no email is sent."""
    mock_send = mocker.patch(
        "routes.password.send_password_reset_email", return_value=True
    )

    resp = client.post(
        "/v1/auth/forgot-password",
        json={"email": "nobody@example.com"},
    )
    assert resp.status_code == 200
    assert "reset link has been sent" in resp.get_json()["message"]

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM password_reset_tokens")
        (count,) = cur.fetchone()
    assert count == 0
    mock_send.assert_not_called()


# ----------------------------------------------------------------------------
# /auth/reset-password
# ----------------------------------------------------------------------------

def _insert_reset_token(
    db, *, user_id, token: str, expires_in: timedelta = timedelta(hours=1),
    used: bool = False,
):
    """Helper: drop a reset-token row in directly. Skips the /forgot-password
    handler so tests can craft expired/used tokens deterministically."""
    expires_at = datetime.now(timezone.utc) + expires_in
    used_at = datetime.now(timezone.utc) if used else None
    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO password_reset_tokens (user_id, token, expires_at, used_at)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, token, expires_at, used_at),
        )
    db.commit()


def test_reset_password_with_valid_token_updates_password_and_marks_used(
    client, db, register_user
):
    body = register_user(email="reset-me@example.com", password="old-password-123")
    user_id = body["user"]["id"]

    _insert_reset_token(db, user_id=user_id, token="valid-reset-token")

    resp = client.post(
        "/v1/auth/reset-password",
        json={"token": "valid-reset-token", "password": "brand-new-password"},
    )
    assert resp.status_code == 200
    assert "Password reset successfully" in resp.get_json()["message"]

    # Token marked used.
    with db.cursor() as cur:
        cur.execute(
            "SELECT used_at FROM password_reset_tokens WHERE token = %s",
            ("valid-reset-token",),
        )
        (used_at,) = cur.fetchone()
    assert used_at is not None

    # Login with the new password works; old password no longer does.
    resp_new = client.post(
        "/v1/auth/login",
        json={"email": "reset-me@example.com", "password": "brand-new-password"},
    )
    assert resp_new.status_code == 200

    resp_old = client.post(
        "/v1/auth/login",
        json={"email": "reset-me@example.com", "password": "old-password-123"},
    )
    assert resp_old.status_code == 401


def test_reset_password_with_used_token_is_rejected(
    client, db, register_user
):
    body = register_user(email="reused@example.com", password="initial-password")
    user_id = body["user"]["id"]

    _insert_reset_token(db, user_id=user_id, token="already-used-token", used=True)

    resp = client.post(
        "/v1/auth/reset-password",
        json={"token": "already-used-token", "password": "new-password-456"},
    )
    # The handler returns 401 when the token row isn't usable; the issue
    # description guessed 400 but we assert what the code actually does.
    assert resp.status_code == 401
    assert "Invalid or expired token" in resp.get_json()["error"]


def test_reset_password_with_expired_token_is_rejected(
    client, db, register_user
):
    body = register_user(email="stale@example.com", password="initial-password")
    user_id = body["user"]["id"]

    _insert_reset_token(
        db,
        user_id=user_id,
        token="expired-reset-token",
        expires_in=timedelta(hours=-1),  # expired an hour ago
    )

    resp = client.post(
        "/v1/auth/reset-password",
        json={"token": "expired-reset-token", "password": "new-password-456"},
    )
    assert resp.status_code == 401
    assert "Invalid or expired token" in resp.get_json()["error"]


# ----------------------------------------------------------------------------
# /auth/change-password
# ----------------------------------------------------------------------------

def test_change_password_with_correct_current_password(
    client, register_user
):
    register_user(email="changer@example.com", password="current-password-1")
    login = client.post(
        "/v1/auth/login",
        json={"email": "changer@example.com", "password": "current-password-1"},
    ).get_json()
    headers = {"Authorization": f"Bearer {login['access_token']}"}

    resp = client.post(
        "/v1/auth/change-password",
        json={
            "current_password": "current-password-1",
            "new_password": "different-password-2",
        },
        headers=headers,
    )
    assert resp.status_code == 200
    assert "Password changed successfully" in resp.get_json()["message"]

    # Old password no longer works; new one does.
    resp_old = client.post(
        "/v1/auth/login",
        json={"email": "changer@example.com", "password": "current-password-1"},
    )
    assert resp_old.status_code == 401

    resp_new = client.post(
        "/v1/auth/login",
        json={"email": "changer@example.com", "password": "different-password-2"},
    )
    assert resp_new.status_code == 200
