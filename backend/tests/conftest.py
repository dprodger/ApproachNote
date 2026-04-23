"""
Shared pytest fixtures for backend tests.

Test isolation strategy
-----------------------
The auth handlers commit multiple times per request (insert user, then insert
refresh_token, then update last_login_at). That makes the classic "wrap each
test in a transaction and rollback on teardown" pattern unworkable — the
handler's commits would survive the rollback. Instead we use TRUNCATE on the
auth-related tables after each test. ~10-30ms per test, no monkey-patching,
no surprises.

Defaults set BEFORE backend imports
-----------------------------------
``RATELIMIT_ENABLED`` and ``JWT_SECRET`` are read at module-import time by
``rate_limit.py`` and ``core.auth_utils`` respectively. They MUST be set
before the ``app`` fixture imports the Flask app, so we set sane defaults
at the very top of this file.
"""

import os
import sys
import uuid
from pathlib import Path

# --- Set env defaults BEFORE any backend imports happen ---------------------
# These need to be in place before ``rate_limit`` and ``core.auth_utils``
# are imported (which happens transitively when the ``app`` fixture imports
# ``app``).
os.environ.setdefault("RATELIMIT_ENABLED", "false")
os.environ.setdefault("JWT_SECRET", "pytest-test-secret")

# Make ``backend/`` importable so we can ``from app import app`` etc.
# (Mirrors what ``scripts/script_base.py`` does for CLI scripts.)
_BACKEND_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_BACKEND_ROOT))

import psycopg  # noqa: E402
import pytest  # noqa: E402


# ----------------------------------------------------------------------------
# Production-safety guard
# ----------------------------------------------------------------------------
#
# April 2026 incident: conftest's autouse TRUNCATE plus individual test
# modules' INSERT/DELETE against recording_release_streaming_links ran
# against PRODUCTION any time a developer had .env pointing at prod and
# ran pytest without explicit env var overrides. Wiped user-contributed
# data and streaming-link rows multiple times before anyone noticed.
#
# This guard refuses to start the test session unless DB_NAME contains
# 'test'. That's a tiny constraint on test-DB naming (call it jazz_test,
# approachnote_test, whatever) in exchange for making the "oops, I ran
# tests against prod" failure mode impossible.
#
# If you genuinely need to run the suite against a DB whose name doesn't
# include 'test', either rename the DB or set PYTEST_I_KNOW_THIS_ISNT_PROD=1
# as an explicit, visible opt-out.

_PROD_SAFETY_BYPASS = "PYTEST_I_KNOW_THIS_ISNT_PROD"


def _assert_test_database_or_die() -> None:
    """Raise pytest.UsageError if the DB env doesn't look like a test DB."""
    db_name = os.environ.get("DB_NAME", "")
    db_host = os.environ.get("DB_HOST", "")

    if "test" in db_name.lower():
        return
    if os.environ.get(_PROD_SAFETY_BYPASS) == "1":
        # Explicit opt-out: developer swears this DB isn't prod.
        # We still print a warning so it's obvious in the test log.
        print(
            f"\n!!! {_PROD_SAFETY_BYPASS}=1 set — skipping test-DB name "
            f"check. DB_NAME={db_name!r} DB_HOST={db_host!r}\n",
            file=sys.stderr,
        )
        return

    raise pytest.UsageError(
        f"Refusing to run the test suite: DB_NAME={db_name!r} does not contain "
        f"'test'. This suite issues TRUNCATE against users/refresh_tokens/"
        f"password_reset_tokens and INSERT/DELETE against "
        f"recording_release_streaming_links from individual test modules. "
        f"It must never run against a non-test database.\n\n"
        f"Fixes:\n"
        f"  - Set DB_NAME=jazz_test (or any name containing 'test') before "
        f"running pytest. See backend/tests/README.md for the full bootstrap.\n"
        f"  - Or, if you're certain this DB is safe to mutate, set "
        f"{_PROD_SAFETY_BYPASS}=1 to bypass this check (logged to stderr)."
    )


# Run the guard at module import time. pytest collects conftest.py before
# any test fixtures run, so raising here stops the session before any
# destructive fixture can execute.
_assert_test_database_or_die()


# ----------------------------------------------------------------------------
# DB connection helpers (separate from the app's pool)
# ----------------------------------------------------------------------------

def _test_db_dsn() -> dict:
    """Build a psycopg.connect kwargs dict from the same env vars the app uses."""
    return {
        "host": os.environ["DB_HOST"],
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
    }


# ----------------------------------------------------------------------------
# Core fixtures
# ----------------------------------------------------------------------------

@pytest.fixture(scope="session")
def app():
    """
    The Flask app under test. Imported once per session.

    The import triggers ``app.py`` to wire blueprints, init the rate limiter,
    install ProxyFix, etc. By the time this fixture returns, the app is in
    the same state it would be in production — minus the gunicorn wrapping.
    """
    # Imported here, not at module top, so the env vars set above are in
    # place first.
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    yield flask_app


@pytest.fixture
def client(app):
    """A fresh Flask test client per test."""
    return app.test_client()


@pytest.fixture
def db():
    """
    A direct psycopg connection to the test database, separate from the
    app's pool. Use this when a test needs to inspect or mutate DB state
    out-of-band (e.g. asserting a row exists after a POST).
    """
    with psycopg.connect(**_test_db_dsn()) as conn:
        yield conn


@pytest.fixture(autouse=True)
def _clean_auth_tables():
    """
    TRUNCATE the auth-related tables after every test. Autouse so individual
    tests don't have to remember to opt in.

    Uses a direct psycopg connection (not the app's pool) to avoid contending
    with whatever the test client just did.
    """
    yield
    with psycopg.connect(**_test_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE password_reset_tokens, refresh_tokens, users "
                "RESTART IDENTITY CASCADE"
            )
        conn.commit()


@pytest.fixture(autouse=True)
def _clean_research_tables():
    """
    TRUNCATE research_jobs and reset the source_quotas seed row after every
    test. Autouse so research-related tests don't have to remember to opt in,
    and tests that don't touch these tables pay only the cost of two cheap
    statements.

    source_quotas isn't truncated — the migration seeds the youtube/day row
    and tests rely on it being present. Reset to a known clean state instead.
    """
    yield
    with psycopg.connect(**_test_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE research_jobs RESTART IDENTITY")
            # Reset every quota row to 0 used and far-future resets_at.
            # Tests that need a specific resets_at can override via UPDATE.
            cur.execute(
                "UPDATE source_quotas "
                "SET units_used = 0, resets_at = now() + interval '1 day'"
            )
        conn.commit()


@pytest.fixture
def make_job(db):
    """Factory: insert a research_jobs row with sensible defaults, return id.

    Override anything via kwargs. Returns the inserted id so the caller can
    immediately query/mutate the row.
    """
    import json
    from uuid import uuid4

    def _make(
        *,
        source: str = "youtube",
        job_type: str = "match_recording",
        target_type: str = "recording",
        target_id: str | None = None,
        payload: dict | None = None,
        status: str = "queued",
        priority: int = 100,
        attempts: int = 0,
        max_attempts: int = 5,
        run_after=None,  # None = now()
    ) -> int:
        target_id = target_id or str(uuid4())
        payload_json = json.dumps(payload or {})
        with db.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research_jobs
                    (source, job_type, target_type, target_id, payload,
                     status, priority, attempts, max_attempts, run_after)
                VALUES (%s, %s, %s, %s, %s::jsonb,
                        %s, %s, %s, %s, COALESCE(%s, now()))
                RETURNING id
                """,
                (
                    source, job_type, target_type, target_id, payload_json,
                    status, priority, attempts, max_attempts, run_after,
                ),
            )
            row = cur.fetchone()
        db.commit()
        return row[0]

    return _make


@pytest.fixture
def quota_row(db):
    """Helpers for inspecting and mutating the youtube/day quota row.

    Returns an object with `.snapshot()` and `.set(units_used=, resets_at=)`.
    Reset by the autouse cleanup fixture, so tests don't need to undo changes.
    """
    class _Quota:
        def snapshot(self) -> dict:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT units_used, units_limit, resets_at "
                    "FROM source_quotas "
                    "WHERE source = 'youtube' AND window_name = 'day'"
                )
                row = cur.fetchone()
            return {
                'units_used': row[0],
                'units_limit': row[1],
                'resets_at': row[2],
            }

        def set(self, *, units_used=None, resets_at=None) -> None:
            sets = []
            params = []
            if units_used is not None:
                sets.append("units_used = %s")
                params.append(units_used)
            if resets_at is not None:
                sets.append("resets_at = %s")
                params.append(resets_at)
            if not sets:
                return
            params.extend(['youtube', 'day'])
            with db.cursor() as cur:
                cur.execute(
                    f"UPDATE source_quotas SET {', '.join(sets)} "
                    "WHERE source = %s AND window_name = %s",
                    params,
                )
            db.commit()

    return _Quota()


@pytest.fixture(autouse=True)
def _stub_external_email(mocker):
    """
    Replace the email-sending entry points with no-op mocks. Autouse so we
    never accidentally hit SendGrid (or fall through to its log-only path,
    which is also misleading in tests).

    Patches both the source location AND the import site in ``routes.auth``,
    because Python's ``from core.email_service import send_welcome_email``
    binds the function in ``routes.auth``'s namespace.
    """
    mocker.patch("core.email_service.send_welcome_email", return_value=None)
    mocker.patch("core.email_service.send_password_reset_email", return_value=None)
    mocker.patch("routes.auth.send_welcome_email", return_value=None)


# ----------------------------------------------------------------------------
# Convenience factories
# ----------------------------------------------------------------------------

@pytest.fixture
def register_user(client):
    """
    Factory that POSTs /auth/register with a unique email and returns the
    parsed response body (which includes ``user``, ``access_token``,
    ``refresh_token``).
    """
    def _register(email: str | None = None,
                  password: str = "test-password-123",
                  display_name: str = "Test User") -> dict:
        if email is None:
            email = f"user-{uuid.uuid4().hex[:8]}@example.com"
        resp = client.post(
            "/auth/register",
            json={"email": email, "password": password, "display_name": display_name},
        )
        assert resp.status_code == 201, (
            f"register helper failed: {resp.status_code} {resp.get_json()}"
        )
        return resp.get_json()
    return _register


@pytest.fixture
def auth_headers(register_user):
    """
    Returns ``{"Authorization": "Bearer <access_token>"}`` for a freshly
    registered user. The user data is exposed via the ``user`` attribute on
    the returned dict for tests that need the user's UUID/email.
    """
    body = register_user()

    class _Headers(dict):
        pass

    headers = _Headers({"Authorization": f"Bearer {body['access_token']}"})
    headers.user = body["user"]
    headers.access_token = body["access_token"]
    headers.refresh_token = body["refresh_token"]
    return headers
