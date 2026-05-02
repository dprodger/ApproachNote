# Backend tests

Pytest suite for the Flask backend. Covers the auth flow, the research
queue / worker, the job-handler plumbing for each source, the pure
matching functions behind Spotify and Apple Music, and per-endpoint
rate-limit enforcement.

## Running locally

Production runs on Supabase. For tests we use a disposable Dockerized
Postgres — no need to install or manage Postgres on your laptop. You
just need Docker Desktop running.

### One-time setup

```bash
# From the repo root:
cp backend/.env.test.example backend/.env.test
cd backend
pip install -r requirements.txt -r requirements-dev.txt
```

### Each test run

```bash
# 1. Start the test DB container and apply schema + migrations.
#    (First run pulls postgres:16; subsequent runs are fast.)
./backend/scripts/test_db.sh up

# 2. Activate the backend venv (pytest lives here).
source backend/venv/bin/activate

# 3. Load test env vars into your shell.
source backend/.env.test

# 4. Run the suite.
pytest backend/tests/
```

When you're done for the day:

```bash
./backend/scripts/test_db.sh down     # stop container, keep data
# or
./backend/scripts/test_db.sh reset    # next run starts from a clean DB
```

Other helpers:

```bash
./backend/scripts/test_db.sh psql     # interactive shell into the test DB
```

CI runs the equivalent in `.github/workflows/pytest.yml` against a
Postgres `services:` container — same image (`postgres:16`), same
schema-bootstrap logic, so local and CI can't drift.

## Production-safety guard

`conftest.py` refuses to run the suite unless `DB_NAME` contains `test`
(case-insensitive). This exists because the fixtures issue `TRUNCATE`
against the auth tables and individual test modules INSERT/DELETE into
`recording_release_streaming_links` — if you accidentally point the
test env at your production DB, those ops will wipe real data. The
April 2026 incident was exactly this mistake.

`backend/scripts/test_db.sh` enforces the same `test`-in-name rule
before running any destructive Docker ops, as a second layer.

If you genuinely need to run against a DB whose name doesn't include
`test`, set `PYTEST_I_KNOW_THIS_ISNT_PROD=1` to bypass the pytest check.
A warning is logged to stderr every run when the bypass is active.
Don't make a habit of it.

## Conventions

- **Test isolation**: an autouse fixture in `conftest.py` `TRUNCATE`s
  `users`, `refresh_tokens`, and `password_reset_tokens` after every test.
  Don't rely on rows surviving across tests.
- **Email**: `core.email_service.send_*` and the `routes.auth` import-site
  bindings are mocked out by another autouse fixture. No tests can
  accidentally hit SendGrid.
- **Rate limiting**: the limiter is wired up at app-import time (conftest
  forces `RATELIMIT_ENABLED=true` to make `init_app` run its full wiring),
  but a suite-wide autouse fixture flips `limiter.enabled = False` so
  tight per-endpoint limits don't cause flakes. `test_rate_limit.py`
  overrides the autouse fixture to re-enable enforcement and reset the
  in-memory counters between tests.
- **External OAuth**: Google / Apple sign-in are covered in `test_oauth.py`.
  The remote token-verification calls
  (`google.oauth2.id_token.verify_oauth2_token` and
  `jwt.PyJWKClient.get_signing_key_from_jwt` + `jwt.decode`) are stubbed
  per test, so nothing hits Google's tokeninfo or Apple's JWKS endpoint.

## Adding a test

For a route that touches the DB, prefer the `client`/`auth_headers`/`register_user`
fixtures over poking the DB directly. They keep tests tight and behaviour-focused.

For a pure-function module (matchers, parsers, validators), write unit tests
in a new `test_<module>.py` — no DB or `client` needed.
