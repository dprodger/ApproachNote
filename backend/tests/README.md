# Backend tests

Pytest suite for the Flask backend. Currently covers the auth flow; matchers,
research queue, and rate-limit smoke tests are tracked as follow-up issues.

## Running locally

You need a Postgres reachable via the same env vars the app uses
(`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`). Bootstrap a
test database from the schema before the first run:

```bash
createdb jazz_test
psql jazz_test -f sql/jazz-db-schema.sql
for f in sql/migrations/[0-9]*.sql; do psql jazz_test -f "$f" || true; done
```

Install dev deps and run the suite:

```bash
cd backend
pip install -r requirements.txt -r requirements-dev.txt

DB_HOST=localhost DB_PORT=5432 DB_NAME=jazz_test \
  DB_USER=$(whoami) DB_PASSWORD='' \
  JWT_SECRET=pytest-test-secret RATELIMIT_ENABLED=false \
  pytest tests/ -v
```

CI runs the equivalent in `.github/workflows/pytest.yml` against a
Postgres `services:` container.

## Production-safety guard

`conftest.py` refuses to run the suite unless `DB_NAME` contains `test`
(case-insensitive). This exists because the fixtures issue `TRUNCATE`
against the auth tables and individual test modules INSERT/DELETE into
`recording_release_streaming_links` — if you accidentally point the
test env at your production DB, those ops will wipe real data. The
April 2026 incident was exactly this mistake.

If you genuinely need to run against a DB whose name doesn't include
`test`, set `PYTEST_I_KNOW_THIS_ISNT_PROD=1` to bypass the check. A
warning is logged to stderr every run when the bypass is active. Don't
make a habit of it.

## Conventions

- **Test isolation**: an autouse fixture in `conftest.py` `TRUNCATE`s
  `users`, `refresh_tokens`, and `password_reset_tokens` after every test.
  Don't rely on rows surviving across tests.
- **Email**: `core.email_service.send_*` and the `routes.auth` import-site
  bindings are mocked out by another autouse fixture. No tests can
  accidentally hit SendGrid.
- **Rate limiting**: disabled via `RATELIMIT_ENABLED=false` in the test env.
  Tests that specifically exercise rate-limiter behavior will need to
  re-enable it in their own fixture.
- **External OAuth**: Google / Apple sign-in is not covered yet — both
  require mocking remote JWKS clients and live in a follow-up issue.

## Adding a test

For a route that touches the DB, prefer the `client`/`auth_headers`/`register_user`
fixtures over poking the DB directly. They keep tests tight and behaviour-focused.

For a pure-function module (matchers, parsers, validators), write unit tests
in a new `test_<module>.py` — no DB or `client` needed.
