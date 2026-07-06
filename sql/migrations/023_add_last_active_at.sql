-- sql/migrations/023_add_last_active_at.sql
--
-- Add users.last_active_at: a "last used the product" timestamp, distinct from
-- last_login_at ("last walked through a sign-in screen").
--
-- last_login_at is only written by the explicit auth endpoints (password /
-- Google / Apple sign-in). A user who signed in once and keeps using the app on
-- a still-valid 30-day refresh token never re-authenticates, so their
-- last_login_at freezes at that first sign-in even while they're active daily.
--
-- last_active_at is instead stamped on token refresh (routes/auth.py
-- refresh_token). With 15-minute access tokens, an actively-used client hits
-- /auth/refresh-token roughly every 15 minutes, making refresh a natural,
-- self-throttled session heartbeat — no per-request DB write. Login also bumps
-- it, so a fresh sign-in counts as activity immediately.
--
-- Existing rows are backfilled from last_login_at so the column isn't uniformly
-- NULL at launch.
--
-- Run: psql $DATABASE_URL -f sql/migrations/023_add_last_active_at.sql

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMP WITH TIME ZONE;

COMMENT ON COLUMN users.last_active_at IS
    'Last time the user was seen actively using the product. Stamped on token refresh (a ~15-minute session heartbeat given 15-minute access tokens) and on login. Distinct from last_login_at, which only tracks explicit sign-in events.';

-- Seed from the last known sign-in so pre-existing users have a sensible value.
UPDATE users
SET last_active_at = last_login_at
WHERE last_active_at IS NULL
  AND last_login_at IS NOT NULL;

-- Admin user list orders by recent activity.
CREATE INDEX IF NOT EXISTS idx_users_last_active_at
    ON users(last_active_at DESC NULLS LAST);

-- Rollback:
--   DROP INDEX IF EXISTS idx_users_last_active_at;
--   ALTER TABLE users DROP COLUMN IF EXISTS last_active_at;
