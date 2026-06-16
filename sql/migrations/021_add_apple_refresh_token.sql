-- sql/migrations/021_add_apple_refresh_token.sql
--
-- Add apple_refresh_token to users so we can revoke a Sign in with Apple grant
-- when the user deletes their account.
--
-- App Store Review Guideline 5.1.1(v) requires that apps offering "Sign in with
-- Apple" also revoke the Apple grant (via Apple's token revocation REST API)
-- when the user deletes their account. To revoke we need an Apple-issued
-- refresh token, which we obtain by exchanging the sign-in authorization_code
-- at https://appleid.apple.com/auth/token. We store that refresh token here and
-- POST it to https://appleid.apple.com/auth/revoke during account deletion.
--
-- Nullable: only populated for users who signed in with Apple AND for whom the
-- one-time authorization_code exchange succeeded. Email/password and Google
-- users leave it NULL.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS apple_refresh_token TEXT;

COMMENT ON COLUMN users.apple_refresh_token IS
    'Apple-issued OAuth refresh token from the Sign in with Apple authorization_code exchange. Used at account deletion to revoke the Apple grant (App Store Guideline 5.1.1(v)). NULL for non-Apple users or when the code exchange failed.';
