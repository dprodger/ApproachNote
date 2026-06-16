# Sign in with Apple — token revocation (App Store 5.1.1(v))

App Store Review Guideline **5.1.1(v)** requires that an app offering "Sign in
with Apple" **also revokes the Apple grant** (via Apple's token-revocation REST
API) when the user deletes their account. In-app account deletion alone is not
sufficient for Sign-in-with-Apple users.

## How it works

1. **Client (iOS/Mac)** — at sign-in, `signInWithApple` in
   `apps/Shared/Auth/AuthenticationManager.swift` reads
   `ASAuthorizationAppleIDCredential.authorizationCode` (a one-time code) and
   forwards it to the backend as `authorization_code` on `POST /auth/apple`.

2. **Backend exchange** — `routes/auth.py` `apple_login()` calls
   `core.apple_auth.exchange_code_for_refresh_token()`, which POSTs the code to
   `https://appleid.apple.com/auth/token` with a signed `client_secret` JWT and
   receives an Apple **refresh token**. That token is stored on
   `users.apple_refresh_token` (migration `021_add_apple_refresh_token.sql`).
   The exchange is **best-effort**: a failure logs a warning and never blocks
   sign-in (we simply won't be able to revoke later).

3. **Backend revoke** — `delete_account()` reads `users.apple_refresh_token`
   and, if present, calls `core.apple_auth.revoke_refresh_token()` which POSTs
   to `https://appleid.apple.com/auth/revoke` before deleting the user row.
   Also best-effort: revocation failures are logged but never block deletion.

The `client_secret` is a short-lived ES256 JWT signed with a **Sign in with
Apple** `.p8` private key (`core.apple_auth.generate_client_secret`). Its `sub`
is the app's bundle identifier (the `client_id`) and its `aud` is
`https://appleid.apple.com`.

### Which bundle ID (client_id)?

The iOS and Mac apps have distinct bundle IDs (`com.approachnote.ios`,
`com.approachnote.mac`), and each is its own Apple `client_id`. A refresh token
is bound to the `client_id` it was issued under. We don't persist which one, so
`revoke_refresh_token()` simply tries each configured bundle ID
(`APPLE_BUNDLE_IDS`) until Apple accepts the revocation — there are only a
couple, and the call is best-effort anyway.

## Required environment variables

Sign in with Apple uses a **different Apple key** than the Apple Music Feed
importer (which owns `APPLE_KEY_ID` / `APPLE_PRIVATE_KEY_PATH` for the Media
API). To avoid clobbering that key, the SIWA helpers read their **own** vars:

| Variable                        | Meaning                                                                 |
| ------------------------------- | ----------------------------------------------------------------------- |
| `APPLE_SIGNIN_KEY_ID`           | Key ID of the **Sign in with Apple** key created in the Developer portal |
| `APPLE_SIGNIN_PRIVATE_KEY_PATH` | Path to that key's `.p8` file                                           |
| `APPLE_SIGNIN_TEAM_ID`          | Apple Developer Team ID (falls back to `APPLE_TEAM_ID` — same team)     |
| `APPLE_BUNDLE_IDS`              | Comma-separated bundle IDs / client_ids (already used for token `aud`)  |

If these are not configured, `apple_auth.is_configured()` returns false and the
exchange/revoke steps are skipped with a logged warning — the rest of auth still
works, but the app is **not** 5.1.1(v) compliant until they're set in
production.

## Apple Developer portal setup

Sign in with Apple uses a **Key** (Certificates, Identifiers & Profiles →
Keys), distinct from the MusicKit/Media key the Apple Music Feed importer uses.

- **One key covers both apps.** Create a single Sign-in-with-Apple key and
  associate it with the **primary App ID** `com.approachnote.ios`. The Mac App
  ID `com.approachnote.mac` is **grouped** under that primary, so the same key
  is authorized to sign `client_secret` JWTs for both apps ("This key will also
  be used for any App IDs grouped with the primary"). Do **not** create a
  separate key for the Mac app.
- **Team ID** is `FX893D85BJ` (the prefix on every App ID, e.g.
  `FX893D85BJ.com.approachnote.ios`). Set it as `APPLE_SIGNIN_TEAM_ID`, or rely
  on the `APPLE_TEAM_ID` fallback.
- **`client_id` = the app's bundle ID.** These native apps authenticate with
  their bundle identifier, which is the `aud` of the identity token
  (`com.approachnote.ios` from iOS, `com.approachnote.mac` from Mac). Both go in
  `APPLE_BUNDLE_IDS`. The grouped share extensions / web admin App IDs don't
  initiate sign-ins against the backend, so they're not needed there.
- **Download the `.p8` immediately** — Apple lets you download a key's private
  file only once. Store it where `APPLE_SIGNIN_PRIVATE_KEY_PATH` points, and
  note its Key ID for `APPLE_SIGNIN_KEY_ID`.

## Deploying to Render

**Never commit the `.p8`** — it's a private key and would live in git history
forever. (The Apple Music Media key is already gitignored; do the same here.)
Render delivers it as a **Secret File**, exactly like the Apple Music key's
`APPLE_PRIVATE_KEY_PATH`.

The revoke/exchange runs in the `/auth/*` routes, which are served by the
**`approachnote-api` web service** — *not* the research worker that holds the
Apple Music key. So put the SIWA credentials on the **web** service:

1. Render dashboard → `approachnote-api` → **Environment → Secret Files** →
   *Add Secret File*. Filename e.g. `AuthKey_XXXXXXXXXX.p8` (Render mounts it at
   `/etc/secrets/AuthKey_XXXXXXXXXX.p8`), and paste the `.p8` contents. Secret
   Files persist across deploys — they're stored in Render's config, not the
   ephemeral checkout.
2. Set the env vars on the same service (declared `sync: false` in
   `render.yaml`):
   - `APPLE_SIGNIN_PRIVATE_KEY_PATH` = `/etc/secrets/AuthKey_XXXXXXXXXX.p8`
   - `APPLE_SIGNIN_KEY_ID` = the key's Key ID
   - `APPLE_SIGNIN_TEAM_ID` = `FX893D85BJ` (optional — falls back to the
     `APPLE_TEAM_ID` already set on this service)
   - `APPLE_BUNDLE_IDS` should already be set (used for identity-token
     verification); the revoke flow reuses it.
3. Redeploy. Verify in a shell: `ls -l /etc/secrets/` and `env | grep APPLE_SIGNIN`.

## Tests

- `backend/tests/test_apple_auth.py` — unit tests for `client_secret` signing
  and the exchange/revoke HTTP plumbing (outbound session mocked).
- `backend/tests/test_auth.py` — integration tests through `/auth/apple`
  (stores the refresh token) and `/auth/delete-account` (revokes, and still
  deletes when revocation fails).
