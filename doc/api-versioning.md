# API versioning

Public client endpoints live under a version prefix on the API host:

```
https://api.approachnote.com/v1/songs/<id>
https://api.approachnote.com/v1/auth/login
```

## What is and isn't versioned

| Surface | Path | Versioned? |
|---|---|---|
| Public client API (songs, recordings, performers, repertoires, transcriptions, videos, favorites, contributions, authority, musicbrainz, images, content-reports, research) | `/v1/...` | yes |
| Auth + password | `/v1/auth/...` | yes |
| Health probe | `/health` | no — load balancers hit it |
| Admin web UI | `/admin/...` (on `admin.approachnote.com`) | no — internal, evolves with the maintainer |

The version prefix is applied at blueprint-registration time in
`backend/routes/__init__.py` (and `app.py` for the auth blueprints), so
individual route decorators stay path-only.

## What counts as a breaking change (bumps the version)

- Removing a field from a response
- Renaming a field
- Changing a field's type or units
- Changing the semantics of an existing field
- Removing an endpoint
- Tightening request validation in a way that previously-valid requests
  now fail

## What doesn't bump the version (additive)

- Adding a new optional field to a response
- Adding a new endpoint
- Adding a new optional query parameter
- Performance, logging, internal refactors

When in doubt: would a client decoding the response with the previous
schema still work? If yes, additive. If no, breaking.

## Adding `/v2` later

When the first breaking change lands:

1. Register the affected blueprint(s) under both `/v1` and `/v2` (or
   create a `_v2` variant for the diverging endpoints).
2. Introduce a serializer indirection only for the endpoints that
   actually diverge — `serialize_song(row, version=2)`. Most handlers
   stay shared; only the response shaping branches.
3. Pin the new shape in tests under `test_<resource>_v2.py`.

The current code does not pre-emptively factor out serializers because
`/v1` is the only version. Spreading an empty branch across 14
blueprints is exactly the premature abstraction we want to avoid; we
add it where the divergence actually lives, when it lives.

## Support window

There's no public commitment yet because there are no public clients
yet. Once there are, the rough plan is: support N-1 for at least one
full app-store release cycle after a breaking change ships, then drop.

## Client override (debug builds only)

The iOS / Mac apps centralise the version path in
`apps/Shared/Services/APIClient.swift`:

```swift
static var apiVersionPath: String {
    #if DEBUG
    if let override = UserDefaults.standard.string(forKey: "APIVersionOverride") {
        return override
    }
    #endif
    return "/v1"
}
```

In a debug build, set the `APIVersionOverride` UserDefaults key (e.g. to
`"/v2"`, or `""` to hit the unversioned legacy surface if it's ever
re-introduced) and relaunch. Release builds ignore the override
entirely, so a stray defaults entry can't pin a TestFlight build to a
stale version.

The share-extension targets (`ShareImporter`, `ShareImporterMac`) can't
link `APIClient`, so they hold their own `apiVersionPath` constant in
`apps/ShareImporter/DatabaseServices.swift`. Keep both in sync when the
default version changes.
