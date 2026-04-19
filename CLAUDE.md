# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Approach Note is a reference application for jazz music study, consisting of:
- **Backend**: Flask API (Python 3.13) serving data from PostgreSQL
- **iOS/Mac Apps**: SwiftUI apps for browsing jazz standards, performers, and recordings

The API is deployed at `https://api.approachnote.com`.

## Development Commands

### Backend

```bash
# Setup
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run locally (port 5001, includes research worker)
python app.py

# Run with gunicorn (production-like)
gunicorn -c gunicorn.conf.py app:app
```

### iOS App

Open `apps/Approach Note.xcodeproj` in Xcode. The app targets iOS and macOS, using SwiftUI.

## Architecture

### Backend Structure

```
backend/
├── app.py                      # Flask app entry point, blueprint registration
├── config.py                   # Logging and app configuration
├── db_utils.py                 # PostgreSQL connection pooling (psycopg3)
├── rate_limit.py               # Rate-limiting helpers
├── monitor.py                  # Process/health monitoring utilities
├── routes/                     # API route blueprints
│   ├── songs.py                # /api/songs endpoints
│   ├── recordings.py           # /api/recordings endpoints
│   ├── performers.py           # /api/performers endpoints
│   ├── repertoires.py          # /api/repertoires (auth-protected)
│   ├── transcriptions.py
│   ├── auth.py                 # JWT authentication
│   ├── password.py             # Password reset flow
│   └── ...
├── core/                       # Cross-cutting modules
│   ├── research_queue.py       # Background worker queue for data enrichment
│   ├── song_research.py        # Orchestrates MusicBrainz + Spotify + Apple Music imports
│   ├── auth_utils.py           # JWT auth helpers (@token_required decorator)
│   ├── email_service.py        # Password-reset email delivery
│   └── cache_utils.py          # Shared caching helpers
├── integrations/               # Per-source API clients and importers
│   ├── spotify/                # client, matcher, matching, db, utils
│   ├── apple_music/            # client, matcher, feed, db
│   ├── musicbrainz/            # utils, release_importer, performer_importer
│   ├── coverart/               # Cover Art Archive: release_importer, utils
│   └── wikipedia/              # utils
└── scripts/                    # Data import/maintenance scripts
```

### Key Backend Patterns

- **Blueprint Registration**: All routes in `routes/__init__.py` via `register_blueprints()`
- **Connection Pooling**: Uses psycopg3 connection pool; set `DB_USE_POOLING=true` before importing `db_utils`
- **Background Worker**: `core.research_queue` runs in-process thread for async data enrichment
- **JWT Auth**: `core.auth_utils` provides `@token_required` decorator for protected endpoints
- **Import paths**: Modules are imported as `from integrations.spotify import matcher` / `from core import research_queue`. `backend/` itself is not a package; `backend/scripts/script_base.py` adds it to `sys.path` so both `core.*` and `integrations.*` resolve.

### Backend tests

Pytest suite under `backend/tests/`. See `backend/tests/README.md` for local
setup. CI runs them in `.github/workflows/pytest.yml` against a Postgres
`services:` container. Auth flow is covered today; matchers / research queue
/ rate-limit smoke are follow-up issues.

### Apps Structure

The `apps/` directory contains iOS, macOS, and shared code:

```
apps/
├── Shared/                      # Code shared between iOS and Mac
│   ├── Auth/
│   │   ├── AuthenticationManager.swift
│   │   ├── KeychainHelper.swift
│   │   └── Models/User.swift
│   ├── Managers/
│   │   ├── FavoritesManager.swift
│   │   └── RepertoireManager.swift
│   └── Support/
│       ├── Models.swift         # Data models
│       ├── NetworkManager.swift # API client (async/await)
│       ├── ApproachNoteTheme.swift      # UI theming
│       ├── HelperViews.swift
│       └── PreviewHelpers.swift # SwiftUI preview data
├── iOS/                         # iOS-specific code
│   ├── App/
│   │   └── ApproachNoteApp.swift
│   ├── Auth/Views/              # iOS auth views
│   ├── Components/              # Reusable UI components
│   ├── Managers/                # iOS-only managers
│   ├── Support/                 # iOS-only support (CachedAsyncImage, Toast)
│   └── Views/                   # iOS views
├── Mac/                         # macOS-specific code
│   ├── App/
│   │   └── ApproachNoteMacApp.swift
│   ├── Auth/                    # Mac auth views
│   └── Views/                   # Mac views
├── ShareImporter/               # iOS share extension
└── ShareImporterMac/            # macOS share extension
```

### Model Changes Checklist

Models live in per-domain files under `Shared/Models/`:
- `Song.swift`, `Recording.swift`, `Performer.swift`, `Release.swift`
- `CommunityData.swift`, `Repertoire.swift`, `SoloTranscription.swift`, `Video.swift`
- `AuthorityRecommendation.swift`, `MusicBrainz.swift`

When adding or modifying fields:
1. Update the struct definition with the new field
2. Update the `CodingKeys` enum if the API field name differs
3. **Update `Shared/Support/PreviewHelpers.swift`** - add the new field to ALL preview instances of that model (e.g., `Recording.preview1`, `Recording.preview2`, `Recording.previewMinimal`)

### Swift Coding Conventions

**Logging**: Never use `print()` in Swift code. Use `os.Logger` via the shared `Log` enum in `Shared/Support/Logging.swift`:

```swift
import os

// Categories
Log.network.debug("GET /songs returned \(count, privacy: .public) results")
Log.auth.info("User logged in")
Log.ui.error("Failed to load view: \(error.localizedDescription)")
Log.data.debug("Saved repertoire")
Log.research.info("Song queued for refresh")

// Log levels
.debug   // Diagnostic trace, API call details
.info    // Success, completion messages
.warning // Conflicts, degraded states
.error   // Failures, catch blocks

// Privacy: redact sensitive values in release builds
Log.auth.info("Login: \(email, privacy: .private)")
Log.network.debug("Status: \(statusCode, privacy: .public)")
```

For the `ShareImporter` / `ShareImporterMac` targets (separate targets, can't access `Log`), create a local `Logger`:
```swift
private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "com.approachnote.ShareImporter", category: "data")
```

**Services architecture**: API calls live in per-domain services under `Shared/Services/` (not in views or view models):
- `SongService`, `PerformerService`, `RecordingService` — stateful (`@MainActor class: ObservableObject`)
- `ResearchService`, `FavoritesService`, `ContributionService`, `MusicBrainzService`, `ContentService` — stateless
- `APIClient` — shared infrastructure (baseURL, diagnostics, URL helpers)

### Database Schema

Core tables (see `sql/jazz-db-schema.sql`):
- `songs` - Jazz standards with MusicBrainz work IDs
- `recordings` - Specific recordings linked to songs
- `performers` - Artists with MusicBrainz artist IDs
- `releases` - MusicBrainz releases with cover art
- `recording_performers` - Junction table with instrument roles
- `users` - Authentication (email/password, Google, Apple)
- `repertoires` / `repertoire_songs` - User song collections

#### Dual MusicBrainz work IDs on `songs`

`songs` carries both `musicbrainz_id` (primary MB work ID) and `second_mb_id`
(secondary MB work ID). Some standards exist as multiple works in MusicBrainz
— for example a tune split across two work entries, or an alternate version
merged into a separate work. When `second_mb_id` is populated, the importer
pulls recordings from both works and tags each recording's
`recordings.source_mb_work_id` with the MB work it came from, so admin tools
can review which recordings originated from the secondary work.

The columns carry `COMMENT ON COLUMN` documentation in the database itself —
see `sql/migrations/002_add_second_mb_id.sql` (introduction) and
`sql/migrations/011_drop_redundant_recording_releases_unique.sql` (refresh).

### External Data Sources

- **MusicBrainz**: Work IDs, recording metadata, performer credits, releases
- **Spotify**: Track matching, album art, streaming links
- **Apple Music**: Track matching, album art via Feed API (bulk catalog) or iTunes Search API
- **Cover Art Archive**: Album artwork via MusicBrainz release IDs
- **Wikipedia**: Artist biographies, song background info

### Data Flow

1. Songs/performers added via admin scripts or iOS share extension
2. `research_queue` triggers background import from MusicBrainz
3. `spotify_matcher` matches recordings to Spotify tracks
4. `apple_music_matcher` matches recordings to Apple Music tracks
5. `caa_release_importer` fetches cover art for releases
6. iOS app fetches enriched data via REST API

## Environment Variables

Backend requires `.env` with:
- `DATABASE_URL` - PostgreSQL connection string
- `JWT_SECRET_KEY` - For authentication tokens
- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` - Spotify API
- `SENDGRID_API_KEY` - For password reset emails

### Apple Music Feed (Optional - for bulk catalog matching)

The Apple Music matcher can use either:
1. **Local catalog** (recommended) - Downloaded via Apple Music Feed API, no rate limits
2. **iTunes Search API** - Free public API but has aggressive rate limiting

To use the Feed API for downloading the full Apple Music catalog:

1. Requires Apple Developer Program membership ($99/year)
2. Create a Media ID in App Store Connect:
   - Go to Users and Access → Integrations → Media API
   - Create a new key and download the .p8 file
3. Set environment variables:
   - `APPLE_MEDIA_ID` - Your Media ID
   - `APPLE_PRIVATE_KEY_PATH` - Path to your .p8 private key file
   - `APPLE_KEY_ID` - The Key ID shown in App Store Connect
   - `APPLE_TEAM_ID` - Your Team ID (Account → Membership)
4. Download the catalog:
   ```bash
   python scripts/download_apple_catalog.py --feed albums
   python scripts/download_apple_catalog.py --feed songs
   ```

Without Feed API credentials, the matcher falls back to the iTunes Search API.
