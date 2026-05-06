# ApproachNote — YouTube Data API v3 Integration Design

**Document version:** 1.0 (2026-04-23)
**API Project:** ApproachNote (jazz reference application)
**Production endpoint:** https://api.approachnote.com
**Public website:** https://www.approachnote.com
**Apple App Store:** ApproachNote (iOS + macOS)

---

## 1. Application overview

ApproachNote is a reference application for studying jazz music. Users browse jazz standards (the canonical body of repertoire jazz musicians learn and perform), see who has recorded each tune, and listen to those recordings while studying chord changes, melody, and improvisations.

The application is delivered as:

- **Native iOS and macOS apps** (SwiftUI). Users browse songs, performers, and recordings, and tap a recording to listen.
- **Public website** at www.approachnote.com (light marketing surface today).
- **REST API** at api.approachnote.com (Python/Flask) that the apps call. Backed by a PostgreSQL database of jazz standards and their recordings.

YouTube fits in as **one of three streaming sources** the app surfaces for each recording (alongside Spotify and Apple Music). For a given recording — say, John Coltrane's 1959 take of *Giant Steps* — the app tries to find the corresponding video on YouTube and presents it as one option for the user to listen to. The video plays via YouTube's standard playback surface (system browser or YouTube app on iOS/macOS); ApproachNote does not embed an in-app player.

YouTube coverage matters because, for older or live jazz recordings, YouTube is often the **only** source available — Spotify and Apple Music catalogs frequently omit them.

---

## 2. User-facing scenario

1. User opens the iOS or macOS app and navigates to a song (e.g., *Take Five*).
2. User taps a specific recording (e.g., the Dave Brubeck Quartet's 1959 studio recording).
3. The recording detail screen shows a "Streaming Sources" section with a button per available service: **Spotify**, **Apple Music**, **YouTube**.
4. User taps "YouTube". The app opens the corresponding `https://www.youtube.com/watch?v=<videoId>` URL via the system's URL handler. On iOS this opens the YouTube app if installed, otherwise Safari; on macOS it opens the default browser.
5. Playback happens entirely on YouTube's surface — no embedded player, no buffering by ApproachNote, no modification of the playback experience.

The app does **not** display YouTube thumbnails, channel names, or video metadata in the listing. The "YouTube" button simply opens the watch URL. (We may add a small YouTube logo/icon to indicate availability per the YouTube Branding Guidelines; that's a planned UI iteration, not a current feature.)

The relevant client-side code is at:
- iOS: [`apps/iOS/Views/RecordingDetailView.swift`](https://github.com/dprodger/ApproachNote/blob/main/apps/iOS/Views/RecordingDetailView.swift) (lines around 152-153)
- macOS: [`apps/Mac/Views/RecordingDetailView.swift`](https://github.com/dprodger/ApproachNote/blob/main/apps/Mac/Views/RecordingDetailView.swift) (lines around 528-580)

---

## 3. System architecture

```
   ┌──────────────────────┐
   │  iOS / macOS apps    │  ← user clicks "YouTube" → opens youtube.com URL
   └──────────┬───────────┘
              │ HTTPS (REST)
              ▼
   ┌──────────────────────┐
   │  api.approachnote.com│  ← serves recording detail; YouTube link is one
   │  (Flask, on Render)  │     field on the streaming_links JSON for a recording
   └──────────┬───────────┘
              │ Postgres
              ▼
   ┌──────────────────────────────────────────────┐
   │  recording_release_streaming_links table     │  ← one row per (recording, service)
   │  service='youtube', service_url, service_id, │     storing the YouTube video ID
   │  service_title, duration_ms, ...             │     and the link metadata
   └──────────────────────────────────────────────┘
              ▲
              │ writes
              │
   ┌──────────────────────────────┐
   │ Background research worker   │  ← separate Render Background Worker service
   │ (research_worker/run.py)     │     drains a Postgres-backed job queue,
   │                              │     calls the YouTube Data API to find a
   │                              │     match for each new recording, writes
   │                              │     the result to the table above.
   └──────────────────────────────┘
                    │
                    │ HTTPS
                    ▼
        YouTube Data API v3
        (search.list, videos.list)
```

### Backend implementation

- **YouTube client** ([`backend/integrations/youtube/client.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/integrations/youtube/client.py)): thin wrapper around `https://www.googleapis.com/youtube/v3/search` and `videos`. Reads the API key from the `YOUTUBE_API_KEY` environment variable. Maintains an on-disk cache of every response (30-day TTL by default) and a per-process quota counter.
- **Matcher** ([`backend/integrations/youtube/matcher.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/integrations/youtube/matcher.py)): per-recording matching logic — builds search queries, scores candidates, picks the best match.
- **Worker handler** ([`backend/research_worker/handlers/youtube.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/research_worker/handlers/youtube.py)): wraps the matcher with quota-budget accounting against a `source_quotas` table in Postgres (single-source-of-truth across processes).

### Data flow when a new song is added or refreshed

1. iOS/Mac app calls `POST /songs/<id>/refresh`.
2. Backend imports the song's recordings from MusicBrainz (the open music metadata DB).
3. After MusicBrainz import, the backend enqueues one YouTube `match_recording` job per recording onto the Postgres-backed `research_jobs` table.
4. The background worker thread for YouTube claims jobs one at a time (`SELECT … FOR UPDATE SKIP LOCKED`), runs the matcher, and writes the chosen video's ID + metadata into `recording_release_streaming_links`.
5. The iOS/Mac app, on its next read of the recording, sees the new YouTube link and shows the button.

---

## 4. API endpoints we use

We use exactly two YouTube Data API v3 endpoints. No other YouTube product or API surface is touched.

| Endpoint | Quota cost | When called | What we extract |
|---|---|---|---|
| `search.list` (`type=video`, `part=snippet`) | 100 units | Looking for candidate videos for a recording. Up to 3 query variations per recording (song title + artist credit, song title + primary artist, song title only) | `videoId`, `channelTitle`, `title` (used to score candidates) |
| `videos.list` (`part=snippet,contentDetails`) | 1 unit per call (up to 50 IDs per call) | After search, fetch durations + canonical metadata for the candidate set so we can compare against MusicBrainz duration | `videoId`, `title`, `channelTitle`, `duration` |

We do **not** call any of: `channels`, `playlists`, `commentThreads`, `liveChat`, `subscriptions`, `captions`, OAuth-scoped endpoints (no user authentication with YouTube), or the YouTube Player API.

### Quota math (per recording match)

Worst case: 3 searches × 100 units + 1 metadata batch × 1 unit = **301 units**.

Typical case: many recordings share the same query, so the on-disk cache often serves most calls. Median actual cost we observe is in the 50–150 unit range per recording.

The worker's quota-accounting layer pre-deducts the worst case and **refunds the unused units** after the matcher reports actual usage, so the daily counter tracks reality, not pessimistic budget. Code: [`backend/research_worker/handlers/youtube.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/research_worker/handlers/youtube.py) (refund logic in the `finally` block).

---

## 5. Matching algorithm

We do not show YouTube data in a free-text search box. Every API call is in service of finding **the canonical YouTube upload of one specific known recording** in our database. The recording is identified by:

- Song title (e.g., "Giant Steps")
- Primary artist credit (e.g., "John Coltrane")
- MusicBrainz duration in milliseconds (the authoritative source of truth)

The matcher in [`integrations/youtube/matching.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/integrations/youtube/matching.py) scores each candidate video on three axes:

1. **Title similarity** — fuzzy string match between the song title and the video title.
2. **Channel/artist match** — bonus when the video's channel matches the artist (especially YouTube "Topic" channels, which are auto-generated authoritative channels for an artist's catalog).
3. **Duration confidence** — accept only candidates whose duration matches the MusicBrainz duration within a tolerance window (typically ±2 seconds for studio recordings, wider for live).

The highest-scoring candidate above a confidence threshold becomes the match. Below threshold, the recording is recorded as "no match" and not retried until the user explicitly requests a refresh.

This means we are **not** mining YouTube content broadly — every search is targeted at finding the YouTube upload of a recording that exists independently in our database.

---

## 6. Caching and storage

Per YouTube Developer Policies III.E.4.c/d (30-day cache cap on API-derived data), our handling is:

### Local on-disk cache
The `YouTubeClient` writes every API response to a JSON file under `backend/cache/youtube/searches/`. Cache key is the SHA1 of the request parameters; TTL is 30 days. On cache hit, no API call is made.

### Database storage
Every successful match writes one row to `recording_release_streaming_links`:

| Column | Value | Notes |
|---|---|---|
| `service` | `'youtube'` | identifies the source |
| `service_id` | YouTube `videoId` | the canonical 11-character ID |
| `service_url` | `https://www.youtube.com/watch?v=<videoId>` | what the app opens on user click |
| `service_title` | YouTube video title | shown nowhere in the UI today; retained for matcher diagnostics and the future "show what we matched against" admin view |
| `duration_ms` | YouTube video duration | used to spot future drift if the underlying video changes |
| `match_confidence` | 0.0 — 1.0 | matcher's confidence score |
| `match_method` | e.g. `'youtube_duration_match'` | which strategy succeeded |
| `created_at`, `updated_at` | timestamps | used to enforce the 30-day refresh cap |

### Refresh-or-delete policy (Dev Policies III.E.4 compliance)

To stay under the 30-day cap **without** re-matching every row in the catalog every 30 days (which scales poorly), we apply a **selective refresh** model:

1. **Active recordings get refreshed.** A recording is "active" if it has been viewed by a logged-in user, or favorited / added to a repertoire, within the last 30 days. A periodic worker job (one per source on the durable queue we just built) finds active YouTube rows older than ~25 days and re-runs `('youtube', 'match_recording')` with `payload.rematch=true`.
2. **Inactive recordings get deleted.** Any YouTube row whose recording has not been touched by a user in 30 days is **deleted** from `recording_release_streaming_links` once it crosses the 30-day mark. The cached on-disk JSON for that recording's queries is also evicted.
3. **On next user view, re-match on demand.** If a user opens a recording whose YouTube row was deleted, the API returns the recording without a YouTube link and immediately enqueues a fresh `match_recording` job. The worker fills it in (typically within seconds), and the next refresh of the recording detail view shows the YouTube button.

Net effect: cached YouTube data lives **at most 30 days** for every row in the database — refreshed if the recording is being used, deleted if not. Total daily refresh load scales with **active corpus size**, not total corpus size.

Implementation tracking: issue [#168](https://github.com/dprodger/ApproachNote/issues/168).

### What we do NOT store
- We do not store thumbnails, view counts, like counts, comments, channel subscriber counts, or any audience-engagement data.
- We do not store user-specific YouTube data (watch history, playlists, etc.) — we never authenticate against YouTube on a user's behalf.
- We do not download or re-host video files in any form.

---

## 7. Privacy and user data

ApproachNote requires a user account (email + password) to access the API. That account is used for application-internal features (favorites, repertoires, song annotations). We do **not** pass any user identity to YouTube — every YouTube API call is server-side, anonymous (API-key authenticated only), and not tied to a specific user.

End users do not authenticate against YouTube through ApproachNote. We do not request or use any YouTube user data.

Privacy policy: linked from the iOS/macOS app and from www.approachnote.com (in progress before official launch in App Store release). It will explicitly disclose:

- Use of the YouTube Data API v3 to find video matches for recordings.
- Storage of YouTube video IDs + titles + durations in our database (with the 30-day refresh policy).
- That clicking a YouTube link in our app opens YouTube, where Google's privacy policy applies.
- Link to https://policies.google.com/privacy.

---

## 8. Compliance with YouTube API Services Terms of Service

Captured in our internal review at [`doc/commercial-api-terms-review.md`](https://github.com/dprodger/ApproachNote/blob/main/doc/commercial-api-terms-review.md). Highlights:

- **No paywalled YouTube playback** (Dev Policies III.F.3). YouTube links in ApproachNote are accessible without a subscription. If we ever introduce paid tiers, YouTube playback stays free.
- **No ads served against YouTube data** (Dev Policies III.G.1.d). ApproachNote shows no advertising of any kind today, and any future ad implementation will exclude YouTube-derived pages.
- **30-day cache cap** (Dev Policies III.E.4.c/d). Implemented via the selective-refresh + delete model described in §6: active recordings are refreshed within 30 days; inactive ones are deleted at the 30-day mark and re-matched on demand if/when a user opens them again. On-disk cache TTL = 30 days.
- **Feature parity with other streaming sources** (Dev Policies III.C.8). YouTube is shown alongside Spotify and Apple Music with equal visual weight — no demotion in UI ranking.
- **No substantial duplication of YouTube** (Dev Policies III.I.1). ApproachNote is a jazz study application; the YouTube data is one input feeding a much larger context (chord changes, performer biographies, recording histories, repertoire management).
- **Privacy policy** (ToS §7): in active drafting, will be live before the official App Store release.
- **Branding** (Branding Guidelines): the "YouTube" label uses the official wordmark (no recoloring); using a YouTube play icon is on the planned UI iteration list.

---

## 9. Why we are requesting an extended quota

We currently operate at the default 10,000 unit/day quota. Our recording catalog is **~100,000 today** and is projected to roughly **double to ~200,000 over the next 12 months** as we expand coverage of historic and live recordings. Our user base is currently small and projected at **<5,000 registered users by the end of the same 12-month window**.

The selective-refresh + delete model described in §6 means daily quota consumption scales with the **active** corpus (recordings actually being viewed/favorited), not the total corpus. A reasonable working assumption is that ~25% of recordings see user activity in any given 30-day window — typical for a long-tail reference catalog where users gravitate toward popular standards.

### Per-call cost reference

| API method | Units per call | When invoked |
|---|---:|---|
| `search.list` (`type=video`, `part=snippet`) | **100** | Up to 3 query variants per match (title+artist credit / title+primary artist / title alone). |
| `videos.list` (`part=snippet,contentDetails`) | **1** | One batched call per match (up to 50 candidate IDs). |

### Per-recording match cost

| Scenario | search.list | videos.list | Total units |
|---|---:|---:|---:|
| Best case (one query hits, mostly cache) | 1 × 100 | 1 × 1 | **101** |
| Median observed | ~1.5 × 100 | 1 × 1 | **~150** |
| Worst case (all 3 query variants miss cache) | 3 × 100 | 1 × 1 | **301** |

### Daily volume at projected scale

Modelling for the 12-month projection (~200,000 total recordings, ~50,000 active):

| Workload | Daily volume | Median units | Subtotal |
|---|---:|---:|---:|
| **30-day selective refresh** of active corpus (50,000 ÷ ~25 days) | 2,000 matches | 250 | 500,000 |
| **New recordings** from song additions (~5 songs/day × ~25 recordings) | 125 matches | 250 | 31,250 |
| **On-demand re-matches** when users open recordings whose stale rows were deleted | ~150 matches | 300 | 45,000 |
| **Admin re-matches / quality fixes** (manual triggers from /admin/research/) | 50 matches | 250 | 12,500 |
| **Burst/safety buffer** (batch imports, retries, growth in active corpus) | — | — | ~150,000 |
| **Daily total** | **~2,325 matches** | — | **~740,000 units** |

Translating to HTTP call counts: 2,325 matches × ~3.5 calls/match ≈ **~8,000 HTTP requests/day**.

The 1,000,000 unit/day request gives us comfortable headroom (~25%) over the projected steady-state load while staying within a single bucket that can be reliably budgeted.

### Why 1,000,000 specifically (not 2M+ for the full corpus)

Refreshing the **entire** 200,000-recording corpus on a 30-day cycle would require ~2M units/day. We deliberately chose the smaller ask, paired with the selective-refresh + delete model, because:

- **It keeps us reliably III.E.4-compliant.** Stale rows for inactive recordings are deleted, not held indefinitely. The refresh worker only spends API budget on recordings users are actually using.
- **It avoids spending API budget on data nobody reads.** A long-tail reference app like ours has many recordings that are rarely viewed; pre-emptively refreshing them doesn't help users.
- **It scales with usage, not catalog size.** As we add more recordings, daily quota grows only with the share of those recordings that becomes active.

### Without the increase

At the default 10,000 unit/day quota we cannot:

- Run the selective-refresh worker — even ~40 active-recording refreshes/day would consume the entire bucket.
- Continue enriching the catalog at our current new-song cadence without immediately running out of headroom.
- Stay III.E.4 compliant: the cached rows we already have would age past 30 days with no mechanism to refresh or delete them within budget.

---

## 10. Code references (for reviewers)

If a Google reviewer wants to verify any of the above, the relevant code is available at a private repository at https://github.com/dprodger/ApproachNote: if necessary, I can grant access to the code repository for inspection.

- **API client** (the only place we issue HTTP requests to YouTube): [`backend/integrations/youtube/client.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/integrations/youtube/client.py)
- **Matcher** (per-recording scoring + selection): [`backend/integrations/youtube/matcher.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/integrations/youtube/matcher.py)
- **Quota-aware worker handler** (where pre-deduct + refund happens): [`backend/research_worker/handlers/youtube.py`](https://github.com/dprodger/ApproachNote/blob/main/backend/research_worker/handlers/youtube.py)
- **Database schema** for stored YouTube link rows: [`sql/jazz-db-schema.sql`](https://github.com/dprodger/ApproachNote/blob/main/sql/jazz-db-schema.sql) — search for `recording_release_streaming_links`
- **iOS user-facing display**: [`apps/iOS/Views/RecordingDetailView.swift`](https://github.com/dprodger/ApproachNote/blob/main/apps/iOS/Views/RecordingDetailView.swift)
- **macOS user-facing display**: [`apps/Mac/Views/RecordingDetailView.swift`](https://github.com/dprodger/ApproachNote/blob/main/apps/Mac/Views/RecordingDetailView.swift)
- **Terms-of-service compliance review**: [`doc/commercial-api-terms-review.md`](https://github.com/dprodger/ApproachNote/blob/main/doc/commercial-api-terms-review.md)

---

## 11. Contact

- Developer: David Rodger (dave@davidrodger.com)
- Repository: https://github.com/dprodger/ApproachNote
- API project: ApproachNote
