# Streaming Services Design Doc

How the Spotify and Apple Music matchers work. Written for a developer who has
just opened `backend/integrations/` for the first time and wants to know what
this machinery does before reading any code.

## What the matchers do

We get our canonical data (songs, recordings, releases, performers) from
MusicBrainz. MusicBrainz doesn't give us playable links, cover art, or preview
audio — the streaming services do. The job of the matchers is to take each of
our MB-sourced **releases** and find the corresponding **album on Spotify** and
**album on Apple Music**, then dig into that album and find the specific
**track** that corresponds to each of our **recordings**.

The happy-path output for a song with N releases is:

- `release_streaming_links` rows for each release × service it was matched on,
  with the service's album ID, artwork URLs, and a confidence score.
- `recording_release_streaming_links` rows for each track inside each matched
  album, with the service's track ID, duration, preview URL, ISRC, etc.

This is what powers the "Play on Spotify" / "Play on Apple Music" buttons, the
album art, the preview clips, and the duration cross-checks.

## Two services, one shape

Both integrations live under `backend/integrations/<service>/` and follow the
same package layout. The names line up deliberately so a new developer who
learns one can read the other.

```
integrations/
├── spotify/
│   ├── client.py       HTTP, OAuth token, rate limit, response cache
│   ├── search.py       Ladder of search strategies against /v1/search
│   ├── matching.py     Text normalization + fuzzy scoring (service-agnostic)
│   ├── diagnostics.py  Track-match failure cache + CSV audit loggers
│   ├── db.py           All SQL — read releases, write streaming_links
│   ├── matcher.py      SpotifyMatcher orchestration class
│   └── utils.py        Thin facade re-exporting SpotifyMatcher (legacy)
│
├── apple_music/
│   ├── client.py       HTTP (iTunes Search API), rate limit, response cache
│   ├── search.py       Ladder of search strategies (catalog OR API)
│   ├── matching.py     Apple-specific validators (reuses spotify/matching.py primitives)
│   ├── feed.py         Apple Music Feed bulk catalog downloader (optional)
│   ├── db.py           All SQL — read releases, write streaming_links
│   └── matcher.py      AppleMusicMatcher orchestration class
```

A few notes on the shape:

- **`matching.py` is stateless.** The normalization (strip accents, lowercase,
  strip "feat. X", strip parentheticals), similarity scoring (`rapidfuzz`),
  ensemble-suffix stripping (`Bill Evans Trio` → `Bill Evans`), and first-name
  variant tables (`Dave` ↔ `David`, `Bill` ↔ `William`) are all pure functions.
- **`apple_music/matching.py` reuses `spotify/matching.py`** for the primitives
  — they're service-agnostic. Apple's `matching.py` only adds the
  Apple-specific validator composition (album validation with compilation-artist
  handling, track matching with disc/track position bonus). If you touch jazz
  nickname handling, you touch it in one place.
- **`matcher.py` is the only module with stats bookkeeping.** Callers get a
  single `stats` dict back. Counters owned by the client (`api_calls`,
  `cache_hits`, `rate_limit_hits`) are pulled forward into `matcher.stats`
  via `_aggregate_client_stats()` before returning.
- **Search functions take `matcher` as their first arg.** They need the
  client (for HTTP), the logger, and the similarity thresholds
  (`min_artist_similarity`, `min_album_similarity`, `min_track_similarity`).
  Passing the matcher in is pragmatic — a cleaner refactor would pass
  `(client, logger, thresholds)` explicitly.

## Execution flow: `match_releases(song)`

The main entrypoint on both matchers is `match_releases(song_identifier)`.
Here's what happens end-to-end when the background worker calls it:

1. **Resolve the song.** `db.find_song_by_id()` or `find_song_by_name()`
   pulls the song row, including any `alt_titles` we'll fall back to if the
   primary title doesn't match.

2. **Load our releases.** `db.get_releases_for_song()` returns every release
   linked to this song, along with whatever streaming data is already there.
   We iterate release-by-release.

3. **Per release, decide: skip, rematch, or fresh search.**
   - Already has a streaming link and not in rematch mode → skip.
   - Already searched with no match (negative cache) → skip unless
     `rematch_failures=True`.
   - Otherwise → fresh search.

4. **Search the service.** `search.search_and_validate_album(matcher, artist,
   album, year)` tries a ladder of strategies (full artist + album, strip
   ensemble suffix, strip "(Live at ...)" suffix, primary-artist-only for
   collaborations, album-only, punctuation-stripped, main-title-before-colon)
   and returns the first candidate that passes `validate_album_match`.

5. **Validate.** `matching.validate_album_match(matcher, candidate, ...)`
   scores artist similarity and album similarity against the matcher's
   thresholds (strict mode: 75/65/85; loose mode: 65/55/75 — artist/album/track
   respectively). Compilation artists ("Various Artists") are flagged — a
   compilation-vs-single-artist mismatch is a hard reject to prevent false
   positives. Name variants (`Dave` → `David`) are tried as a second pass if
   the primary similarity fails.

6. **Write album-level data.** `db.upsert_release_streaming_link()` (Apple) or
   `db.update_release_spotify_data()` (Spotify) writes the album ID, URL,
   artwork, and confidence score.

7. **Match tracks inside the album.** The matcher fetches the album's
   tracklist (from the service's API or local catalog) and then, for each of
   our recordings linked to this release, `matching.find_matching_track`
   picks the best candidate by title similarity + a position bonus when the
   disc/track numbers line up. Spotify adds a duration-confidence check that
   can hard-reject a title match if the track length is wildly off (with an
   "album context rescue" escape hatch for obvious good matches on otherwise
   unique tracklists).

8. **Write track-level data.** `db.upsert_track_streaming_link()` stores the
   track ID, preview URL, duration, and ISRC on the `recording_releases`
   junction.

9. **Negative-cache the misses.** If the album was found but no track inside
   it matched, Spotify writes a `track_failures/` JSON file so the next run
   skips the round trip. If no album matched at all, Apple writes
   `mark_release_searched` on the release row so it stays skipped until an
   explicit rematch.

## Where Spotify and Apple diverge

Mostly the shape is identical. Two real differences:

### Auth

- **Spotify** needs OAuth client credentials. `SpotifyClient` manages a
  `(access_token, token_expires)` pair and refreshes on demand using
  `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET`.
- **Apple Music** (iTunes Search API) is public. No auth. Just hit
  `https://itunes.apple.com/search?term=...`.

### Data source: API vs. local catalog

Apple Music has a second data source that Spotify doesn't: the Apple Music
**Feed API** provides bulk catalog downloads (Parquet files delivered via an
Apple Developer account). We load that into a local DuckDB (or MotherDuck)
database via `apple_music/feed.py`, and `apple_music/search.py` prefers local
catalog lookups over the iTunes API when the catalog is available.

Why: the iTunes Search API has aggressive rate limits that make batch matching
slow. The local catalog has no rate limits and can return dozens of candidates
in one query. It's enabled via `APPLE_MUSIC_CATALOG_DB=md:apple_music_feed`
(MotherDuck) or falls back to a local DuckDB file. If it's not configured,
the matcher transparently falls back to the iTunes API.

The `AppleMusicMatcher` also supports `local_catalog_only=True` — useful for
"catalog-only" batch runs where you don't want any network traffic. Without
that flag, local misses fall through to the iTunes API.

## How matchers get invoked

Three entry points, all go through `AppleMusicMatcher` / `SpotifyMatcher`:

1. **Background research worker** — `core.song_research.research_song()`
   runs on the `research_queue` thread whenever a song needs refresh. This is
   the 90% path. Triggered by admin-UI refresh requests and the iOS share
   extension.

2. **Admin routes** — `routes/admin.py` exposes per-song "re-match Spotify" /
   "re-match Apple Music" endpoints for ad-hoc use in the admin UI.

3. **CLI scripts** — `scripts/match_spotify_tracks.py`,
   `scripts/match_apple_tracks.py`, plus a bunch of backfill / audit scripts
   (`backfill_apple_artwork.py`, `audit_spotify_tracks.py`, etc.) for bulk
   operations and data-quality work.

All three construct a matcher with the same knobs (`dry_run`, `strict_mode`,
`force_refresh`, `rematch`, `rematch_failures`, `artist_filter`,
`progress_callback`) and call `match_releases(song)`.

## Persistence

Two normalized tables hold the output. Any streaming service fits the same
shape — the `service` column is `'spotify'` or `'apple_music'`.

```
release_streaming_links              → album-level data
  (release_id, service) UNIQUE
  service_id, service_url, artwork_*, match_confidence, match_method

recording_release_streaming_links    → track-level data
  (recording_release_id, service) UNIQUE
  service_id, service_url, duration_ms, preview_url, isrc, service_title,
  match_confidence, match_method
```

`match_method` is one of `fuzzy_search` (the usual path), `album_context`
(Spotify's duration-rejection rescue), or `manual` (admin override).

There's also legacy per-service columns on `releases` and
`recording_releases` (`spotify_album_id`, `spotify_track_id`,
`apple_music_album_id`, `apple_music_track_id`). Those are mirrored from
`*_streaming_links` and are what the API returns today. New code should read
from `*_streaming_links`.

## Caching, rate limiting, negative caching

Each `*Client` owns a JSON-on-disk response cache under the path returned by
`core.cache_utils.get_cache_dir(service)`. Every search and lookup is cached
by a deterministic hash of its inputs, with a default 30-day TTL
(`cache_days`). The cache stores both successful responses and `None` /
`"no match"` responses via a `_CACHE_MISS` sentinel — the distinction matters
because "we've never asked" and "we asked and got nothing" should behave
differently on a rerun.

The client also implements rate-limit handling. Spotify's is more involved
because it honors the `Retry-After` header with exponential backoff
(`SpotifyRateLimitError` carries the retry hint). Apple's iTunes API is less
consistent — the client backs off on 403/429 with a fixed-then-exponential
schedule.

Two layers of negative caching sit on top of the HTTP cache:

- **Spotify's track-match failure cache** (`diagnostics.py`) remembers
  `(song_id, release_id, spotify_album_id)` tuples where the album matched
  but no track inside it did. On a rerun we skip the DB round trip and the
  album-tracks fetch.
- **Apple's `mark_release_searched`** (`db.py`) stamps a timestamp on the
  release row when no album matched at all. Releases with that stamp are
  skipped on the next run unless `rematch_failures=True`.

Both are bypassed by `force_refresh=True` (the "deep refresh" mode from
`research_song`) and by `rematch_all=True` in the matcher.

## Similarity thresholds and strictness

Every matcher takes a `strict_mode` boolean. In strict mode the thresholds
are artist 75 / album 65 / track 85. In loose mode they drop to 65/55/75.
Default is strict. Loose mode is mostly for rescue runs on back-catalog
releases where the artist name in MB is stylized or the album name varies.

Confidence scores written to the DB are a 0.0–1.0 blend: 40% artist
similarity, 50% album similarity, up to 10% year bonus (if the release year
matches within 1). Track confidence is just title similarity + position
bonus, normalized.

## Gotchas worth knowing

- **`integrations/spotify/utils.py` is a facade.** It re-exports
  `SpotifyMatcher` from `matcher.py` for historical reasons — there's still
  code that imports `from integrations.spotify.utils import SpotifyMatcher`.
  New code should import from `integrations.spotify.matcher` directly.

- **Album artwork on Apple Music comes from two places.** The Apple Music
  Feed catalog has album metadata but no artwork URLs, so if we match via
  the local catalog we then do a single `client.lookup_album(id)` round trip
  to pull artwork from the iTunes API. Worth knowing if you see a stray API
  call in what looks like a pure catalog-mode run.

- **Duration-confidence rejects a match that titles agree on.** Spotify
  sometimes returns an album-context rescue under the `album_context` match
  method. That means "we think this is actually correct because every other
  track on the album also matched, even though the duration is way off".
  Check `match_method` before troubleshooting a specific track.

- **Spotify stats are on the matcher, but the counters come from the
  client.** `_aggregate_client_stats()` pulls them forward at the end of
  each `match_releases` call. Don't read `matcher.stats['api_calls']` in
  the middle of a run and expect it to be current — read
  `matcher.client.stats['api_calls']` if you need a live value.

- **The matcher accepts a `progress_callback(phase, current, total)`.**
  The admin-UI and `song_research` background worker wire this up to push
  progress through a WebSocket / SSE channel. Don't block in a callback —
  it runs on the matcher thread.

## Further reading

- Issue #115 — the Spotify split that established this shape.
- Issue #159 — the Apple Music matcher split that followed #115.
- `sql/jazz-db-schema.sql` (tables `release_streaming_links`,
  `recording_release_streaming_links`) for the persistence model.
- `core/song_research.py` for the research-worker flow that's the primary
  caller.
