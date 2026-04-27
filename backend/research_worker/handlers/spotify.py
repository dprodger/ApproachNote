"""
Spotify handlers on the durable queue.

Two job types are registered here:

1. ('spotify', 'match_song'), target_type='song'
   Wraps integrations/spotify/matcher.SpotifyMatcher.match_releases — a
   per-song operation that walks every release of the song and matches
   each against Spotify. Same matcher the existing in-process pipeline
   uses (core.song_research), so behaviour is identical.

2. ('spotify', 'backfill_durations'), target_type='song'
   For one song, scans recording_release_streaming_links rows where
   service='spotify' and duration_ms IS NULL, and fills duration_ms
   from Spotify's batch /v1/tracks endpoint (50 IDs per call). Used to
   backfill historic rows that were inserted before the matcher started
   capturing duration. Issue #100.

Quota accounting: skipped. Spotify uses HTTP 429 rate limits, not a daily
budget like YouTube. The SpotifyClient already retries 429s internally
with exponential backoff. If those retries exhaust, we surface the
failure as RetryableError so the worker reschedules with its own backoff.

Result mapping (match_song):
    success=True                    -> dict of stats, status=done
    "Song not found"                -> PermanentError (won't fix on retry)
    "No releases found"             -> dict with matched=False (nothing to do)
    any other failure               -> RetryableError (treat as transient)

Result mapping (backfill_durations):
    no candidate links              -> {links_updated: 0, reason: 'no_candidates'}
    one or more batches succeeded   -> dict of stats, status=done
    every batch returned None       -> RetryableError (rate-limit / outage)
"""

from __future__ import annotations

from typing import Any

from db_utils import get_db_connection
from integrations.spotify.client import SpotifyClient
from integrations.spotify.matcher import SpotifyMatcher

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


# Substrings of result['error'] that mean "the input was bad / target gone";
# retrying won't help.
_PERMANENT_ERROR_MARKERS = ('song not found',)

# Substrings that mean "nothing to match" — not a failure, just a no-op.
_NO_OP_ERROR_MARKERS = ('no releases found',)


@handler('spotify', 'match_song')
def match_song(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Match every release of one song to Spotify. Returns stats summary."""
    song_id = ctx.target_id
    rematch = bool(payload.get('rematch', False))

    matcher = SpotifyMatcher(rematch=rematch, logger=ctx.log)
    result = matcher.match_releases(song_id)

    if result.get('success'):
        stats = result.get('stats') or {}
        return {
            'matched': stats.get('releases_with_spotify', 0) > 0,
            'releases_processed': stats.get('releases_processed', 0),
            'releases_with_spotify': stats.get('releases_with_spotify', 0),
            'releases_updated': stats.get('releases_updated', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'cache_hits': stats.get('cache_hits', 0),
            'api_calls': stats.get('api_calls', 0),
            'rate_limit_hits': stats.get('rate_limit_hits', 0),
        }

    error = result.get('error') or 'unknown error'
    error_lower = error.lower()

    if any(marker in error_lower for marker in _PERMANENT_ERROR_MARKERS):
        raise PermanentError(f"Spotify: {error}")

    if any(marker in error_lower for marker in _NO_OP_ERROR_MARKERS):
        # The matcher reports this via success=False but it's not a failure.
        # Record it as a clean no-op so the job goes to 'done', not 'dead'.
        return {
            'matched': False,
            'reason': 'no_releases',
            'releases_processed': 0,
        }

    # Rate-limit-after-retries, network blips, parse errors, etc. Let the
    # worker schedule a backoff and try again.
    raise RetryableError(f"Spotify match failed: {error}")


# ---------------------------------------------------------------------------
# backfill_durations — issue #100
# ---------------------------------------------------------------------------

# Spotify's GET /v1/tracks batch endpoint accepts up to 50 IDs per call.
_BATCH_SIZE = 50

# Read candidate links for a single song. recording_release_streaming_links
# keys on recording_release_id, so we walk through the recording_releases
# junction to reach a recording (and from there a song).
_SELECT_SONG_LINKS_SQL = """
    SELECT rrsl.id, rrsl.service_id
    FROM recording_release_streaming_links rrsl
    JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
    JOIN recordings r ON r.id = rr.recording_id
    WHERE r.song_id = %s
      AND rrsl.service = 'spotify'
      AND rrsl.duration_ms IS NULL
      AND rrsl.service_id IS NOT NULL
"""

_UPDATE_DURATION_SQL = """
    UPDATE recording_release_streaming_links
    SET duration_ms = %s, updated_at = NOW()
    WHERE id = %s
"""


@handler('spotify', 'backfill_durations')
def backfill_durations(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Fill duration_ms on this song's Spotify streaming links.

    For each recording_release_streaming_links row belonging to one of the
    song's recordings where service='spotify' and duration_ms IS NULL,
    fetch the track via Spotify's batch endpoint and write the result back.

    Returns a stats dict on success. Raises RetryableError if EVERY batch
    in the run failed (so the next attempt can take another swing) — but
    a partial success (some batches OK, some 429'd) is treated as done so
    the per-link work that succeeded sticks.
    """
    song_id = ctx.target_id

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(_SELECT_SONG_LINKS_SQL, (song_id,))
            links = cur.fetchall()

    if not links:
        return {
            'links_updated': 0,
            'reason': 'no_candidates',
            'links_found': 0,
        }

    spotify_client = SpotifyClient(logger=ctx.log)

    stats = {
        'links_found': len(links),
        'links_updated': 0,
        'tracks_not_found': 0,
        'tracks_no_duration': 0,
        'batches': 0,
        'batches_failed': 0,
    }

    for batch_start in range(0, len(links), _BATCH_SIZE):
        batch = links[batch_start:batch_start + _BATCH_SIZE]
        track_ids = [link['service_id'] for link in batch]

        tracks_data = spotify_client.get_tracks_batch(track_ids)
        if tracks_data is None:
            # Client already retried 429s/network blips internally; treat
            # this batch as transient. Other batches in the same run may
            # still succeed.
            stats['batches_failed'] += 1
            continue

        stats['batches'] += 1

        for link in batch:
            track_id = link['service_id']
            link_id = link['id']

            track_data = tracks_data.get(track_id)
            if not track_data:
                stats['tracks_not_found'] += 1
                continue

            duration_ms = track_data.get('duration_ms')
            if not duration_ms:
                stats['tracks_no_duration'] += 1
                continue

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_UPDATE_DURATION_SQL, (duration_ms, link_id))
                conn.commit()
            stats['links_updated'] += 1

    # If every batch in this run failed, give the worker a shot at retrying.
    # A partial-success run is considered done — re-running the sweep later
    # will pick up whatever still has duration_ms IS NULL.
    if stats['batches'] == 0 and stats['batches_failed'] > 0:
        raise RetryableError(
            f"All {stats['batches_failed']} Spotify batch call(s) failed for "
            f"song {song_id}; will retry."
        )

    return stats
