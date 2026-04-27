"""
Spotify duration-mismatch rematch sweep (issue #100, second phase).

Walks songs where at least one Spotify streaming link's duration_ms
differs from the linked recording's canonical duration_ms by more than
a threshold, and enqueues one ('spotify', 'rematch_duration_mismatches')
job per song onto the durable research queue.

The handler — see research_worker/handlers/spotify.py — wraps the same
SpotifyMatcher path the `match_spotify_tracks.py --duration-mismatches`
CLI has used historically. It re-runs matching narrowly on the
mismatched releases; if it finds a better track, it swaps the link, and
otherwise leaves the existing match in place.

Threshold defaults to 60s (60_000 ms) — same as the
/admin/duration-mismatches review page and the matcher CLI. 60s is wide
enough to ignore Spotify's usual 1-2s duration drift, narrow enough to
catch wrong-track matches (e.g. a 4-min recording linked to a 2-min
track).

Per-song was chosen over per-link for the same reasons as the backfill
sweep: matches the existing match_song job shape, dedups cleanly, and
the matcher itself is per-song-shaped under the hood.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from integrations.spotify.db import get_songs_with_duration_mismatches


logger = logging.getLogger(__name__)


DEFAULT_THRESHOLD_MS = 60_000


def find_candidate_song_ids(
    threshold_ms: int = DEFAULT_THRESHOLD_MS,
    limit: Optional[int] = None,
) -> list[str]:
    """Return song UUIDs that own one or more Spotify streaming links
    whose duration_ms differs from the linked recording's duration_ms by
    more than `threshold_ms`. Pass `limit` to cap the result set."""
    songs = get_songs_with_duration_mismatches(threshold_ms=threshold_ms)
    song_ids = [str(row['id']) for row in songs]
    if limit is not None:
        song_ids = song_ids[:limit]
    return song_ids


def enqueue_sweep(
    threshold_ms: int = DEFAULT_THRESHOLD_MS,
    limit: Optional[int] = None,
    priority: int = 110,
) -> dict[str, int]:
    """Find candidate songs and enqueue one rematch job per song.

    The default priority (110) sits behind user-initiated jobs (50) and
    plain-vanilla research (100) so a bulk cleanup pass doesn't starve
    normal traffic on the worker thread.

    The threshold is passed through to the handler via payload so each
    job re-checks against the same value the sweep used to enqueue it.
    Otherwise a bulk run at threshold=60_000 could be silently widened by
    the handler reading a different default later.

    Returns:
        {'candidates': N, 'enqueued': M, 'errors': E, 'threshold_ms': T}
        where `enqueued` counts successful enqueue() returns. The unique
        index in research_jobs collapses a re-sweep down to the existing
        (queued|running) job for the same song, so calling this twice in
        a row at the same threshold is safe and idempotent.
    """
    song_ids = find_candidate_song_ids(threshold_ms=threshold_ms, limit=limit)
    base_stats = {
        'candidates': len(song_ids),
        'enqueued': 0,
        'errors': 0,
        'threshold_ms': threshold_ms,
    }
    if not song_ids:
        return base_stats

    enqueued = 0
    errors = 0
    for song_id in song_ids:
        try:
            job_id = research_jobs.enqueue(
                source=research_jobs.SOURCE_SPOTIFY,
                job_type='rematch_duration_mismatches',
                target_type=research_jobs.TARGET_SONG,
                target_id=song_id,
                payload={'threshold_ms': threshold_ms},
                priority=priority,
            )
        except Exception:
            logger.exception(
                "spotify_rematch_mismatches: failed to enqueue song %s",
                song_id,
            )
            errors += 1
            continue

        if job_id is None:
            errors += 1
            continue

        enqueued += 1

    logger.info(
        "spotify_rematch_mismatches: threshold_ms=%d candidates=%d "
        "enqueued=%d errors=%d",
        threshold_ms, len(song_ids), enqueued, errors,
    )
    base_stats['enqueued'] = enqueued
    base_stats['errors'] = errors
    return base_stats
