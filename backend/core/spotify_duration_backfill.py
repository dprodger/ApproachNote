"""
Spotify duration-backfill sweep (issue #100).

`recording_release_streaming_links` rows inserted before the Spotify matcher
captured `duration_ms` carry it as NULL. Without a populated duration we
can't surface duration mismatches in the admin review UI, and the matcher
itself can't decide whether a track is a confident match.

This module is the producer side of the fix. It scans the table for songs
that have at least one Spotify link missing a duration, and enqueues one
('spotify', 'backfill_durations') job per song onto the durable research
queue. The handler in `research_worker/handlers/spotify.py` does the
actual Spotify API + DB work — see that file for batch / retry semantics.

Per-song was chosen over per-link because:

  - It matches the existing ('spotify', 'match_song') job shape, so the
    research_jobs unique index dedups cleanly and the admin dashboard
    filters Just Work.
  - The handler can batch all of one song's missing links into 50-track
    Spotify API calls — far fewer round-trips than one job per link.
  - One song's worth of work is bounded; the worker doesn't have to
    babysit a 35,000-row sweep job.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from db_utils import get_db_connection


logger = logging.getLogger(__name__)


# Distinct songs that own at least one Spotify streaming link with NULL
# duration_ms. recording_release_streaming_links keys on recording_release_id,
# so we walk recording_releases → recordings to reach the song.
# ORDER BY MAX(rrsl.created_at) DESC mirrors the original
# scripts/backfill_spotify_durations.py ordering — newest links first,
# so a partial sweep covers the most-recently-added rows.
_CANDIDATE_SONGS_SQL_TEMPLATE = """
    SELECT r.song_id AS song_id
    FROM recording_release_streaming_links rrsl
    JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
    JOIN recordings r ON r.id = rr.recording_id
    WHERE rrsl.service = 'spotify'
      AND rrsl.duration_ms IS NULL
      AND rrsl.service_id IS NOT NULL
      AND r.song_id IS NOT NULL
    GROUP BY r.song_id
    ORDER BY MAX(rrsl.created_at) DESC
    {limit_clause}
"""


def find_candidate_song_ids(limit: Optional[int] = None) -> list[str]:
    """Return song UUIDs that own one or more Spotify links missing
    duration_ms. Newest-link-first ordering. Pass `limit` to cap the
    result set."""
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = _CANDIDATE_SONGS_SQL_TEMPLATE.format(limit_clause=limit_clause)
    params: tuple = (limit,) if limit is not None else ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row['song_id']) for row in rows]


def enqueue_sweep(limit: Optional[int] = None,
                  priority: int = 110) -> dict[str, int]:
    """Find candidate songs and enqueue one backfill job per song.

    The default priority (110) sits behind user-initiated jobs (50) and
    plain-vanilla research (100), so a bulk sweep doesn't starve normal
    traffic on the worker thread.

    Returns:
        {'candidates': N, 'enqueued': M, 'errors': E}
        where `enqueued` counts successful enqueue() returns. The unique
        index in research_jobs collapses a re-sweep down to the existing
        (queued|running) job for the same song, so calling this twice in
        a row is safe and idempotent.
    """
    song_ids = find_candidate_song_ids(limit=limit)
    if not song_ids:
        return {'candidates': 0, 'enqueued': 0, 'errors': 0}

    enqueued = 0
    errors = 0
    for song_id in song_ids:
        try:
            job_id = research_jobs.enqueue(
                source=research_jobs.SOURCE_SPOTIFY,
                job_type='backfill_durations',
                target_type=research_jobs.TARGET_SONG,
                target_id=song_id,
                payload={},
                priority=priority,
            )
        except Exception:
            logger.exception(
                "spotify_duration_backfill: failed to enqueue song %s",
                song_id,
            )
            errors += 1
            continue

        if job_id is None:
            errors += 1
            continue

        enqueued += 1

    logger.info(
        "spotify_duration_backfill: candidates=%d enqueued=%d errors=%d",
        len(song_ids), enqueued, errors,
    )
    return {
        'candidates': len(song_ids),
        'enqueued': enqueued,
        'errors': errors,
    }
