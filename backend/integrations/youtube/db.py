"""
YouTube DB helpers.

Thin Postgres layer for the YouTube matcher:
- load_recording: pull everything the matcher needs to evaluate a single
  recording (title, artist, duration, song metadata, default_recording_release_id)
- load_recordings_for_song: same but for every recording of a song
- upsert_youtube_link: write a match into recording_release_streaming_links
  with service='youtube'. Respects manual overrides.
"""

import logging
from typing import Any, Dict, List, Optional

from db_utils import get_db_connection

from integrations.youtube.client import SERVICE_NAME, build_youtube_video_url

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

_RECORDING_QUERY = """
    SELECT
        r.id                                AS recording_id,
        r.title                             AS recording_title,
        r.duration_ms                       AS duration_ms,
        r.default_release_id                AS default_release_id,
        s.id                                AS song_id,
        s.title                             AS song_title,
        s.composer                          AS composer,
        s.alt_titles                        AS alt_titles,
        rel.artist_credit                   AS default_release_artist,
        rel.title                           AS default_release_title,
        rel.release_year                    AS release_year,
        rr.id                               AS default_recording_release_id,
        EXISTS (
            SELECT 1 FROM recording_release_streaming_links rrsl
            JOIN recording_releases rr2 ON rr2.id = rrsl.recording_release_id
            WHERE rr2.recording_id = r.id AND rrsl.service = %s
        )                                   AS has_youtube,
        EXISTS (
            SELECT 1 FROM recording_release_streaming_links rrsl
            JOIN recording_releases rr2 ON rr2.id = rrsl.recording_release_id
            WHERE rr2.recording_id = r.id AND rrsl.service IN ('spotify', 'apple_music')
        )                                   AS has_streaming_match
    FROM recordings r
    JOIN songs s ON r.song_id = s.id
    LEFT JOIN releases rel ON r.default_release_id = rel.id
    LEFT JOIN recording_releases rr
           ON rr.recording_id = r.id
          AND rr.release_id = r.default_release_id
"""


def load_recording(recording_id: str) -> Optional[Dict[str, Any]]:
    """Everything the matcher needs to evaluate one recording, or None if missing."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _RECORDING_QUERY + " WHERE r.id = %s",
                (SERVICE_NAME, recording_id),
            )
            row = cur.fetchone()
            return dict(row) if row else None


def load_recordings_for_song(song_id: str) -> List[Dict[str, Any]]:
    """Every recording of a song, in a form the matcher can evaluate."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _RECORDING_QUERY + " WHERE s.id = %s ORDER BY r.recording_year NULLS LAST, r.id",
                (SERVICE_NAME, song_id),
            )
            return [dict(r) for r in cur.fetchall()]


def find_song_by_name(song_name: str) -> Optional[Dict[str, Any]]:
    """Partial-title match. Used by the CLI when caller passed --name instead of --id."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, composer, musicbrainz_id
                FROM songs
                WHERE unaccent(title) ILIKE unaccent(%s)
                ORDER BY title
                LIMIT 1
                """,
                (f'%{song_name}%',),
            )
            row = cur.fetchone()
            return dict(row) if row else None


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def upsert_youtube_for_recording(
    recording_id: str,
    video_id: str,
    video_title: str,
    duration_ms: Optional[int],
    match_confidence: float,
    match_method: str,
    dry_run: bool = False,
    log: Optional[logging.Logger] = None,
) -> int:
    """
    Fan out a YouTube match across every recording_release row for a recording.

    Unlike Spotify/Apple where a track link is scoped to a specific album
    (=one recording_release row), a YouTube video is *the audio itself* —
    it applies to the recording regardless of which release it appears on.
    So we upsert into every recording_release row the recording has.

    The per-row ON CONFLICT clause preserves manual overrides (match_method=
    'manual'), so any rows that were set via the UI stay untouched while
    fresh rows get the matcher's values.

    Returns the number of rows written (0 if none because all were manual
    overrides or the recording has no recording_releases).
    """
    log = log or logger
    video_url = build_youtube_video_url(video_id)

    if dry_run:
        log.info(
            f"      [DRY RUN] Would upsert YouTube on all recording_releases: "
            f"{video_id} (conf={match_confidence:.2f}, method={match_method})"
        )
        return 0

    with get_db_connection() as conn:
        try:
            with conn.cursor() as cur:
                # Single statement: upsert one row per recording_release.
                # The WHERE on the DO UPDATE skips manual-override rows.
                cur.execute(
                    """
                    INSERT INTO recording_release_streaming_links (
                        recording_release_id, service, service_id, service_url,
                        service_title, duration_ms,
                        match_confidence, match_method, matched_at
                    )
                    SELECT rr.id, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                    FROM recording_releases rr
                    WHERE rr.recording_id = %s
                    ON CONFLICT (recording_release_id, service)
                    DO UPDATE SET
                        service_id       = EXCLUDED.service_id,
                        service_url      = EXCLUDED.service_url,
                        service_title    = EXCLUDED.service_title,
                        duration_ms      = EXCLUDED.duration_ms,
                        match_confidence = EXCLUDED.match_confidence,
                        match_method     = EXCLUDED.match_method,
                        matched_at       = CURRENT_TIMESTAMP,
                        updated_at       = CURRENT_TIMESTAMP
                    WHERE recording_release_streaming_links.match_method != 'manual'
                       OR recording_release_streaming_links.match_method IS NULL
                    RETURNING id
                    """,
                    (
                        SERVICE_NAME, video_id, video_url,
                        video_title, duration_ms,
                        match_confidence, match_method,
                        recording_id,
                    ),
                )
                rows_written = len(cur.fetchall())
                conn.commit()
            log.info(
                f"      ✓ Wrote YouTube link to {rows_written} recording_release row(s): "
                f"{video_id} (conf={match_confidence:.2f})"
            )
            return rows_written
        except Exception as e:
            log.error(f"Failed to upsert YouTube link: {e}")
            conn.rollback()
            return 0
