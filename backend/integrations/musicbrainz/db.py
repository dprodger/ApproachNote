"""
MusicBrainz DB helpers.

Pure Postgres reads and writes extracted from MBReleaseImporter so the
importer can stay orchestration-only. Shape: module-level functions that
take `conn` (or `cur`) plus primitives and return data or simple result
flags. Callers own stats bookkeeping — several functions return a status
flag (e.g. `inserted`) so the caller can increment the right stats key.

Not moved in this step (still on MBReleaseImporter because they read/write
per-instance state):
- _create_release / _get_or_create_format / _get_or_create_packaging /
  _get_status_id / _load_lookup_caches — thread through the lookup caches
- _get_jazzbot_user_id / _create_vocal_instrumental_contribution — thread
  through the JazzBot user ID cache
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from db_utils import get_db_connection

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Song lookup
# ---------------------------------------------------------------------------

def find_song_by_name(song_name: str, log: Optional[logging.Logger] = None) -> Optional[Dict[str, Any]]:
    """Find a song by (partial) title. Opens its own DB connection."""
    log = log or _logger
    log.info(f"Searching for song: {song_name}")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id, second_mb_id
                FROM songs
                WHERE title ILIKE %s
                ORDER BY title
            """, (f'%{song_name}%',))

            results = cur.fetchall()

            if not results:
                log.warning(f"No songs found matching: {song_name}")
                return None

            if len(results) > 1:
                log.info(f"Found {len(results)} songs, using first match:")
                for r in results[:5]:
                    log.info(f"  - {r['title']}")

            song = results[0]
            return dict(song)


def find_song_by_id(song_id: str, log: Optional[logging.Logger] = None) -> Optional[Dict[str, Any]]:
    """Find a song by UUID. Opens its own DB connection."""
    log = log or _logger
    log.info(f"Looking up song by ID: {song_id}")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, composer, musicbrainz_id, second_mb_id
                FROM songs
                WHERE id = %s
            """, (song_id,))

            result = cur.fetchone()
            return dict(result) if result else None


# ---------------------------------------------------------------------------
# Batch pre-fetch helpers
#
# These exist so the importer can do one query up-front per song instead of
# per-recording / per-release. Empty-input → empty-output; no side effects.
# ---------------------------------------------------------------------------

def get_recordings_with_performers(conn, mb_recording_ids: List[str], song_id: str) -> Set[str]:
    """
    MB recording IDs that already have at least one performer linked, filtered
    by song_id (medleys can have the same MB recording under different songs).
    """
    if not mb_recording_ids:
        return set()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT r.musicbrainz_id
            FROM recordings r
            INNER JOIN recording_performers rp ON r.id = rp.recording_id
            WHERE r.musicbrainz_id = ANY(%s)
              AND r.song_id = %s
        """, (mb_recording_ids, song_id))

        return {row['musicbrainz_id'] for row in cur.fetchall()}


def get_existing_recordings_batch(conn, mb_recording_ids: List[str], song_id: str) -> Dict[str, str]:
    """
    Map of MB recording ID → our DB recording ID, filtered by song_id.

    Medley recordings in MB link to multiple works (songs); each song should
    have its own recording row even if they share the MB recording ID, so
    song_id is required.
    """
    if not mb_recording_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT musicbrainz_id, id
            FROM recordings
            WHERE musicbrainz_id = ANY(%s)
              AND song_id = %s
        """, (mb_recording_ids, song_id))

        return {row['musicbrainz_id']: row['id'] for row in cur.fetchall()}


def get_all_recording_release_links(conn, recording_ids: List[str]) -> Dict[str, Set[str]]:
    """Map of our recording_id → set of linked release_ids."""
    if not recording_ids:
        return {}

    result: Dict[str, Set[str]] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT recording_id, release_id
            FROM recording_releases
            WHERE recording_id = ANY(%s)
        """, (recording_ids,))

        for row in cur.fetchall():
            rec_id = row['recording_id']
            if rec_id not in result:
                result[rec_id] = set()
            result[rec_id].add(row['release_id'])

    return result


def get_existing_release_ids(conn, mb_release_ids: List[str]) -> Dict[str, str]:
    """Map of MB release ID → our DB release ID."""
    if not mb_release_ids:
        return {}

    with conn.cursor() as cur:
        cur.execute("""
            SELECT musicbrainz_release_id, id
            FROM releases
            WHERE musicbrainz_release_id = ANY(%s)
        """, (mb_release_ids,))

        return {row['musicbrainz_release_id']: row['id'] for row in cur.fetchall()}


def get_existing_recording_release_links(conn, recording_id: str, release_ids: List[str]) -> Set[str]:
    """Set of release IDs already linked to this recording."""
    if not release_ids or not recording_id:
        return set()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT release_id
            FROM recording_releases
            WHERE recording_id = %s AND release_id = ANY(%s)
        """, (recording_id, release_ids))

        return {row['release_id'] for row in cur.fetchall()}


def get_release_id_by_mb_id(conn, mb_release_id: str) -> Optional[str]:
    """Our release UUID by MB release ID."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM releases WHERE musicbrainz_release_id = %s
        """, (mb_release_id,))
        result = cur.fetchone()
        return result['id'] if result else None


# ---------------------------------------------------------------------------
# Recording writes
# ---------------------------------------------------------------------------

def create_recording(
    conn,
    song_id: str,
    mb_recording_id: str,
    date_info: Dict[str, Any],
    source_mb_work_id: Optional[str] = None,
    title: Optional[str] = None,
    duration_ms: Optional[int] = None,
    dry_run: bool = False,
    log: Optional[logging.Logger] = None,
) -> Tuple[Optional[str], bool]:
    """
    Insert a recording row (ON CONFLICT DO UPDATE bumps updated_at).

    Returns:
        (recording_id, inserted) — inserted=True on a fresh insert, False if
        this was a concurrent-insert resolve. recording_id is None in dry-run.
    """
    log = log or _logger

    if dry_run:
        source = date_info.get('recording_date_source', 'unknown')
        year = date_info.get('recording_year')
        title_info = f", title='{title}'" if title else ""
        log.info(f"  [DRY RUN] Would create recording: MB:{mb_recording_id} "
                 f"(year={year}, source={source}{title_info})")
        return None, False

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recordings (
                song_id, recording_year, recording_date,
                recording_date_source, recording_date_precision, mb_first_release_date,
                is_canonical, musicbrainz_id, source_mb_work_id, title, duration_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (musicbrainz_id, song_id) WHERE musicbrainz_id IS NOT NULL DO UPDATE
                SET updated_at = CURRENT_TIMESTAMP
            RETURNING id, (xmax = 0) AS inserted
        """, (
            song_id,
            date_info.get('recording_year'),
            date_info.get('recording_date'),
            date_info.get('recording_date_source'),
            date_info.get('recording_date_precision'),
            date_info.get('mb_first_release_date'),
            False,
            mb_recording_id,
            source_mb_work_id,
            title,
            duration_ms,
        ))

        result = cur.fetchone()
        recording_id = result['id']
        inserted = result['inserted']

        if inserted:
            source = date_info.get('recording_date_source', 'none')
            year = date_info.get('recording_year', '?')
            log.info(f"  ✓ Created recording: MB:{mb_recording_id[:8]}... (year={year}, source={source})")
        else:
            log.debug(f"  Recording exists (concurrent insert resolved)")

        return recording_id, inserted


def update_recording_date_if_better(
    conn,
    recording_id: str,
    date_info: Dict[str, Any],
    dry_run: bool = False,
    log: Optional[logging.Logger] = None,
) -> bool:
    """
    Upgrade a recording's date only if MB has performer-relation session dates
    and we currently have nothing better than first-release-date.
    """
    log = log or _logger

    if not recording_id or not date_info:
        return False

    new_source = date_info.get('recording_date_source')
    if new_source != 'mb_performer_relation':
        return False

    if dry_run:
        log.info(f"  [DRY RUN] Would update recording date to {date_info.get('recording_date')}")
        return True

    with conn.cursor() as cur:
        cur.execute("""
            SELECT recording_date_source FROM recordings WHERE id = %s
        """, (recording_id,))
        row = cur.fetchone()

        if not row:
            return False

        current_source = row['recording_date_source']

        # Only overwrite None or the weaker 'mb_first_release' source
        if current_source not in (None, 'mb_first_release'):
            return False

        cur.execute("""
            UPDATE recordings
            SET recording_date = %s,
                recording_year = %s,
                recording_date_source = %s,
                recording_date_precision = %s
            WHERE id = %s
        """, (
            date_info.get('recording_date'),
            date_info.get('recording_year'),
            date_info.get('recording_date_source'),
            date_info.get('recording_date_precision'),
            recording_id,
        ))

        log.info(f"  Updated recording date: {date_info.get('recording_date')} "
                 f"(source: {new_source})")
        return True


def get_or_create_recording(
    conn,
    song_id: str,
    mb_recording_id: str,
    date_info: Dict[str, Any],
    source_mb_work_id: Optional[str] = None,
    dry_run: bool = False,
    log: Optional[logging.Logger] = None,
) -> Tuple[Optional[str], str]:
    """
    Look up an existing (MB recording, song) pair; insert if missing.

    Returns (recording_id, status). status is one of:
    - 'existing' — row already existed
    - 'created' — new insert
    - 'dryrun' — would have inserted but dry_run=True
    """
    log = log or _logger

    with conn.cursor() as cur:
        # Medleys: same MB recording can appear under different songs, so
        # match on (musicbrainz_id, song_id) rather than musicbrainz_id alone.
        cur.execute("""
            SELECT id FROM recordings
            WHERE musicbrainz_id = %s AND song_id = %s
        """, (mb_recording_id, song_id))
        result = cur.fetchone()

        if result:
            log.debug(f"  Recording exists (by MB ID)")
            return result['id'], 'existing'

        if dry_run:
            log.info(f"  [DRY RUN] Would create recording: MB:{mb_recording_id}")
            return None, 'dryrun'

        cur.execute("""
            INSERT INTO recordings (
                song_id, recording_year, recording_date,
                recording_date_source, recording_date_precision, mb_first_release_date,
                is_canonical, musicbrainz_id, source_mb_work_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (musicbrainz_id, song_id) WHERE musicbrainz_id IS NOT NULL DO UPDATE
                SET updated_at = CURRENT_TIMESTAMP
            RETURNING id, (xmax = 0) AS inserted
        """, (
            song_id,
            date_info.get('recording_year'),
            date_info.get('recording_date'),
            date_info.get('recording_date_source'),
            date_info.get('recording_date_precision'),
            date_info.get('mb_first_release_date'),
            False,
            mb_recording_id,
            source_mb_work_id,
        ))

        result = cur.fetchone()
        recording_id = result['id']
        inserted = result['inserted']

        if inserted:
            source = date_info.get('recording_date_source', 'none')
            year = date_info.get('recording_year', '?')
            log.info(f"  ✓ Created recording: MB:{mb_recording_id[:8]}... (year={year}, source={source})")
            return recording_id, 'created'
        else:
            log.debug(f"  Recording exists (concurrent insert resolved)")
            return recording_id, 'existing'


# ---------------------------------------------------------------------------
# Release linking
# ---------------------------------------------------------------------------

def maybe_set_default_release(cur, recording_id: str, release_id: str) -> None:
    """Set default_release_id on a recording if it doesn't already have one."""
    cur.execute("""
        UPDATE recordings
        SET default_release_id = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
          AND default_release_id IS NULL
    """, (release_id, recording_id))


def link_recording_to_release(
    conn,
    recording_id: str,
    release_id: str,
    mb_recording_id: str,
    mb_release: Dict[str, Any],
    log: Optional[logging.Logger] = None,
) -> None:
    """
    Insert a recording_releases row, finding track/disc position by matching
    the MB recording ID inside the MB release's media/tracks tree. Also sets
    default_release_id on the recording if it was unset.
    """
    log = log or _logger

    track_number = None
    disc_number = None

    media = mb_release.get('media') or mb_release.get('medium-list') or []
    for medium in media:
        # MB 'position' is the 1-indexed disc number.
        medium_position = medium.get('position', 1)
        tracks = medium.get('tracks') or medium.get('track-list') or []

        for track in tracks:
            track_recording = track.get('recording') or {}
            track_recording_id = track_recording.get('id')

            if track_recording_id == mb_recording_id:
                # MB 'position' (int) is the track number; 'number' can be a
                # vinyl-style label like "A6" so we don't use it.
                track_number = track.get('position')
                disc_number = medium_position
                log.debug(f"      Found track position: disc {disc_number}, track {track_number}")
                break

        if track_number is not None:
            break

    if track_number is None:
        log.debug(f"      Could not find track position for recording {mb_recording_id[:8]}")

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO recording_releases (recording_id, release_id, track_number, disc_number)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (recording_id, release_id) DO NOTHING
        """, (recording_id, release_id, track_number, disc_number))

        maybe_set_default_release(cur, recording_id, release_id)
