"""
Apple Music Database Operations

Database queries and updates for Apple Music matching.
Uses the normalized streaming_links tables instead of per-service columns.

Tables used:
- release_streaming_links: Album-level Apple Music links
- recording_release_streaming_links: Track-level Apple Music links
- release_imagery: Album artwork (source='Apple')
"""

import logging
from typing import List, Optional, Dict
from datetime import datetime

from db_utils import get_db_connection, find_song_by_name as db_find_song_by_name

logger = logging.getLogger(__name__)

# Service identifier - must match what we store in the streaming_links tables
SERVICE_NAME = 'apple_music'


# ============================================================================
# LOOKUP FUNCTIONS
# ============================================================================

def find_song_by_name(song_name: str) -> Optional[dict]:
    """
    Look up song by name (case-insensitive, accent-insensitive).

    Delegates to db_utils.find_song_by_name which handles:
    - Smart apostrophe normalization (', ', etc.)
    - Accent normalization (è, é, ñ, etc.)

    Examples:
        - "Si tu vois ma mere" matches "Si tu vois ma mère"
        - "Naima" matches "Naïma"
    """
    return db_find_song_by_name(song_name)


def find_song_by_id(song_id: str) -> Optional[dict]:
    """Look up song by ID"""
    if song_id.startswith('song-'):
        song_id = song_id[5:]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, composer, alt_titles FROM songs WHERE id = %s",
                (song_id,)
            )
            return cur.fetchone()


def get_releases_for_song(song_id: str, artist_filter: str = None) -> List[dict]:
    """
    Get all releases for a song, with existing Apple Music link status.

    Returns releases linked to recordings of this song, along with
    whether they already have an Apple Music link in the streaming tables.

    Args:
        song_id: Our database song ID
        artist_filter: Optional filter by performer name

    Returns:
        List of release dicts with:
        - id, title, artist_credit, release_year
        - has_apple_music: bool indicating if already matched
        - apple_music_album_id: existing ID if matched
        - performers: list of performer info
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT
                    rel.id,
                    rel.title,
                    rel.artist_credit,
                    rel.release_year,
                    rel.apple_music_searched_at,
                    -- Check for existing Apple Music link
                    rsl.service_id as apple_music_album_id,
                    rsl.service_url as apple_music_url,
                    rsl.id IS NOT NULL as has_apple_music,
                    -- Get performers from linked recording
                    (SELECT json_agg(
                        json_build_object(
                            'name', p.name,
                            'role', rp.role,
                            'instrument', i.name
                        ) ORDER BY
                            CASE rp.role
                                WHEN 'leader' THEN 1
                                WHEN 'sideman' THEN 2
                                ELSE 3
                            END,
                            p.name
                    )
                    FROM recording_performers rp
                    JOIN performers p ON rp.performer_id = p.id
                    LEFT JOIN instruments i ON rp.instrument_id = i.id
                    WHERE rp.recording_id = rr.recording_id
                    ) as performers
                FROM releases rel
                JOIN recording_releases rr ON rel.id = rr.release_id
                JOIN recordings rec ON rr.recording_id = rec.id
                LEFT JOIN release_streaming_links rsl
                    ON rel.id = rsl.release_id AND rsl.service = %s
                WHERE rec.song_id = %s
            """

            params = [SERVICE_NAME, song_id]

            if artist_filter:
                query += """
                    AND EXISTS (
                        SELECT 1
                        FROM recording_performers rp2
                        JOIN performers p2 ON rp2.performer_id = p2.id
                        WHERE rp2.recording_id = rec.id
                        AND LOWER(p2.name) = LOWER(%s)
                    )
                """
                params.append(artist_filter)

            query += """
                GROUP BY rel.id, rel.title, rel.artist_credit, rel.release_year,
                         rel.apple_music_searched_at, rsl.service_id, rsl.service_url, rsl.id, rr.recording_id
                ORDER BY rel.release_year
            """

            cur.execute(query, params)
            return cur.fetchall()


def get_recordings_for_release(song_id: str, release_id: str, conn=None) -> List[dict]:
    """
    Get recordings linked to a specific release for a specific song.

    Returns info needed to match individual tracks.

    Args:
        song_id: Our database song ID
        release_id: Our database release ID
        conn: Optional existing database connection. If provided, uses it
              instead of opening a new connection (avoids idle connection
              timeout issues when called from within a transaction).

    Returns:
        List of recording dicts with:
        - recording_id, recording_release_id
        - song_title, disc_number, track_number
        - apple_music_track_id (if already matched)
    """
    def _execute(c):
        with c.cursor() as cur:
            cur.execute("""
                SELECT
                    rr.id as recording_release_id,
                    rr.recording_id,
                    s.title as song_title,
                    rr.disc_number,
                    rr.track_number,
                    -- Check for existing Apple Music track link
                    rrsl.service_id as apple_music_track_id,
                    rrsl.service_url as apple_music_track_url
                FROM recording_releases rr
                JOIN recordings rec ON rr.recording_id = rec.id
                JOIN songs s ON rec.song_id = s.id
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rr.id = rrsl.recording_release_id AND rrsl.service = %s
                WHERE rr.release_id = %s
                  AND rec.song_id = %s
                ORDER BY rr.disc_number, rr.track_number
            """, (SERVICE_NAME, release_id, song_id))
            return cur.fetchall()

    if conn is not None:
        return _execute(conn)
    else:
        with get_db_connection() as new_conn:
            return _execute(new_conn)


def get_releases_without_apple_music() -> List[dict]:
    """
    Get releases that have Spotify but not Apple Music links.

    Useful for backfilling Apple Music data on releases we already matched
    to Spotify.

    Returns:
        List of release dicts needing Apple Music matching
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    rel.id,
                    rel.title,
                    rel.artist_credit,
                    rel.release_year
                FROM releases rel
                -- Has Spotify
                WHERE EXISTS (
                    SELECT 1 FROM release_streaming_links rsl
                    WHERE rsl.release_id = rel.id AND rsl.service = 'spotify'
                )
                -- But no Apple Music
                AND NOT EXISTS (
                    SELECT 1 FROM release_streaming_links rsl
                    WHERE rsl.release_id = rel.id AND rsl.service = %s
                )
                ORDER BY rel.title
            """, (SERVICE_NAME,))
            return cur.fetchall()


# ============================================================================
# UPDATE FUNCTIONS
# ============================================================================

def mark_release_searched(
    conn,
    release_id: str,
    dry_run: bool = False,
    log: logging.Logger = None
) -> None:
    """
    Mark a release as having been searched for Apple Music.

    Sets apple_music_searched_at to current timestamp.
    Used to cache "no match found" results so we don't re-search.

    Args:
        conn: Database connection
        release_id: Release ID to update
        dry_run: If True, don't actually update
        log: Logger for debug output
    """
    if dry_run:
        if log:
            log.debug(f"    [DRY RUN] Would mark release {release_id} as searched")
        return

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE releases
            SET apple_music_searched_at = NOW()
            WHERE id = %s
        """, (release_id,))
        conn.commit()


def is_album_manual_override(conn, release_id: str, service: str = SERVICE_NAME) -> bool:
    """
    Check if an existing album link is a manual override that should be preserved.

    Args:
        conn: Database connection
        release_id: Our release ID
        service: Streaming service name (default: apple_music)

    Returns:
        True if this album has a manual override, False otherwise
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT match_method FROM release_streaming_links
            WHERE release_id = %s AND service = %s
        """, (release_id, service))
        row = cur.fetchone()
        if row and row.get('match_method') == 'manual':
            return True
        return False


def is_track_manual_override(conn, recording_release_id: str, service: str = SERVICE_NAME) -> bool:
    """
    Check if an existing track link is a manual override that should be preserved.

    Args:
        conn: Database connection
        recording_release_id: ID from recording_releases junction table
        service: Streaming service name (default: apple_music)

    Returns:
        True if this track has a manual override, False otherwise
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT match_method FROM recording_release_streaming_links
            WHERE recording_release_id = %s AND service = %s
        """, (recording_release_id, service))
        row = cur.fetchone()
        if row and row.get('match_method') == 'manual':
            return True
        return False


def upsert_release_streaming_link(
    conn,
    release_id: str,
    service_id: str,
    service_url: str,
    match_confidence: float = None,
    match_method: str = None,
    dry_run: bool = False,
    log: logging.Logger = None
) -> bool:
    """
    Insert or update an Apple Music album link for a release.

    Uses the normalized release_streaming_links table.
    Skips update if there's a manual override (match_method='manual').

    Args:
        conn: Database connection
        release_id: Our release ID
        service_id: Apple Music collection/album ID
        service_url: Apple Music album URL
        match_confidence: Confidence score 0.0-1.0
        match_method: How the match was made (e.g., 'fuzzy_search')
        dry_run: If True, don't actually update
        log: Logger instance

    Returns:
        True if successful
    """
    log = log or logger

    if dry_run:
        log.info(f"    [DRY RUN] Would add Apple Music link: {service_id}")
        return True

    # Check for manual override - don't overwrite manually added links
    if is_album_manual_override(conn, release_id, SERVICE_NAME):
        log.debug(f"    Skipping album update - manual override exists for release {release_id}")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO release_streaming_links (
                    release_id, service, service_id, service_url,
                    match_confidence, match_method, matched_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (release_id, service)
                DO UPDATE SET
                    service_id = EXCLUDED.service_id,
                    service_url = EXCLUDED.service_url,
                    match_confidence = EXCLUDED.match_confidence,
                    match_method = EXCLUDED.match_method,
                    matched_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE release_streaming_links.match_method != 'manual'
                   OR release_streaming_links.match_method IS NULL
            """, (
                release_id, SERVICE_NAME, service_id, service_url,
                match_confidence, match_method
            ))
            conn.commit()
            return True
    except Exception as e:
        log.error(f"Failed to upsert release streaming link: {e}")
        conn.rollback()
        return False


def upsert_track_streaming_link(
    conn,
    recording_release_id: str,
    service_id: str,
    service_url: str,
    duration_ms: int = None,
    preview_url: str = None,
    isrc: str = None,
    service_title: str = None,
    match_confidence: float = None,
    match_method: str = None,
    dry_run: bool = False,
    log: logging.Logger = None
) -> bool:
    """
    Insert or update an Apple Music track link.

    Uses the normalized recording_release_streaming_links table.
    Skips update if there's a manual override (match_method='manual').

    Args:
        conn: Database connection
        recording_release_id: Our recording_releases junction table ID
        service_id: Apple Music track ID
        service_url: Apple Music track URL
        duration_ms: Track duration in milliseconds
        preview_url: 30-second preview URL
        isrc: ISRC code if available
        service_title: Track title as it appears on Apple Music
        match_confidence: Confidence score 0.0-1.0
        match_method: How the match was made
        dry_run: If True, don't actually update
        log: Logger instance

    Returns:
        True if successful
    """
    log = log or logger

    if dry_run:
        log.debug(f"      [DRY RUN] Would add Apple Music track: {service_id}")
        return True

    # Check for manual override - don't overwrite manually added links
    if is_track_manual_override(conn, recording_release_id, SERVICE_NAME):
        log.debug(f"      Skipping track update - manual override exists for recording_release {recording_release_id}")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO recording_release_streaming_links (
                    recording_release_id, service, service_id, service_title, service_url,
                    duration_ms, preview_url, isrc,
                    match_confidence, match_method, matched_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (recording_release_id, service)
                DO UPDATE SET
                    service_id = EXCLUDED.service_id,
                    service_title = COALESCE(EXCLUDED.service_title, recording_release_streaming_links.service_title),
                    service_url = EXCLUDED.service_url,
                    duration_ms = EXCLUDED.duration_ms,
                    preview_url = EXCLUDED.preview_url,
                    isrc = EXCLUDED.isrc,
                    match_confidence = EXCLUDED.match_confidence,
                    match_method = EXCLUDED.match_method,
                    matched_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE recording_release_streaming_links.match_method != 'manual'
                   OR recording_release_streaming_links.match_method IS NULL
            """, (
                recording_release_id, SERVICE_NAME, service_id, service_title, service_url,
                duration_ms, preview_url, isrc,
                match_confidence, match_method
            ))
            conn.commit()
            return True
    except Exception as e:
        log.error(f"Failed to upsert track streaming link: {e}")
        conn.rollback()
        return False


def update_recording_release_track_title(
    conn,
    recording_release_id: str,
    track_title: str,
    dry_run: bool = False,
    log: logging.Logger = None
) -> bool:
    """
    Update the track_title column in recording_releases table.

    This stores the track title as it appears on the streaming service,
    which may differ from the canonical song title.

    Args:
        conn: Database connection
        recording_release_id: ID from the recording_releases junction table
        track_title: Track title from Apple Music
        dry_run: If True, don't actually update
        log: Logger instance

    Returns:
        True if successful
    """
    log = log or logger

    if not track_title:
        return False

    if dry_run:
        log.debug(f"      [DRY RUN] Would update track_title: {track_title}")
        return True

    try:
        with conn.cursor() as cur:
            # Only update if track_title is not already set (don't overwrite Spotify data)
            cur.execute("""
                UPDATE recording_releases
                SET track_title = COALESCE(track_title, %s)
                WHERE id = %s
            """, (track_title, recording_release_id))
            conn.commit()
            return True
    except Exception as e:
        log.error(f"Failed to update track_title: {e}")
        conn.rollback()
        return False


def upsert_release_imagery(
    conn,
    release_id: str,
    artwork: Dict[str, str],
    source_id: str = None,
    dry_run: bool = False,
    log: logging.Logger = None
) -> bool:
    """
    Insert or update Apple Music album artwork in release_imagery table.

    Args:
        conn: Database connection
        release_id: Our release ID
        artwork: Dict with 'small', 'medium', 'large' URLs
        source_id: Apple Music album ID (for reference)
        dry_run: If True, don't actually update
        log: Logger instance

    Returns:
        True if successful
    """
    log = log or logger

    if not artwork or not any(artwork.values()):
        return False

    if dry_run:
        log.info(f"    [DRY RUN] Would add Apple Music artwork")
        return True

    try:
        # Generate Apple Music album URL for attribution
        source_url = f"https://music.apple.com/album/{source_id}" if source_id else None

        with conn.cursor() as cur:
            # Insert Front cover from Apple Music
            cur.execute("""
                INSERT INTO release_imagery (
                    release_id, source, source_id, source_url, type,
                    image_url_small, image_url_medium, image_url_large
                )
                VALUES (%s, 'Apple', %s, %s, 'Front', %s, %s, %s)
                ON CONFLICT ON CONSTRAINT release_imagery_unique
                DO UPDATE SET
                    image_url_small = EXCLUDED.image_url_small,
                    image_url_medium = EXCLUDED.image_url_medium,
                    image_url_large = EXCLUDED.image_url_large,
                    source_id = EXCLUDED.source_id,
                    source_url = EXCLUDED.source_url,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                release_id, source_id, source_url,
                artwork.get('small'),
                artwork.get('medium'),
                artwork.get('large')
            ))
            conn.commit()
            return True
    except Exception as e:
        log.error(f"Failed to upsert release imagery: {e}")
        conn.rollback()
        return False


def get_apple_music_stats() -> Dict:
    """
    Get statistics about Apple Music coverage in the database.

    Returns:
        Dict with counts of releases/tracks with Apple Music links
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Count releases with Apple Music links
            cur.execute("""
                SELECT COUNT(DISTINCT release_id)
                FROM release_streaming_links
                WHERE service = %s
            """, (SERVICE_NAME,))
            releases_with_am = cur.fetchone()[0]

            # Count total releases
            cur.execute("SELECT COUNT(*) FROM releases")
            total_releases = cur.fetchone()[0]

            # Count tracks with Apple Music links
            cur.execute("""
                SELECT COUNT(*)
                FROM recording_release_streaming_links
                WHERE service = %s
            """, (SERVICE_NAME,))
            tracks_with_am = cur.fetchone()[0]

            # Count total recording_releases
            cur.execute("SELECT COUNT(*) FROM recording_releases")
            total_tracks = cur.fetchone()[0]

            return {
                'releases_with_apple_music': releases_with_am,
                'total_releases': total_releases,
                'tracks_with_apple_music': tracks_with_am,
                'total_tracks': total_tracks,
                'release_coverage': f"{(releases_with_am / total_releases * 100):.1f}%" if total_releases > 0 else "0%",
                'track_coverage': f"{(tracks_with_am / total_tracks * 100):.1f}%" if total_tracks > 0 else "0%",
            }
