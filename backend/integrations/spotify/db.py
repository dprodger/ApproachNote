"""
Spotify Database Operations

All database queries and updates for Spotify matching.
These functions interact with the Jazz Reference PostgreSQL database.
"""

import logging
from typing import Dict, List, Optional

from db_utils import get_db_connection, find_song_by_name as db_find_song_by_name

logger = logging.getLogger(__name__)


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
    # Strip 'song-' prefix if present
    if song_id.startswith('song-'):
        song_id = song_id[5:]
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, composer, alt_titles FROM songs WHERE id = %s",
                (song_id,)
            )
            return cur.fetchone()


def get_recordings_for_song(song_id: str, artist_filter: str = None) -> List[dict]:
    """
    Get all recordings for a song, optionally filtered by artist

    UPDATED: Recording-Centric Architecture
    - Spotify URL now comes from the best release (via default_release_id or subquery)
    - Performers come from recording_performers table

    UPDATED: Normalized Streaming Links
    - Spotify track URLs now come from recording_release_streaming_links table

    Returns:
        List of recording dicts with 'id', 'album_title', 'recording_year',
        'spotify_url' (from best release), 'performers' (list with 'name' and 'role')
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Base query - get Spotify URL from default release or best available
            query = """
                SELECT
                    r.id,
                    def_rel.title as album_title,
                    r.recording_year,
                    -- Get Spotify URL from default release, or best available release
                    COALESCE(
                        -- Default release: check streaming links table
                        (SELECT rrsl.service_url
                         FROM recording_releases rr
                         JOIN recording_release_streaming_links rrsl
                             ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                         WHERE rr.release_id = r.default_release_id AND rr.recording_id = r.id
                        ),
                        -- Default release: album-level fallback
                        (SELECT 'https://open.spotify.com/album/' || rel.spotify_album_id
                         FROM releases rel
                         WHERE rel.id = r.default_release_id
                           AND rel.spotify_album_id IS NOT NULL
                        ),
                        -- Any release: check streaming links table
                        (SELECT rrsl.service_url
                         FROM recording_releases rr
                         JOIN recording_release_streaming_links rrsl
                             ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                         WHERE rr.recording_id = r.id
                         LIMIT 1
                        ),
                        -- Any release: album-level fallback
                        (SELECT 'https://open.spotify.com/album/' || rel.spotify_album_id
                         FROM recording_releases rr
                         JOIN releases rel ON rr.release_id = rel.id
                         WHERE rr.recording_id = r.id
                           AND rel.spotify_album_id IS NOT NULL
                         ORDER BY rel.release_year DESC NULLS LAST
                         LIMIT 1
                        )
                    ) as spotify_url,
                    json_agg(
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
                    ) FILTER (WHERE p.id IS NOT NULL) as performers
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                LEFT JOIN recording_performers rp ON r.id = rp.recording_id
                LEFT JOIN performers p ON rp.performer_id = p.id
                LEFT JOIN instruments i ON rp.instrument_id = i.id
                WHERE r.song_id = %s
            """
            
            # Add artist filter if specified
            params = [song_id]
            if artist_filter:
                query += """
                    AND EXISTS (
                        SELECT 1 
                        FROM recording_performers rp2
                        JOIN performers p2 ON rp2.performer_id = p2.id
                        WHERE rp2.recording_id = r.id
                        AND LOWER(p2.name) = LOWER(%s)
                    )
                """
                params.append(artist_filter)
            
            query += """
                GROUP BY r.id, def_rel.title, r.recording_year, r.default_release_id
                ORDER BY r.recording_year
            """
            
            cur.execute(query, params)
            return cur.fetchall()


def get_releases_for_song(song_id: str, artist_filter: str = None) -> List[dict]:
    """
    Get all releases for a song (via recording_releases junction),
    optionally filtered by artist
    
    UPDATED: Recording-Centric Architecture
    - Performers now come from recording_performers (not release_performers)
    - release_performers is now for release-specific credits (producers, etc.)
    
    Returns:
        List of release dicts with 'id', 'title', 'artist_credit', 'release_year',
        'spotify_album_url' (constructed from ID), 'spotify_album_id', 'performers' (list with 'name' and 'role')
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT
                    rel.id,
                    rel.title,
                    rel.artist_credit,
                    rel.release_year,
                    CASE WHEN rel.spotify_album_id IS NOT NULL
                         THEN 'https://open.spotify.com/album/' || rel.spotify_album_id END as spotify_album_url,
                    rel.spotify_album_id,
                    -- Get performers from the linked recording (not from release)
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
                WHERE rec.song_id = %s
            """
            
            params = [song_id]
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
                GROUP BY rel.id, rel.title, rel.artist_credit, rel.release_year, rel.spotify_album_id, rr.recording_id
                ORDER BY rel.release_year
            """
            
            cur.execute(query, params)
            return cur.fetchall()


def get_releases_with_duration_mismatches(song_id: str, threshold_ms: int = 60000,
                                          artist_filter: str = None) -> List[dict]:
    """
    Get releases for a song where at least one Spotify streaming link has a
    duration mismatch greater than the given threshold.

    Returns the same shape as get_releases_for_song() so it can be used as a
    drop-in replacement.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT
                    rel.id,
                    rel.title,
                    rel.artist_credit,
                    rel.release_year,
                    CASE WHEN rel.spotify_album_id IS NOT NULL
                         THEN 'https://open.spotify.com/album/' || rel.spotify_album_id END as spotify_album_url,
                    rel.spotify_album_id,
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
                JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                WHERE rec.song_id = %s
                  AND rec.duration_ms IS NOT NULL
                  AND rrsl.duration_ms IS NOT NULL
                  AND ABS(rec.duration_ms - rrsl.duration_ms) > %s
                  -- Skip rows the admin has manually verified — they're
                  -- intentionally locked to this Spotify track even when the
                  -- duration would otherwise look mismatched.
                  AND (rrsl.match_method IS NULL OR rrsl.match_method != 'manual')
            """

            params = [song_id, threshold_ms]
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
                GROUP BY rel.id, rel.title, rel.artist_credit, rel.release_year, rel.spotify_album_id, rr.recording_id
                ORDER BY rel.release_year
            """

            cur.execute(query, params)
            return cur.fetchall()


def get_songs_with_duration_mismatches(threshold_ms: int = 60000) -> List[dict]:
    """
    Get all songs that have at least one Spotify streaming link with a
    duration mismatch greater than the given threshold.

    Returns:
        List of dicts with 'id' and 'title'.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.id, s.title
                FROM songs s
                JOIN recordings rec ON rec.song_id = s.id
                JOIN recording_releases rr ON rr.recording_id = rec.id
                JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                WHERE rec.duration_ms IS NOT NULL
                  AND rrsl.duration_ms IS NOT NULL
                  AND ABS(rec.duration_ms - rrsl.duration_ms) > %s
                  -- Skip admin-verified matches; see get_releases_with_duration_mismatches.
                  AND (rrsl.match_method IS NULL OR rrsl.match_method != 'manual')
                ORDER BY s.title
            """, (threshold_ms,))
            return cur.fetchall()


def get_releases_without_artwork() -> List[dict]:
    """Get releases with Spotify ID but no Spotify artwork in release_imagery"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.title,
                       CASE WHEN r.spotify_album_id IS NOT NULL
                            THEN 'https://open.spotify.com/album/' || r.spotify_album_id END as spotify_album_url,
                       r.spotify_album_id
                FROM releases r
                WHERE r.spotify_album_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM release_imagery ri
                      WHERE ri.release_id = r.id
                        AND ri.source = 'Spotify'
                  )
                ORDER BY r.title
            """)
            return cur.fetchall()


def get_recordings_for_release(song_id: str, release_id: str, conn=None) -> List[dict]:
    """
    Get recordings linked to a specific release for a specific song

    Args:
        song_id: Our database song ID
        release_id: Our database release ID
        conn: Optional existing database connection. If provided, uses it
              instead of opening a new connection (avoids idle connection
              timeout issues when called from within a transaction).

    Returns:
        List of recording dicts with 'recording_id', 'song_title',
        'recording_title', 'recording_leader_name', 'disc_number',
        'track_number', 'spotify_track_id' (existing if any),
        'recording_duration_ms'.

        `recording_title` is the MB recording's own title (e.g.
        "Well You Needn't (opening)"). The matcher uses it to disambiguate
        same-song-multiple-variations cases — see match_tracks_for_release
        in integrations/spotify/matcher.py.

        `recording_leader_name` is the name of the performer credited
        with role='leader' on this recording (NULL when no leader is
        credited, or there are multiple leaders — we take the first by
        name to keep this query stable). The matcher uses it to validate
        track-level artists on compilation-album matches, where the
        album-level artist check (Various Artists) carries no signal.
    """
    def _execute(c):
        with c.cursor() as cur:
            # Get Spotify track IDs from the streaming links table
            cur.execute("""
                SELECT
                    rr.recording_id,
                    s.title as song_title,
                    rec.title as recording_title,
                    rr.disc_number,
                    rr.track_number,
                    rrsl.service_id as spotify_track_id,
                    rec.duration_ms as recording_duration_ms,
                    (
                        SELECT p.name
                        FROM recording_performers rp
                        JOIN performers p ON p.id = rp.performer_id
                        WHERE rp.recording_id = rec.id
                          AND rp.role = 'leader'
                        ORDER BY p.name
                        LIMIT 1
                    ) as recording_leader_name
                FROM recording_releases rr
                JOIN recordings rec ON rr.recording_id = rec.id
                JOIN songs s ON rec.song_id = s.id
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                WHERE rr.release_id = %s
                  AND rec.song_id = %s
                ORDER BY rr.disc_number, rr.track_number
            """, (release_id, song_id))
            return cur.fetchall()

    if conn is not None:
        return _execute(conn)
    else:
        with get_db_connection() as new_conn:
            return _execute(new_conn)


def update_release_spotify_data(conn, release_id: str, spotify_data: dict,
                                dry_run: bool = False, log: logging.Logger = None):
    """
    Update release with Spotify album ID and cover artwork.
    (URL is constructed on-demand from ID, not stored)

    Artwork is stored in the release_imagery table.
    Skips update if there's a manual override (match_method='manual').

    Args:
        conn: Database connection
        release_id: Our release ID
        spotify_data: Dict with 'id', 'album_art' keys (url is ignored - constructed from ID)
        dry_run: If True, don't actually update
        log: Logger instance
    """
    log = log or logger

    album_id = spotify_data.get('id')
    album_art = spotify_data.get('album_art', {})

    if dry_run:
        log.info(f"    [DRY RUN] Would update release with Spotify ID: {album_id}")
        if album_art.get('medium'):
            log.info(f"    [DRY RUN] Would add cover artwork")
        return

    # Check for manual override - don't overwrite manually added links
    if is_album_manual_override(conn, release_id, 'spotify'):
        log.debug(f"    Skipping album update - manual override exists for release {release_id}")
        return

    # Store artwork in release_imagery table
    if album_art:
        upsert_release_imagery(conn, release_id, album_art, source_id=album_id, log=log)

    # Update releases table with spotify_album_id
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE releases
            SET spotify_album_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (album_id, release_id))

        # Also insert into normalized streaming links table
        service_url = f'https://open.spotify.com/album/{album_id}'
        cur.execute("""
            INSERT INTO release_streaming_links (
                release_id, service, service_id, service_url,
                match_method, matched_at
            )
            VALUES (%s, 'spotify', %s, %s, 'fuzzy_search', CURRENT_TIMESTAMP)
            ON CONFLICT (release_id, service)
            DO UPDATE SET
                service_id = EXCLUDED.service_id,
                service_url = EXCLUDED.service_url,
                match_method = EXCLUDED.match_method,
                matched_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE release_streaming_links.match_method != 'manual'
               OR release_streaming_links.match_method IS NULL
        """, (release_id, album_id, service_url))
        # Note: commit is handled by the caller's context manager


def clear_release_spotify_data(conn, release_id: str,
                               dry_run: bool = False, log: logging.Logger = None):
    """
    Clear Spotify album data from a release.

    Used during rematch when a previously matched release no longer passes
    validation. Skips releases with manual overrides.

    Args:
        conn: Database connection
        release_id: Our release ID
        dry_run: If True, don't actually update
        log: Logger instance
    """
    log = log or logger

    if dry_run:
        log.info(f"    [DRY RUN] Would clear Spotify data from release {release_id}")
        return

    # Don't clear manual overrides
    if is_album_manual_override(conn, release_id, 'spotify'):
        log.debug(f"    Skipping clear - manual override exists for release {release_id}")
        return

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE releases
            SET spotify_album_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (release_id,))

        # Remove from streaming links table
        cur.execute("""
            DELETE FROM release_streaming_links
            WHERE release_id = %s AND service = 'spotify'
              AND (match_method != 'manual' OR match_method IS NULL)
        """, (release_id,))

        # Remove Spotify-sourced imagery
        cur.execute("""
            DELETE FROM release_imagery
            WHERE release_id = %s AND source = 'Spotify'
        """, (release_id,))


def update_release_artwork(conn, release_id: str, album_art: dict,
                          dry_run: bool = False, log: logging.Logger = None):
    """
    Update release with cover artwork only.

    Artwork is stored in the release_imagery table.
    """
    log = log or logger

    if dry_run:
        log.info(f"    [DRY RUN] Would update with cover artwork")
        return

    # Look up existing spotify_album_id for source_id
    spotify_album_id = None
    with conn.cursor() as cur:
        cur.execute("SELECT spotify_album_id FROM releases WHERE id = %s", (release_id,))
        row = cur.fetchone()
        if row:
            spotify_album_id = row.get('spotify_album_id')

    # Store in release_imagery table
    upsert_release_imagery(conn, release_id, album_art, source_id=spotify_album_id, log=log)


def upsert_release_imagery(
    conn,
    release_id: str,
    artwork: Dict[str, str],
    source_id: str = None,
    dry_run: bool = False,
    log: logging.Logger = None
) -> bool:
    """
    Insert or update Spotify album artwork in release_imagery table.

    Args:
        conn: Database connection
        release_id: Our release ID
        artwork: Dict with 'small', 'medium', 'large' URLs
        source_id: Spotify album ID (for reference)
        dry_run: If True, don't actually update
        log: Logger instance

    Returns:
        True if successful
    """
    log = log or logger

    if not artwork or not any(artwork.values()):
        return False

    if dry_run:
        log.info(f"    [DRY RUN] Would add Spotify artwork to release_imagery")
        return True

    try:
        # Generate Spotify album URL for attribution
        source_url = f"https://open.spotify.com/album/{source_id}" if source_id else None

        with conn.cursor() as cur:
            # Insert Front cover from Spotify
            cur.execute("""
                INSERT INTO release_imagery (
                    release_id, source, source_id, source_url, type,
                    image_url_small, image_url_medium, image_url_large
                )
                VALUES (%s, 'Spotify', %s, %s, 'Front', %s, %s, %s)
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
            # Note: commit is handled by the caller's context manager
            return True
    except Exception as e:
        log.error(f"Failed to upsert Spotify release imagery: {e}")
        return False


def is_track_manual_override(conn, recording_release_id: str, service: str = 'spotify') -> bool:
    """
    Check if an existing track link is a manual override that should be preserved.

    Args:
        conn: Database connection
        recording_release_id: ID from recording_releases junction table
        service: Streaming service name (default: 'spotify')

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


def set_track_link_manual_override(conn, streaming_link_id: str,
                                   manual: bool = True,
                                   log: logging.Logger = None) -> bool:
    """Flip a streaming-link row's match_method to/from 'manual'.

    'manual' is the magic value that update_recording_release_track_id
    and clear_recording_release_track honour as a "do not touch" flag —
    so once an admin marks a (recording, Spotify track) pair as manually
    verified, the matcher leaves it alone on every subsequent re-run.

    On unverify (manual=False) the method is reset to 'fuzzy_search';
    the next matcher pass will re-evaluate from scratch and either
    re-confirm or replace it. We don't try to remember the previous
    auto-method — there's no schema slot for it and rediscovery is the
    correct behaviour.

    Returns True if a row was updated, False if no matching row exists.
    """
    log = log or logger
    new_method = 'manual' if manual else 'fuzzy_search'
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE recording_release_streaming_links
            SET match_method = %s, updated_at = NOW()
            WHERE id = %s AND service = 'spotify'
            """,
            (new_method, streaming_link_id),
        )
        if cur.rowcount == 0:
            return False
    log.info(
        "Streaming link %s match_method set to '%s'",
        streaming_link_id, new_method,
    )
    return True


def block_streaming_track(conn, song_id: str, track_id: str,
                          service: str = 'spotify',
                          reason: str = None,
                          log: logging.Logger = None) -> bool:
    """Add an entry to bad_streaming_matches at block_level='track'.

    Tells the matcher to never re-link this Spotify track to recordings
    of `song_id`. The matcher consults this list via
    get_blocked_tracks_for_song on every track-match attempt, so the
    block survives all subsequent rematch / refresh / sweep cycles.

    Used by the admin "Reject" action — call this in the same transaction
    as the streaming-link delete so a wrong link gets both removed AND
    blocklisted in one shot.

    Returns True when a new row was inserted, False when an equivalent
    block already existed (the unique constraint
    bad_streaming_matches_unique collapses duplicates).
    """
    log = log or logger
    with conn.cursor() as cur:
        # Column-list ON CONFLICT instead of constraint-name ON CONFLICT —
        # the constraint is named in the migration file but anonymous in
        # sql/jazz-db-schema.sql, so test environments bootstrapped from
        # the schema file don't have the named constraint to reference.
        # The column list matches both definitions.
        cur.execute(
            """
            INSERT INTO bad_streaming_matches
                (service, block_level, service_id, song_id, reason)
            VALUES (%s, 'track', %s, %s, %s)
            ON CONFLICT (service, block_level, service_id, song_id)
            DO NOTHING
            RETURNING id
            """,
            (service, track_id, song_id, reason),
        )
        new_row = cur.fetchone()
    if new_row:
        log.info(
            "Blocked %s track %s for song %s (reason=%r)",
            service, track_id, song_id, reason,
        )
        return True
    log.debug(
        "%s track %s already blocked for song %s — no-op",
        service, track_id, song_id,
    )
    return False


def is_album_manual_override(conn, release_id: str, service: str = 'spotify') -> bool:
    """
    Check if an existing album link is a manual override that should be preserved.

    Args:
        conn: Database connection
        release_id: Our release ID
        service: Streaming service name (default: 'spotify')

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


def update_recording_release_track_id(conn, recording_id: str, release_id: str,
                                      track_id: str, track_url: str = None,
                                      disc_number: int = None, track_number: int = None,
                                      track_title: str = None, duration_ms: int = None,
                                      match_confidence: float = None,
                                      match_method: str = 'fuzzy_search',
                                      dry_run: bool = False, log: logging.Logger = None):
    """
    Update recording_releases with Spotify track info and insert into streaming links table.

    Inserts into recording_release_streaming_links table for track-level Spotify data.
    Skips update if there's a manual override (match_method='manual').

    Args:
        conn: Database connection
        recording_id: Our recording ID
        release_id: Our release ID
        track_id: Spotify track ID
        track_url: Deprecated - ignored (URL constructed from ID)
        disc_number: Disc number from Spotify (updates existing value)
        track_number: Track number from Spotify (updates existing value)
        track_title: Track title from Spotify (stored in track_title column)
        duration_ms: Track duration in milliseconds from Spotify
        dry_run: If True, don't actually update
        log: Logger instance
    """
    log = log or logger

    if dry_run:
        log.debug(f"      [DRY RUN] Would update recording_releases with track: {track_id} (disc {disc_number}, track {track_number})")
        return

    with conn.cursor() as cur:
        # Get the recording_release_id first
        cur.execute("""
            SELECT id FROM recording_releases
            WHERE recording_id = %s AND release_id = %s
        """, (recording_id, release_id))
        row = cur.fetchone()
        if not row:
            log.warning(f"      No recording_releases row found for recording {recording_id}, release {release_id}")
            return
        recording_release_id = row['id']

        # Check for manual override - don't overwrite manually added links
        if is_track_manual_override(conn, recording_release_id, 'spotify'):
            log.debug(f"      Skipping track update - manual override exists for recording_release {recording_release_id}")
            return

        # Update disc/track positional info on recording_releases
        # Note: track_title is reserved for MusicBrainz data.
        # Spotify track names go to recording_release_streaming_links.service_title.
        cur.execute("""
            UPDATE recording_releases
            SET disc_number = COALESCE(%s, disc_number),
                track_number = COALESCE(%s, track_number)
            WHERE recording_id = %s AND release_id = %s
        """, (disc_number, track_number, recording_id, release_id))

        # Insert/update the streaming links table (only if not a manual override)
        service_url = f'https://open.spotify.com/track/{track_id}'
        cur.execute("""
            INSERT INTO recording_release_streaming_links (
                recording_release_id, service, service_id, service_title, service_url,
                duration_ms, match_confidence, match_method, matched_at
            )
            VALUES (%s, 'spotify', %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (recording_release_id, service)
            DO UPDATE SET
                service_id = EXCLUDED.service_id,
                service_title = COALESCE(EXCLUDED.service_title, recording_release_streaming_links.service_title),
                service_url = EXCLUDED.service_url,
                duration_ms = EXCLUDED.duration_ms,
                match_confidence = EXCLUDED.match_confidence,
                match_method = EXCLUDED.match_method,
                matched_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE recording_release_streaming_links.match_method != 'manual'
               OR recording_release_streaming_links.match_method IS NULL
        """, (recording_release_id, track_id, track_title, service_url, duration_ms, match_confidence, match_method))
        # Note: commit is handled by the caller's context manager


def clear_recording_release_track(conn, recording_id: str, release_id: str,
                                  dry_run: bool = False, log: logging.Logger = None):
    """
    Remove a Spotify streaming link from a recording-release pair.

    Used during rematch when a track is rejected (e.g., duration mismatch)
    or no longer matches. Skips manual overrides.
    """
    log = log or logger

    if dry_run:
        log.debug(f"      [DRY RUN] Would clear Spotify track link for recording {recording_id}, release {release_id}")
        return

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM recording_releases
            WHERE recording_id = %s AND release_id = %s
        """, (recording_id, release_id))
        row = cur.fetchone()
        if not row:
            return
        recording_release_id = row['id']

        cur.execute("""
            DELETE FROM recording_release_streaming_links
            WHERE recording_release_id = %s AND service = 'spotify'
              AND (match_method != 'manual' OR match_method IS NULL)
        """, (recording_release_id,))

        if cur.rowcount > 0:
            log.debug(f"      Cleared stale Spotify track link for recording {recording_id}")


def update_recording_default_release(conn, song_id: str, release_id: str,
                                     dry_run: bool = False, log: logging.Logger = None):
    """
    Update recordings linked to a release to set it as their default_release.
    
    This is called when a release is successfully matched to Spotify.
    Only updates recordings that don't already have a default_release_id,
    or if the new release has better data (has Spotify).
    
    Args:
        conn: Database connection
        song_id: Our song ID (to filter recordings)
        release_id: Release ID to set as default
        dry_run: If True, don't actually update
        log: Logger instance
    """
    log = log or logger
    
    if dry_run:
        log.debug(f"    [DRY RUN] Would set default_release_id for linked recordings")
        return
    
    with conn.cursor() as cur:
        # Update recordings that:
        # 1. Are linked to this release
        # 2. Are for this song
        # 3. Either have no default_release_id OR their current default has no Spotify
        cur.execute("""
            UPDATE recordings r
            SET default_release_id = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE r.song_id = %s
              AND EXISTS (
                  SELECT 1 FROM recording_releases rr 
                  WHERE rr.recording_id = r.id AND rr.release_id = %s
              )
              AND (
                  r.default_release_id IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM releases rel 
                      WHERE rel.id = r.default_release_id 
                        AND rel.spotify_album_id IS NOT NULL
                  )
              )
        """, (release_id, song_id, release_id))
        
        updated_count = cur.rowcount
        if updated_count > 0:
            log.debug(f"    Set default_release_id on {updated_count} recording(s)")
        else:
            log.debug(f"    No recordings needed default_release_id update (already set)")


# ============================================================================
# BAD MATCH BLOCKLIST FUNCTIONS
# ============================================================================

def is_track_blocked(song_id: str, track_id: str, service: str = 'spotify') -> bool:
    """
    Check if a streaming track ID is blocked from matching a song.

    Args:
        song_id: Our database song ID
        track_id: Streaming service track ID (e.g., Spotify track ID)
        service: Streaming service name (default: 'spotify')

    Returns:
        True if this track is blocked from matching this song, False otherwise
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM bad_streaming_matches
                WHERE service = %s
                  AND block_level = 'track'
                  AND service_id = %s
                  AND song_id = %s
                LIMIT 1
            """, (service, track_id, song_id))
            return cur.fetchone() is not None


def is_album_blocked(song_id: str, album_id: str, service: str = 'spotify') -> bool:
    """
    Check if a streaming album ID is blocked from matching a song's releases.

    Args:
        song_id: Our database song ID
        album_id: Streaming service album ID (e.g., Spotify album ID)
        service: Streaming service name (default: 'spotify')

    Returns:
        True if this album is blocked from matching this song, False otherwise
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM bad_streaming_matches
                WHERE service = %s
                  AND block_level = 'album'
                  AND service_id = %s
                  AND song_id = %s
                LIMIT 1
            """, (service, album_id, song_id))
            return cur.fetchone() is not None


def get_blocked_tracks_for_song(song_id: str, service: str = 'spotify', conn=None) -> List[str]:
    """
    Get all blocked track IDs for a song.

    Args:
        song_id: Our database song ID
        service: Streaming service name (default: 'spotify')
        conn: Optional existing database connection. If provided, uses it
              instead of opening a new connection (avoids idle connection
              timeout issues when called from within a transaction).

    Returns:
        List of blocked track IDs
    """
    def _execute(c):
        with c.cursor() as cur:
            cur.execute("""
                SELECT service_id FROM bad_streaming_matches
                WHERE service = %s
                  AND block_level = 'track'
                  AND song_id = %s
            """, (service, song_id))
            rows = cur.fetchall()
            return [row['service_id'] for row in rows]

    if conn is not None:
        return _execute(conn)
    else:
        with get_db_connection() as new_conn:
            return _execute(new_conn)


def get_blocked_albums_for_song(song_id: str, service: str = 'spotify') -> List[str]:
    """
    Get all blocked album IDs for a song.

    Args:
        song_id: Our database song ID
        service: Streaming service name (default: 'spotify')

    Returns:
        List of blocked album IDs
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT service_id FROM bad_streaming_matches
                WHERE service = %s
                  AND block_level = 'album'
                  AND song_id = %s
            """, (service, song_id))
            rows = cur.fetchall()
            return [row['service_id'] for row in rows]