# routes/songs.py
"""
Song API Routes - Recording-Centric Performer Architecture

UPDATED: Recording-Centric Architecture
- Performers come from recording_performers table (not release_performers)
- Spotify URL and album art come from default_release or best release via subqueries
- Dropped columns (spotify_url, spotify_track_id, album_art_*) removed from recordings table
- Album title now comes from default release (releases.title via default_release_id)

UPDATED: Release Imagery Support
- Album art comes from release_imagery table (CAA, Spotify, Apple Music)

Provides endpoints for listing, searching, and creating songs.
"""
from flask import Blueprint, jsonify, request, g
import logging
import json
import time
import db_utils as db_tools
from utils.helpers import safe_strip
from middleware.auth_middleware import require_auth

logger = logging.getLogger(__name__)
songs_bp = Blueprint('songs', __name__)


# ============================================================================
# SQL FRAGMENTS FOR ALBUM ART (same as recordings.py)
# ============================================================================
# All imagery now comes from release_imagery table (CAA, Spotify, Apple Music)
# Priority: default_release first, then any linked release

ALBUM_ART_SMALL_SQL = """
    COALESCE(
        -- 1. release_imagery (Front) for default release, prefer CAA for consistency with back covers
        (SELECT ri.image_url_small FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        -- 2. release_imagery (Front) for any linked release
        (SELECT ri.image_url_small
         FROM recording_releases rr_sub
         JOIN release_imagery ri ON rr_sub.release_id = ri.release_id
         WHERE rr_sub.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as best_cover_art_small"""

ALBUM_ART_MEDIUM_SQL = """
    COALESCE(
        (SELECT ri.image_url_medium FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.image_url_medium
         FROM recording_releases rr_sub
         JOIN release_imagery ri ON rr_sub.release_id = ri.release_id
         WHERE rr_sub.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as best_cover_art_medium"""

ALBUM_ART_LARGE_SQL = """
    COALESCE(
        (SELECT ri.image_url_large FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.image_url_large
         FROM recording_releases rr_sub
         JOIN release_imagery ri ON rr_sub.release_id = ri.release_id
         WHERE rr_sub.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as best_cover_art_large"""

# Source info for front cover (for watermark/attribution)
ALBUM_ART_SOURCE_SQL = """
    COALESCE(
        (SELECT ri.source::text FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.source::text
         FROM recording_releases rr_sub
         JOIN release_imagery ri ON rr_sub.release_id = ri.release_id
         WHERE rr_sub.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as best_cover_art_source"""

ALBUM_ART_SOURCE_URL_SQL = """
    COALESCE(
        (SELECT ri.source_url FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.source_url
         FROM recording_releases rr_sub
         JOIN release_imagery ri ON rr_sub.release_id = ri.release_id
         WHERE rr_sub.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as best_cover_art_source_url"""

# Back cover art - ONLY from the default release for consistency with front cover
# (No fallback to linked releases to avoid showing mismatched front/back from different releases)
BACK_COVER_SMALL_SQL = """
    (SELECT ri.image_url_small FROM release_imagery ri
     WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
     LIMIT 1) as back_cover_art_small"""

BACK_COVER_MEDIUM_SQL = """
    (SELECT ri.image_url_medium FROM release_imagery ri
     WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
     LIMIT 1) as back_cover_art_medium"""

BACK_COVER_LARGE_SQL = """
    (SELECT ri.image_url_large FROM release_imagery ri
     WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
     LIMIT 1) as back_cover_art_large"""

HAS_BACK_COVER_SQL = """
    EXISTS(
        SELECT 1 FROM release_imagery ri
        WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
    ) as has_back_cover"""

# Source info for back cover (for watermark/attribution)
BACK_COVER_SOURCE_SQL = """
    (SELECT ri.source::text FROM release_imagery ri
     WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
     LIMIT 1) as back_cover_source"""

BACK_COVER_SOURCE_URL_SQL = """
    (SELECT ri.source_url FROM release_imagery ri
     WHERE ri.release_id = r.default_release_id AND ri.type = 'Back'
     LIMIT 1) as back_cover_source_url"""

# For authority recommendations - uses 'r' alias for recordings
AUTHORITY_ALBUM_ART_SQL = """
    COALESCE(
        (SELECT ri.image_url_large FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.image_url_large
         FROM recording_releases rr
         JOIN release_imagery ri ON rr.release_id = ri.release_id
         WHERE rr.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as matched_album_art"""

AUTHORITY_ALBUM_ART_SOURCE_SQL = """
    COALESCE(
        (SELECT ri.source::text FROM release_imagery ri
         WHERE ri.release_id = r.default_release_id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1),
        (SELECT ri.source::text
         FROM recording_releases rr
         JOIN release_imagery ri ON rr.release_id = ri.release_id
         WHERE rr.recording_id = r.id AND ri.type = 'Front'
         ORDER BY CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
         LIMIT 1)
    ) as matched_album_art_source"""

# ============================================================================
# SQL FRAGMENTS FOR SPOTIFY URLS (Normalized Streaming Links)
# ============================================================================
# Spotify track URLs come from recording_release_streaming_links table

SPOTIFY_URL_FROM_DEFAULT_RELEASE_SQL = """
    (SELECT rrsl.service_url
     FROM recording_releases rr_sub
     JOIN recording_release_streaming_links rrsl
         ON rrsl.recording_release_id = rr_sub.id AND rrsl.service = 'spotify'
     WHERE rr_sub.release_id = r.default_release_id AND rr_sub.recording_id = r.id
    )"""

SPOTIFY_URL_FROM_ANY_RELEASE_SQL = """
    (SELECT rrsl.service_url
     FROM recording_releases rr_sub
     JOIN recording_release_streaming_links rrsl
         ON rrsl.recording_release_id = rr_sub.id AND rrsl.service = 'spotify'
     WHERE rr_sub.recording_id = r.id
     LIMIT 1
    )"""

# For has_spotify availability check - checks normalized streaming_links table
HAS_SPOTIFY_SQL = """
    EXISTS(SELECT 1 FROM recording_releases rr2
           JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr2.id
           WHERE rr2.recording_id = r.id AND rrsl.service = 'spotify')"""


# All song-related endpoints:
# - GET /api/songs
# - POST /api/songs
# - GET /api/songs/<song_id>
# - GET /api/songs/search


@songs_bp.route('/songs', methods=['GET'])
def get_songs():
    """Get all songs or search songs by title"""
    search_query = request.args.get('search', '')
    
    try:
        if search_query:
            # unaccent() on both sides so "Besame" matches "Bésame" (#165)
            query = """
                SELECT id, title, composer, composed_year, composed_key, structure, musicbrainz_id, wikipedia_url, song_reference, external_references,
                       created_at, updated_at
                FROM songs
                WHERE unaccent(title) ILIKE unaccent(%s)
                   OR unaccent(composer) ILIKE unaccent(%s)
                   OR EXISTS (SELECT 1 FROM unnest(alt_titles) AS alt WHERE unaccent(alt) ILIKE unaccent(%s))
                ORDER BY title
            """
            params = (f'%{search_query}%', f'%{search_query}%', f'%{search_query}%')
        else:
            query = """
                SELECT id, title, composer, composed_year, composed_key, structure, musicbrainz_id, wikipedia_url, song_reference, external_references,
                       created_at, updated_at
                FROM songs
                ORDER BY title
            """
            params = None

        songs = db_tools.execute_query(query, params)
        return jsonify(songs)
        
    except Exception as e:
        logger.error(f"Error fetching songs: {e}")
        return jsonify({'error': 'Failed to fetch songs', 'detail': str(e)}), 500



@songs_bp.route('/songs/<song_id>/summary', methods=['GET'])
def get_song_summary(song_id):
    """
    Get song metadata, transcriptions, and featured (authoritative) recordings ONLY.

    This is a fast endpoint for initial page load. It returns:
    - Song metadata (title, composer, structure, external references, etc.)
    - Transcriptions for the song
    - Only featured recordings (those with authority recommendations)

    The full recordings list should be loaded separately via /api/songs/<song_id>/recordings
    """
    try:
        # Query for song + transcriptions + featured recordings only
        combined_query = f"""
            WITH song_data AS (
                SELECT
                    s.id, s.title, s.composer, s.composed_year, s.composed_key,
                    s.structure, s.song_reference,
                    s.musicbrainz_id, s.wikipedia_url, s.external_references,
                    s.created_at, s.updated_at,
                    (SELECT COUNT(*)
                     FROM song_authority_recommendations
                     WHERE song_id = s.id) as authority_recommendation_count,
                    (SELECT COUNT(*) FROM recordings WHERE song_id = s.id) as recording_count,
                    -- Check if any recording has streaming links (for play button)
                    EXISTS(
                        SELECT 1 FROM recordings r2
                        JOIN recording_releases rr2 ON rr2.recording_id = r2.id
                        JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr2.id
                        WHERE r2.song_id = s.id
                    ) OR EXISTS(
                        SELECT 1 FROM recordings r2
                        JOIN recording_releases rr2 ON rr2.recording_id = r2.id
                        JOIN releases rel2 ON rr2.release_id = rel2.id
                        WHERE r2.song_id = s.id AND rel2.spotify_album_id IS NOT NULL
                    ) as has_any_streaming
                FROM songs s
                WHERE s.id = %s
            ),
            featured_recordings AS (
                SELECT
                    r.id,
                    r.title,
                    def_rel.title as album_title,
                    def_rel.artist_credit as artist_credit,
                    r.recording_date,
                    r.recording_year,
                    r.label,
                    r.default_release_id,
                    -- Spotify track URL from normalized streaming_links table
                    COALESCE(
                        {SPOTIFY_URL_FROM_DEFAULT_RELEASE_SQL},
                        {SPOTIFY_URL_FROM_ANY_RELEASE_SQL}
                    ) as best_spotify_url,
                    {ALBUM_ART_SMALL_SQL},
                    {ALBUM_ART_MEDIUM_SQL},
                    {ALBUM_ART_LARGE_SQL},
                    {ALBUM_ART_SOURCE_SQL},
                    {ALBUM_ART_SOURCE_URL_SQL},
                    {BACK_COVER_SMALL_SQL},
                    {BACK_COVER_MEDIUM_SQL},
                    {BACK_COVER_LARGE_SQL},
                    {HAS_BACK_COVER_SQL},
                    {BACK_COVER_SOURCE_SQL},
                    {BACK_COVER_SOURCE_URL_SQL},
                    r.musicbrainz_id,
                    r.is_canonical,
                    r.notes,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id', p.id,
                                'name', p.name,
                                'sort_name', p.sort_name,
                                'instrument', i.name,
                                'role', rp.role
                            ) ORDER BY
                                CASE rp.role
                                    WHEN 'leader' THEN 1
                                    WHEN 'sideman' THEN 2
                                    ELSE 3
                                END,
                                COALESCE(p.sort_name, p.name)
                        ) FILTER (WHERE p.id IS NOT NULL),
                        '[]'::json
                    ) as performers,
                    COUNT(DISTINCT sar.id) as authority_count,
                    COALESCE(
                        array_agg(DISTINCT sar.source) FILTER (WHERE sar.source IS NOT NULL),
                        ARRAY[]::text[]
                    ) as authority_sources,
                    -- Check if any streaming link exists (for play button visibility)
                    (
                        EXISTS(SELECT 1 FROM recording_releases rr2
                               JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr2.id
                               WHERE rr2.recording_id = r.id)
                        OR EXISTS(SELECT 1 FROM recording_releases rr2
                                  JOIN releases rel2 ON rr2.release_id = rel2.id
                                  WHERE rr2.recording_id = r.id AND rel2.spotify_album_id IS NOT NULL)
                    ) as has_streaming,
                    -- Get available streaming services as array
                    COALESCE(
                        (SELECT array_agg(DISTINCT rrsl.service)
                         FROM recording_releases rr2
                         JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr2.id
                         WHERE rr2.recording_id = r.id),
                        ARRAY[]::varchar[]
                    ) as streaming_services
                FROM recordings r
                INNER JOIN song_authority_recommendations sar ON r.id = sar.recording_id
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                LEFT JOIN recording_performers rp ON r.id = rp.recording_id
                LEFT JOIN performers p ON rp.performer_id = p.id
                LEFT JOIN instruments i ON rp.instrument_id = i.id
                WHERE r.song_id = %s
                GROUP BY r.id, def_rel.title, def_rel.artist_credit, r.recording_date, r.recording_year,
                         r.label, r.default_release_id,
                         r.musicbrainz_id, r.is_canonical, r.notes
                ORDER BY r.recording_year ASC NULLS LAST
            ),
            transcriptions_data AS (
                SELECT
                    st.id,
                    st.song_id,
                    st.recording_id,
                    st.youtube_url,
                    st.created_at,
                    st.updated_at,
                    def_rel.title as album_title,
                    r.recording_year
                FROM solo_transcriptions st
                LEFT JOIN recordings r ON st.recording_id = r.id
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE st.song_id = %s
                ORDER BY r.recording_year DESC
            )
            SELECT
                (SELECT row_to_json(song_data.*) FROM song_data) as song,
                (SELECT json_agg(featured_recordings.*) FROM featured_recordings) as featured_recordings,
                (SELECT json_agg(transcriptions_data.*) FROM transcriptions_data) as transcriptions
        """

        result = db_tools.execute_query(combined_query, (song_id, song_id, song_id), fetch_one=True)

        if not result or not result['song']:
            return jsonify({'error': 'Song not found'}), 404

        song_dict = result['song']
        featured_recordings = result['featured_recordings'] if result['featured_recordings'] else []
        transcriptions = result['transcriptions'] if result['transcriptions'] else []

        song_dict['featured_recordings'] = featured_recordings
        song_dict['transcriptions'] = transcriptions
        song_dict['transcription_count'] = len(transcriptions)

        return jsonify(song_dict)

    except Exception as e:
        logger.error(f"Error fetching song summary: {e}")
        return jsonify({'error': 'Failed to fetch song summary', 'detail': str(e)}), 500


def fetch_song_recordings_listing(song_id, sort_by='year'):
    """
    Canonical "song's recording list" data fetch.

    Returns the same payload the iOS/Mac apps see from
    GET /api/songs/<song_id>/recordings, but as a plain list of dicts
    (no jsonify, no Flask response). Admin pages call this directly so
    they're guaranteed to render the *same* primary-release pick, cover
    art priority, and streaming flags as the app — there's exactly one
    SQL definition of "what you see in a recordings list".

    Args:
        song_id: UUID string
        sort_by: 'year' (default) or 'name' (by leader's last name)

    Returns:
        List of recording dicts (empty list if no recordings).
    """
    if sort_by == 'name':
        recordings_order = """
            (
                SELECT COALESCE(p2.sort_name, p2.name, 'ZZZ')
                FROM recording_performers rp2
                JOIN performers p2 ON rp2.performer_id = p2.id
                WHERE rp2.recording_id = r.id AND rp2.role = 'leader'
                ORDER BY COALESCE(p2.sort_name, p2.name)
                LIMIT 1
            ) ASC NULLS LAST,
            r.recording_year ASC NULLS LAST
        """
    else:
        recordings_order = "r.recording_year ASC NULLS LAST"

    recordings_query = f"""
        WITH
        -- CTE 1: Front cover art (default release preferred, fallback to any linked release)
        front_art AS (
            SELECT DISTINCT ON (sub.recording_id)
                sub.recording_id,
                sub.image_url_small, sub.image_url_medium, sub.image_url_large,
                sub.source, sub.source_url
            FROM (
                SELECT r.id as recording_id,
                       ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                       ri.source::text as source, ri.source_url,
                       1 as priority,
                       CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END as source_order
                FROM recordings r
                JOIN release_imagery ri ON ri.release_id = r.default_release_id AND ri.type = 'Front'
                WHERE r.song_id = %s
                UNION ALL
                SELECT r.id, ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                       ri.source::text, ri.source_url,
                       2 as priority,
                       CASE WHEN ri.source = 'MusicBrainz' THEN 0 ELSE 1 END
                FROM recordings r
                JOIN recording_releases rr ON rr.recording_id = r.id
                JOIN release_imagery ri ON ri.release_id = rr.release_id AND ri.type = 'Front'
                WHERE r.song_id = %s
            ) sub
            ORDER BY sub.recording_id, sub.priority, sub.source_order
        ),
        -- CTE 2: Back cover art (default release only, for consistency with front)
        back_art AS (
            SELECT DISTINCT ON (r.id)
                r.id as recording_id,
                ri.image_url_small, ri.image_url_medium, ri.image_url_large,
                ri.source::text as source, ri.source_url,
                TRUE as has_back_cover
            FROM recordings r
            JOIN release_imagery ri ON ri.release_id = r.default_release_id AND ri.type = 'Back'
            WHERE r.song_id = %s
        ),
        -- CTE 3: Streaming availability (all services in one pass)
        streaming AS (
            SELECT
                rr.recording_id,
                bool_or(TRUE) as has_streaming,
                bool_or(rrsl.service = 'spotify') as has_spotify,
                bool_or(rrsl.service = 'apple_music') as has_apple_music,
                bool_or(rrsl.service = 'youtube') as has_youtube,
                array_agg(DISTINCT rrsl.service) as streaming_services
            FROM recording_releases rr
            JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr.id
            WHERE rr.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            GROUP BY rr.recording_id
        ),
        -- CTE 4: Best Spotify URL per recording (prefer default release)
        spotify_urls AS (
            SELECT DISTINCT ON (rr.recording_id)
                rr.recording_id,
                rrsl.service_url as best_spotify_url
            FROM recording_releases rr
            JOIN recording_release_streaming_links rrsl
                ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
            WHERE rr.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            ORDER BY rr.recording_id,
                CASE WHEN rr.release_id = (
                    SELECT default_release_id FROM recordings WHERE id = rr.recording_id
                ) THEN 0 ELSE 1 END
        ),
        -- CTE 5: Lightweight community data — only is_instrumental consensus
        -- (used for vocal/instrumental filter in recordings list; full community
        -- data is fetched by the recording detail endpoint when needed)
        community AS (
            SELECT
                rc.recording_id,
                jsonb_build_object(
                    'consensus', jsonb_build_object(
                        'is_instrumental', (
                            SELECT is_instrumental FROM recording_contributions rc2
                            WHERE rc2.recording_id = rc.recording_id AND rc2.is_instrumental IS NOT NULL
                            GROUP BY is_instrumental ORDER BY COUNT(*) DESC, MAX(updated_at) DESC LIMIT 1
                        )
                    ),
                    'counts', jsonb_build_object(
                        'instrumental', COUNT(*) FILTER (WHERE rc.is_instrumental IS NOT NULL)
                    )
                ) as community_data
            FROM recording_contributions rc
            WHERE rc.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            GROUP BY rc.recording_id
        )
        -- Main query: JOIN pre-computed CTEs instead of correlated subqueries
        -- Fields deliberately excluded from this LIST-VIEW payload because
        -- no list-row view reads them (recording detail re-fetches via
        -- /api/recordings/<id>, which carries the full payload):
        --   r.musicbrainz_id, r.default_release_id, r.recording_date,
        --   r.label, r.notes
        -- See backend/tests/test_song_recordings.py for the field contract.
        SELECT
            r.id,
            r.title,
            def_rel.title as album_title,
            def_rel.artist_credit as artist_credit,
            r.recording_year,
            su.best_spotify_url,
            fa.image_url_small as best_cover_art_small,
            fa.image_url_medium as best_cover_art_medium,
            fa.image_url_large as best_cover_art_large,
            fa.source as best_cover_art_source,
            fa.source_url as best_cover_art_source_url,
            ba.image_url_small as back_cover_art_small,
            ba.image_url_medium as back_cover_art_medium,
            ba.image_url_large as back_cover_art_large,
            COALESCE(ba.has_back_cover, FALSE) as has_back_cover,
            ba.source as back_cover_source,
            ba.source_url as back_cover_source_url,
            r.is_canonical,
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', p.id,
                        'name', p.name,
                        'sort_name', p.sort_name,
                        'instrument', i.name,
                        'role', rp.role
                    ) ORDER BY
                        CASE rp.role
                            WHEN 'leader' THEN 1
                            WHEN 'sideman' THEN 2
                            ELSE 3
                        END,
                        COALESCE(p.sort_name, p.name)
                ) FILTER (WHERE p.id IS NOT NULL),
                '[]'::json
            ) as performers,
            COUNT(DISTINCT sar.id) as authority_count,
            COALESCE(
                array_agg(DISTINCT sar.source) FILTER (WHERE sar.source IS NOT NULL),
                ARRAY[]::text[]
            ) as authority_sources,
            COALESCE(st.has_streaming, FALSE) as has_streaming,
            COALESCE(st.has_spotify, FALSE) as has_spotify,
            COALESCE(st.has_apple_music, FALSE) as has_apple_music,
            COALESCE(st.has_youtube, FALSE) as has_youtube,
            COALESCE(st.streaming_services, ARRAY[]::varchar[]) as streaming_services,
            cm.community_data
        FROM recordings r
        LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
        LEFT JOIN recording_performers rp ON r.id = rp.recording_id
        LEFT JOIN performers p ON rp.performer_id = p.id
        LEFT JOIN instruments i ON rp.instrument_id = i.id
        LEFT JOIN song_authority_recommendations sar ON r.id = sar.recording_id
        LEFT JOIN front_art fa ON fa.recording_id = r.id
        LEFT JOIN back_art ba ON ba.recording_id = r.id
        LEFT JOIN streaming st ON st.recording_id = r.id
        LEFT JOIN spotify_urls su ON su.recording_id = r.id
        LEFT JOIN community cm ON cm.recording_id = r.id
        WHERE r.song_id = %s
        GROUP BY r.id, def_rel.title, def_rel.artist_credit, r.recording_year,
                 r.is_canonical,
                 su.best_spotify_url,
                 fa.image_url_small, fa.image_url_medium, fa.image_url_large, fa.source, fa.source_url,
                 ba.image_url_small, ba.image_url_medium, ba.image_url_large, ba.has_back_cover, ba.source, ba.source_url,
                 st.has_streaming, st.has_spotify, st.has_apple_music, st.has_youtube, st.streaming_services,
                 cm.community_data
        ORDER BY {recordings_order}
    """

    # CTEs use song_id 6 times + main WHERE uses it once = 7 params
    recordings = db_tools.execute_query(
        recordings_query,
        (song_id, song_id, song_id, song_id, song_id, song_id, song_id)
    )
    return recordings or []


@songs_bp.route('/songs/<song_id>/recordings', methods=['GET'])
def get_song_recordings(song_id):
    """
    Get all recordings for a song with performers and authority info.

    This is the heavier endpoint that loads all recordings. Call this after
    the summary endpoint has returned to populate the full recordings list.

    Supports sort parameter: 'year' (default) or 'name' (by leader's last name)

    PERFORMANCE: Uses CTEs to pre-compute album art, streaming data, and
    community contributions in bulk, then JOINs them. This avoids ~25
    correlated subqueries per recording row which caused O(N*25) query
    evaluations for songs with many recordings.
    """
    try:
        sort_by = request.args.get('sort', 'year')
        recordings = fetch_song_recordings_listing(song_id, sort_by=sort_by)
        return jsonify({
            'song_id': song_id,
            'recordings': recordings,
            'recording_count': len(recordings),
        })
    except Exception as e:
        logger.error(f"Error fetching song recordings: {e}")
        return jsonify({'error': 'Failed to fetch recordings', 'detail': str(e)}), 500


@songs_bp.route('/songs/<song_id>/recordings/shell', methods=['GET'])
def get_song_recordings_shell(song_id):
    """
    Shell (metadata-only) payload for a song's recordings list.

    This is the "first paint" half of the shell+hydrate pattern described in
    backend/tests/test_song_recordings_shell.py. The iOS/Mac app loads this
    to render group headers (by decade or by artist), run the vocal /
    instrumental / instrument / streaming-service filters, and show skeleton
    rows; it then calls GET /api/recordings/batch with the IDs of rows that
    scroll into view to fill in cover art and full performer data.

    Compared to GET /api/songs/<id>/recordings, this endpoint omits
    everything that's only needed to render a row *visually*:
      * cover art URLs (front + back, 3 sizes each) + source/source_url
      * sidemen in performers[] (leader only, 1 object)
      * community_data jsonb (just the is_instrumental consensus as a bool)
      * streaming_services array (has_* flags are enough for filters)
      * authority_sources array (authority_count is enough for the badge)
      * has_back_cover, best_spotify_url

    The trade-off: ~150 bytes/row vs ~1.5KB/row on the existing endpoint.
    For "Ain't Misbehavin'" (759 recordings) that's ~110KB raw vs ~750KB —
    about 7x smaller gzipped.

    See backend/tests/test_song_recordings_shell.py for the field contract.
    """
    try:
        sort_by = request.args.get('sort', 'year')

        # Shell does not need the CTEs the full endpoint uses for cover art
        # or full streaming link details — a much simpler query suffices.
        #
        # Leader: DISTINCT ON picks a single leader per recording and
        # exposes leader_sort_name so the sort=name branch can ORDER BY it
        # without the correlated subquery the full endpoint uses.
        #
        # instruments_present: flat array of all instruments on the
        # recording. The iOS instrument-family filter reads this, so we
        # need *every* performer's instrument — not just the leader's.
        #
        # is_instrumental: most-common consensus value (same tiebreaker
        # as the full endpoint's community CTE), but surfaced as a plain
        # bool column rather than wrapped in a community_data jsonb.

        if sort_by == 'name':
            shell_order = """
                COALESCE(l.leader_sort_name, l.leader_name) ASC NULLS LAST,
                r.recording_year ASC NULLS LAST
            """
        else:
            shell_order = "r.recording_year ASC NULLS LAST"

        shell_query = f"""
            WITH
            leader AS (
                SELECT DISTINCT ON (rp.recording_id)
                    rp.recording_id,
                    p.name as leader_name,
                    p.sort_name as leader_sort_name,
                    json_build_array(
                        json_build_object(
                            'id', p.id,
                            'name', p.name,
                            'sort_name', p.sort_name,
                            'instrument', i.name,
                            'role', 'leader'
                        )
                    ) as performers
                FROM recording_performers rp
                JOIN performers p ON rp.performer_id = p.id
                LEFT JOIN instruments i ON rp.instrument_id = i.id
                WHERE rp.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
                  AND rp.role = 'leader'
                ORDER BY rp.recording_id, COALESCE(p.sort_name, p.name)
            ),
            instruments_present AS (
                SELECT
                    rp.recording_id,
                    array_agg(DISTINCT i.name) FILTER (WHERE i.name IS NOT NULL) as instruments
                FROM recording_performers rp
                LEFT JOIN instruments i ON rp.instrument_id = i.id
                WHERE rp.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
                GROUP BY rp.recording_id
            ),
            streaming AS (
                SELECT
                    rr.recording_id,
                    bool_or(TRUE) as has_streaming,
                    bool_or(rrsl.service = 'spotify') as has_spotify,
                    bool_or(rrsl.service = 'apple_music') as has_apple_music,
                    bool_or(rrsl.service = 'youtube') as has_youtube
                FROM recording_releases rr
                JOIN recording_release_streaming_links rrsl ON rrsl.recording_release_id = rr.id
                WHERE rr.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
                GROUP BY rr.recording_id
            ),
            community_vocal AS (
                SELECT DISTINCT
                    rc.recording_id,
                    (
                        SELECT is_instrumental FROM recording_contributions rc2
                        WHERE rc2.recording_id = rc.recording_id
                          AND rc2.is_instrumental IS NOT NULL
                        GROUP BY is_instrumental
                        ORDER BY COUNT(*) DESC, MAX(updated_at) DESC
                        LIMIT 1
                    ) as is_instrumental
                FROM recording_contributions rc
                WHERE rc.recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
            ),
            -- authority_count as a pre-aggregated CTE so the main query needs
            -- no GROUP BY (json performers has no equality operator and so
            -- can't appear in a GROUP BY clause).
            authority AS (
                SELECT recording_id, COUNT(*) as authority_count
                FROM song_authority_recommendations
                WHERE recording_id IN (SELECT id FROM recordings WHERE song_id = %s)
                GROUP BY recording_id
            )
            SELECT
                r.id,
                r.title,
                def_rel.title as album_title,
                def_rel.artist_credit as artist_credit,
                r.recording_year,
                r.is_canonical,
                COALESCE(l.performers, '[]'::json) as performers,
                COALESCE(ip.instruments, ARRAY[]::text[]) as instruments_present,
                cv.is_instrumental,
                COALESCE(st.has_streaming, FALSE) as has_streaming,
                COALESCE(st.has_spotify, FALSE) as has_spotify,
                COALESCE(st.has_apple_music, FALSE) as has_apple_music,
                COALESCE(st.has_youtube, FALSE) as has_youtube,
                COALESCE(a.authority_count, 0) as authority_count
            FROM recordings r
            LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
            LEFT JOIN leader l ON l.recording_id = r.id
            LEFT JOIN instruments_present ip ON ip.recording_id = r.id
            LEFT JOIN streaming st ON st.recording_id = r.id
            LEFT JOIN community_vocal cv ON cv.recording_id = r.id
            LEFT JOIN authority a ON a.recording_id = r.id
            WHERE r.song_id = %s
            ORDER BY {shell_order}
        """

        # Timing breakdown: the client log currently shows ~2-3s for this
        # endpoint on Render's Starter tier (0.5 shared CPU) while the raw
        # SQL is well under 100ms in EXPLAIN ANALYZE. Instrument the two
        # server-side phases we can measure so a Render log line tells us
        # which one is eating the wall time (query vs. Python-side JSON
        # serialization). If both are small and responseTimeMS is still
        # high, the remaining time is in Flask/gunicorn/Render ingress.
        t_start = time.perf_counter()

        # 5 CTEs use song_id once each + main WHERE uses it once = 6 params
        recordings = db_tools.execute_query(
            shell_query,
            (song_id, song_id, song_id, song_id, song_id, song_id)
        )
        t_query_done = time.perf_counter()

        response = jsonify({
            'song_id': song_id,
            'recordings': recordings if recordings else [],
            'recording_count': len(recordings) if recordings else 0
        })
        t_serialize_done = time.perf_counter()

        row_count = len(recordings) if recordings else 0
        query_ms = (t_query_done - t_start) * 1000
        serialize_ms = (t_serialize_done - t_query_done) * 1000
        logger.info(
            f"shell song={song_id} rows={row_count} "
            f"query_ms={query_ms:.0f} serialize_ms={serialize_ms:.0f}"
        )
        return response

    except Exception as e:
        logger.error(f"Error fetching song recordings shell: {e}", exc_info=True)
        return jsonify({'error': 'Failed to fetch shell', 'detail': str(e)}), 500


@songs_bp.route('/songs/<song_id>', methods=['GET'])
def get_song_detail(song_id):
    """
    Get detailed information about a specific song with all recordings, performers, transcriptions,
    AND authority recommendations.

    ULTRA-OPTIMIZED VERSION: Uses a SINGLE query with CTEs instead of 4+ separate queries.
    This eliminates network round trips between Render and Supabase.

    UPDATED: Recording-Centric Architecture
    - Removed dropped columns (spotify_url, spotify_track_id, album_art_*) from recordings
    - Spotify/artwork data now comes entirely from releases via subqueries
    - Performers come from recording_performers table

    UPDATED: Release Imagery Support
    - Album art checks release_imagery table first (CAA images)
    - Falls back to releases table (Spotify images) if no release_imagery exists

    NEW: Includes authority recommendation counts and sort parameter support.

    NOTE: For better performance, consider using /api/songs/<song_id>/summary followed by
    /api/songs/<song_id>/recordings for progressive loading.
    """
    try:
        # Get sort preference from query parameter (default: 'year')
        # UPDATED: Changed from authority/year/canonical to name/year
        sort_by = request.args.get('sort', 'year')
        
        # Build ORDER BY clause based on sort preference
        if sort_by == 'name':
            # Sort alphabetically by leader's sort_name (or name as fallback)
            recordings_order = """
                (
                    SELECT COALESCE(p2.sort_name, p2.name, 'ZZZ')
                    FROM recording_performers rp2
                    JOIN performers p2 ON rp2.performer_id = p2.id
                    WHERE rp2.recording_id = r.id AND rp2.role = 'leader'
                    ORDER BY COALESCE(p2.sort_name, p2.name)
                    LIMIT 1
                ) ASC NULLS LAST,
                r.recording_year ASC NULLS LAST
            """
        else:  # 'year' - default
            # Sort by recording year, oldest first
            recordings_order = "r.recording_year ASC NULLS LAST"
        
        # ONE QUERY to get everything - song, recordings+performers+authority, transcriptions
        # UPDATED: Album art now uses release_imagery priority
        combined_query = f"""
            WITH song_data AS (
                SELECT
                    s.id, s.title, s.composer, s.composed_year, s.composed_key,
                    s.structure, s.song_reference,
                    s.musicbrainz_id, s.wikipedia_url, s.external_references,
                    s.created_at, s.updated_at,
                    -- NEW: Count total authority recommendations for this song
                    (SELECT COUNT(*)
                     FROM song_authority_recommendations
                     WHERE song_id = s.id) as authority_recommendation_count
                FROM songs s
                WHERE s.id = %s
            ),
            recordings_with_performers AS (
                SELECT
                    r.id,
                    r.title,
                    def_rel.title as album_title,
                    def_rel.artist_credit as artist_credit,
                    r.recording_date,
                    r.recording_year,
                    r.label,
                    r.default_release_id,
                    -- Spotify track URL from normalized streaming_links table
                    COALESCE(
                        {SPOTIFY_URL_FROM_DEFAULT_RELEASE_SQL},
                        {SPOTIFY_URL_FROM_ANY_RELEASE_SQL}
                    ) as best_spotify_url,
                    -- Album art with release_imagery priority
                    {ALBUM_ART_SMALL_SQL},
                    {ALBUM_ART_MEDIUM_SQL},
                    {ALBUM_ART_LARGE_SQL},
                    {ALBUM_ART_SOURCE_SQL},
                    {ALBUM_ART_SOURCE_URL_SQL},
                    -- Back cover art (CAA only)
                    {BACK_COVER_SMALL_SQL},
                    {BACK_COVER_MEDIUM_SQL},
                    {BACK_COVER_LARGE_SQL},
                    {HAS_BACK_COVER_SQL},
                    {BACK_COVER_SOURCE_SQL},
                    {BACK_COVER_SOURCE_URL_SQL},
                    r.musicbrainz_id,
                    r.is_canonical,
                    r.notes,
                    -- Performers aggregation from recording_performers
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'id', p.id,
                                'name', p.name,
                                'sort_name', p.sort_name,
                                'instrument', i.name,
                                'role', rp.role
                            ) ORDER BY
                                CASE rp.role
                                    WHEN 'leader' THEN 1
                                    WHEN 'sideman' THEN 2
                                    ELSE 3
                                END,
                                COALESCE(p.sort_name, p.name)
                        ) FILTER (WHERE p.id IS NOT NULL),
                        '[]'::json
                    ) as performers,
                    -- Authority recommendation count per recording
                    COUNT(DISTINCT sar.id) as authority_count,
                    -- Array of authority sources
                    COALESCE(
                        array_agg(DISTINCT sar.source) FILTER (WHERE sar.source IS NOT NULL),
                        ARRAY[]::text[]
                    ) as authority_sources
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                LEFT JOIN recording_performers rp ON r.id = rp.recording_id
                LEFT JOIN performers p ON rp.performer_id = p.id
                LEFT JOIN instruments i ON rp.instrument_id = i.id
                LEFT JOIN song_authority_recommendations sar
                    ON r.id = sar.recording_id
                WHERE r.song_id = %s
                GROUP BY r.id, def_rel.title, def_rel.artist_credit, r.recording_date, r.recording_year,
                         r.label, r.default_release_id,
                         r.musicbrainz_id, r.is_canonical, r.notes
                ORDER BY {recordings_order}
            ),
            transcriptions_data AS (
                SELECT
                    st.id,
                    st.song_id,
                    st.recording_id,
                    st.youtube_url,
                    st.created_at,
                    st.updated_at,
                    def_rel.title as album_title,
                    r.recording_year
                FROM solo_transcriptions st
                LEFT JOIN recordings r ON st.recording_id = r.id
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE st.song_id = %s
                ORDER BY r.recording_year DESC
            )
            SELECT
                (SELECT row_to_json(song_data.*) FROM song_data) as song,
                (SELECT json_agg(recordings_with_performers.*) FROM recordings_with_performers) as recordings,
                (SELECT json_agg(transcriptions_data.*) FROM transcriptions_data) as transcriptions
        """
        
        # Execute the single query with song_id passed 3 times (for each CTE)
        result = db_tools.execute_query(combined_query, (song_id, song_id, song_id), fetch_one=True)
        
        if not result or not result['song']:
            return jsonify({'error': 'Song not found'}), 404
        
        # Build response from the single query result
        song_dict = result['song']
        recordings = result['recordings'] if result['recordings'] else []
        transcriptions = result['transcriptions'] if result['transcriptions'] else []
        
        song_dict['recordings'] = recordings
        song_dict['recording_count'] = len(recordings)
        song_dict['transcriptions'] = transcriptions
        song_dict['transcription_count'] = len(transcriptions)
        
        return jsonify(song_dict)
        
    except Exception as e:
        logger.error(f"Error fetching song detail: {e}")
        return jsonify({'error': 'Failed to fetch song details', 'detail': str(e)}), 500


# NOTES ON CHANGES:
# 
# 1. SONG DATA CTE - Added subquery for authority_recommendation_count
#    - Uses (SELECT COUNT(*) FROM song_authority_recommendations WHERE song_id = s.id)
#    - No joins needed, keeps it simple
#
# 2. RECORDINGS CTE - Recording-Centric Architecture Updates
#    - REMOVED: r.spotify_url, r.spotify_track_id, r.album_art_small/medium/large (dropped columns)
#    - ADDED: r.default_release_id to GROUP BY
#    - Spotify URL comes from best_spotify_url subquery (default release or best available)
#    - Cover art comes from best_cover_art_* subqueries with release_imagery priority
#    - Performers still from recording_performers (unchanged)
#
# 3. ORDER BY - Made dynamic based on sort parameter
#    - Default 'year' sorts by recording year
#    - 'name' sorts by leader's last name
#
# 4. RELEASE IMAGERY - Added priority for CAA images
#    - Checks release_imagery table first (type='Front')
#    - Falls back to releases table (Spotify images)


@songs_bp.route('/songs', methods=['POST'])
@require_auth
def create_song():
    """Create a new song (requires authentication)"""
    try:
        data = request.get_json()

        if not data:
            return jsonify({'error': 'No data provided'}), 400

        title = safe_strip(data.get('title'))
        composer = safe_strip(data.get('composer'))
        composed_year = data.get('composed_year')
        composed_key = safe_strip(data.get('composed_key'))
        structure = safe_strip(data.get('structure'))
        song_reference = safe_strip(data.get('song_reference'))
        musicbrainz_id = safe_strip(data.get('musicbrainz_id'))
        wikipedia_url = safe_strip(data.get('wikipedia_url'))
        external_references = data.get('external_references')

        # Set created_by to the authenticated user's ID
        created_by = str(g.current_user['id'])

        if not title:
            return jsonify({'error': 'Title is required'}), 400

        # Build dynamic INSERT
        fields = ['title']
        values = [title]
        placeholders = ['%s']

        if composer:
            fields.append('composer')
            values.append(composer)
            placeholders.append('%s')

        if composed_year is not None:
            fields.append('composed_year')
            values.append(int(composed_year))
            placeholders.append('%s')

        if composed_key:
            fields.append('composed_key')
            values.append(composed_key)
            placeholders.append('%s')

        if structure:
            fields.append('structure')
            values.append(structure)
            placeholders.append('%s')
        
        if song_reference:
            fields.append('song_reference')
            values.append(song_reference)
            placeholders.append('%s')
        
        if musicbrainz_id:
            fields.append('musicbrainz_id')
            values.append(musicbrainz_id)
            placeholders.append('%s')
        
        if wikipedia_url:
            fields.append('wikipedia_url')
            values.append(wikipedia_url)
            placeholders.append('%s')
        
        if external_references:
            fields.append('external_references')
            if isinstance(external_references, dict):
                values.append(json.dumps(external_references))
            else:
                values.append(external_references)
            placeholders.append('%s')

        if created_by:
            fields.append('created_by')
            values.append(created_by)
            placeholders.append('%s')

        query = f"""
            INSERT INTO songs ({', '.join(fields)})
            VALUES ({', '.join(placeholders)})
            RETURNING id, title, composer, composed_year, composed_key, structure,
                      musicbrainz_id, wikipedia_url, song_reference, external_references,
                      created_at, updated_at, created_by
        """
        
        result = db_tools.execute_query(query, values, fetch_one=True)

        logger.info(f"Created new song: {title} (ID: {result['id']}, created_by: {created_by})")
        
        return jsonify({
            'success': True,
            'message': 'Song created successfully',
            'song': result
        }), 201
        
    except Exception as e:
        logger.error(f"Error creating song: {e}")
        return jsonify({'error': 'Failed to create song', 'detail': str(e)}), 500


@songs_bp.route('/songs/search', methods=['GET'])
def search_songs():
    """Search songs by title and optionally composer - returns first 20 matches"""
    search_query = request.args.get('q', '').strip()
    
    if not search_query:
        return jsonify([])
    
    try:
        # unaccent() on both sides so "Besame" matches "Bésame" (#165)
        query = """
            SELECT id, title, composer, musicbrainz_id
            FROM songs
            WHERE unaccent(title) ILIKE unaccent(%s)
               OR unaccent(composer) ILIKE unaccent(%s)
               OR EXISTS (SELECT 1 FROM unnest(alt_titles) AS alt WHERE unaccent(alt) ILIKE unaccent(%s))
            ORDER BY
                CASE WHEN unaccent(title) ILIKE unaccent(%s) THEN 0 ELSE 1 END,
                title
            LIMIT 20
        """
        params = (
            f'%{search_query}%',
            f'%{search_query}%',
            f'%{search_query}%',
            f'{search_query}%'  # Exact prefix match ranked higher
        )
        
        songs = db_tools.execute_query(query, params)
        return jsonify(songs if songs else [])
        
    except Exception as e:
        logger.error(f"Error searching songs: {e}")
        return jsonify({'error': 'Search failed', 'detail': str(e)}), 500


# ============================================================================
# 2. UPDATE ENDPOINT: PATCH /api/songs/<song_id>
# ============================================================================

@songs_bp.route('/songs/<song_id>', methods=['PATCH'])
def update_song(song_id):
    """
    Update a song's MusicBrainz ID and/or Wikipedia URL
    
    This is a targeted update endpoint primarily for iOS app to set
    identifiers after manual research.
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        musicbrainz_id = safe_strip(data.get('musicbrainz_id'))
        wikipedia_url = safe_strip(data.get('wikipedia_url'))
        
        if not musicbrainz_id and not wikipedia_url:
            return jsonify({'error': 'At least one field (musicbrainz_id or wikipedia_url) must be provided'}), 400
        
        with db_tools.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Check song exists
                cur.execute("SELECT id, title FROM songs WHERE id = %s", (song_id,))
                song = cur.fetchone()
                
                if not song:
                    return jsonify({'error': 'Song not found'}), 404
                
                # Check if another song already has this MusicBrainz ID (if provided)
                if musicbrainz_id:
                    cur.execute("""
                        SELECT id, title FROM songs 
                        WHERE musicbrainz_id = %s AND id != %s
                    """, (musicbrainz_id, song_id))
                    
                    conflict = cur.fetchone()
                    if conflict:
                        return jsonify({
                            'error': 'Another song already has this MusicBrainz ID',
                            'conflicting_song': conflict
                        }), 409
                
                # Build dynamic UPDATE query based on provided fields
                update_fields = []
                params = []
                
                if musicbrainz_id:
                    update_fields.append("musicbrainz_id = %s")
                    params.append(musicbrainz_id)
                
                if wikipedia_url:
                    update_fields.append("wikipedia_url = %s")
                    params.append(wikipedia_url)
                
                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                params.append(song_id)
                
                # Update the song
                cur.execute(f"""
                    UPDATE songs
                    SET {', '.join(update_fields)}
                    WHERE id = %s
                    RETURNING id, title, composer, musicbrainz_id, wikipedia_url
                """, params)
                
                updated_song = cur.fetchone()
                
        logger.info(f"Updated song {song_id} - MusicBrainz ID: {musicbrainz_id}, Wikipedia URL: {wikipedia_url}")
        
        return jsonify({
            'success': True,
            'message': 'Song updated successfully',
            'song': updated_song
        })
        
    except Exception as e:
        logger.error(f"Error updating song: {e}")
        return jsonify({
            'success': False,
            'error': 'An error occurred while updating the song'
        }), 500


# ============================================================================
# 3. NEW ENDPOINT: GET /api/songs/<song_id>/authority_recommendations
# ============================================================================

@songs_bp.route('/songs/<song_id>/authority_recommendations', methods=['GET'])
def get_song_authority_recommendations(song_id):
    """
    Get all authority recommendations for a song (matched and unmatched)
    
    UPDATED: Recording-Centric Architecture
    - Spotify URL and album art now come from releases via subqueries
    - Dropped columns no longer exist on recordings table
    
    UPDATED: Release Imagery Support
    - Album art checks release_imagery table first (CAA images)
    - Falls back to releases table (Spotify images)
    """
    try:
        with db_tools.get_db_connection() as conn:
            with conn.cursor() as cur:
                # Verify song exists
                cur.execute("SELECT id FROM songs WHERE id = %s", (song_id,))
                if not cur.fetchone():
                    return jsonify({'error': 'Song not found'}), 404
                
                # Get all recommendations with optional recording details
                # UPDATED: Spotify URL and album art from releases with release_imagery priority
                # UPDATED: matched_album_title now comes from default release
                cur.execute(f"""
                    SELECT sar.id, sar.source, sar.recommendation_text, sar.source_url,
                           sar.artist_name, sar.album_title, sar.recording_year,
                           sar.itunes_album_id, sar.itunes_track_id,
                           sar.recording_id,
                           def_rel.title as matched_album_title,
                           r.recording_year as matched_year,
                           -- Spotify track URL from normalized streaming_links table
                           COALESCE(
                               (SELECT rrsl.service_url
                                FROM recording_releases rr
                                JOIN recording_release_streaming_links rrsl
                                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                                WHERE rr.release_id = r.default_release_id
                                  AND rr.recording_id = r.id
                               ),
                               (SELECT rrsl.service_url
                                FROM recording_releases rr
                                JOIN recording_release_streaming_links rrsl
                                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                                WHERE rr.recording_id = r.id
                                LIMIT 1
                               )
                           ) as matched_spotify_url,
                           -- Album art with release_imagery priority
                           {AUTHORITY_ALBUM_ART_SQL},
                           {AUTHORITY_ALBUM_ART_SOURCE_SQL}
                    FROM song_authority_recommendations sar
                    LEFT JOIN recordings r ON sar.recording_id = r.id
                    LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                    WHERE sar.song_id = %s
                    ORDER BY
                        CASE WHEN sar.recording_id IS NOT NULL THEN 0 ELSE 1 END,
                        sar.source,
                        sar.artist_name
                """, (song_id,))
                
                recommendations = cur.fetchall()
                
                return jsonify({
                    'song_id': song_id,
                    'recommendations': recommendations,
                    'total_count': len(recommendations),
                    'matched_count': sum(1 for r in recommendations if r['recording_id']),
                    'unmatched_count': sum(1 for r in recommendations if not r['recording_id'])
                })
                
    except Exception as e:
        logger.error(f"Error fetching authority recommendations: {e}")
        return jsonify({'error': 'Failed to fetch recommendations', 'detail': str(e)}), 500


# ============================================================================
# HELPER FUNCTION: Format External References
# ============================================================================

def format_external_references(external_refs):
    """
    Format external_references JSONB field for display
    Returns list of {source, url, display_name} objects
    """
    if not external_refs:
        return []
    
    # Mapping of source keys to display names
    source_names = {
        'wikipedia': 'Wikipedia',
        'jazzstandards': 'JazzStandards.com',
        'allmusic': 'AllMusic',
        'discogs': 'Discogs',
        'musicbrainz': 'MusicBrainz'
    }
    
    formatted = []
    for key, url in external_refs.items():
        formatted.append({
            'source': key,
            'url': url,
            'display_name': source_names.get(key, key.title())
        })
    return formatted


# ============================================================================
# LIGHTWEIGHT INDEX ENDPOINT
# ============================================================================

@songs_bp.route('/songs/index', methods=['GET'])
def get_songs_index():
    """
    Get lightweight list of all songs for building the list view.

    Returns only id, title, composer, composed_year - minimal data for fast loading.
    This enables the iOS app to show the songs list quickly while loading
    detailed data only when a song is selected.

    Query Parameters:
        search: Filter songs by title or composer (case-insensitive partial match)

    Returns:
        Array of {id, title, composer, composed_year} objects
    """
    search_query = request.args.get('search', '')

    try:
        if search_query:
            # unaccent() on both sides so "Besame" matches "Bésame" (#165)
            query = """
                SELECT id, title, composer, composed_year
                FROM songs
                WHERE unaccent(title) ILIKE unaccent(%s)
                   OR unaccent(composer) ILIKE unaccent(%s)
                   OR EXISTS (SELECT 1 FROM unnest(alt_titles) AS alt WHERE unaccent(alt) ILIKE unaccent(%s))
                ORDER BY title
            """
            params = (f'%{search_query}%', f'%{search_query}%', f'%{search_query}%')
        else:
            query = """
                SELECT id, title, composer, composed_year
                FROM songs
                ORDER BY title
            """
            params = None

        songs = db_tools.execute_query(query, params)
        return jsonify(songs)

    except Exception as e:
        logger.error(f"Error fetching songs index: {e}")
        return jsonify({'error': 'Failed to fetch songs index', 'detail': str(e)}), 500