#!/usr/bin/env python3
"""
Audit Spotify track matches for recordings of a song.

Five modes:
1. --audit (default): Check existing spotify_track_ids against Spotify API,
   report whether track name and artist seem like good matches.

2. --dry-run: Ignore existing Spotify data, perform fresh matching,
   compare to what's stored and report what would change (no DB updates).

3. --update: Same as dry-run but actually persist the changes.

4. --verify: Check track/album consistency - verify that each stored
   spotify_track_id actually exists on the associated spotify_album_id.

5. --orphaned-albums: Find releases where we have a spotify_album_id but
   no spotify_track_id - indicates album was matched but track wasn't found.

Usage:
    # Audit existing matches for a song
    python scripts/audit_spotify_tracks.py "Take Five"

    # Dry-run re-match (see what would change)
    python scripts/audit_spotify_tracks.py "Take Five" --dry-run

    # Re-match and update (only processes releases without Spotify URLs)
    python scripts/audit_spotify_tracks.py "Take Five" --update

    # Re-match ALL releases including those with existing Spotify URLs
    python scripts/audit_spotify_tracks.py "Take Five" --update --rematch

    # Verify track/album consistency
    python scripts/audit_spotify_tracks.py "Take Five" --verify

    # Find releases with album but no track match
    python scripts/audit_spotify_tracks.py "Take Five" --orphaned-albums

    # Find and FIX orphaned albums (remove bad spotify_album_id)
    python scripts/audit_spotify_tracks.py "Take Five" --orphaned-albums --fix

    # Force refresh (bypass cache)
    python scripts/audit_spotify_tracks.py "Take Five" --force-refresh
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from db_utils import get_db_connection
from integrations.spotify.matcher import SpotifyMatcher
from integrations.spotify.matching import (
    calculate_similarity,
    normalize_for_comparison,
    match_track_to_recording,
)
from integrations.spotify.search import search_spotify_album
from integrations.spotify.db import find_song_by_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Thresholds for match quality assessment
GOOD_MATCH_THRESHOLD = 85
SUSPECT_MATCH_THRESHOLD = 65


def get_existing_spotify_data(song_id: str) -> list:
    """
    Get all recordings for a song that have existing Spotify track IDs,
    along with release and performer info.

    Checks both the normalized streaming_links table and legacy spotify_track_id column.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    r.id as recording_id,
                    def_rel.title as album_title,
                    r.recording_year,
                    rr.release_id,
                    COALESCE(rrsl.service_id, rr.spotify_track_id) as spotify_track_id,
                    COALESCE(rrsl.service_url,
                        CASE WHEN rr.spotify_track_id IS NOT NULL
                             THEN 'https://open.spotify.com/track/' || rr.spotify_track_id END
                    ) as spotify_track_url,
                    rel.title as release_title,
                    rsl_sp.service_id as spotify_album_id,
                    rsl_sp.service_url as spotify_album_url,
                    rel.artist_credit,
                    -- Get leader/primary performer
                    (
                        SELECT p.name
                        FROM recording_performers rp
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rp.recording_id = r.id AND rp.role = 'leader'
                        LIMIT 1
                    ) as leader_name
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                JOIN recording_releases rr ON r.id = rr.recording_id
                JOIN releases rel ON rr.release_id = rel.id
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                LEFT JOIN release_streaming_links rsl_sp
                    ON rsl_sp.release_id = rel.id AND rsl_sp.service = 'spotify'
                WHERE r.song_id = %s
                  AND (rrsl.service_id IS NOT NULL OR rr.spotify_track_id IS NOT NULL)
                ORDER BY r.recording_year DESC NULLS LAST, rel.title
            """, (song_id,))
            return cur.fetchall()


def get_releases_for_audit(song_id: str) -> list:
    """
    Get all releases for a song (for re-matching), including those without Spotify data.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First get distinct releases, then add JSON fields
            cur.execute("""
                WITH distinct_releases AS (
                    SELECT DISTINCT ON (rel.id)
                        rel.id as release_id,
                        rel.title as release_title,
                        rel.release_year,
                        rsl_sp.service_id as spotify_album_id,
                        rsl_sp.service_url as spotify_album_url,
                        rel.artist_credit
                    FROM releases rel
                    JOIN recording_releases rr ON rel.id = rr.release_id
                    JOIN recordings r ON rr.recording_id = r.id
                    LEFT JOIN release_streaming_links rsl_sp
                        ON rsl_sp.release_id = rel.id AND rsl_sp.service = 'spotify'
                    WHERE r.song_id = %s
                    ORDER BY rel.id, rel.release_year DESC NULLS LAST
                )
                SELECT
                    dr.*,
                    -- Get existing track matches for this release/song combo
                    (
                        SELECT json_agg(json_build_object(
                            'recording_id', rr.recording_id,
                            'spotify_track_id', rr.spotify_track_id,
                            'spotify_track_url', rr.spotify_track_url
                        ))
                        FROM recording_releases rr
                        JOIN recordings rec ON rr.recording_id = rec.id
                        WHERE rr.release_id = dr.release_id
                          AND rec.song_id = %s
                    ) as existing_tracks,
                    -- Get leader for display
                    (
                        SELECT p.name
                        FROM recording_releases rr
                        JOIN recordings rec ON rr.recording_id = rec.id
                        JOIN recording_performers rp ON rec.id = rp.recording_id
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rr.release_id = dr.release_id
                          AND rec.song_id = %s
                          AND rp.role = 'leader'
                        LIMIT 1
                    ) as leader_name
                FROM distinct_releases dr
                ORDER BY dr.release_year DESC NULLS LAST, dr.release_title
            """, (song_id, song_id, song_id))
            return cur.fetchall()


def assess_match_quality(song_title: str, our_artist: str,
                         spotify_track_name: str, spotify_artists: list) -> dict:
    """
    Assess the quality of a Spotify track match.

    Returns dict with:
        - track_similarity: similarity score for track name
        - artist_similarity: best similarity score for artist
        - overall_quality: 'good', 'suspect', or 'bad'
        - details: explanation string
    """
    # Calculate track name similarity
    track_sim = calculate_similarity(song_title, spotify_track_name)

    # Calculate artist similarity (best match among Spotify artists)
    artist_sim = 0
    best_spotify_artist = None
    if our_artist and spotify_artists:
        for sp_artist in spotify_artists:
            sim = calculate_similarity(our_artist, sp_artist)
            if sim > artist_sim:
                artist_sim = sim
                best_spotify_artist = sp_artist

    # Determine overall quality
    if track_sim >= GOOD_MATCH_THRESHOLD and artist_sim >= GOOD_MATCH_THRESHOLD:
        quality = 'good'
    elif track_sim >= SUSPECT_MATCH_THRESHOLD and artist_sim >= SUSPECT_MATCH_THRESHOLD:
        quality = 'suspect'
    elif track_sim >= GOOD_MATCH_THRESHOLD and artist_sim < SUSPECT_MATCH_THRESHOLD:
        # Track matches well but artist doesn't - might be a cover or compilation
        quality = 'suspect'
    elif track_sim < SUSPECT_MATCH_THRESHOLD:
        quality = 'bad'
    else:
        quality = 'suspect'

    return {
        'track_similarity': track_sim,
        'artist_similarity': artist_sim,
        'overall_quality': quality,
        'spotify_track_name': spotify_track_name,
        'spotify_artists': spotify_artists,
        'best_spotify_artist': best_spotify_artist,
    }


def audit_existing_matches(song_name: str, matcher: SpotifyMatcher) -> dict:
    """
    Mode 1: Audit existing Spotify track matches.

    Fetches track details from Spotify and compares to our song title and artist.
    """
    # Find the song
    song = find_song_by_name(song_name)
    if not song:
        logger.error(f"Song not found: {song_name}")
        return None

    logger.info(f"Auditing: {song['title']} (composer: {song['composer']})")
    logger.info(f"Song ID: {song['id']}")
    logger.info("")

    # Get existing Spotify data
    existing = get_existing_spotify_data(song['id'])

    if not existing:
        logger.info("No recordings with Spotify track IDs found.")
        return {'song': song, 'results': [], 'summary': {}}

    logger.info(f"Found {len(existing)} recording/release combinations with Spotify tracks")
    logger.info("")

    results = []
    stats = {'good': 0, 'suspect': 0, 'bad': 0, 'error': 0}

    seen_track_ids = set()

    for rec in existing:
        track_id = rec['spotify_track_id']

        # Skip duplicates (same track on multiple recordings)
        if track_id in seen_track_ids:
            continue
        seen_track_ids.add(track_id)

        # Prefer artist_credit (from release) over leader_name for Spotify comparison
        # because Spotify credits are at album level and often include ensemble names
        # e.g., "Gene Krupa and His Orchestra" vs just "Gene Krupa"
        our_artist = rec['artist_credit'] or rec['leader_name'] or 'Unknown'

        logger.info(f"Checking: {rec['release_title']} ({our_artist})")
        logger.info(f"  Track ID: {track_id}")

        # Fetch from Spotify
        track_details = matcher.client.get_track_details(track_id)

        if not track_details:
            logger.warning(f"  ERROR: Could not fetch track from Spotify")
            stats['error'] += 1
            results.append({
                'release_title': rec['release_title'],
                'our_artist': our_artist,
                'spotify_track_id': track_id,
                'error': 'Could not fetch from Spotify',
                'quality': 'error'
            })
            continue

        spotify_track_name = track_details.get('name', '')
        spotify_artists = [a['name'] for a in track_details.get('artists', [])]

        # Assess match quality
        assessment = assess_match_quality(
            song['title'],
            our_artist,
            spotify_track_name,
            spotify_artists
        )

        quality = assessment['overall_quality']
        stats[quality] += 1

        # Format output
        quality_icon = {'good': '✓', 'suspect': '?', 'bad': '✗'}[quality]
        logger.info(f"  Spotify: \"{spotify_track_name}\" by {', '.join(spotify_artists)}")
        logger.info(f"  Track similarity: {assessment['track_similarity']:.0f}%  |  "
                   f"Artist similarity: {assessment['artist_similarity']:.0f}%")
        logger.info(f"  Assessment: {quality_icon} {quality.upper()}")
        logger.info("")

        results.append({
            'release_title': rec['release_title'],
            'release_id': str(rec['release_id']),
            'recording_id': str(rec['recording_id']),
            'our_artist': our_artist,
            'spotify_track_id': track_id,
            'spotify_track_url': rec['spotify_track_url'],
            'spotify_track_name': spotify_track_name,
            'spotify_artists': spotify_artists,
            'track_similarity': assessment['track_similarity'],
            'artist_similarity': assessment['artist_similarity'],
            'quality': quality
        })

    # Summary
    logger.info("=" * 60)
    logger.info("AUDIT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Good matches:    {stats['good']}")
    logger.info(f"Suspect matches: {stats['suspect']}")
    logger.info(f"Bad matches:     {stats['bad']}")
    logger.info(f"Errors:          {stats['error']}")
    logger.info("=" * 60)

    return {
        'song': {
            'id': str(song['id']),
            'title': song['title'],
            'composer': song['composer']
        },
        'audited_at': datetime.now().isoformat(),
        'results': results,
        'summary': stats
    }


def dry_run_rematch(song_name: str, matcher: SpotifyMatcher) -> dict:
    """
    Mode 2: Dry-run re-match.

    Performs fresh matching and compares to existing data without making changes.
    """
    # Find the song
    song = find_song_by_name(song_name)
    if not song:
        logger.error(f"Song not found: {song_name}")
        return None

    logger.info(f"Dry-run re-match: {song['title']} (composer: {song['composer']})")
    logger.info(f"Song ID: {song['id']}")
    logger.info("")

    # Get current releases with their existing Spotify data
    releases = get_releases_for_audit(song['id'])

    if not releases:
        logger.info("No releases found for this song.")
        return {'song': song, 'results': [], 'summary': {}}

    logger.info(f"Found {len(releases)} releases to check")
    logger.info("")

    results = []
    stats = {'confirmed': 0, 'would_change': 0, 'would_add': 0, 'would_remove': 0, 'no_match': 0}

    for rel in releases:
        # Prefer artist_credit (from release) over leader_name for Spotify comparison
        # because Spotify credits are at album level and often include ensemble names
        artist = rel['artist_credit'] or rel['leader_name'] or 'Unknown'
        existing_album_id = rel['spotify_album_id']
        existing_tracks = rel['existing_tracks'] or []

        logger.info(f"Release: {rel['release_title']} ({artist}, {rel['release_year'] or '?'})")
        logger.info(f"  Existing album ID: {existing_album_id or 'None'}")

        # Perform fresh album search
        new_album_match = search_spotify_album(
            matcher,
            rel['release_title'],
            artist,
            song['title']  # for track verification
        )

        if new_album_match:
            new_album_id = new_album_match['id']
            logger.info(f"  New album match: {new_album_match['name']} (ID: {new_album_id})")

            # Check album-level change
            if existing_album_id == new_album_id:
                logger.info(f"  Album: CONFIRMED (same album)")
            elif existing_album_id:
                logger.info(f"  Album: WOULD CHANGE from {existing_album_id}")
            else:
                logger.info(f"  Album: WOULD ADD")

            # Now check track matching
            spotify_tracks = matcher.client.get_album_tracks(new_album_id)
            if spotify_tracks:
                matched_track = match_track_to_recording(
                    matcher.logger,
                    matcher.stats,
                    matcher.min_track_similarity,
                    song['title'],
                    spotify_tracks,
                    alt_titles=song.get('alt_titles')
                )

                if matched_track:
                    new_track_id = matched_track['id']

                    # Find existing track ID for comparison
                    existing_track_id = None
                    for et in existing_tracks:
                        if et.get('spotify_track_id'):
                            existing_track_id = et['spotify_track_id']
                            break

                    if existing_track_id == new_track_id:
                        status = 'confirmed'
                        logger.info(f"  Track: CONFIRMED - \"{matched_track['name']}\"")
                    elif existing_track_id:
                        status = 'would_change'
                        logger.info(f"  Track: WOULD CHANGE to \"{matched_track['name']}\" (ID: {new_track_id})")
                        logger.info(f"         from existing ID: {existing_track_id}")
                    else:
                        status = 'would_add'
                        logger.info(f"  Track: WOULD ADD - \"{matched_track['name']}\" (ID: {new_track_id})")

                    stats[status] += 1
                    results.append({
                        'release_title': rel['release_title'],
                        'release_id': str(rel['release_id']),
                        'artist': artist,
                        'status': status,
                        'existing_album_id': existing_album_id,
                        'new_album_id': new_album_id,
                        'existing_track_id': existing_track_id,
                        'new_track_id': new_track_id,
                        'new_track_name': matched_track['name']
                    })
                else:
                    # Album matched but no track found
                    existing_track_id = None
                    for et in existing_tracks:
                        if et.get('spotify_track_id'):
                            existing_track_id = et['spotify_track_id']
                            break

                    if existing_track_id:
                        status = 'would_remove'
                        logger.info(f"  Track: WOULD REMOVE (no track match in new album)")
                    else:
                        status = 'no_match'
                        logger.info(f"  Track: NO MATCH (track not found in album)")

                    stats[status] += 1
                    results.append({
                        'release_title': rel['release_title'],
                        'release_id': str(rel['release_id']),
                        'artist': artist,
                        'status': status,
                        'existing_album_id': existing_album_id,
                        'new_album_id': new_album_id,
                        'existing_track_id': existing_track_id,
                        'new_track_id': None
                    })
            else:
                logger.info(f"  Track: ERROR - Could not fetch album tracks")
        else:
            # No album match found
            existing_track_id = None
            for et in existing_tracks:
                if et.get('spotify_track_id'):
                    existing_track_id = et['spotify_track_id']
                    break

            if existing_album_id or existing_track_id:
                status = 'would_remove'
                logger.info(f"  WOULD REMOVE - No album match found")
            else:
                status = 'no_match'
                logger.info(f"  NO MATCH - No album found")

            stats[status] += 1
            results.append({
                'release_title': rel['release_title'],
                'release_id': str(rel['release_id']),
                'artist': artist,
                'status': status,
                'existing_album_id': existing_album_id,
                'new_album_id': None,
                'existing_track_id': existing_track_id,
                'new_track_id': None
            })

        logger.info("")

    # Summary
    logger.info("=" * 60)
    logger.info("DRY-RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Confirmed (no change): {stats['confirmed']}")
    logger.info(f"Would change:          {stats['would_change']}")
    logger.info(f"Would add:             {stats['would_add']}")
    logger.info(f"Would remove:          {stats['would_remove']}")
    logger.info(f"No match found:        {stats['no_match']}")
    logger.info("=" * 60)

    return {
        'song': {
            'id': str(song['id']),
            'title': song['title'],
            'composer': song['composer']
        },
        'checked_at': datetime.now().isoformat(),
        'mode': 'dry-run',
        'results': results,
        'summary': stats
    }


def update_matches(song_name: str, matcher: SpotifyMatcher) -> dict:
    """
    Mode 3: Re-match and update.

    Uses the existing SpotifyMatcher.match_releases() to perform actual updates.
    """
    logger.info(f"Re-matching and updating: {song_name}")
    logger.info("")

    # Use the existing matcher (which is configured with dry_run=False)
    result = matcher.match_releases(song_name)

    if result['success']:
        matcher.print_summary()
    else:
        logger.error(f"Error: {result.get('error', 'Unknown error')}")

    return result


def get_tracks_with_albums(song_id: str) -> list:
    """
    Get all recording_releases that have both a spotify_track_id
    and an associated release with a spotify_album_id.

    Checks both the normalized streaming_links table and legacy spotify_track_id column.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    r.id as recording_id,
                    def_rel.title as album_title,
                    r.recording_year,
                    rr.release_id,
                    COALESCE(rrsl.service_id, rr.spotify_track_id) as spotify_track_id,
                    COALESCE(rrsl.service_url,
                        CASE WHEN rr.spotify_track_id IS NOT NULL
                             THEN 'https://open.spotify.com/track/' || rr.spotify_track_id END
                    ) as spotify_track_url,
                    rel.title as release_title,
                    rel.release_year,
                    rsl_sp.service_id as spotify_album_id,
                    rsl_sp.service_url as spotify_album_url,
                    rel.artist_credit,
                    (
                        SELECT p.name
                        FROM recording_performers rp
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rp.recording_id = r.id AND rp.role = 'leader'
                        LIMIT 1
                    ) as leader_name
                FROM recordings r
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                JOIN recording_releases rr ON r.id = rr.recording_id
                JOIN releases rel ON rr.release_id = rel.id
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                JOIN release_streaming_links rsl_sp
                    ON rsl_sp.release_id = rel.id AND rsl_sp.service = 'spotify'
                WHERE r.song_id = %s
                  AND (rrsl.service_id IS NOT NULL OR rr.spotify_track_id IS NOT NULL)
                  AND rsl_sp.service_id IS NOT NULL
                ORDER BY rel.release_year DESC NULLS LAST, rel.title
            """, (song_id,))
            return cur.fetchall()


def verify_track_album_consistency(song_name: str, matcher: SpotifyMatcher) -> dict:
    """
    Mode 4: Verify track/album consistency.

    For each recording_release with a spotify_track_id, verify that
    the track actually exists on the associated release's spotify_album_id.
    """
    # Find the song
    song = find_song_by_name(song_name)
    if not song:
        logger.error(f"Song not found: {song_name}")
        return None

    logger.info(f"Verifying track/album consistency: {song['title']} (composer: {song['composer']})")
    logger.info(f"Song ID: {song['id']}")
    logger.info("")

    # Get all tracks with albums
    tracks = get_tracks_with_albums(song['id'])

    if not tracks:
        logger.info("No recording/releases with both track and album IDs found.")
        return {'song': song, 'results': [], 'summary': {}}

    logger.info(f"Found {len(tracks)} recording/release combinations to verify")
    logger.info("")

    results = []
    stats = {'consistent': 0, 'inconsistent': 0, 'error': 0}

    # Cache album tracks to avoid redundant API calls
    album_tracks_cache = {}

    for rec in tracks:
        track_id = rec['spotify_track_id']
        album_id = rec['spotify_album_id']
        artist = rec['artist_credit'] or rec['leader_name'] or 'Unknown'

        logger.info(f"Checking: {rec['release_title']} ({artist}, {rec['release_year'] or '?'})")
        logger.info(f"  Track ID: {track_id}")
        logger.info(f"  Album ID: {album_id}")

        # Get album tracks (from cache or API)
        if album_id not in album_tracks_cache:
            album_tracks = matcher.client.get_album_tracks(album_id)
            album_tracks_cache[album_id] = album_tracks
        else:
            album_tracks = album_tracks_cache[album_id]

        if album_tracks is None:
            logger.warning(f"  ERROR: Could not fetch album tracks from Spotify")
            stats['error'] += 1
            results.append({
                'release_title': rec['release_title'],
                'release_id': str(rec['release_id']),
                'recording_id': str(rec['recording_id']),
                'artist': artist,
                'release_year': rec['release_year'],
                'spotify_track_id': track_id,
                'spotify_track_url': rec['spotify_track_url'],
                'spotify_album_id': album_id,
                'spotify_album_url': rec['spotify_album_url'],
                'status': 'error',
                'error': 'Could not fetch album tracks'
            })
            logger.info("")
            continue

        # Check if track is in album
        album_track_ids = [t['id'] for t in album_tracks]
        track_found = track_id in album_track_ids

        if track_found:
            # Find the track details
            track_info = next((t for t in album_tracks if t['id'] == track_id), None)
            track_name = track_info['name'] if track_info else 'Unknown'
            track_number = track_info.get('track_number', '?') if track_info else '?'

            logger.info(f"  ✓ CONSISTENT - Track found on album (#{track_number}: \"{track_name}\")")
            stats['consistent'] += 1
            results.append({
                'release_title': rec['release_title'],
                'release_id': str(rec['release_id']),
                'recording_id': str(rec['recording_id']),
                'artist': artist,
                'release_year': rec['release_year'],
                'spotify_track_id': track_id,
                'spotify_track_url': rec['spotify_track_url'],
                'spotify_album_id': album_id,
                'spotify_album_url': rec['spotify_album_url'],
                'status': 'consistent',
                'track_name': track_name,
                'track_number': track_number
            })
        else:
            logger.warning(f"  ✗ INCONSISTENT - Track NOT found on album!")
            logger.warning(f"    Track URL: {rec['spotify_track_url']}")
            logger.warning(f"    Album URL: {rec['spotify_album_url']}")
            logger.warning(f"    Album has {len(album_tracks)} tracks")
            stats['inconsistent'] += 1
            results.append({
                'release_title': rec['release_title'],
                'release_id': str(rec['release_id']),
                'recording_id': str(rec['recording_id']),
                'artist': artist,
                'release_year': rec['release_year'],
                'spotify_track_id': track_id,
                'spotify_track_url': rec['spotify_track_url'],
                'spotify_album_id': album_id,
                'spotify_album_url': rec['spotify_album_url'],
                'status': 'inconsistent',
                'album_track_count': len(album_tracks)
            })

        logger.info("")

    # Summary
    logger.info("=" * 60)
    logger.info("VERIFICATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Consistent:   {stats['consistent']}")
    logger.info(f"Inconsistent: {stats['inconsistent']}")
    logger.info(f"Errors:       {stats['error']}")
    logger.info("=" * 60)

    if stats['inconsistent'] > 0:
        logger.info("")
        logger.info("INCONSISTENT ENTRIES (track not on album):")
        logger.info("-" * 60)
        for r in results:
            if r['status'] == 'inconsistent':
                logger.info(f"  {r['release_title']} ({r['artist']})")
                logger.info(f"    Track: {r['spotify_track_url']}")
                logger.info(f"    Album: {r['spotify_album_url']}")
                logger.info("")

    return {
        'song': {
            'id': str(song['id']),
            'title': song['title'],
            'composer': song['composer']
        },
        'verified_at': datetime.now().isoformat(),
        'mode': 'verify',
        'results': results,
        'summary': stats
    }


def get_orphaned_albums(song_id: str) -> list:
    """
    Get releases that have a spotify_album_id but where the associated
    recording_releases have no spotify_track_id.

    Checks both the normalized streaming_links table and legacy spotify_track_id column.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    rel.id as release_id,
                    rel.title as release_title,
                    rel.release_year,
                    rel.musicbrainz_release_id,
                    rsl_sp.service_id as spotify_album_id,
                    rsl_sp.service_url as spotify_album_url,
                    rel.artist_credit,
                    (
                        SELECT p.name
                        FROM recording_releases rr2
                        JOIN recordings r2 ON rr2.recording_id = r2.id
                        JOIN recording_performers rp ON r2.id = rp.recording_id
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rr2.release_id = rel.id
                          AND r2.song_id = %s
                          AND rp.role = 'leader'
                        LIMIT 1
                    ) as leader_name
                FROM releases rel
                JOIN recording_releases rr ON rel.id = rr.release_id
                JOIN recordings r ON rr.recording_id = r.id
                LEFT JOIN recording_release_streaming_links rrsl
                    ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                JOIN release_streaming_links rsl_sp
                    ON rsl_sp.release_id = rel.id AND rsl_sp.service = 'spotify'
                WHERE r.song_id = %s
                  AND rsl_sp.service_id IS NOT NULL
                  AND rrsl.service_id IS NULL
                  AND (rr.spotify_track_id IS NULL OR rr.spotify_track_id = '')
                ORDER BY rel.release_year DESC NULLS LAST, rel.title
            """, (song_id, song_id))
            return cur.fetchall()


def clear_spotify_album_from_release(release_id: str) -> bool:
    """
    Clear the Spotify album link from a release.
    Returns True if a row was removed.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM release_streaming_links
                WHERE release_id = %s AND service = 'spotify'
            """, (release_id,))
            removed = cur.rowcount
            conn.commit()
            return removed > 0


def find_orphaned_albums(song_name: str, matcher: SpotifyMatcher, fix: bool = False) -> dict:
    """
    Mode 5: Find orphaned albums.

    Find releases where we have a spotify_album_id but no spotify_track_id.
    This indicates the album was matched but the track wasn't found.

    If fix=True, removes the spotify_album_id from releases where the track
    is not on the album.
    """
    # Find the song
    song = find_song_by_name(song_name)
    if not song:
        logger.error(f"Song not found: {song_name}")
        return None

    if fix:
        logger.info(f"Finding and FIXING orphaned albums: {song['title']} (composer: {song['composer']})")
    else:
        logger.info(f"Finding orphaned albums: {song['title']} (composer: {song['composer']})")
    logger.info(f"Song ID: {song['id']}")
    logger.info("")

    # Get orphaned albums
    orphaned = get_orphaned_albums(song['id'])

    if not orphaned:
        logger.info("No orphaned albums found (all albums with IDs have track matches).")
        return {'song': song, 'results': [], 'summary': {'orphaned': 0}}

    logger.info(f"Found {len(orphaned)} releases with album but no track match")
    logger.info("")

    results = []

    for rel in orphaned:
        artist = rel['artist_credit'] or rel['leader_name'] or 'Unknown'
        album_id = rel['spotify_album_id']
        mb_release_id = rel['musicbrainz_release_id']
        mb_url = f"https://musicbrainz.org/release/{mb_release_id}" if mb_release_id else None

        logger.info(f"Release: {rel['release_title']} ({artist}, {rel['release_year'] or '?'})")
        logger.info(f"  MusicBrainz: {mb_url or 'N/A'}")
        logger.info(f"  Spotify:     {rel['spotify_album_url']}")

        # Fetch album details to show what tracks ARE on it
        album_tracks = matcher.client.get_album_tracks(album_id)

        if album_tracks:
            logger.info(f"  Album has {len(album_tracks)} tracks:")
            # Show first few track names to help diagnose
            for i, track in enumerate(album_tracks[:5]):
                logger.info(f"    {i+1}. {track['name']}")
            if len(album_tracks) > 5:
                logger.info(f"    ... and {len(album_tracks) - 5} more")

            # Check if song title appears in any track (fuzzy)
            song_title_lower = song['title'].lower()
            possible_matches = []
            for track in album_tracks:
                track_name_lower = track['name'].lower()
                # Simple substring check
                if song_title_lower in track_name_lower or track_name_lower in song_title_lower:
                    possible_matches.append(track)
                # Check for common variations
                elif 'river' in song_title_lower and 'river' in track_name_lower:
                    possible_matches.append(track)

            if possible_matches:
                logger.info(f"  Possible track matches found:")
                for t in possible_matches:
                    logger.info(f"    → \"{t['name']}\" (ID: {t['id']})")
                fixed = False
            else:
                logger.warning(f"  No tracks on album appear to match \"{song['title']}\"")
                # If fix mode and no possible matches, clear the album ID
                if fix:
                    if clear_spotify_album_from_release(rel['release_id']):
                        logger.info(f"  ✓ FIXED - Removed spotify_album_id from release")
                        fixed = True
                    else:
                        logger.error(f"  ✗ FAILED to remove spotify_album_id")
                        fixed = False
                else:
                    fixed = False

            results.append({
                'release_title': rel['release_title'],
                'release_id': str(rel['release_id']),
                'artist': artist,
                'release_year': rel['release_year'],
                'musicbrainz_release_id': mb_release_id,
                'musicbrainz_url': mb_url,
                'spotify_album_id': album_id,
                'spotify_album_url': rel['spotify_album_url'],
                'album_track_count': len(album_tracks),
                'album_tracks': [t['name'] for t in album_tracks],
                'possible_matches': [{'name': t['name'], 'id': t['id']} for t in possible_matches],
                'fixed': fixed
            })
        else:
            logger.warning(f"  Could not fetch album tracks from Spotify")
            results.append({
                'release_title': rel['release_title'],
                'release_id': str(rel['release_id']),
                'artist': artist,
                'release_year': rel['release_year'],
                'musicbrainz_release_id': mb_release_id,
                'musicbrainz_url': mb_url,
                'spotify_album_id': album_id,
                'spotify_album_url': rel['spotify_album_url'],
                'error': 'Could not fetch album tracks'
            })

        logger.info("")

    # Summary
    logger.info("=" * 60)
    logger.info("ORPHANED ALBUMS SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Releases with album but no track: {len(orphaned)}")

    with_possible = sum(1 for r in results if r.get('possible_matches'))
    without_song = sum(1 for r in results if not r.get('possible_matches') and not r.get('error'))
    fixed_count = sum(1 for r in results if r.get('fixed'))

    logger.info(f"  - With possible track matches: {with_possible}")
    logger.info(f"  - Track not on album: {without_song}")
    if fix:
        logger.info(f"  - Fixed (album ID removed): {fixed_count}")
    logger.info("=" * 60)

    return {
        'song': {
            'id': str(song['id']),
            'title': song['title'],
            'composer': song['composer']
        },
        'checked_at': datetime.now().isoformat(),
        'mode': 'orphaned-albums',
        'fix_mode': fix,
        'results': results,
        'summary': {
            'orphaned': len(orphaned),
            'with_possible_matches': with_possible,
            'track_not_on_album': without_song,
            'fixed': fixed_count
        }
    }


def main():
    parser = argparse.ArgumentParser(
        description='Audit Spotify track matches for recordings of a song'
    )
    parser.add_argument('song_name', help='Name of the song to audit')

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--audit', action='store_true', default=True,
                           help='Audit existing matches (default)')
    mode_group.add_argument('--dry-run', action='store_true',
                           help='Dry-run re-match (show what would change)')
    mode_group.add_argument('--update', action='store_true',
                           help='Re-match and update database')
    mode_group.add_argument('--verify', action='store_true',
                           help='Verify track/album consistency')
    mode_group.add_argument('--orphaned-albums', action='store_true',
                           help='Find releases with album but no track match')

    parser.add_argument('--force-refresh', action='store_true',
                       help='Bypass Spotify cache, fetch fresh data')
    parser.add_argument('--rematch', action='store_true',
                       help='Re-evaluate releases that already have Spotify URLs (use with --update)')
    parser.add_argument('--fix', action='store_true',
                       help='Fix issues found (use with --orphaned-albums to remove bad album IDs)')
    parser.add_argument('--output', '-o', help='Output JSON file path')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine mode
    if args.update:
        mode = 'update'
        dry_run = False
    elif args.dry_run:
        mode = 'dry-run'
        dry_run = True
    elif args.verify:
        mode = 'verify'
        dry_run = True
    elif args.orphaned_albums:
        mode = 'orphaned-albums'
        dry_run = True
    else:
        mode = 'audit'
        dry_run = True

    # Initialize matcher
    matcher = SpotifyMatcher(
        dry_run=dry_run,
        force_refresh=args.force_refresh,
        rematch=args.rematch,
        logger=logger
    )

    # Run appropriate mode
    if mode == 'audit':
        results = audit_existing_matches(args.song_name, matcher)
    elif mode == 'dry-run':
        results = dry_run_rematch(args.song_name, matcher)
    elif mode == 'verify':
        results = verify_track_album_consistency(args.song_name, matcher)
    elif mode == 'orphaned-albums':
        results = find_orphaned_albums(args.song_name, matcher, fix=args.fix)
    else:  # update
        results = update_matches(args.song_name, matcher)

    # Save results to file if requested
    if args.output and results:
        output_path = Path(args.output)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"\nResults saved to: {output_path}")


if __name__ == '__main__':
    main()
