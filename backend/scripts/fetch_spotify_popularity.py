#!/usr/bin/env python3
"""
Fetch Spotify popularity data for recordings of a song.

This script takes a song name, finds all recordings that have Spotify track IDs,
fetches popularity data from the Spotify API, and saves results to a JSON file.

Usage:
    python scripts/fetch_spotify_popularity.py --name "Take Five"
    python scripts/fetch_spotify_popularity.py --name "All The Things You Are" --output results.json
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
from integrations.spotify.db import find_song_by_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_recordings_with_spotify_tracks(song_id: str) -> list:
    """
    Get all recordings for a song that have Spotify track IDs.

    Checks both the normalized streaming_links table and legacy spotify_track_id column.

    Returns list of dicts with recording info and spotify track details.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # First get distinct recording/track combinations
            # COALESCE prefers normalized streaming_links over legacy column
            cur.execute("""
                WITH recording_tracks AS (
                    SELECT DISTINCT ON (COALESCE(rrsl.service_id, rr.spotify_track_id))
                        r.id as recording_id,
                        def_rel.title as album_title,
                        r.recording_year,
                        r.musicbrainz_id,
                        COALESCE(rrsl.service_id, rr.spotify_track_id) as spotify_track_id,
                        COALESCE(rrsl.service_url,
                            CASE WHEN rr.spotify_track_id IS NOT NULL
                                 THEN 'https://open.spotify.com/track/' || rr.spotify_track_id END
                        ) as spotify_track_url,
                        rel.title as release_title,
                        rsl_sp.service_id as spotify_album_id,
                        rsl_sp.service_url as spotify_album_url
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
                    ORDER BY COALESCE(rrsl.service_id, rr.spotify_track_id), r.recording_year DESC NULLS LAST
                )
                SELECT
                    rt.*,
                    -- Get leader/primary performer
                    (
                        SELECT p.name
                        FROM recording_performers rp
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rp.recording_id = rt.recording_id AND rp.role = 'leader'
                        LIMIT 1
                    ) as leader_name,
                    -- Get all performers as JSON
                    (
                        SELECT json_agg(json_build_object(
                            'name', p.name,
                            'role', rp.role
                        ) ORDER BY
                            CASE rp.role WHEN 'leader' THEN 1 WHEN 'sideman' THEN 2 ELSE 3 END,
                            p.name
                        )
                        FROM recording_performers rp
                        JOIN performers p ON rp.performer_id = p.id
                        WHERE rp.recording_id = rt.recording_id
                    ) as performers
                FROM recording_tracks rt
                ORDER BY rt.recording_year DESC NULLS LAST, rt.album_title
            """, (song_id,))

            return cur.fetchall()


def fetch_popularity_data(song_name: str, output_file: str = None) -> dict:
    """
    Fetch Spotify popularity data for all recordings of a song.

    Args:
        song_name: Name of the song to look up
        output_file: Optional output file path (defaults to song_name_popularity.json)

    Returns:
        Dict with song info and popularity data for each recording
    """
    # Initialize Spotify matcher (for API access)
    matcher = SpotifyMatcher(dry_run=True)

    # Find the song
    song = find_song_by_name(song_name)
    if not song:
        logger.error(f"Song not found: {song_name}")
        return None

    logger.info(f"Found song: {song['title']} (composer: {song['composer']})")
    logger.info(f"Song ID: {song['id']}")

    # Get recordings with Spotify tracks
    recordings = get_recordings_with_spotify_tracks(song['id'])

    if not recordings:
        logger.warning(f"No recordings with Spotify tracks found for '{song_name}'")
        return None

    logger.info(f"Found {len(recordings)} recordings with Spotify track IDs")

    # Fetch popularity for each track
    results = {
        'song': {
            'id': str(song['id']),
            'title': song['title'],
            'composer': song['composer'],
        },
        'fetched_at': datetime.now().isoformat(),
        'recordings': []
    }

    seen_track_ids = set()  # Avoid duplicate API calls for same track

    for rec in recordings:
        track_id = rec['spotify_track_id']

        # Skip if we've already fetched this track
        if track_id in seen_track_ids:
            logger.debug(f"Skipping duplicate track ID: {track_id}")
            continue
        seen_track_ids.add(track_id)

        logger.info(f"Fetching: {rec['album_title']} ({rec['recording_year'] or 'Unknown year'}) - {rec['leader_name'] or 'Unknown artist'}")

        # Fetch track details from Spotify
        track_details = matcher.client.get_track_details(track_id)

        if track_details:
            popularity = track_details.get('popularity', 0)
            duration_ms = track_details.get('duration_ms', 0)

            recording_data = {
                'recording_id': str(rec['recording_id']),
                'album_title': rec['album_title'],
                'release_title': rec['release_title'],
                'recording_year': rec['recording_year'],
                'leader': rec['leader_name'],
                'performers': rec['performers'] or [],
                'spotify': {
                    'track_id': track_id,
                    'track_url': rec['spotify_track_url'],
                    'album_id': rec['spotify_album_id'],
                    'album_url': rec['spotify_album_url'],
                    'popularity': popularity,
                    'duration_ms': duration_ms,
                    'track_name': track_details.get('name'),
                    'artists': [a['name'] for a in track_details.get('artists', [])],
                    'album_name': track_details.get('album', {}).get('name'),
                }
            }

            results['recordings'].append(recording_data)
            logger.info(f"  -> Popularity: {popularity}")
        else:
            logger.warning(f"  -> Could not fetch track details for {track_id}")

    # Sort by popularity (descending)
    results['recordings'].sort(key=lambda x: x['spotify']['popularity'], reverse=True)

    # Add summary stats
    if results['recordings']:
        popularities = [r['spotify']['popularity'] for r in results['recordings']]
        results['summary'] = {
            'total_recordings': len(results['recordings']),
            'popularity_max': max(popularities),
            'popularity_min': min(popularities),
            'popularity_avg': sum(popularities) / len(popularities),
            'popularity_median': sorted(popularities)[len(popularities) // 2],
        }

    # Determine output file
    if not output_file:
        safe_name = song_name.lower().replace(' ', '_').replace("'", "")
        output_file = f"{safe_name}_popularity.json"

    # Save to file
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logger.info(f"\nResults saved to: {output_path}")
    logger.info(f"Total recordings with popularity data: {len(results['recordings'])}")

    if results.get('summary'):
        logger.info(f"Popularity range: {results['summary']['popularity_min']} - {results['summary']['popularity_max']}")
        logger.info(f"Average popularity: {results['summary']['popularity_avg']:.1f}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Fetch Spotify popularity data for recordings of a song'
    )
    parser.add_argument('--name', '-n', required=True, help='Name of the song to look up')
    parser.add_argument('--output', '-o', help='Output JSON file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    results = fetch_popularity_data(args.name, args.output)

    if results:
        print(f"\n{'='*80}")
        print(f"TOP 10 RECORDINGS BY POPULARITY")
        print(f"{'='*80}")
        for i, rec in enumerate(results['recordings'][:10], 1):
            pop = rec['spotify']['popularity']
            artist = rec['leader'] or 'Unknown'
            album = rec['album_title'] or 'Unknown'
            year = rec['recording_year'] or '?'
            track_id = rec['spotify']['track_id']
            print(f"{i:2}. [{pop:3}] {artist} - {album} ({year})")
            print(f"         https://open.spotify.com/track/{track_id}")
        print(f"{'='*80}")


if __name__ == '__main__':
    main()
