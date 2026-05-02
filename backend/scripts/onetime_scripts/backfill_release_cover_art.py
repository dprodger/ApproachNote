#!/usr/bin/env python3
"""
Backfill cover art for releases that have Spotify track URLs but no album art.

This script finds releases that:
1. Have recordings with spotify_track_url in recording_releases
2. But have no cover_art_small/medium/large set on the release

It fetches the album art from Spotify using the track ID and updates the release.

Usage:
    python scripts/backfill_release_cover_art.py [--dry-run]
    python scripts/backfill_release_cover_art.py --execute
    python scripts/backfill_release_cover_art.py --limit 100 --execute
"""

import os
import sys
import time
import argparse
import logging
import base64
import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['DB_USE_POOLING'] = 'true'

from dotenv import load_dotenv
from db_utils import get_db_connection

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReleaseArtworkBackfill:
    def __init__(self, dry_run=True, limit=None):
        self.dry_run = dry_run
        self.limit = limit
        self.access_token = None
        self.token_expires = 0
        self.stats = {
            'releases_found': 0,
            'releases_updated': 0,
            'releases_skipped': 0,
            'errors': 0
        }

    def get_spotify_token(self):
        """Get Spotify access token using client credentials flow"""
        client_id = os.environ.get('SPOTIFY_CLIENT_ID')
        client_secret = os.environ.get('SPOTIFY_CLIENT_SECRET')

        if not client_id or not client_secret:
            raise ValueError("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET environment variables")

        # Check if token is still valid
        if self.access_token and time.time() < self.token_expires:
            return self.access_token

        logger.info("Fetching Spotify access token...")

        auth_str = f"{client_id}:{client_secret}"
        auth_b64 = base64.b64encode(auth_str.encode()).decode()

        headers = {
            'Authorization': f'Basic {auth_b64}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.post(
            'https://accounts.spotify.com/api/token',
            headers=headers,
            data={'grant_type': 'client_credentials'}
        )
        response.raise_for_status()

        token_data = response.json()
        self.access_token = token_data['access_token']
        expires_in = token_data['expires_in']
        self.token_expires = time.time() + expires_in - 60

        logger.info(f"Spotify token obtained (expires in {expires_in}s)")
        return self.access_token

    def extract_track_id(self, spotify_url):
        """Extract Spotify track ID from URL"""
        if not spotify_url:
            return None

        if '/track/' in spotify_url:
            return spotify_url.split('/track/')[-1].split('?')[0]
        elif ':track:' in spotify_url:
            return spotify_url.split(':track:')[-1]
        return None

    def get_album_art_from_track(self, track_id):
        """Get album artwork URLs from a Spotify track"""
        token = self.get_spotify_token()

        headers = {'Authorization': f'Bearer {token}'}
        url = f'https://api.spotify.com/v1/tracks/{track_id}'

        try:
            time.sleep(0.05)  # Rate limiting
            response = requests.get(url, headers=headers)

            if response.status_code == 401:
                self.access_token = None
                return self.get_album_art_from_track(track_id)  # Retry with new token

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))
                logger.warning(f"Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                return self.get_album_art_from_track(track_id)

            response.raise_for_status()
            data = response.json()

            images = data.get('album', {}).get('images', [])
            album_id = data.get('album', {}).get('id')
            album_url = f"https://open.spotify.com/album/{album_id}" if album_id else None

            # Spotify returns images in descending size order (640, 300, 64)
            album_art = {
                'large': images[0].get('url') if len(images) >= 1 else None,
                'medium': images[1].get('url') if len(images) >= 2 else None,
                'small': images[2].get('url') if len(images) >= 3 else None,
                'album_id': album_id,
                'album_url': album_url
            }

            return album_art

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error getting track {track_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting track {track_id}: {e}")
            return None

    def find_releases_without_art(self, cur):
        """Find releases that have Spotify track IDs but no cover art"""
        limit_clause = f"LIMIT {self.limit}" if self.limit else ""

        # Note: this is a historical one-time backfill that depends on the
        # cover_art_small/medium/large columns (since dropped). It is not
        # runnable against the current schema.
        cur.execute(f"""
            SELECT DISTINCT
                rel.id,
                rel.title,
                rel.release_year,
                rel.cover_art_small,
                rsl.service_id as spotify_album_id,
                (SELECT rr.spotify_track_id
                 FROM recording_releases rr
                 WHERE rr.release_id = rel.id
                   AND rr.spotify_track_id IS NOT NULL
                 LIMIT 1) as spotify_track_id,
                (SELECT CASE WHEN rr.spotify_track_id IS NOT NULL
                             THEN 'https://open.spotify.com/track/' || rr.spotify_track_id END
                 FROM recording_releases rr
                 WHERE rr.release_id = rel.id
                   AND rr.spotify_track_id IS NOT NULL
                 LIMIT 1) as spotify_track_url
            FROM releases rel
            LEFT JOIN release_streaming_links rsl
                ON rsl.release_id = rel.id AND rsl.service = 'spotify'
            WHERE rel.cover_art_small IS NULL
              AND EXISTS (
                  SELECT 1 FROM recording_releases rr
                  WHERE rr.release_id = rel.id
                    AND rr.spotify_track_id IS NOT NULL
              )
            ORDER BY rel.release_year DESC NULLS LAST
            {limit_clause}
        """)
        return cur.fetchall()

    def update_release_art(self, cur, release_id, album_art):
        """Update release with album artwork (URL is constructed from ID, not stored)"""
        cur.execute("""
            UPDATE releases
            SET cover_art_small = %s,
                cover_art_medium = %s,
                cover_art_large = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            album_art.get('small'),
            album_art.get('medium'),
            album_art.get('large'),
            release_id
        ))
        if album_art.get('album_id'):
            service_url = f"https://open.spotify.com/album/{album_art['album_id']}"
            cur.execute("""
                INSERT INTO release_streaming_links (
                    release_id, service, service_id, service_url,
                    match_method, matched_at
                )
                VALUES (%s, 'spotify', %s, %s, 'fuzzy_search', CURRENT_TIMESTAMP)
                ON CONFLICT (release_id, service) DO NOTHING
            """, (release_id, album_art['album_id'], service_url))

    def run(self):
        """Main method to backfill cover art"""
        logger.info("=" * 70)
        logger.info("Release Cover Art Backfill from Spotify")
        logger.info("=" * 70)

        if self.dry_run:
            logger.info("*** DRY RUN MODE - No database changes will be made ***")
        logger.info("")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Find releases needing art
                releases = self.find_releases_without_art(cur)
                self.stats['releases_found'] = len(releases)

                logger.info(f"Found {len(releases)} releases with Spotify tracks but no cover art")
                logger.info("")

                if not releases:
                    return

                for i, release in enumerate(releases, 1):
                    release_id = release['id']
                    title = release['title']
                    year = release['release_year']
                    track_url = release['spotify_track_url']

                    logger.info(f"[{i}/{len(releases)}] {title} ({year or '?'})")

                    # Extract track ID
                    track_id = self.extract_track_id(track_url)
                    if not track_id:
                        logger.warning(f"  Could not extract track ID from: {track_url}")
                        self.stats['errors'] += 1
                        continue

                    # Get album art from Spotify
                    album_art = self.get_album_art_from_track(track_id)
                    if not album_art or not album_art.get('small'):
                        logger.warning(f"  No album art available from Spotify")
                        self.stats['releases_skipped'] += 1
                        continue

                    logger.info(f"  Found art: {album_art.get('small')[:60]}...")

                    if not self.dry_run:
                        self.update_release_art(cur, release_id, album_art)
                        self.stats['releases_updated'] += 1
                    else:
                        logger.info(f"  [DRY RUN] Would update release")

                if not self.dry_run:
                    conn.commit()

        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Releases found:   {self.stats['releases_found']}")
        logger.info(f"Releases updated: {self.stats['releases_updated']}")
        logger.info(f"Releases skipped: {self.stats['releases_skipped']}")
        logger.info(f"Errors:           {self.stats['errors']}")


def main():
    parser = argparse.ArgumentParser(
        description='Backfill cover art for releases with Spotify track URLs'
    )
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Show what would be done (default: True)')
    parser.add_argument('--execute', action='store_true',
                        help='Actually execute the changes')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of releases to process')

    args = parser.parse_args()
    dry_run = not args.execute

    backfill = ReleaseArtworkBackfill(dry_run=dry_run, limit=args.limit)
    backfill.run()


if __name__ == '__main__':
    main()
