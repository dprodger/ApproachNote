#!/usr/bin/env python3
"""
Match releases to Spotify albums by comparing track lists.

This script handles cases where album titles differ between MusicBrainz and Spotify
but the track lists are substantially the same (compilations, reissues, live albums, etc.)

Algorithm:
1. Get unmatched releases for an artist from our database
2. Fetch all Spotify albums for that artist
3. For each unmatched release, compare track lists against all Spotify albums
4. Score matches based on: track count, title similarity, track position
5. If score exceeds threshold, update the database

Usage:
    # Dry run - show what would be matched without making changes
    python match_releases_by_tracklist.py --artist "Oscar Peterson"

    # Actually apply the matches
    python match_releases_by_tracklist.py --artist "Oscar Peterson" --apply

    # Show debug info for matching decisions
    python match_releases_by_tracklist.py --artist "Oscar Peterson" --debug

    # Adjust match threshold (default: 70)
    python match_releases_by_tracklist.py --artist "Oscar Peterson" --threshold 80
"""

import os
import sys
import json
import logging
import argparse
import hashlib
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ['DB_USE_POOLING'] = 'true'

from script_base import ScriptBase, run_script
from db_utils import get_db_connection
from integrations.spotify.client import SpotifyClient, _CACHE_MISS
from rapidfuzz import fuzz
from integrations.spotify.matching import calculate_similarity, normalize_for_comparison
from integrations.spotify.db import (
    update_release_spotify_data,
    update_recording_default_release,
    update_recording_release_track_id
)
from integrations.musicbrainz.utils import MusicBrainzSearcher
from dotenv import load_dotenv

load_dotenv()


@dataclass
class TrackInfo:
    """Represents a track for comparison"""
    title: str
    position: int  # Overall position (flattened across discs)
    disc_number: int = 1
    track_number: int = 1
    spotify_track_id: Optional[str] = None
    spotify_track_url: Optional[str] = None
    normalized_title: str = ""  # Pre-normalized for faster comparison


@dataclass
class TrackMatch:
    """A matched track pair between MusicBrainz and Spotify"""
    mb_title: str
    mb_position: int
    mb_disc_number: int
    mb_track_number: int
    spotify_title: str
    spotify_position: int
    spotify_track_id: str
    spotify_track_url: str
    similarity: float


@dataclass
class MatchResult:
    """Result of comparing a release to a Spotify album"""
    spotify_album_id: str
    spotify_album_name: str
    spotify_url: str
    album_art: Dict[str, str]
    score: float
    track_matches: int
    total_mb_tracks: int
    total_spotify_tracks: int
    matched_tracks: List[TrackMatch]  # Full track match info
    unmatched_mb_tracks: List[str]
    reason: str


class TrackListMatcher:
    """Matches releases to Spotify albums by comparing track lists"""

    def __init__(self, logger: logging.Logger, threshold: float = 70.0,
                 cache_days: int = 30, force_refresh: bool = False):
        self.logger = logger
        self.threshold = threshold
        self.client = SpotifyClient(
            cache_days=cache_days,
            force_refresh=force_refresh,
            logger=logger
        )
        self.mb_client = MusicBrainzSearcher(force_refresh=force_refresh)
        self.stats = {
            'releases_processed': 0,
            'releases_matched': 0,
            'releases_no_match': 0,
            'releases_updated': 0,
            'releases_no_mb_id': 0,
            'tracks_updated': 0,
            'api_calls': 0,
            'cache_hits': 0,
        }

    def get_unmatched_releases_for_artist(self, artist_name: str) -> List[dict]:
        """Get releases without Spotify URL for a given artist"""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Find releases linked to recordings by this artist that don't have Spotify
                cur.execute("""
                    SELECT DISTINCT
                        rel.id,
                        rel.title,
                        rel.artist_credit,
                        rel.release_year,
                        rel.musicbrainz_release_id
                    FROM releases rel
                    JOIN recording_releases rr ON rel.id = rr.release_id
                    JOIN recordings rec ON rr.recording_id = rec.id
                    JOIN recording_performers rp ON rec.id = rp.recording_id
                    JOIN performers p ON rp.performer_id = p.id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM release_streaming_links rsl
                        WHERE rsl.release_id = rel.id AND rsl.service = 'spotify'
                    )
                      AND LOWER(p.name) = LOWER(%s)
                    ORDER BY rel.title
                """, (artist_name,))
                return cur.fetchall()

    def get_tracks_for_release_from_mb(self, mb_release_id: str) -> List[TrackInfo]:
        """Get all tracks for a release from MusicBrainz"""
        if not mb_release_id:
            return []

        release_data = self.mb_client.get_release_details(mb_release_id)
        if not release_data:
            return []

        tracks = []
        position = 0
        for medium in release_data.get('media', []):
            disc_number = medium.get('position', 1)
            for track in medium.get('tracks', []):
                position += 1
                tracks.append(TrackInfo(
                    title=track.get('title', ''),
                    disc_number=disc_number,
                    track_number=track.get('position', position),
                    position=position
                ))

        return tracks

    def get_spotify_artist_id(self, artist_name: str) -> Optional[str]:
        """Search for artist on Spotify and return their ID"""
        cache_path = self._get_artist_cache_path(artist_name)
        cached = self.client._load_from_cache(cache_path)

        if cached is not _CACHE_MISS:
            self.stats['cache_hits'] += 1
            return cached

        token = self.client.get_spotify_auth_token()
        if not token:
            return None

        try:
            import requests
            response = self.client._make_api_request(
                'get',
                'https://api.spotify.com/v1/search',
                headers={'Authorization': f'Bearer {token}'},
                params={
                    'q': f'artist:"{artist_name}"',
                    'type': 'artist',
                    'limit': 5
                },
                timeout=10
            )
            response.raise_for_status()
            self.stats['api_calls'] += 1

            data = response.json()
            artists = data.get('artists', {}).get('items', [])

            # Find best match
            best_match = None
            best_score = 0
            for artist in artists:
                score = calculate_similarity(artist_name, artist['name'])
                if score > best_score:
                    best_score = score
                    best_match = artist

            if best_match and best_score >= 80:
                artist_id = best_match['id']
                self.client._save_to_cache(cache_path, artist_id)
                self.logger.debug(f"Found Spotify artist: {best_match['name']} (ID: {artist_id}, score: {best_score}%)")
                return artist_id

            self.client._save_to_cache(cache_path, None)
            return None

        except Exception as e:
            self.logger.error(f"Error searching for artist: {e}")
            return None

    def get_spotify_artist_albums(self, artist_id: str) -> List[dict]:
        """Fetch all albums for an artist from Spotify"""
        cache_path = self._get_artist_albums_cache_path(artist_id)
        cached = self.client._load_from_cache(cache_path)

        if cached is not _CACHE_MISS:
            self.stats['cache_hits'] += 1
            return cached

        token = self.client.get_spotify_auth_token()
        if not token:
            return []

        try:
            import requests
            all_albums = []
            offset = 0
            limit = 50

            while True:
                response = self.client._make_api_request(
                    'get',
                    f'https://api.spotify.com/v1/artists/{artist_id}/albums',
                    headers={'Authorization': f'Bearer {token}'},
                    params={
                        'include_groups': 'album,compilation',
                        'limit': limit,
                        'offset': offset
                    },
                    timeout=10
                )
                response.raise_for_status()
                self.stats['api_calls'] += 1

                data = response.json()
                albums = data.get('items', [])
                all_albums.extend(albums)

                if len(albums) < limit:
                    break
                offset += limit

            self.logger.debug(f"Found {len(all_albums)} albums for artist")
            self.client._save_to_cache(cache_path, all_albums)
            return all_albums

        except Exception as e:
            self.logger.error(f"Error fetching artist albums: {e}")
            return []

    def get_spotify_album_tracks(self, album_id: str) -> List[TrackInfo]:
        """Fetch tracks for a Spotify album"""
        cache_path = self.client._get_album_cache_path(album_id)
        cached = self.client._load_from_cache(cache_path)

        if cached is not _CACHE_MISS:
            self.stats['cache_hits'] += 1
            # Convert cached data to TrackInfo (including Spotify IDs and normalized title)
            return [TrackInfo(
                title=t['name'],
                disc_number=t.get('disc_number', 1),
                track_number=t.get('track_number', i + 1),
                position=i + 1,
                spotify_track_id=t.get('id'),
                spotify_track_url=t.get('url'),
                normalized_title=normalize_for_comparison(t['name'])
            ) for i, t in enumerate(cached)]

        token = self.client.get_spotify_auth_token()
        if not token:
            return []

        try:
            import requests
            response = self.client._make_api_request(
                'get',
                f'https://api.spotify.com/v1/albums/{album_id}/tracks',
                headers={'Authorization': f'Bearer {token}'},
                params={'limit': 50},
                timeout=10
            )
            response.raise_for_status()
            self.stats['api_calls'] += 1

            data = response.json()
            items = data.get('items', [])

            # Cache in the format expected by spotify_matcher
            cache_data = [{
                'id': item['id'],
                'name': item['name'],
                'track_number': item['track_number'],
                'disc_number': item['disc_number'],
                'url': item['external_urls']['spotify']
            } for item in items]
            self.client._save_to_cache(cache_path, cache_data)

            # Return as TrackInfo (including Spotify IDs and normalized title)
            return [TrackInfo(
                title=item['name'],
                disc_number=item['disc_number'],
                track_number=item['track_number'],
                position=i + 1,
                spotify_track_id=item['id'],
                spotify_track_url=item['external_urls']['spotify'],
                normalized_title=normalize_for_comparison(item['name'])
            ) for i, item in enumerate(items)]

        except Exception as e:
            self.logger.error(f"Error fetching album tracks: {e}")
            return []

    def compare_track_lists(self, mb_tracks: List[TrackInfo],
                           spotify_tracks: List[TrackInfo],
                           spotify_album: dict) -> MatchResult:
        """
        Compare MusicBrainz track list against Spotify album track list.

        Scoring:
        - Each matched track contributes to the score
        - Track count similarity affects the score
        - Order similarity provides a small bonus
        """
        if not mb_tracks or not spotify_tracks:
            return MatchResult(
                spotify_album_id=spotify_album['id'],
                spotify_album_name=spotify_album['name'],
                spotify_url=spotify_album['external_urls']['spotify'],
                album_art=self._extract_album_art(spotify_album),
                score=0,
                track_matches=0,
                total_mb_tracks=len(mb_tracks),
                total_spotify_tracks=len(spotify_tracks),
                matched_tracks=[],
                unmatched_mb_tracks=[t.title for t in mb_tracks],
                reason="Empty track list"
            )

        matched_tracks: List[TrackMatch] = []
        unmatched_mb_tracks = []
        used_spotify_indices = set()

        # Pre-normalize MB track titles once
        mb_normalized = {id(t): normalize_for_comparison(t.title) for t in mb_tracks}

        # For each MusicBrainz track, find best matching Spotify track
        for mb_track in mb_tracks:
            best_match: Optional[TrackInfo] = None
            best_score = 0
            best_idx = -1
            mb_norm = mb_normalized[id(mb_track)]

            for idx, sp_track in enumerate(spotify_tracks):
                if idx in used_spotify_indices:
                    continue

                # Use pre-normalized Spotify title if available, otherwise normalize
                sp_norm = sp_track.normalized_title if sp_track.normalized_title else normalize_for_comparison(sp_track.title)

                # Direct fuzzy comparison on normalized strings (faster than calculate_similarity)
                score = fuzz.token_sort_ratio(mb_norm, sp_norm)

                # Bonus for matching position (only if tracks in similar position)
                position_diff = abs(mb_track.position - sp_track.position)
                if position_diff <= 2 and score >= 70:
                    score = min(100, score + 5)

                if score > best_score:
                    best_score = score
                    best_match = sp_track
                    best_idx = idx

            # Accept match if similarity is high enough
            if best_match and best_score >= 75:
                matched_tracks.append(TrackMatch(
                    mb_title=mb_track.title,
                    mb_position=mb_track.position,
                    mb_disc_number=mb_track.disc_number,
                    mb_track_number=mb_track.track_number,
                    spotify_title=best_match.title,
                    spotify_position=best_match.position,
                    spotify_track_id=best_match.spotify_track_id,
                    spotify_track_url=best_match.spotify_track_url,
                    similarity=best_score
                ))
                used_spotify_indices.add(best_idx)
            else:
                unmatched_mb_tracks.append(mb_track.title)

        # Calculate overall score
        track_match_ratio = len(matched_tracks) / len(mb_tracks) if mb_tracks else 0

        # Penalize if track counts are very different
        count_ratio = min(len(mb_tracks), len(spotify_tracks)) / max(len(mb_tracks), len(spotify_tracks))

        # Final score: primarily based on track matches, with count ratio as a factor
        score = track_match_ratio * 100 * (0.8 + 0.2 * count_ratio)

        # If Spotify album has significantly more tracks, check if MB tracks form a
        # contiguous sequence (like disc 1 of a 2-disc set or original album in a reissue).
        # This prevents matching a small album to a large compilation where tracks are scattered.
        is_contiguous = True
        if len(spotify_tracks) > len(mb_tracks) * 1.5 and len(matched_tracks) >= 2:
            is_contiguous = self._check_contiguous_sequence(matched_tracks)
            if not is_contiguous:
                # Tracks are scattered throughout a larger compilation - reject this match
                score = 0

        # Determine reason
        if not is_contiguous:
            reason = f"Tracks scattered in larger album (not contiguous sequence)"
        elif score >= self.threshold:
            reason = f"Good match: {len(matched_tracks)}/{len(mb_tracks)} tracks matched"
        elif len(matched_tracks) == 0:
            reason = "No matching tracks found"
        else:
            reason = f"Below threshold: {len(matched_tracks)}/{len(mb_tracks)} tracks matched ({score:.1f}%)"

        return MatchResult(
            spotify_album_id=spotify_album['id'],
            spotify_album_name=spotify_album['name'],
            spotify_url=spotify_album['external_urls']['spotify'],
            album_art=self._extract_album_art(spotify_album),
            score=score,
            track_matches=len(matched_tracks),
            total_mb_tracks=len(mb_tracks),
            total_spotify_tracks=len(spotify_tracks),
            matched_tracks=matched_tracks,
            unmatched_mb_tracks=unmatched_mb_tracks,
            reason=reason
        )

    def _extract_album_art(self, spotify_album: dict) -> Dict[str, str]:
        """Extract album art URLs from Spotify album"""
        album_art = {}
        for image in spotify_album.get('images', []):
            height = image.get('height', 0)
            if height >= 600:
                album_art['large'] = image['url']
            elif height >= 300:
                album_art['medium'] = image['url']
            elif height >= 64:
                album_art['small'] = image['url']
        return album_art

    def _check_contiguous_sequence(self, matched_tracks: List[TrackMatch], max_gap: int = 3) -> bool:
        """
        Check if matched tracks form a contiguous sequence in the Spotify album.

        This detects whether the MB release is "wholly contained" within the Spotify album
        (like disc 1 of a 2-disc set, or the original album in an expanded reissue).

        Args:
            matched_tracks: List of matched tracks with MB and Spotify positions
            max_gap: Maximum allowed gap between consecutive Spotify positions (default 3,
                     to allow for a few bonus tracks inserted)

        Returns:
            True if tracks form a contiguous sequence, False if scattered
        """
        if len(matched_tracks) < 2:
            return True

        # Sort matched tracks by MB position to get the intended order
        sorted_by_mb = sorted(matched_tracks, key=lambda t: t.mb_position)

        # Get the Spotify positions in MB order
        spotify_positions = [t.spotify_position for t in sorted_by_mb]

        # Check if Spotify positions are roughly sequential (allowing small gaps)
        # The positions should be monotonically increasing with small gaps
        for i in range(1, len(spotify_positions)):
            prev_pos = spotify_positions[i - 1]
            curr_pos = spotify_positions[i]

            # Current position should be greater than previous (maintaining order)
            # and the gap should be small (allowing for a few bonus tracks)
            if curr_pos <= prev_pos:
                # Out of order - tracks are scattered
                return False
            if curr_pos - prev_pos > max_gap:
                # Too big a gap - tracks are scattered across compilation
                return False

        return True

    def _get_artist_cache_path(self, artist_name: str) -> Path:
        """Get cache path for artist ID lookup"""
        safe_name = hashlib.md5(artist_name.lower().encode()).hexdigest()
        return self.client.cache_dir / 'artists' / f'artist_{safe_name}.json'

    def _get_artist_albums_cache_path(self, artist_id: str) -> Path:
        """Get cache path for artist albums"""
        cache_dir = self.client.cache_dir / 'artist_albums'
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f'albums_{artist_id}.json'

    def preload_spotify_album_tracks(self, spotify_albums: List[dict]) -> Dict[str, List[TrackInfo]]:
        """
        Pre-load all Spotify album tracks into memory to avoid repeated cache reads.

        Returns:
            Dict mapping album ID to list of TrackInfo
        """
        self.logger.debug(f"Pre-loading tracks for {len(spotify_albums)} Spotify albums...")
        album_tracks = {}
        for album in spotify_albums:
            tracks = self.get_spotify_album_tracks(album['id'])
            if tracks:
                album_tracks[album['id']] = tracks
        self.logger.debug(f"Loaded tracks for {len(album_tracks)} albums")
        return album_tracks

    def find_best_match(self, release: dict, spotify_albums: List[dict],
                        preloaded_tracks: Dict[str, List[TrackInfo]] = None) -> Optional[MatchResult]:
        """Find the best matching Spotify album for a release"""
        mb_release_id = release.get('musicbrainz_release_id')

        if not mb_release_id:
            self.logger.debug(f"  No MusicBrainz release ID")
            return None

        mb_tracks = self.get_tracks_for_release_from_mb(mb_release_id)

        if not mb_tracks:
            self.logger.debug(f"  No tracks found in MusicBrainz")
            return None

        self.logger.debug(f"  MusicBrainz release has {len(mb_tracks)} tracks")

        best_result = None
        best_score = 0

        for album in spotify_albums:
            # Use preloaded tracks if available, otherwise fetch
            if preloaded_tracks and album['id'] in preloaded_tracks:
                spotify_tracks = preloaded_tracks[album['id']]
            else:
                spotify_tracks = self.get_spotify_album_tracks(album['id'])

            if not spotify_tracks:
                continue

            result = self.compare_track_lists(mb_tracks, spotify_tracks, album)

            if result.score > best_score:
                best_score = result.score
                best_result = result

            # Early termination if we find a very high score match
            if best_score >= 95:
                break

        return best_result

    def match_artist_releases(self, artist_name: str, apply: bool = False,
                               limit: int = None, min_tracks: int = 2) -> Dict:
        """
        Match all unmatched releases for an artist.

        Args:
            artist_name: Artist name to match
            apply: If True, update the database
            limit: Maximum number of releases to process (for testing)
            min_tracks: Minimum number of matched tracks required (default: 2)

        Returns:
            Dict with results and statistics
        """
        results = {
            'artist': artist_name,
            'matches': [],
            'no_matches': [],
            'errors': []
        }

        # Get Spotify artist ID
        self.logger.info(f"Searching for artist: {artist_name}")
        artist_id = self.get_spotify_artist_id(artist_name)

        if not artist_id:
            self.logger.error(f"Could not find artist on Spotify: {artist_name}")
            return results

        # Get all Spotify albums for this artist
        self.logger.info(f"Fetching Spotify albums for artist...")
        spotify_albums = self.get_spotify_artist_albums(artist_id)
        self.logger.info(f"Found {len(spotify_albums)} Spotify albums")

        if not spotify_albums:
            self.logger.warning("No Spotify albums found for artist")
            return results

        # Pre-load all Spotify album tracks into memory (one-time cost)
        self.logger.info(f"Pre-loading Spotify album tracks...")
        preloaded_tracks = self.preload_spotify_album_tracks(spotify_albums)
        self.logger.info(f"Loaded tracks for {len(preloaded_tracks)} albums")

        # Get unmatched releases
        unmatched_releases = self.get_unmatched_releases_for_artist(artist_name)
        self.logger.info(f"Found {len(unmatched_releases)} unmatched releases in database")

        if limit:
            unmatched_releases = unmatched_releases[:limit]
            self.logger.info(f"Processing first {limit} releases (--limit)")

        self.logger.info("")

        if not unmatched_releases:
            return results

        # Process each release
        total_releases = len(unmatched_releases)
        for idx, release in enumerate(unmatched_releases, 1):
            self.stats['releases_processed'] += 1
            title = release['title']
            year = release['release_year']

            self.logger.info(f"[{idx}/{total_releases}] Processing: {title} ({year or 'unknown year'})")

            # Skip if no MusicBrainz release ID
            if not release.get('musicbrainz_release_id'):
                self.logger.info(f"  SKIP: No MusicBrainz release ID")
                self.stats['releases_no_mb_id'] += 1
                continue

            try:
                best_match = self.find_best_match(release, spotify_albums, preloaded_tracks)

                # Require minimum matched tracks to avoid false positives on compilations
                if best_match and best_match.score >= self.threshold and best_match.track_matches >= min_tracks:
                    self.stats['releases_matched'] += 1
                    results['matches'].append({
                        'release': release,
                        'match': best_match
                    })

                    self.logger.info(f"  MATCH: {best_match.spotify_album_name}")
                    self.logger.info(f"    Score: {best_match.score:.1f}%")
                    self.logger.info(f"    Tracks: {best_match.track_matches}/{best_match.total_mb_tracks} MB tracks matched (Spotify: {best_match.total_spotify_tracks})")
                    self.logger.info(f"    MB Release: https://musicbrainz.org/release/{release['musicbrainz_release_id']}")
                    self.logger.info(f"    Spotify: {best_match.spotify_url}")

                    if best_match.unmatched_mb_tracks:
                        self.logger.debug(f"    Unmatched MB tracks: {best_match.unmatched_mb_tracks}")

                    # Apply if requested
                    if apply:
                        self._apply_match(release, best_match)
                        self.stats['releases_updated'] += 1
                        self.logger.info(f"    Updated in database")
                else:
                    self.stats['releases_no_match'] += 1
                    results['no_matches'].append({
                        'release': release,
                        'best_attempt': best_match
                    })

                    if best_match:
                        self.logger.info(f"  NO MATCH (best: {best_match.spotify_album_name}, score: {best_match.score:.1f}%)")
                    else:
                        self.logger.info(f"  NO MATCH (no candidates)")

            except Exception as e:
                self.logger.error(f"  Error processing release: {e}")
                results['errors'].append({
                    'release': release,
                    'error': str(e)
                })

            self.logger.info("")

        return results

    def _apply_match(self, release: dict, match: MatchResult):
        """Apply a match to the database - updates both release and recording_releases"""
        spotify_data = {
            'url': match.spotify_url,
            'id': match.spotify_album_id,
            'album_art': match.album_art
        }

        with get_db_connection() as conn:
            # Update the release with Spotify album info
            update_release_spotify_data(
                conn,
                release['id'],
                spotify_data,
                dry_run=False,
                log=self.logger
            )

            # Update recording_releases with individual track Spotify URLs
            # First, get the recording_releases for this release with their track positions
            # Check both normalized streaming_links table and legacy column
            tracks_updated = 0
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT rr.recording_id, rr.disc_number, rr.track_number
                    FROM recording_releases rr
                    LEFT JOIN recording_release_streaming_links rrsl
                        ON rrsl.recording_release_id = rr.id AND rrsl.service = 'spotify'
                    WHERE rr.release_id = %s
                      AND rrsl.service_id IS NULL
                      AND rr.spotify_track_id IS NULL
                """, (release['id'],))
                recording_releases = cur.fetchall()

            # Match each recording_release to a Spotify track by position
            for rr in recording_releases:
                recording_id = rr['recording_id']
                disc_num = rr['disc_number'] or 1
                track_num = rr['track_number']

                # Find the matched track at this position
                for track_match in match.matched_tracks:
                    if (track_match.mb_disc_number == disc_num and
                        track_match.mb_track_number == track_num and
                        track_match.spotify_track_id):
                        update_recording_release_track_id(
                            conn,
                            recording_id,
                            release['id'],
                            track_match.spotify_track_id,
                            track_match.spotify_track_url,
                            dry_run=False,
                            log=self.logger
                        )
                        tracks_updated += 1
                        break

            if tracks_updated > 0:
                self.stats['tracks_updated'] += tracks_updated
                self.logger.debug(f"    Updated {tracks_updated} recording_releases with Spotify track IDs")


def main() -> bool:
    script = ScriptBase(
        name="match_releases_by_tracklist",
        description="Match releases to Spotify albums by comparing track lists",
        epilog="""
Examples:
  # Dry run - show potential matches
  python match_releases_by_tracklist.py --artist "Oscar Peterson"

  # Apply matches to database
  python match_releases_by_tracklist.py --artist "Oscar Peterson" --apply

  # Show detailed matching info
  python match_releases_by_tracklist.py --artist "Oscar Peterson" --debug

  # Use stricter matching threshold
  python match_releases_by_tracklist.py --artist "Oscar Peterson" --threshold 80
        """
    )

    script.parser.add_argument(
        '--artist',
        required=True,
        help='Artist name to match releases for'
    )

    script.parser.add_argument(
        '--apply',
        action='store_true',
        help='Apply matches to database (default is dry run)'
    )

    script.parser.add_argument(
        '--threshold',
        type=float,
        default=70.0,
        help='Minimum match score to accept (default: 70)'
    )

    script.parser.add_argument(
        '--cache-days',
        type=int,
        default=30,
        help='Days before cache expires (default: 30)'
    )

    script.parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of releases to process (for testing)'
    )

    script.parser.add_argument(
        '--min-tracks',
        type=int,
        default=2,
        help='Minimum matched tracks required to accept (default: 2)'
    )

    script.add_debug_arg()
    script.add_force_refresh_arg()

    args = script.parse_args()

    # Print header
    modes = {
        "DRY RUN": not args.apply,
        "FORCE REFRESH": args.force_refresh,
    }
    script.print_header(modes)

    script.logger.info(f"Artist: {args.artist}")
    script.logger.info(f"Match threshold: {args.threshold}%")
    script.logger.info("")

    # Create matcher and run
    matcher = TrackListMatcher(
        logger=script.logger,
        threshold=args.threshold,
        cache_days=args.cache_days,
        force_refresh=args.force_refresh
    )

    results = matcher.match_artist_releases(
        args.artist,
        apply=args.apply,
        limit=args.limit,
        min_tracks=args.min_tracks
    )

    # Print summary
    script.print_summary({
        'Releases processed': matcher.stats['releases_processed'],
        'Matches found': matcher.stats['releases_matched'],
        'No match': matcher.stats['releases_no_match'],
        'No MB release ID': matcher.stats['releases_no_mb_id'],
        'Releases updated': matcher.stats['releases_updated'],
        'Tracks updated': matcher.stats['tracks_updated'],
        'Cache hits': matcher.stats['cache_hits'],
        'API calls': matcher.stats['api_calls'],
    }, title="MATCHING SUMMARY")

    # Print match details
    if results['matches']:
        script.logger.info("")
        script.logger.info("MATCHED RELEASES:")
        script.logger.info("-" * 60)
        for item in results['matches']:
            rel = item['release']
            match = item['match']
            script.logger.info(f"  {rel['title']} ({rel['release_year'] or '?'})")
            script.logger.info(f"    -> {match.spotify_album_name}")
            script.logger.info(f"       Score: {match.score:.1f}%, Tracks: {match.track_matches}/{match.total_mb_tracks} (Spotify: {match.total_spotify_tracks})")
            script.logger.info(f"       MB: https://musicbrainz.org/release/{rel['musicbrainz_release_id']}")
            script.logger.info(f"       Spotify: {match.spotify_url}")
            # Show matched tracks
            if match.matched_tracks:
                script.logger.info(f"       Matched tracks:")
                for tm in match.matched_tracks:
                    if tm.mb_title == tm.spotify_title:
                        script.logger.info(f"         [{tm.mb_disc_number}:{tm.mb_track_number}] {tm.mb_title}")
                    else:
                        script.logger.info(f"         [{tm.mb_disc_number}:{tm.mb_track_number}] {tm.mb_title} -> {tm.spotify_title}")
            # Show unmatched tracks
            if match.unmatched_mb_tracks:
                script.logger.info(f"       Unmatched MB tracks: {', '.join(match.unmatched_mb_tracks)}")

    if results['no_matches'] and args.debug:
        script.logger.info("")
        script.logger.info("UNMATCHED RELEASES:")
        script.logger.info("-" * 60)
        for item in results['no_matches']:
            rel = item['release']
            best = item.get('best_attempt')
            script.logger.info(f"  {rel['title']} ({rel['release_year'] or '?'})")
            if rel.get('musicbrainz_release_id'):
                script.logger.info(f"    MB: https://musicbrainz.org/release/{rel['musicbrainz_release_id']}")
            if best:
                script.logger.info(f"    Best attempt: {best.spotify_album_name} ({best.score:.1f}%)")
                script.logger.info(f"    Spotify: {best.spotify_url}")

    return True


if __name__ == "__main__":
    run_script(main)
