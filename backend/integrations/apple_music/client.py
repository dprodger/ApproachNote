"""
Apple Music / iTunes API Client

Handles low-level iTunes API concerns:
- Rate limiting with exponential backoff
- Response caching to minimize API calls

The iTunes Search API is simpler than Spotify - no OAuth required.
All endpoints are public and free to use.

API Documentation: https://developer.apple.com/library/archive/documentation/AudioVideo/Conceptual/iTuneSearchAPI/

Used by AppleMusicMatcher for all API interactions.
"""

import re
import time
import logging
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict, List
from urllib.parse import quote_plus
import requests

from core.cache_utils import get_cache_dir
from core.http_client import make_session

logger = logging.getLogger(__name__)

# Sentinel value to distinguish "no cache exists" from "cached None (no match found)"
_CACHE_MISS = object()

# Service identifier for the streaming_links tables
SERVICE_NAME = 'apple_music'


class AppleMusicRateLimitError(Exception):
    """Raised when iTunes API rate limit is hit"""
    def __init__(self, retry_after: int = None):
        self.retry_after = retry_after
        msg = f"iTunes rate limit exceeded. Retry after {retry_after} seconds." if retry_after else "iTunes rate limit exceeded."
        super().__init__(msg)


class AppleMusicClient:
    """
    Low-level iTunes/Apple Music API client with rate limiting and caching.

    The iTunes API is simpler than Spotify:
    - No OAuth required (all endpoints are public)
    - Search: https://itunes.apple.com/search?term=X&entity=Y
    - Lookup: https://itunes.apple.com/lookup?id=X
    """

    BASE_URL = "https://itunes.apple.com"

    def __init__(self, cache_days: int = 30, force_refresh: bool = False,
                 rate_limit_delay: float = 0.5, max_retries: int = 5,
                 country: str = "US", logger: logging.Logger = None):
        """
        Initialize Apple Music Client

        Args:
            cache_days: Number of days before cache is considered stale
            force_refresh: If True, always fetch fresh data ignoring cache
            rate_limit_delay: Base delay between API calls (seconds)
                             iTunes API is more restrictive than Spotify,
                             so we use a higher default (0.5s vs 0.2s)
            max_retries: Maximum number of retries for rate-limited requests
            country: Country code for storefront (default: US)
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.country = country

        # Cache configuration
        self.cache_days = cache_days
        self.force_refresh = force_refresh

        # Rate limiting configuration
        # iTunes API doesn't provide Retry-After headers, so we use conservative defaults
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.last_request_time = 0
        self.rate_limited_until = 0  # Timestamp when rate limit cooldown ends

        # Setup cache directories
        self.cache_dir = get_cache_dir('apple_music')
        self.search_cache_dir = self.cache_dir / 'searches'
        self.album_cache_dir = self.cache_dir / 'albums'
        self.track_cache_dir = self.cache_dir / 'tracks'

        # Create subdirectories
        self.search_cache_dir.mkdir(parents=True, exist_ok=True)
        self.album_cache_dir.mkdir(parents=True, exist_ok=True)
        self.track_cache_dir.mkdir(parents=True, exist_ok=True)

        # HTTP session for connection reuse
        self.session = make_session(accept_json=False)

        # Stats tracking
        self.stats = {
            'cache_hits': 0,
            'api_calls': 0,
            'rate_limit_hits': 0,
            'rate_limit_waits': 0
        }

        self.logger.debug(f"Apple Music cache: {self.cache_dir} (expires after {cache_days} days)")
        self.logger.debug(f"Rate limit: {rate_limit_delay}s delay, {max_retries} max retries")

    # ========================================================================
    # RATE LIMITING
    # ========================================================================

    def _wait_for_rate_limit(self):
        """Enforce minimum delay between requests"""
        if self.rate_limit_delay > 0:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - elapsed
                time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _make_api_request(self, url: str, params: Dict = None) -> requests.Response:
        """
        Make an API request with rate limit handling and retries

        iTunes API doesn't provide Retry-After headers and is very aggressive
        about rate limiting. We use long exponential backoff and a cooldown
        period after getting rate limited.

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Response object

        Raises:
            AppleMusicRateLimitError: If rate limit exceeded after all retries
        """
        # Check if we're still in a cooldown period from previous rate limiting
        if self.rate_limited_until > time.time():
            wait_remaining = int(self.rate_limited_until - time.time())
            self.logger.warning(f"In rate limit cooldown. Waiting {wait_remaining}s before retrying...")
            time.sleep(wait_remaining + 1)

        retry_count = 0
        # Start with 10 second delay since iTunes is aggressive about rate limiting
        base_delay = 10

        while retry_count <= self.max_retries:
            self._wait_for_rate_limit()

            try:
                self.stats['api_calls'] += 1
                response = self.session.get(url, params=params, timeout=15)

                # iTunes API returns 403 when rate limited (not 429)
                if response.status_code in (429, 403):
                    self.stats['rate_limit_hits'] += 1

                    if retry_count >= self.max_retries:
                        # Set a 2-minute cooldown before trying again
                        self.rate_limited_until = time.time() + 120
                        self.logger.error(f"Rate limit exhausted. Entering 2-minute cooldown.")
                        raise AppleMusicRateLimitError(120)

                    # Exponential backoff: 10s, 20s, 40s, 80s, 160s
                    wait_time = base_delay * (2 ** retry_count)
                    self.logger.warning(f"Rate limit hit (attempt {retry_count + 1}/{self.max_retries + 1}). "
                                       f"Waiting {wait_time}s")
                    self.stats['rate_limit_waits'] += 1
                    time.sleep(wait_time)
                    retry_count += 1
                    continue

                # Successful request - clear any cooldown
                self.rate_limited_until = 0
                return response

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed: {e}")
                raise

        raise AppleMusicRateLimitError()

    # ========================================================================
    # CACHING
    # ========================================================================

    def _get_search_cache_path(self, query: str, entity: str) -> Path:
        """Get cache file path for a search query"""
        cache_key = f"{query}||{entity}"
        query_hash = hashlib.md5(cache_key.encode()).hexdigest()
        safe_query = re.sub(r'[^a-zA-Z0-9_-]', '_', query.lower())[:50]
        filename = f"search_{safe_query}_{query_hash}.json"
        return self.search_cache_dir / filename

    def _get_album_cache_path(self, album_id: str) -> Path:
        """Get cache file path for an album lookup"""
        return self.album_cache_dir / f"album_{album_id}.json"

    def _get_track_cache_path(self, track_id: str) -> Path:
        """Get cache file path for a track lookup"""
        return self.track_cache_dir / f"track_{track_id}.json"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache file exists and is not expired"""
        if self.force_refresh:
            return False
        if not cache_path.exists():
            return False

        file_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age_days = (datetime.now() - file_mtime).days
        return age_days < self.cache_days

    def _load_from_cache(self, cache_path: Path) -> Any:
        """Load data from cache if valid"""
        if not self._is_cache_valid(cache_path):
            return _CACHE_MISS

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.stats['cache_hits'] += 1
                self.logger.debug(f"Cache hit: {cache_path.name}")
                return data
        except (json.JSONDecodeError, IOError) as e:
            self.logger.warning(f"Failed to load cache {cache_path}: {e}")
            try:
                cache_path.unlink()
            except:
                pass
            return _CACHE_MISS

    def _save_to_cache(self, cache_path: Path, data: Any) -> None:
        """Save data to cache file"""
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            self.logger.debug(f"Cached: {cache_path.name}")
        except (IOError, TypeError) as e:
            self.logger.warning(f"Failed to cache {cache_path}: {e}")

    # ========================================================================
    # API METHODS
    # ========================================================================

    def search_albums(self, artist_name: str, album_title: str = None,
                      limit: int = 25) -> List[Dict]:
        """
        Search for albums on iTunes/Apple Music

        Args:
            artist_name: Artist name to search for
            album_title: Optional album title to include in search
            limit: Maximum results to return (default 25)

        Returns:
            List of album dicts with keys:
            - id: Apple Music collection ID
            - name: Album/collection name
            - artist: Artist name
            - release_date: Release date string
            - track_count: Number of tracks
            - artwork: Dict with 'small', 'medium', 'large' URLs
            - url: Apple Music album URL
        """
        # Build search query
        if album_title:
            query = f"{artist_name} {album_title}"
        else:
            query = artist_name

        # Check cache
        cache_path = self._get_search_cache_path(query, 'album')
        cached = self._load_from_cache(cache_path)
        if cached is not _CACHE_MISS:
            return cached

        # Make API request
        url = f"{self.BASE_URL}/search"
        params = {
            'term': query,
            'entity': 'album',
            'limit': limit,
            'country': self.country
        }

        try:
            response = self._make_api_request(url, params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Album search failed: {e}")
            return []

        # Parse results
        albums = []
        for item in data.get('results', []):
            if item.get('wrapperType') != 'collection':
                continue

            album = self._parse_album_result(item)
            if album:
                albums.append(album)

        # Cache results
        self._save_to_cache(cache_path, albums)

        return albums

    def search_tracks(self, artist_name: str, track_title: str,
                      limit: int = 25) -> List[Dict]:
        """
        Search for tracks on iTunes/Apple Music

        Args:
            artist_name: Artist name
            track_title: Track/song title
            limit: Maximum results

        Returns:
            List of track dicts
        """
        query = f"{artist_name} {track_title}"

        cache_path = self._get_search_cache_path(query, 'song')
        cached = self._load_from_cache(cache_path)
        if cached is not _CACHE_MISS:
            return cached

        url = f"{self.BASE_URL}/search"
        params = {
            'term': query,
            'entity': 'song',
            'limit': limit,
            'country': self.country
        }

        try:
            response = self._make_api_request(url, params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Track search failed: {e}")
            return []

        tracks = []
        for item in data.get('results', []):
            if item.get('wrapperType') != 'track':
                continue
            track = self._parse_track_result(item)
            if track:
                tracks.append(track)

        self._save_to_cache(cache_path, tracks)
        return tracks

    def lookup_album(self, album_id: str) -> Optional[Dict]:
        """
        Look up an album by its Apple Music collection ID

        Args:
            album_id: Apple Music collection ID

        Returns:
            Album dict or None if not found
        """
        cache_path = self._get_album_cache_path(album_id)
        cached = self._load_from_cache(cache_path)
        if cached is not _CACHE_MISS:
            return cached

        url = f"{self.BASE_URL}/lookup"
        params = {
            'id': album_id,
            'country': self.country
        }

        try:
            response = self._make_api_request(url, params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Album lookup failed: {e}")
            return None

        if data.get('resultCount', 0) == 0:
            self._save_to_cache(cache_path, None)
            return None

        item = data['results'][0]
        album = self._parse_album_result(item)
        self._save_to_cache(cache_path, album)
        return album

    def lookup_album_tracks(self, album_id: str) -> List[Dict]:
        """
        Look up all tracks on an album

        Args:
            album_id: Apple Music collection ID

        Returns:
            List of track dicts for this album
        """
        # This uses a slightly different cache key
        cache_path = self.album_cache_dir / f"album_{album_id}_tracks.json"
        cached = self._load_from_cache(cache_path)
        if cached is not _CACHE_MISS:
            return cached

        url = f"{self.BASE_URL}/lookup"
        params = {
            'id': album_id,
            'entity': 'song',
            'country': self.country
        }

        try:
            response = self._make_api_request(url, params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Album tracks lookup failed: {e}")
            return []

        tracks = []
        for item in data.get('results', []):
            if item.get('wrapperType') != 'track':
                continue
            track = self._parse_track_result(item)
            if track:
                tracks.append(track)

        self._save_to_cache(cache_path, tracks)
        return tracks

    def lookup_track(self, track_id: str) -> Optional[Dict]:
        """
        Look up a track by its Apple Music track ID

        Args:
            track_id: Apple Music track ID

        Returns:
            Track dict or None
        """
        cache_path = self._get_track_cache_path(track_id)
        cached = self._load_from_cache(cache_path)
        if cached is not _CACHE_MISS:
            return cached

        url = f"{self.BASE_URL}/lookup"
        params = {
            'id': track_id,
            'country': self.country
        }

        try:
            response = self._make_api_request(url, params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Track lookup failed: {e}")
            return None

        if data.get('resultCount', 0) == 0:
            self._save_to_cache(cache_path, None)
            return None

        item = data['results'][0]
        track = self._parse_track_result(item)
        self._save_to_cache(cache_path, track)
        return track

    # ========================================================================
    # RESULT PARSING
    # ========================================================================

    def _parse_album_result(self, item: Dict) -> Optional[Dict]:
        """Parse an album result from the iTunes API"""
        try:
            album_id = str(item.get('collectionId'))
            artwork_url = item.get('artworkUrl100', '')

            return {
                'id': album_id,
                'name': item.get('collectionName'),
                'artist': item.get('artistName'),
                'release_date': item.get('releaseDate'),
                'track_count': item.get('trackCount'),
                'url': item.get('collectionViewUrl'),
                'artwork': self._parse_artwork_urls(artwork_url),
                'explicit': item.get('collectionExplicitness') == 'explicit',
                'genre': item.get('primaryGenreName'),
            }
        except Exception as e:
            self.logger.warning(f"Failed to parse album result: {e}")
            return None

    def _parse_track_result(self, item: Dict) -> Optional[Dict]:
        """Parse a track result from the iTunes API"""
        try:
            track_id = str(item.get('trackId'))
            album_id = str(item.get('collectionId')) if item.get('collectionId') else None
            artwork_url = item.get('artworkUrl100', '')

            return {
                'id': track_id,
                'name': item.get('trackName'),
                'artist': item.get('artistName'),
                'album_id': album_id,
                'album_name': item.get('collectionName'),
                'track_number': item.get('trackNumber'),
                'disc_number': item.get('discNumber', 1),
                'duration_ms': item.get('trackTimeMillis'),
                'release_date': item.get('releaseDate'),
                'url': item.get('trackViewUrl'),
                'preview_url': item.get('previewUrl'),
                'artwork': self._parse_artwork_urls(artwork_url),
                'isrc': item.get('isrc'),  # iTunes sometimes provides ISRC
            }
        except Exception as e:
            self.logger.warning(f"Failed to parse track result: {e}")
            return None

    def _parse_artwork_urls(self, base_url: str) -> Dict[str, str]:
        """
        Parse artwork URL and generate multiple sizes

        iTunes provides artworkUrl100 (100x100). We can modify the URL
        to get different sizes by replacing the dimensions.

        Args:
            base_url: The artworkUrl100 from iTunes API

        Returns:
            Dict with 'small', 'medium', 'large' URLs
        """
        if not base_url:
            return {'small': None, 'medium': None, 'large': None}

        # iTunes artwork URLs have format: .../100x100bb.jpg
        # We can replace to get different sizes
        return {
            'small': base_url.replace('100x100', '100x100'),    # 100x100
            'medium': base_url.replace('100x100', '300x300'),   # 300x300
            'large': base_url.replace('100x100', '600x600'),    # 600x600
        }


def build_apple_music_album_url(album_id: str, country: str = "us") -> str:
    """
    Construct an Apple Music album URL from an album ID

    Args:
        album_id: Apple Music collection ID
        country: Country code (default: us)

    Returns:
        URL like https://music.apple.com/us/album/1234567890
    """
    return f"https://music.apple.com/{country}/album/{album_id}"


def build_apple_music_track_url(track_id: str, country: str = "us") -> str:
    """
    Construct an Apple Music track URL from a track ID

    Args:
        track_id: Apple Music track ID
        country: Country code (default: us)

    Returns:
        URL like https://music.apple.com/us/song/1234567890
    """
    return f"https://music.apple.com/{country}/song/{track_id}"
