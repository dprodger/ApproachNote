#!/usr/bin/env python3
"""
Cover Art Archive Utilities

Shared utilities for interacting with the Cover Art Archive (CAA) API.
Provides caching support similar to mb_utils.py.

The Cover Art Archive is a joint project between MusicBrainz and Internet Archive
that provides cover art for music releases.

API Documentation: https://musicbrainz.org/doc/Cover_Art_Archive/API
"""

import json
import logging
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

import requests

from core.cache_utils import get_cache_dir
from core.http_client import make_session

logger = logging.getLogger(__name__)


class CoverArtArchiveError(Exception):
    """
    Raised when a CAA request fails in a way that cannot be distinguished
    from a valid response (network error, unexpected HTTP status, unparseable body).

    Callers should catch this and skip any side effects that assume a successful
    check — most importantly, do not stamp ``cover_art_checked_at`` on the release,
    so the standalone backfill will retry it on a later run.
    """


class CoverArtArchiveClient:
    """
    Client for Cover Art Archive API with caching support.
    
    The CAA API currently has no rate limiting, but we implement
    conservative rate limiting as a courtesy and for future-proofing.
    
    API Endpoints:
    - /release/{mbid}/ - JSON listing of all cover art for a release
    - /release/{mbid}/front - Redirect to front cover image
    - /release/{mbid}/back - Redirect to back cover image
    
    Response codes:
    - 200/307: Success (307 redirects to actual image)
    - 400: Invalid MBID format
    - 404: No release or no cover art
    - 503: Rate limit exceeded
    """
    
    BASE_URL = 'https://coverartarchive.org'
    
    def __init__(self, cache_days: int = 30, force_refresh: bool = False):
        """
        Initialize CAA client with caching support.
        
        Args:
            cache_days: Number of days before cache is considered stale
            force_refresh: If True, bypass cache and fetch fresh data
        """
        self.session = make_session()
        
        # Rate limiting (CAA has no limit, but be courteous)
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500ms between requests
        
        # Exponential backoff settings
        self.max_retries = 3
        self.base_delay = 1.0
        
        # Cache configuration
        self.cache_days = cache_days
        self.force_refresh = force_refresh
        
        # Track API calls
        self.last_made_api_call = False
        self.api_calls_made = 0
        self.cache_hits = 0
        
        # Get cache directory using shared utility
        self.cache_dir = get_cache_dir('coverart')
        self.release_cache_dir = self.cache_dir / 'releases'
        
        # Create subdirectories
        self.release_cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"CAA cache directory: {self.cache_dir}")
    
    def _get_release_cache_path(self, release_mbid: str) -> Path:
        """
        Get the cache file path for a release's cover art listing.
        
        Args:
            release_mbid: MusicBrainz release ID
            
        Returns:
            Path object for the cache file
        """
        filename = f"release_{release_mbid}.json"
        return self.release_cache_dir / filename
    
    def _is_cache_valid(self, cache_path: Path) -> bool:
        """
        Check if cache file exists and is not expired.
        
        Args:
            cache_path: Path to cache file
            
        Returns:
            bool: True if cache is valid and not expired
        """
        if not cache_path.exists():
            return False
        
        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        
        is_valid = age.days < self.cache_days
        if is_valid:
            logger.debug(f"Cache valid (age: {age.days} days): {cache_path.name}")
        else:
            logger.debug(f"Cache expired (age: {age.days} days): {cache_path.name}")
        
        return is_valid
    
    def _load_from_cache(self, cache_path: Path) -> Optional[Dict]:
        """
        Load data from cache file.
        
        Args:
            cache_path: Path to cache file
            
        Returns:
            Cached data dict, or None if not in cache
        """
        if not self._is_cache_valid(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                logger.debug(f"Loaded from cache: {cache_path.name}")
                self.cache_hits += 1
                return cache_data
        except Exception as e:
            logger.warning(f"Failed to load cache file {cache_path}: {e}")
            return None
    
    def _save_to_cache(self, cache_path: Path, data: Any):
        """
        Save data to cache file.
        
        Args:
            cache_path: Path to cache file
            data: Data to cache (will be JSON serialized)
        """
        try:
            cache_data = {
                'data': data,
                'cached_at': datetime.now().isoformat()
            }
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"Saved to cache: {cache_path.name}")
        except Exception as e:
            logger.warning(f"Failed to save cache file {cache_path}: {e}")
    
    def _rate_limit(self):
        """Enforce rate limiting for CAA API."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, allow_redirects: bool = True) -> Optional[requests.Response]:
        """
        Make an HTTP request with retry logic and exponential backoff.
        
        Args:
            url: URL to fetch
            allow_redirects: Whether to follow redirects (default True for CAA)
            
        Returns:
            Response object, or None on failure
        
        Note:
            CAA returns 307 redirects that point to index.json files.
            We must follow these redirects to get the actual JSON data.
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            self._rate_limit()
            
            try:
                response = self.session.get(
                    url,
                    timeout=15,
                    allow_redirects=allow_redirects
                )
                
                # Success cases
                if response.status_code in (200, 307):
                    self.api_calls_made += 1
                    return response
                
                # No cover art - this is expected for many releases
                if response.status_code == 404:
                    self.api_calls_made += 1
                    logger.debug(f"No cover art found (404): {url}")
                    return response
                
                # Retry transient server errors (5xx) and rate-limit (429)
                # with exponential backoff. CAA occasionally returns 500 during
                # upstream outages; these are almost always transient.
                if response.status_code >= 500 or response.status_code == 429:
                    delay = self.base_delay * (2 ** attempt)
                    logger.warning(
                        f"Transient status {response.status_code} for {url}, "
                        f"retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue

                # Other error (4xx other than 404/429) — don't retry
                logger.warning(f"Unexpected status {response.status_code} for {url}")
                return response
                
            except requests.exceptions.Timeout as e:
                last_error = e
                delay = self.base_delay * (2 ** attempt)
                logger.warning(f"Request timeout, retrying in {delay}s...")
                time.sleep(delay)
                
            except requests.exceptions.RequestException as e:
                last_error = e
                delay = self.base_delay * (2 ** attempt)
                logger.warning(f"Request error: {e}, retrying in {delay}s...")
                time.sleep(delay)
        
        logger.error(f"Failed after {self.max_retries} retries: {last_error}")
        return None
    
    def get_release_cover_art(self, release_mbid: str) -> Optional[Dict[str, Any]]:
        """
        Get cover art listing for a MusicBrainz release.
        
        This fetches the JSON index of all available cover art for a release,
        including URLs for different thumbnail sizes.
        
        Args:
            release_mbid: MusicBrainz release ID (UUID)
            
        Returns:
            Dict with 'images' array and 'release' URL, or None if no cover art.
            Returns {'no_cover_art': True} if release exists but has no art.
            
        Example response:
            {
                "images": [
                    {
                        "types": ["Front"],
                        "front": true,
                        "back": false,
                        "image": "http://coverartarchive.org/.../123.jpg",
                        "thumbnails": {
                            "250": "http://coverartarchive.org/.../123-250.jpg",
                            "500": "http://coverartarchive.org/.../123-500.jpg",
                            "1200": "http://coverartarchive.org/.../123-1200.jpg",
                            "small": "...",
                            "large": "..."
                        },
                        "id": "123",
                        "approved": true,
                        "comment": ""
                    }
                ],
                "release": "http://musicbrainz.org/release/..."
            }
        """
        # Check cache first (unless force_refresh)
        cache_path = self._get_release_cache_path(release_mbid)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"Using cached cover art for release {release_mbid}")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Make API request
        self.last_made_api_call = True
        url = f"{self.BASE_URL}/release/{release_mbid}/"

        logger.debug(f"Fetching cover art for release: {release_mbid}")
        response = self._make_request(url)

        if response is None:
            raise CoverArtArchiveError(
                f"CAA request failed for release {release_mbid} after retries"
            )

        # No cover art for this release
        if response.status_code == 404:
            # Cache the negative result to avoid repeated lookups
            result = {'no_cover_art': True, 'release_mbid': release_mbid}
            self._save_to_cache(cache_path, result)
            return result

        # Any status other than 200/404 is a failure we can't safely treat
        # as "no art available" — raise so the caller skips stamping the release
        # as checked and the backfill can retry later.
        if response.status_code != 200:
            raise CoverArtArchiveError(
                f"Unexpected status {response.status_code} for release {release_mbid}"
            )

        # Parse JSON response
        try:
            data = response.json()
            self._save_to_cache(cache_path, data)
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse CAA response: {e}")
            raise CoverArtArchiveError(
                f"Failed to parse CAA response for release {release_mbid}: {e}"
            ) from e
    
    def get_front_cover_url(self, release_mbid: str) -> Optional[str]:
        """
        Get the URL for the front cover of a release.
        
        This is a convenience method that extracts the front cover URL
        from the full cover art listing.
        
        Args:
            release_mbid: MusicBrainz release ID
            
        Returns:
            URL to the front cover image, or None if not available (including
            when the CAA request failed — this helper is best-effort and
            callers that need to detect failures should call
            ``get_release_cover_art`` directly).
        """
        try:
            data = self.get_release_cover_art(release_mbid)
        except CoverArtArchiveError:
            return None

        if not data or data.get('no_cover_art'):
            return None

        for image in data.get('images', []):
            if image.get('front'):
                return image.get('image')

        return None
    
    def _ensure_https(self, url: Optional[str]) -> Optional[str]:
        """
        Convert HTTP URLs to HTTPS for iOS App Transport Security compatibility.
        
        Args:
            url: URL that may be HTTP or HTTPS
            
        Returns:
            URL with https:// scheme, or None if input is None
        """
        if url and url.startswith('http://'):
            return url.replace('http://', 'https://', 1)
        return url
    
    def extract_imagery_data(self, release_mbid: str) -> List[Dict[str, Any]]:
        """
        Extract imagery data suitable for database insertion.
        
        Processes the raw CAA response and returns a list of dicts
        ready for insertion into the release_imagery table.
        
        Note: All URLs are converted to HTTPS for iOS ATS compatibility.
        
        Args:
            release_mbid: MusicBrainz release ID
            
        Returns:
            List of dicts with keys matching release_imagery columns:
            - source: 'MusicBrainz'
            - source_id: CAA image ID
            - source_url: Full image URL (HTTPS)
            - type: 'Front' or 'Back'
            - image_url_small: 250px thumbnail (HTTPS)
            - image_url_medium: 500px thumbnail (HTTPS)
            - image_url_large: 1200px thumbnail (HTTPS)
            - checksum: None (CAA doesn't provide checksums in API)
            - comment: Image comment
            - approved: Whether approved
        """
        data = self.get_release_cover_art(release_mbid)
        
        if not data or data.get('no_cover_art'):
            return []
        
        results = []
        
        for image in data.get('images', []):
            image_types = image.get('types', [])
            thumbnails = image.get('thumbnails', {})
            
            # We only care about Front and Back
            for img_type in image_types:
                if img_type in ('Front', 'Back'):
                    # Get URLs and ensure they're HTTPS
                    small_url = self._ensure_https(thumbnails.get('250') or thumbnails.get('small'))
                    medium_url = self._ensure_https(thumbnails.get('500') or thumbnails.get('large'))
                    large_url = self._ensure_https(thumbnails.get('1200') or image.get('image'))
                    source_url = self._ensure_https(image.get('image'))
                    
                    results.append({
                        'source': 'MusicBrainz',
                        'source_id': str(image.get('id', '')),
                        'source_url': source_url,
                        'type': img_type,
                        'image_url_small': small_url,
                        'image_url_medium': medium_url,
                        'image_url_large': large_url,
                        'checksum': None,  # CAA doesn't provide in API response
                        'comment': image.get('comment') or None,
                        'approved': image.get('approved', True)
                    })
                    break  # Only add once per image even if multiple types match
        
        return results
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about API usage.
        
        Returns:
            Dict with 'api_calls' and 'cache_hits' counts
        """
        return {
            'api_calls': self.api_calls_made,
            'cache_hits': self.cache_hits
        }