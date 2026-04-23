"""
YouTube Data API v3 client.

Thin wrapper around two endpoints:
- search.list  — find videos matching a query (cost: 100 units/call)
- videos.list  — fetch details for up to 50 video IDs (cost: 1 unit/call)

Those costs are the reason this client exists. The free quota is 10,000
units/day, so roughly 100 matches/day unless we cache aggressively. Every
call is cached by argument hash with a `cache_days` TTL; every call
increments a per-session quota counter and hard-stops at `max_units` to
leave headroom for song-research hits later.

Does NOT handle the overall matcher flow (scoring, DB writes) — that lives
in matcher.py.
"""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from core.cache_utils import get_cache_dir

logger = logging.getLogger(__name__)

# Service identifier for the streaming_links tables
SERVICE_NAME = 'youtube'

# YouTube Data API v3 base URL
BASE_URL = "https://www.googleapis.com/youtube/v3"

# Quota costs (from Google docs — subject to change but stable for years)
QUOTA_COST_SEARCH = 100
QUOTA_COST_VIDEOS = 1

# Sentinel to distinguish "never cached" from "cached as empty"
_CACHE_MISS = object()


def build_youtube_video_url(video_id: str) -> str:
    """YouTube watch URL for a video ID."""
    return f"https://www.youtube.com/watch?v={video_id}"


class YouTubeQuotaExceededError(Exception):
    """Raised when the per-session quota budget would be exceeded by the next call."""


class YouTubeAPIError(Exception):
    """Raised on non-recoverable API errors (403 other than quota, 400, etc)."""


class YouTubeClient:
    """
    Thin YouTube Data API v3 client with on-disk cache + per-session quota budget.

    Not thread-safe (matches the other matchers). Each matcher run should
    instantiate its own client.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_days: int = 30,
        force_refresh: bool = False,
        max_units: int = 9500,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            api_key: YouTube Data API v3 key. Defaults to env YOUTUBE_API_KEY.
            cache_days: Days before a cached response is considered stale.
            force_refresh: If True, bypass cache reads (still writes on miss).
            max_units: Hard stop when this many quota units have been spent
                in this session. Default 9500 leaves 500/day for song-research.
            logger: Logger instance.
        """
        self.api_key = api_key or os.environ.get('YOUTUBE_API_KEY')
        if not self.api_key:
            raise RuntimeError(
                "YOUTUBE_API_KEY is not set. Add it to backend/.env or pass api_key="
            )

        self.logger = logger or logging.getLogger(__name__)
        self.cache_days = cache_days
        self.force_refresh = force_refresh
        self.max_units = max_units

        cache_root = get_cache_dir('youtube')
        self.search_cache_dir = cache_root / 'searches'
        self.videos_cache_dir = cache_root / 'videos'
        self.search_cache_dir.mkdir(parents=True, exist_ok=True)
        self.videos_cache_dir.mkdir(parents=True, exist_ok=True)

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)',
        })

        # Stats — mirrors what the other matchers expose so a CLI can print
        # a consistent summary. `quota_units` is the YouTube-specific one.
        self.stats = {
            'cache_hits': 0,
            'api_calls': 0,
            'quota_units': 0,
        }

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _cache_path(self, subdir: Path, key: str) -> Path:
        safe = hashlib.md5(key.encode()).hexdigest()
        return subdir / f"{safe}.json"

    def _load_cache(self, cache_path: Path) -> Any:
        """Return cached value, or _CACHE_MISS if absent/stale."""
        if self.force_refresh or not cache_path.exists():
            return _CACHE_MISS

        age_days = (time.time() - cache_path.stat().st_mtime) / 86400
        if age_days > self.cache_days:
            return _CACHE_MISS

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            return payload.get('data')
        except Exception as e:
            self.logger.warning(f"Cache read failed {cache_path.name}: {e}")
            return _CACHE_MISS

    def _save_cache(self, cache_path: Path, data: Any) -> None:
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'data': data,
                    'cached_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.warning(f"Cache write failed {cache_path.name}: {e}")

    # ------------------------------------------------------------------
    # Quota accounting
    # ------------------------------------------------------------------

    def _reserve_quota(self, cost: int) -> None:
        """Raise YouTubeQuotaExceededError if this call would exceed max_units."""
        if self.stats['quota_units'] + cost > self.max_units:
            raise YouTubeQuotaExceededError(
                f"Would exceed session budget: "
                f"{self.stats['quota_units']} + {cost} > {self.max_units}"
            )

    def _charge_quota(self, cost: int) -> None:
        self.stats['quota_units'] += cost
        self.stats['api_calls'] += 1

    # ------------------------------------------------------------------
    # API calls
    # ------------------------------------------------------------------

    def search(
        self,
        query: str,
        max_results: int = 10,
        video_category_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        search.list — returns a list of candidate video snippets.

        Each result dict has at least `videoId`, `title`, `channelTitle`,
        `channelId`, `publishedAt`. Duration is NOT included — that requires
        a videos.list follow-up (see `get_videos`).

        Returns [] when there are no matches. Cached result dicts are
        returned as-is (may be [] for "known-no-match").
        """
        cache_key = f"q={query}|n={max_results}|cat={video_category_id or ''}"
        cache_path = self._cache_path(self.search_cache_dir, cache_key)

        cached = self._load_cache(cache_path)
        if cached is not _CACHE_MISS:
            self.stats['cache_hits'] += 1
            self.logger.debug(f"  [cache hit] search: {query[:60]}")
            return cached

        self._reserve_quota(QUOTA_COST_SEARCH)
        self.logger.debug(f"  [api] search: {query[:60]}")

        params = {
            'key': self.api_key,
            'part': 'snippet',
            'q': query,
            'type': 'video',
            'maxResults': max_results,
        }
        if video_category_id:
            params['videoCategoryId'] = video_category_id

        data = self._get('/search', params)
        self._charge_quota(QUOTA_COST_SEARCH)

        results = []
        for item in data.get('items', []):
            snippet = item.get('snippet', {})
            results.append({
                'videoId': item.get('id', {}).get('videoId'),
                'title': snippet.get('title'),
                'description': snippet.get('description'),
                'channelTitle': snippet.get('channelTitle'),
                'channelId': snippet.get('channelId'),
                'publishedAt': snippet.get('publishedAt'),
            })

        self._save_cache(cache_path, results)
        return results

    def get_videos(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        videos.list — fetch details (title, duration, channel, stats) for up
        to 50 video IDs at a time.

        Duration comes through as an ISO-8601 string (e.g. 'PT4M33S') —
        caller is responsible for parsing via `parse_iso8601_duration_ms`.

        Caching is per-individual-video-ID: if only 3 of 50 IDs are cache
        misses, only those 3 make an API call. This is different from
        `search` where the full result set is cached as one blob.
        """
        if not video_ids:
            return []

        # Split into cached vs uncached
        results_by_id: Dict[str, Dict[str, Any]] = {}
        misses: List[str] = []
        for vid in video_ids:
            cache_path = self._cache_path(self.videos_cache_dir, vid)
            cached = self._load_cache(cache_path)
            if cached is not _CACHE_MISS:
                self.stats['cache_hits'] += 1
                results_by_id[vid] = cached
            else:
                misses.append(vid)

        if misses:
            # Fetch in batches of 50 (the API max). 1 quota unit per call
            # regardless of how many IDs are in the batch.
            for i in range(0, len(misses), 50):
                batch = misses[i:i + 50]
                self._reserve_quota(QUOTA_COST_VIDEOS)
                self.logger.debug(f"  [api] videos.list ({len(batch)} IDs)")

                params = {
                    'key': self.api_key,
                    'part': 'snippet,contentDetails,statistics',
                    'id': ','.join(batch),
                }
                data = self._get('/videos', params)
                self._charge_quota(QUOTA_COST_VIDEOS)

                seen = set()
                for item in data.get('items', []):
                    normalized = _normalize_video(item)
                    vid = normalized['videoId']
                    seen.add(vid)
                    results_by_id[vid] = normalized
                    cache_path = self._cache_path(self.videos_cache_dir, vid)
                    self._save_cache(cache_path, normalized)

                # Cache misses (video deleted / private) as None so we don't
                # re-fetch them on every run.
                for vid in batch:
                    if vid not in seen:
                        cache_path = self._cache_path(self.videos_cache_dir, vid)
                        self._save_cache(cache_path, None)

        # Preserve input order, skip any that resolved to None
        return [results_by_id[vid] for vid in video_ids
                if vid in results_by_id and results_by_id[vid] is not None]

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Thin GET wrapper — raises on error, returns parsed JSON."""
        url = BASE_URL + path
        try:
            response = self.session.get(url, params=params, timeout=15)
        except requests.exceptions.RequestException as e:
            raise YouTubeAPIError(f"Network error: {e}") from e

        if response.status_code == 200:
            return response.json()

        # Parse error details
        try:
            err = response.json().get('error', {})
            reason = (err.get('errors') or [{}])[0].get('reason')
            message = err.get('message', response.text[:200])
        except Exception:
            reason = None
            message = response.text[:200]

        if response.status_code == 403 and reason in ('quotaExceeded', 'rateLimitExceeded'):
            raise YouTubeQuotaExceededError(
                f"YouTube quota exceeded (reason={reason}): {message}"
            )

        raise YouTubeAPIError(
            f"YouTube API {response.status_code} ({reason}): {message}"
        )


# ---------------------------------------------------------------------------
# Response normalization + small parsers (module-level so matcher can use)
# ---------------------------------------------------------------------------

def _normalize_video(item: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten the videos.list response item into a single dict."""
    snippet = item.get('snippet') or {}
    details = item.get('contentDetails') or {}
    stats = item.get('statistics') or {}
    return {
        'videoId': item.get('id'),
        'title': snippet.get('title'),
        'description': snippet.get('description'),
        'channelTitle': snippet.get('channelTitle'),
        'channelId': snippet.get('channelId'),
        'publishedAt': snippet.get('publishedAt'),
        'duration_iso': details.get('duration'),
        'duration_ms': parse_iso8601_duration_ms(details.get('duration')),
        'viewCount': _int_or_none(stats.get('viewCount')),
        'likeCount': _int_or_none(stats.get('likeCount')),
    }


def parse_iso8601_duration_ms(iso: Optional[str]) -> Optional[int]:
    """
    Parse a YouTube ISO-8601 duration ('PT4M33S', 'PT1H2M3S', 'PT45S') to ms.
    Returns None on malformed input. YouTube never emits the date-portion
    part of ISO-8601 (no weeks/months), so this only handles PT*H*M*S.
    """
    if not iso or not iso.startswith('PT'):
        return None

    hours = minutes = seconds = 0
    num = ''
    for ch in iso[2:]:
        if ch.isdigit():
            num += ch
        elif ch == 'H':
            hours = int(num or '0')
            num = ''
        elif ch == 'M':
            minutes = int(num or '0')
            num = ''
        elif ch == 'S':
            seconds = int(num or '0')
            num = ''
        else:
            return None
    return (hours * 3600 + minutes * 60 + seconds) * 1000


def _int_or_none(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None
