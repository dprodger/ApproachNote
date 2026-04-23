"""
MusicBrainz API Client

Low-level HTTP client for the MusicBrainz web service (plus the Wikidata
Wikipedia-URL lookup that happens alongside it). Owns:

- The requests.Session with the User-Agent MB requires
- Rate limiting (~1.66 req/s to stay under the public endpoint's quota)
- Retry + exponential backoff on 503/429/connection errors
- A JSON-on-disk response cache per entity type (works, artists, recordings,
  releases, wikidata), with a `cache_days` TTL and negative-result caching

The searcher also does two small non-HTTP things that live here because they
only make sense in the context of a search: `normalize_title` (for matching
MB responses to our stored titles) and `_escape_lucene_query` (for building
MB's Lucene-syntax queries safely). These will likely move to parsing.py /
matching.py in a later step.

Used by:
- integrations/musicbrainz/release_importer.py (MBReleaseImporter)
- integrations/musicbrainz/performer_importer.py (PerformerImporter)
- integrations/musicbrainz/utils.py (song-update helpers)
- Many scripts/ that import MusicBrainzSearcher (via the utils.py facade)
"""

import time
import logging
import requests
import json
import hashlib
import re
from datetime import datetime
from pathlib import Path

from core.cache_utils import get_cache_dir

logger = logging.getLogger(__name__)


class MusicBrainzSearcher:
    """Shared MusicBrainz search functionality with caching"""
    
    def __init__(self, cache_days=30, force_refresh=False):
        """
        Initialize MusicBrainz searcher with caching support
        
        Args:
            cache_days: Number of days before cache is considered stale
            force_refresh: If True, always fetch fresh data ignoring cache
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)',
            'Accept': 'application/json'
        })
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.6  # ~150 requests/minute with proper User-Agent
        
        # Cache configuration
        self.cache_days = cache_days
        self.force_refresh = force_refresh
        
        # Track whether last operation made an API call
        self.last_made_api_call = False
        
        # Get cache directories using the shared utility
        # This ensures we use the persistent disk mount on Render
        self.cache_dir = get_cache_dir('musicbrainz')
        self.search_cache_dir = self.cache_dir / 'searches'
        self.artist_cache_dir = self.cache_dir / 'artists'
        self.work_cache_dir = self.cache_dir / 'works'
        self.recording_cache_dir = self.cache_dir / 'recordings'
        self.release_cache_dir = self.cache_dir / 'releases'
        self.wikidata_cache_dir = self.cache_dir / 'wikidata'
        
        # Create subdirectories
        self.search_cache_dir.mkdir(parents=True, exist_ok=True)
        self.artist_cache_dir.mkdir(parents=True, exist_ok=True)
        self.work_cache_dir.mkdir(parents=True, exist_ok=True)
        self.recording_cache_dir.mkdir(parents=True, exist_ok=True)
        self.release_cache_dir.mkdir(parents=True, exist_ok=True)
        self.wikidata_cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"MusicBrainz cache directory: {self.cache_dir}")

    def verify_musicbrainz_reference(self, artist_name, mb_id, context):
        """
        Verify that a MusicBrainz artist ID is valid
        
        Args:
            artist_name: Name of the artist
            mb_id: MusicBrainz artist ID (UUID)
            context: Dict with sample_songs for verification
            
        Returns:
            Dict with 'valid' (bool), 'confidence' (str), 'reason' (str)
        """
        try:
            logger.debug(f"Verifying MusicBrainz ID: {mb_id}")
            
            # Use the cached detail lookup
            data = self.get_artist_details(mb_id)
            
            if data is None:
                return {
                    'valid': False,
                    'confidence': 'certain',
                    'reason': 'MusicBrainz ID not found (404)'
                }
            
            # Check name similarity
            mb_name = data.get('name', '').lower()
            artist_name_lower = artist_name.lower()
            
            if mb_name != artist_name_lower:
                # Check if it's a close match
                if mb_name not in artist_name_lower and artist_name_lower not in mb_name:
                    return {
                        'valid': False,
                        'confidence': 'high',
                        'reason': f'Name mismatch: searched for "{artist_name}", MusicBrainz has "{data.get("name")}"'
                    }
            
            # Name matches, this is valid
            return {
                'valid': True,
                'confidence': 'high',
                'reason': f'Name matches: "{data.get("name")}"'
            }
            
        except requests.exceptions.Timeout:
            return {
                'valid': False,
                'confidence': 'uncertain',
                'reason': 'Request timed out'
            }
        except Exception as e:
            logger.error(f"Unexpected error verifying MusicBrainz: {e}", exc_info=True)
            return {
                'valid': False,
                'confidence': 'uncertain',
                'reason': f'Verification error: {str(e)}'
            }
    
    def _get_work_search_cache_path(self, title, composer):
        """
        Get the cache file path for a work search query
        
        Args:
            title: Song title
            composer: Composer name
            
        Returns:
            Path object for the cache file
        """
        query_string = f"{title}||{composer or ''}"
        query_hash = hashlib.md5(query_string.encode()).hexdigest()
        safe_title = re.sub(r'[^a-zA-Z0-9_-]', '_', title.lower())[:50]
        filename = f"work_{safe_title}_{query_hash}.json"
        return self.search_cache_dir / filename
    
    def _get_artist_search_cache_path(self, artist_name):
        """
        Get the cache file path for an artist search query
        
        Args:
            artist_name: Artist name to search for
            
        Returns:
            Path object for the cache file
        """
        query_hash = hashlib.md5(artist_name.encode()).hexdigest()
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', artist_name.lower())[:50]
        filename = f"artist_search_{safe_name}_{query_hash}.json"
        return self.search_cache_dir / filename
    
    def _get_artist_detail_cache_path(self, mb_id):
        """
        Get the cache file path for an artist detail lookup
        
        Args:
            mb_id: MusicBrainz artist ID
            
        Returns:
            Path object for the cache file
        """
        filename = f"artist_{mb_id}.json"
        return self.artist_cache_dir / filename
    
    def _get_work_detail_cache_path(self, work_id):
        """
        Get the cache file path for a work detail lookup
        
        Args:
            work_id: MusicBrainz work ID
            
        Returns:
            Path object for the cache file
        """
        filename = f"work_{work_id}.json"
        return self.work_cache_dir / filename
    
    def _get_recording_detail_cache_path(self, recording_id):
        """
        Get the cache file path for a recording detail lookup
        
        Args:
            recording_id: MusicBrainz recording ID
            
        Returns:
            Path object for the cache file
        """
        filename = f"recording_{recording_id}.json"
        return self.recording_cache_dir / filename
    
    def _get_release_detail_cache_path(self, release_id):
        """
        Get the cache file path for a release detail lookup
        
        Args:
            release_id: MusicBrainz release ID
            
        Returns:
            Path object for the cache file
        """
        filename = f"release_{release_id}.json"
        return self.release_cache_dir / filename
    
    def _get_wikidata_cache_path(self, wikidata_id):
        """
        Get the cache file path for a Wikidata lookup
        
        Args:
            wikidata_id: Wikidata ID (e.g., 'Q12345')
            
        Returns:
            Path object for the cache file
        """
        filename = f"wikidata_{wikidata_id}.json"
        return self.wikidata_cache_dir / filename
    
    def _is_cache_valid(self, cache_path):
        """
        Check if cache file exists and is not expired
        
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
    
    def _load_from_cache(self, cache_path):
        """
        Load data from cache file
        
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
                return cache_data
        except Exception as e:
            logger.warning(f"Failed to load cache file {cache_path}: {e}")
            return None
    
    def _save_to_cache(self, cache_path, data):
        """
        Save data to cache file
        
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
    
    def rate_limit(self):
        """Enforce rate limiting for MusicBrainz API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            logger.debug("rate_limit: sleep")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def normalize_title(self, title):
        """
        Normalize title for comparison by handling various punctuation differences
        
        Args:
            title: Title to normalize
        
        Returns:
            Normalized title string
        """
        normalized = title.lower()
        
        # Replace all types of apostrophes with standard apostrophe
        # Includes: ' (right single quotation), ʼ (modifier letter apostrophe), 
        # ` (grave accent), ´ (acute accent)
        apostrophe_variants = [''', ''', 'ʼ', '`', '´']
        for variant in apostrophe_variants:
            normalized = normalized.replace(variant, "'")
        
        # Replace different types of dashes/hyphens
        dash_variants = ['–', '—', '−']  # en dash, em dash, minus
        for variant in dash_variants:
            normalized = normalized.replace(variant, '-')
        
        # Replace different types of quotes
        quote_variants = ['"', '"', '„', '«', '»']  # smart quotes, guillemets
        for variant in quote_variants:
            normalized = normalized.replace(variant, '"')
        
        return normalized
    
    def search_musicbrainz_work(self, title, composer):
        """
        Search MusicBrainz for a work by title and composer
        
        Uses multiple search strategies to maximize chances of finding a match:
        1. Try with exact phrase in quotes (most precise)
        2. Fall back to unquoted search if needed (broader)
        3. Don't over-constrain with composer (can filter results instead)
        
        Args:
            title: Song title
            composer: Composer name(s)
        
        Returns:
            MusicBrainz Work ID if found, None otherwise
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_work_search_cache_path(title, composer)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached work search result (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Perform search
        self.last_made_api_call = True
        self.rate_limit()
        
        # Strategy 1: Search with exact title phrase (no composer constraint)
        # We don't add composer to query because it's often too restrictive
        # Better to get more results and filter by title match
        query = f'work:"{title}"'
        
        logger.debug(f"    Searching MusicBrainz: {query}")
        
        try:
            response = self.session.get(
                'https://musicbrainz.org/ws/2/work/',
                params={
                    'query': query,
                    'fmt': 'json',
                    'limit': 10  # Get more results since we're not filtering by composer
                },
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            works = data.get('works', [])
            
            # If no results with quoted search, try unquoted
            if not works:
                logger.debug(f"    No results with quoted search, trying unquoted...")
                self.rate_limit()
                
                query = title
                logger.debug(f"    Searching MusicBrainz: {query}")
                
                response = self.session.get(
                    'https://musicbrainz.org/ws/2/work/',
                    params={
                        'query': query,
                        'fmt': 'json',
                        'limit': 10
                    },
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()
                works = data.get('works', [])
            
            if not works:
                logger.debug(f"    ✗ No MusicBrainz works found")
                # Cache the negative result too
                self._save_to_cache(cache_path, None)
                return None
            
            # Normalize search title for comparison
            normalized_search_title = self.normalize_title(title)
            
            # Look for exact or very close title match
            for work in works:
                work_title = work.get('title', '')
                normalized_work_title = self.normalize_title(work_title)
                
                # Check for exact match after normalization
                if normalized_work_title == normalized_search_title:
                    mb_id = work['id']
                    logger.debug(f"    ✓ Found: '{work['title']}' (ID: {mb_id})")
                    
                    # Show composer if available
                    if 'artist-relation-list' in work:
                        composers = [r['artist']['name'] for r in work['artist-relation-list'] 
                                   if r['type'] == 'composer']
                        if composers:
                            logger.debug(f"       Composer(s): {', '.join(composers)}")
                    
                    # Cache the result
                    self._save_to_cache(cache_path, mb_id)
                    return mb_id
            
            # If no exact match, show what was found
            logger.debug(f"    ⚠ Found {len(works)} works but no exact match:")
            for work in works[:3]:
                logger.debug(f"       - '{work['title']}'")

            # Cache the negative result
            self._save_to_cache(cache_path, None)
            return None

        except requests.exceptions.Timeout:
            logger.warning(f"    ⚠ MusicBrainz search timed out")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"    ✗ MusicBrainz search failed: {e}")
            return None
        except Exception as e:
            logger.error(f"    ✗ Error searching MusicBrainz: {e}")
            return None

    def search_works_multi(self, title, limit=5):
        """
        Search MusicBrainz for works by title, returning multiple results.

        Unlike search_musicbrainz_work(), this returns all matching results
        (not just exact matches) so the user can select the correct one.

        Args:
            title: Song title to search for
            limit: Maximum number of results to return (default 5)

        Returns:
            List of dicts with keys: id, title, composers, score, type, musicbrainz_url
        """
        self.last_made_api_call = True
        self.rate_limit()

        # Normalize apostrophes - MusicBrainz typically uses curly apostrophe (')
        # Convert straight apostrophe to curly for better matching
        normalized_title = title.replace("'", "'")

        # Search with the title as a phrase
        query = f'work:"{normalized_title}"'

        logger.debug(f"Searching MusicBrainz works (multi): {query}")

        try:
            response = self.session.get(
                'https://musicbrainz.org/ws/2/work/',
                params={
                    'query': query,
                    'fmt': 'json',
                    'limit': limit
                },
                timeout=10
            )
            response.raise_for_status()

            data = response.json()
            works = data.get('works', [])

            # If no results with quoted search, try unquoted
            if not works:
                logger.debug("No results with quoted search, trying unquoted...")
                self.rate_limit()

                response = self.session.get(
                    'https://musicbrainz.org/ws/2/work/',
                    params={
                        'query': normalized_title,
                        'fmt': 'json',
                        'limit': limit
                    },
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()
                works = data.get('works', [])

            if not works:
                logger.debug("No MusicBrainz works found")
                return []

            # Transform results into our format
            results = []
            for work in works:
                work_id = work.get('id')
                work_title = work.get('title', '')
                work_type = work.get('type')
                score = work.get('score')

                # Extract composers from artist-relation-list if present
                composers = []
                if 'relations' in work:
                    for rel in work['relations']:
                        if rel.get('type') in ['composer', 'writer', 'lyricist']:
                            artist = rel.get('artist', {})
                            name = artist.get('name')
                            if name and name not in composers:
                                composers.append(name)

                results.append({
                    'id': work_id,
                    'title': work_title,
                    'composers': composers if composers else None,
                    'score': score,
                    'type': work_type,
                    'musicbrainz_url': f'https://musicbrainz.org/work/{work_id}'
                })

            logger.debug(f"Found {len(results)} MusicBrainz works")
            return results

        except requests.exceptions.Timeout:
            logger.warning("MusicBrainz search timed out")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"MusicBrainz search failed: {e}")
            return []
        except Exception as e:
            logger.error(f"Error searching MusicBrainz: {e}")
            return []

    def _escape_lucene_query(self, text):
        """
        Escape special characters for Lucene query syntax
        
        Args:
            text: Text to escape
            
        Returns:
            Escaped text safe for Lucene queries
        """
        # Lucene special characters that need escaping
        special_chars = ['\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':']
        
        escaped = text
        for char in special_chars:
            escaped = escaped.replace(char, f'\\{char}')
        
        return escaped
    
    def search_musicbrainz_artist(self, artist_name):
        """
        Search MusicBrainz for an artist
        
        Args:
            artist_name: Name to search for
            
        Returns:
            List of matching artist dicts with 'id', 'name', 'score', etc.
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_artist_search_cache_path(artist_name)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached artist search result (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data', [])
        
        # Perform search
        self.last_made_api_call = True
        self.rate_limit()
        
        try:
            url = "https://musicbrainz.org/ws/2/artist/"
            # Escape special Lucene characters in the artist name
            escaped_name = self._escape_lucene_query(artist_name)
            params = {
                'query': f'artist:"{escaped_name}"',
                'fmt': 'json',
                'limit': 5
            }
            
            logger.debug(f"Searching MusicBrainz for artist: {artist_name}")
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.debug(f"MusicBrainz search failed (status {response.status_code})")
                return []
            
            data = response.json()
            artists = data.get('artists', [])
            
            # Cache the results
            self._save_to_cache(cache_path, artists)
            
            return artists
            
        except Exception as e:
            logger.error(f"Error searching MusicBrainz for {artist_name}: {e}")
            return []
    

    def get_artist_details(self, mb_id, max_retries=3):
        """
        Get artist details from MusicBrainz with retry logic
        
        Args:
            mb_id: MusicBrainz artist ID
            max_retries: Maximum number of retry attempts (default 3)
            
        Returns:
            Artist data dict or None
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_artist_detail_cache_path(mb_id)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached artist details (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Fetch from API with retry logic
        for attempt in range(max_retries):
            try:
                # Exponential backoff: 2s, 4s, 8s...
                if attempt > 0:
                    backoff_time = 2 ** (attempt + 1)
                    logger.warning(f"BACKOFF: MusicBrainz artist fetch retry {attempt + 1}/{max_retries}, "
                                   f"waiting {backoff_time}s before retry (mb_id={mb_id})")
                    time.sleep(backoff_time)
                
                # Rate limiting for first attempt
                if attempt == 0:
                    self.last_made_api_call = True
                    self.rate_limit()
                
                url = f"https://musicbrainz.org/ws/2/artist/{mb_id}"
                params = {
                    'fmt': 'json',
                    'inc': 'recordings+tags'
                }
                
                logger.debug(f"Fetching MusicBrainz artist details: {mb_id}")
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    # Cache the successful result
                    self._save_to_cache(cache_path, data)
                    return data
                elif response.status_code == 404:
                    # Cache the negative result (404 is not transient)
                    self._save_to_cache(cache_path, None)
                    logger.warning(f"Artist not found in MusicBrainz: {mb_id}")
                    return None
                elif response.status_code == 503:
                    # Service unavailable - retry
                    logger.warning(f"MusicBrainz service unavailable (503), will retry...")
                    if attempt < max_retries - 1:
                        continue
                    logger.error("All retry attempts failed (503)")
                    return None
                else:
                    logger.error(f"MusicBrainz API error {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"BACKOFF: Connection error on artist fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - connection error (mb_id={mb_id})")
                return None
            except requests.exceptions.Timeout as e:
                logger.warning(f"BACKOFF: Timeout on artist fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - timeout (mb_id={mb_id})")
                return None
            except Exception as e:
                logger.error(f"Error fetching artist details from MusicBrainz: {e}")
                if attempt < max_retries - 1:
                    logger.warning("BACKOFF: Retrying after unexpected error...")
                    continue
                return None
        
        return None

    def get_work_recordings(self, work_id, max_retries=3):
        """
        Get recordings for a MusicBrainz work with retry logic
        
        Args:
            work_id: MusicBrainz work ID
            max_retries: Maximum number of retry attempts (default 3)
            
        Returns:
            Dict with work data including recording relations, or None if not found
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_work_detail_cache_path(work_id)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached work recordings (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Fetch from API with retry logic
        for attempt in range(max_retries):
            try:
                # Exponential backoff: 2s, 4s, 8s...
                if attempt > 0:
                    backoff_time = 2 ** (attempt + 1)
                    logger.warning(f"BACKOFF: MusicBrainz work fetch retry {attempt + 1}/{max_retries}, "
                                   f"waiting {backoff_time}s before retry (work_id={work_id})")
                    time.sleep(backoff_time)
                
                # Rate limiting for first attempt
                if attempt == 0:
                    self.last_made_api_call = True
                    self.rate_limit()
                
                url = f"https://musicbrainz.org/ws/2/work/{work_id}"
                params = {
                    'inc': 'artist-rels+recording-rels+url-rels',
                    'fmt': 'json'
                }
                
                logger.debug(f"Fetching MusicBrainz work recordings: {work_id}")
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    # Cache the successful result
                    self._save_to_cache(cache_path, data)
                    return data
                elif response.status_code == 404:
                    # Cache the negative result (404 is not transient)
                    self._save_to_cache(cache_path, None)
                    logger.warning(f"Work not found in MusicBrainz: {work_id}")
                    return None
                elif response.status_code == 503:
                    # Service unavailable - retry
                    logger.warning(f"MusicBrainz service unavailable (503), will retry...")
                    if attempt < max_retries - 1:
                        continue
                    logger.error("All retry attempts failed (503)")
                    return None
                else:
                    logger.error(f"MusicBrainz API error {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"BACKOFF: Connection error on work fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - connection error (work_id={work_id})")
                return None
            except requests.exceptions.Timeout as e:
                logger.warning(f"BACKOFF: Timeout on work fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - timeout (work_id={work_id})")
                return None
            except Exception as e:
                logger.error(f"Error fetching work recordings from MusicBrainz: {e}")
                if attempt < max_retries - 1:
                    logger.warning("BACKOFF: Retrying after unexpected error...")
                    continue
                return None
        
        return None
        
    def get_recording_details(self, recording_id, max_retries=3):
        """
        Get detailed information about a MusicBrainz recording with retry logic
        
        Args:
            recording_id: MusicBrainz recording ID
            max_retries: Maximum number of retry attempts (default 3)
            
        Returns:
            Dict with recording details, or None if not found
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_recording_detail_cache_path(recording_id)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached recording details (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Fetch from API with retry logic
        for attempt in range(max_retries):
            try:
                # Exponential backoff: 2s, 4s, 8s...
                if attempt > 0:
                    backoff_time = 2 ** (attempt + 1)
                    logger.warning(f"BACKOFF: MusicBrainz recording fetch retry {attempt + 1}/{max_retries}, "
                                   f"waiting {backoff_time}s before retry (recording_id={recording_id})")
                    time.sleep(backoff_time)
                
                # Rate limiting for first attempt
                if attempt == 0:
                    self.last_made_api_call = True
                    self.rate_limit()
                
                url = f"https://musicbrainz.org/ws/2/recording/{recording_id}"
                params = {
                    'inc': 'releases+artist-credits+artist-rels+isrcs',
                    'fmt': 'json'
                }
                
                logger.debug(f"Fetching MusicBrainz recording details: {recording_id}")
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    # Cache the successful result
                    self._save_to_cache(cache_path, data)
                    return data
                elif response.status_code == 404:
                    # Cache the negative result (404 is not transient)
                    self._save_to_cache(cache_path, None)
                    logger.warning(f"Recording not found in MusicBrainz: {recording_id}")
                    return None
                elif response.status_code == 503:
                    # Service unavailable - retry
                    logger.warning(f"MusicBrainz service unavailable (503), will retry...")
                    if attempt < max_retries - 1:
                        continue
                    logger.error("All retry attempts failed (503)")
                    return None
                else:
                    logger.error(f"MusicBrainz API error {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"BACKOFF: Connection error on recording fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - connection error (recording_id={recording_id})")
                return None
            except requests.exceptions.Timeout as e:
                logger.warning(f"BACKOFF: Timeout on recording fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - timeout (recording_id={recording_id})")
                return None
            except Exception as e:
                logger.error(f"Error fetching recording details from MusicBrainz: {e}")
                if attempt < max_retries - 1:
                    logger.warning("BACKOFF: Retrying after unexpected error...")
                    continue
                return None
        
        return None
    
        
    def get_release_details(self, release_id, max_retries=3):
        """
        Get detailed information about a MusicBrainz release
        
        Args:
            release_id: MusicBrainz release ID
            max_retries: Maximum number of retry attempts (default 3)
            
        Returns:
            Dict with release details, or None if not found
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_release_detail_cache_path(release_id)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached release details (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Fetch from API with retry logic
        for attempt in range(max_retries):
            try:
                # Exponential backoff: 2s, 4s, 8s...
                if attempt > 0:
                    backoff_time = 2 ** (attempt + 1)
                    logger.warning(f"BACKOFF: MusicBrainz release fetch retry {attempt + 1}/{max_retries}, "
                                   f"waiting {backoff_time}s before retry (release_id={release_id})")
                    time.sleep(backoff_time)
                
                # Rate limiting for first attempt
                if attempt == 0:
                    self.last_made_api_call = True
                    self.rate_limit()
                
                url = f"https://musicbrainz.org/ws/2/release/{release_id}"
                params = {
                    'inc': 'artist-credits+recordings+artist-rels',
                    'fmt': 'json'
                }

                logger.info(f"Fetching MusicBrainz release details: {release_id}")

                # Use tuple timeout: (connect_timeout, read_timeout)
                # This is more robust than a single timeout for network issues
                response = self.session.get(url, params=params, timeout=(10, 30))
                
                if response.status_code == 200:
                    data = response.json()
                    # Cache the successful result
                    self._save_to_cache(cache_path, data)
                    return data
                elif response.status_code == 404:
                    # Cache the negative result (404 is not transient)
                    self._save_to_cache(cache_path, None)
                    logger.warning(f"Release not found in MusicBrainz: {release_id}")
                    return None
                elif response.status_code == 503:
                    # Service unavailable - retry
                    logger.warning(f"MusicBrainz service unavailable (503), will retry...")
                    if attempt < max_retries - 1:
                        continue
                    logger.error("All retry attempts failed (503)")
                    return None
                elif response.status_code == 429:
                    # Rate limited - use longer backoff
                    logger.warning(f"BACKOFF: MusicBrainz rate limit (429), will retry with longer delay...")
                    if attempt < max_retries - 1:
                        time.sleep(5)  # Extra delay for rate limiting
                        continue
                    logger.error("All retry attempts failed (429 rate limit)")
                    return None
                else:
                    logger.error(f"MusicBrainz API error {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"BACKOFF: Connection error on release fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - connection error (release_id={release_id})")
                return None
            except requests.exceptions.Timeout as e:
                logger.warning(f"BACKOFF: Timeout on release fetch (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error(f"All retry attempts failed - timeout (release_id={release_id})")
                return None
            except Exception as e:
                logger.error(f"Error fetching release details from MusicBrainz: {e}")
                if attempt < max_retries - 1:
                    logger.warning(f"BACKOFF: Retrying after unexpected error...")
                    continue
                return None
        
        return None
    
    def clear_cache(self, search_only=False):
        """
        Clear the MusicBrainz cache
        
        Args:
            search_only: If True, only clear search cache (not artist details)
        """
        import shutil
        
        if search_only:
            if self.search_cache_dir.exists():
                shutil.rmtree(self.search_cache_dir)
                self.search_cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Cleared MusicBrainz search cache")
        else:
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)
                self.search_cache_dir.mkdir(parents=True, exist_ok=True)
                self.artist_cache_dir.mkdir(parents=True, exist_ok=True)
                self.work_cache_dir.mkdir(parents=True, exist_ok=True)
                self.recording_cache_dir.mkdir(parents=True, exist_ok=True)
                self.release_cache_dir.mkdir(parents=True, exist_ok=True)
                self.wikidata_cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Cleared all MusicBrainz cache")
    
    def get_wikipedia_from_wikidata(self, wikidata_id):
        """
        Get English Wikipedia URL from a Wikidata ID
        
        Args:
            wikidata_id: Wikidata ID (e.g., 'Q12345')
            
        Returns:
            Wikipedia URL string, or None if not found
        """
        # Check cache first (unless force_refresh is enabled)
        cache_path = self._get_wikidata_cache_path(wikidata_id)
        if not self.force_refresh:
            cached = self._load_from_cache(cache_path)
            if cached:
                logger.debug(f"  Using cached Wikidata lookup (cached: {cached['cached_at'][:10]})")
                self.last_made_api_call = False
                return cached.get('data')
        
        # Fetch from Wikidata API
        # Note: Wikidata has more lenient rate limits than MusicBrainz
        # We'll still rate limit but at 0.5 seconds instead of 1 second
        self.last_made_api_call = True
        
        # Apply lighter rate limiting for Wikidata (0.5 seconds)
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < 0.5:
            time.sleep(0.5 - time_since_last)
        self.last_request_time = time.time()
        
        try:
            url = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbgetentities',
                'ids': wikidata_id,
                'props': 'sitelinks',
                'format': 'json'
            }
            
            logger.debug(f"Fetching Wikipedia URL from Wikidata: {wikidata_id}")
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.debug(f"Wikidata API returned status {response.status_code}")
                self._save_to_cache(cache_path, None)
                return None
            
            data = response.json()
            
            # Extract English Wikipedia sitelink
            entities = data.get('entities', {})
            entity = entities.get(wikidata_id, {})
            sitelinks = entity.get('sitelinks', {})
            enwiki = sitelinks.get('enwiki', {})
            
            if not enwiki:
                logger.debug(f"No English Wikipedia link found for Wikidata ID {wikidata_id}")
                self._save_to_cache(cache_path, None)
                return None
            
            # Construct Wikipedia URL from title
            title = enwiki.get('title')
            if title:
                # URL encode the title (spaces become underscores in Wikipedia URLs)
                import urllib.parse
                encoded_title = urllib.parse.quote(title.replace(' ', '_'))
                wikipedia_url = f"https://en.wikipedia.org/wiki/{encoded_title}"
                
                # Cache the result
                self._save_to_cache(cache_path, wikipedia_url)
                
                logger.debug(f"Found Wikipedia URL from Wikidata: {wikipedia_url}")
                return wikipedia_url
            
            self._save_to_cache(cache_path, None)
            return None
            
        except Exception as e:
            logger.error(f"Error fetching Wikipedia URL from Wikidata: {e}")
            return None
