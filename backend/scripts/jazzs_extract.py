#!/usr/bin/env python3
"""
Extract Recording Recommendations from JazzStandards.com
Fetches JazzStandards.com pages for songs and extracts "Recommendations for this tune",
storing them in the song_authority_recommendations table with caching support.
"""

import sys
import argparse
import html
import logging
import json
import os
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup

from db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/extract_jazzstandards_recs.log')
    ]
)
logger = logging.getLogger(__name__)

# Cache configuration
# Cache is peer to backend directory: ApproachNote/cache/jazzstandards
CACHE_DIR = Path(__file__).parent.parent.parent / 'cache' / 'jazzstandards'
ITUNES_CACHE_DIR = Path(__file__).parent.parent.parent / 'cache' / 'itunes'
CACHE_DAYS = 30  # Default cache expiration


class JazzStandardsRecommendationExtractor:
    """Extracts and stores recording recommendations from JazzStandards.com"""
    
    def __init__(self, dry_run: bool = False, force_refresh: bool = False, cache_days: int = CACHE_DAYS):
        self.dry_run = dry_run
        self.force_refresh = force_refresh
        self.cache_days = cache_days
        self.existing_rec_counts = {}  # Pre-fetched recommendation counts by song_id
        self.stats = {
            'songs_processed': 0,
            'pages_fetched': 0,
            'cache_hits': 0,
            'recommendations_found': 0,
            'recommendations_stored': 0,
            'already_stored': 0,
            'itunes_api_calls': 0,
            'itunes_enriched': 0,
            'errors': 0
        }
        
        # Setup cache directory
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        ITUNES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Setup session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)'
        })
    
    def get_cache_path(self, url: str) -> Path:
        """Generate cache file path for a URL"""
        # Create a safe filename from the URL
        safe_name = re.sub(r'[^\w\-]', '_', url)
        # Limit filename length
        if len(safe_name) > 200:
            safe_name = safe_name[:200]
        return CACHE_DIR / f"{safe_name}.json"
    
    def get_cached_page(self, url: str) -> Optional[str]:
        """Get cached HTML content if available and not expired. Returns HTML string."""
        if self.force_refresh:
            return None
        
        cache_path = self.get_cache_path(url)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            
            # Check expiration
            cached_at = datetime.fromisoformat(cached_data['cached_at'])
            expiration = cached_at + timedelta(days=self.cache_days)
            
            if datetime.now() < expiration:
                logger.debug(f"  Cache HIT: {url}")
                self.stats['cache_hits'] += 1
                return cached_data['content']  # Return just the HTML string
            else:
                logger.debug(f"  Cache EXPIRED: {url}")
                return None
                
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.debug(f"  Cache error: {e}")
            return None
    
    def cache_page(self, url: str, content: str):
        """Cache raw HTML page content only. Always re-parse when reading from cache."""
        cache_path = self.get_cache_path(url)
        
        cache_data = {
            'url': url,
            'content': content,
            'cached_at': datetime.now().isoformat()
        }
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2)
            logger.debug(f"  Cached HTML: {url}")
        except Exception as e:
            logger.warning(f"Failed to cache page: {e}")
    
    def fetch_page(self, url: str) -> Optional[str]:
        """Fetch page content from URL"""
        logger.debug(f"  Fetching: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            self.stats['pages_fetched'] += 1
            return response.text
            
        except requests.RequestException as e:
            logger.error(f"  Failed to fetch {url}: {e}")
            self.stats['errors'] += 1
            return None
    
    def extract_recommendations(self, html_content: str) -> List[Dict]:
        """
        Extract recommendations from JazzStandards.com page HTML.
        Looks for "Recommendations for this tune" section and parses recommendations.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        recommendations = []

        # Extract JavaScript music data first (contains full, non-truncated names)
        js_music_data = self.extract_js_music_data(html_content)

        # Look for the recommendations section
        # The exact HTML structure may vary, so we'll look for common patterns

        # Pattern 1: Look for text containing "recommendation"
        # Find ALL tags with "recommendations for this tune", then pick the most specific one
        all_rec_tags = soup.find_all(lambda tag: 
                                      tag.get_text(strip=True) and
                                      'recommendations for this tune' in tag.get_text(strip=True).lower())
        
        # Pick the smallest/most specific tag (excluding huge containers like html, body)
        rec_heading = None
        min_length = float('inf')
        
        for tag in all_rec_tags:
            text_len = len(tag.get_text(strip=True))
            # Exclude very large containers (probably html, body, or main content divs)
            # The actual heading should be < 500 chars
            if text_len < 500 and text_len < min_length:
                rec_heading = tag
                min_length = text_len
        
        # Always look for JSRecommendationInset tables (even without heading)
        # Some pages use iTunes panel format without standard heading
        rec_tables = soup.find_all('table', class_='JSRecommendationInset')
        
        if rec_heading:
            logger.debug("  Pattern 1: Found recommendations heading")
            logger.debug(f"    Heading text: '{rec_heading.get_text(strip=True)[:100]}'")
            logger.debug(f"    Heading tag: <{rec_heading.name}>")
        
        if rec_tables:
            logger.debug(f"  Found {len(rec_tables)} JSRecommendationInset tables")
            
            # Process each table - check if it's iTunes panel format or standard format
            # iTunes panel format: Table with JSiTunesPanelColumn cells
            # Standard format: Each table is one recommendation
            
            for table_idx, table in enumerate(rec_tables, 1):
                itunes_columns = table.find_all('td', class_='JSiTunesPanelColumn')
                
                if len(itunes_columns) > 0:
                    # This is an iTunes panel format table
                    logger.debug(f"  Table {table_idx}: iTunes panel format ({len(itunes_columns)} columns)")
                    itunes_recs = self.parse_itunes_panel(table, js_music_data)
                    if itunes_recs:
                        recommendations.extend(itunes_recs)
                        logger.debug(f"    ✓ Extracted {len(itunes_recs)} recommendations from iTunes panel")
                    else:
                        logger.debug("    iTunes panel parsing failed")
                else:
                    # This is a standard recommendation table
                    logger.debug(f"  Table {table_idx}: Standard format")
                    # Parse each recommendation table
                    i = len(recommendations) + 1
                    # Get the first <td> which contains all the metadata
                    first_td = table.find('td')
                    if not first_td:
                        logger.debug(f"    Rec #{i}: No <td> found, skipping")
                        continue
                    
                    # Parse structured HTML instead of text blob
                    rec = self.parse_recommendation_html(first_td, table)
                    
                    if rec and rec['artist_name']:
                        # Get full text for recommendation_text field
                        full_text = first_td.get_text(strip=True)
                        # Add description if available
                        desc_row = table.find('tr', class_='JSBody')
                        if desc_row:
                            desc_td = desc_row.find('td')
                            if desc_td:
                                desc_text = desc_td.get_text(strip=True)
                                if len(desc_text) > 200:
                                    desc_text = desc_text[:200]
                                full_text = f"{full_text} {desc_text}"
                        
                        rec['recommendation_text'] = full_text
                        recommendations.append(rec)
                        
                        logger.debug(f"    Parsing recommendation #{i}:")
                        logger.debug(f"      ✓ Artist: {rec['artist_name']}")
                        logger.debug(f"      ✓ Album: {rec['album_title'] or '(none)'}")
                        logger.debug(f"      ✓ Year: {rec['recording_year'] or '(none)'}")
                        if rec.get('itunes_album_id'):
                            logger.debug(f"      ✓ iTunes Album ID: {rec['itunes_album_id']}")
                        if rec.get('itunes_track_id'):
                            logger.debug(f"      ✓ iTunes Track ID: {rec['itunes_track_id']}")
                    else:
                        logger.debug(f"    Rec #{i}: Could not parse")
            
            logger.debug(f"  Pattern 1 result: {len(recommendations)} recommendations extracted")
        
        # Pattern 2: Look for links to recordings/albums in specific sections
        # (This is a backup if Pattern 1 doesn't work)
        if not recommendations:
            logger.debug("  Pattern 1 found no recommendations, trying Pattern 2...")
            # Try to find any structured data about recordings
            album_links = soup.find_all('a', href=lambda x: x and ('/album' in x or '/recording' in x))
            logger.debug(f"    Found {len(album_links)} album/recording links on page")
            
            for i, link in enumerate(album_links[:20], 1):  # Limit to first 20 to avoid noise
                text = link.get_text(strip=True)
                parent_text = link.parent.get_text(strip=True) if link.parent else text
                
                logger.debug(f"    Parsing link #{i}: {parent_text[:100]}...")
                
                rec = self.parse_recommendation_text(parent_text)
                if rec:
                    recommendations.append(rec)
                    logger.debug(f"      ✓ Extracted: {rec['artist_name']}" + 
                               (f" - {rec['album_title']}" if rec['album_title'] else "") +
                               (f" ({rec['recording_year']})" if rec['recording_year'] else ""))
            
            logger.debug(f"  Pattern 2 result: {len(recommendations)} recommendations from {min(20, len(album_links))} links")
        
        # Deduplicate recommendations (same track may appear in multiple sections)
        if len(recommendations) > 1:
            deduplicated = self.deduplicate_recommendations(recommendations)
            if len(deduplicated) < len(recommendations):
                logger.debug(f"  Removed {len(recommendations) - len(deduplicated)} duplicate(s)")
                recommendations = deduplicated
        
        logger.debug(f"  Total extracted: {len(recommendations)} recommendations")
        return recommendations
    
    def _normalize_for_dedup(self, text: str) -> str:
        """Normalize text for deduplication comparison."""
        if not text:
            return ''
        # Lowercase
        text = text.lower().strip()
        # Normalize & to and
        text = text.replace('&', 'and')
        # Remove punctuation except spaces
        text = re.sub(r'[^\w\s]', '', text)
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text)
        return text

    def deduplicate_recommendations(self, recommendations: List[Dict]) -> List[Dict]:
        """
        Deduplicate recommendations using multiple matching strategies.

        A recommendation is considered a duplicate if it matches on ANY of:
        1. iTunes track ID (most specific)
        2. iTunes album ID + artist name (same album by same artist)
        3. Artist name + album title + year (for non-iTunes entries)
        4. Normalized album title (handles '&' vs 'and', punctuation differences)

        When duplicates are found, prefer the one with more complete metadata.
        """
        seen_keys = {}  # Maps keys to list index in deduplicated
        deduplicated = []

        for rec in recommendations:
            # Generate all possible matching keys for this recommendation
            matching_keys = []

            # Key 1: iTunes track ID (most specific)
            if rec.get('itunes_track_id'):
                matching_keys.append(f"track_{rec['itunes_track_id']}")

            # Key 2: iTunes album ID + artist (same album by same artist)
            if rec.get('itunes_album_id') and rec.get('artist_name'):
                key = f"album_{rec['itunes_album_id']}_{rec['artist_name'].lower().strip()}"
                matching_keys.append(key)

            # Key 3: Artist + album + year (for matching non-iTunes entries)
            if rec.get('artist_name') and rec.get('album_title') and rec.get('recording_year'):
                key = f"artist_album_year_{rec['artist_name'].lower().strip()}_{rec['album_title'].lower().strip()}_{rec['recording_year']}"
                matching_keys.append(key)

            # Key 4: Artist + album (weaker match, no year)
            if rec.get('artist_name') and rec.get('album_title'):
                key = f"artist_album_{rec['artist_name'].lower().strip()}_{rec['album_title'].lower().strip()}"
                matching_keys.append(key)

            # Key 5: Normalized album title only (catches "The Great Kai & J.J." vs "The Great Kai and J.J")
            # This is aggressive but helps with same-album different-formatting duplicates
            if rec.get('album_title'):
                norm_album = self._normalize_for_dedup(rec['album_title'])
                if len(norm_album) >= 10:  # Only for reasonably specific album titles
                    matching_keys.append(f"norm_album_{norm_album}")
            
            logger.debug(f"    Checking: {rec.get('artist_name')} - {rec.get('album_title')}")
            logger.debug(f"      Keys: {matching_keys}")
            
            # Check if any of these keys match an existing recommendation
            matched_key = None
            for key in matching_keys:
                if key in seen_keys:
                    matched_key = key
                    break
            
            if matched_key:
                # Found a duplicate
                existing_idx = seen_keys[matched_key]
                existing_rec = deduplicated[existing_idx]
                
                # Prefer the one with more metadata
                existing_score = self._recommendation_completeness_score(existing_rec)
                new_score = self._recommendation_completeness_score(rec)
                
                logger.debug(f"      Duplicate found (key: {matched_key})")
                logger.debug(f"        Existing score: {existing_score}, New score: {new_score}")
                
                if new_score > existing_score:
                    # Replace with the more complete version
                    deduplicated[existing_idx] = rec
                    # Update all keys to point to this index
                    for key in matching_keys:
                        seen_keys[key] = existing_idx
                    logger.debug(f"        → Replaced with better metadata")
                else:
                    # Keep existing, skip this one
                    logger.debug(f"        → Keeping existing")
            else:
                # New recommendation - add it
                idx = len(deduplicated)
                deduplicated.append(rec)
                # Register all keys for this recommendation
                for key in matching_keys:
                    seen_keys[key] = idx
                logger.debug(f"      → Added as new recommendation")
        
        return deduplicated
    
    def _recommendation_completeness_score(self, rec: Dict) -> int:
        """Calculate a score based on how complete the recommendation metadata is"""
        score = 0
        if rec.get('artist_name'):
            score += 1
        if rec.get('album_title'):
            score += 1
        if rec.get('recording_year'):
            score += 1
        if rec.get('itunes_album_id'):
            score += 2
        if rec.get('itunes_track_id'):
            score += 2
        return score
    
    def extract_itunes_ids(self, table_elem) -> Dict[str, Optional[int]]:
        """
        Extract iTunes album and track IDs from links in the recommendation table.
        
        iTunes URL pattern: https://itunes.apple.com/us/album/id123456789?i=987654321
        - id123456789 = album ID
        - ?i=987654321 = track ID (optional)
        """
        result = {
            'itunes_album_id': None,
            'itunes_track_id': None
        }
        
        # Find all iTunes links
        itunes_links = table_elem.find_all('a', href=re.compile(r'itunes\.apple\.com'))
        
        for link in itunes_links:
            href = link.get('href', '')
            
            # Extract album ID: /album/id123456789
            album_match = re.search(r'/album/id(\d+)', href)
            if album_match and not result['itunes_album_id']:
                result['itunes_album_id'] = int(album_match.group(1))
            
            # Extract track ID: ?i=123456789 or &i=123456789
            track_match = re.search(r'[?&]i=(\d+)', href)
            if track_match and not result['itunes_track_id']:
                result['itunes_track_id'] = int(track_match.group(1))
        
        return result
    
    def fetch_itunes_metadata(self, itunes_id: int, lookup_type: str = 'album') -> Optional[Dict]:
        """
        Fetch metadata from iTunes API.
        
        Args:
            itunes_id: The iTunes album or track ID
            lookup_type: 'album' or 'track'
        
        Returns:
            Dict with artistName, collectionName, releaseDate, etc.
        """
        cache_key = f"itunes_{lookup_type}_{itunes_id}"
        cache_path = ITUNES_CACHE_DIR / f"{cache_key}.json"
        
        # Check cache
        if not self.force_refresh and cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                
                cached_at = datetime.fromisoformat(cached_data['cached_at'])
                expiration = cached_at + timedelta(days=self.cache_days)
                
                if datetime.now() < expiration:
                    logger.debug(f"    iTunes cache HIT: {itunes_id}")
                    return cached_data.get('metadata')
            except Exception as e:
                logger.debug(f"    iTunes cache error: {e}")
        
        # Fetch from iTunes API
        try:
            url = f"https://itunes.apple.com/lookup?id={itunes_id}&country=US"
            logger.debug(f"    Fetching iTunes metadata: {url}")
            
            self.stats['itunes_api_calls'] += 1
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('resultCount', 0) > 0:
                metadata = data['results'][0]
                
                # Cache the response
                cache_data = {
                    'itunes_id': itunes_id,
                    'lookup_type': lookup_type,
                    'metadata': metadata,
                    'cached_at': datetime.now().isoformat()
                }
                
                with open(cache_path, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2)
                
                logger.debug(f"    ✓ iTunes: {metadata.get('artistName')} - {metadata.get('collectionName')}")
                return metadata
            else:
                logger.debug(f"    iTunes API returned no results for ID: {itunes_id}")
                return None
                
        except Exception as e:
            logger.warning(f"Error fetching iTunes metadata: {e}")
            return None

    def search_itunes(self, artist_name: str, song_title: str) -> Optional[Dict]:
        """
        Search iTunes by artist name and song title.
        Used as fallback when ID lookups fail (stale IDs).

        Returns:
            Dict with track metadata including collectionName, releaseDate, etc.
        """
        try:
            # Build search query - combine artist and song title
            # Use urllib.parse.quote_plus to properly encode special chars like &
            from urllib.parse import quote_plus
            query = quote_plus(f"{artist_name} {song_title}")
            url = f"https://itunes.apple.com/search?term={query}&entity=song&limit=10&country=US"
            logger.debug(f"        Searching iTunes: {artist_name} + {song_title}")

            self.stats['itunes_api_calls'] += 1

            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('resultCount', 0) > 0:
                # Collect all candidate matches with artist name matching
                artist_lower = artist_name.lower()
                candidates = []
                for result in data['results']:
                    result_artist = result.get('artistName', '').lower()
                    # Check if artist names match reasonably well
                    if artist_lower in result_artist or result_artist in artist_lower:
                        candidates.append(result)

                # If no artist matches, use all results as candidates
                if not candidates:
                    candidates = data['results']

                # If we have multiple candidates, prefer one with best DB match
                if len(candidates) > 1:
                    best_candidate = None
                    best_score = 0
                    for candidate in candidates:
                        album_name = candidate.get('collectionName', '')
                        itunes_artist = candidate.get('artistName', '')
                        if album_name:
                            # Use iTunes result artist for better matching
                            score = self._album_db_match_score(album_name, itunes_artist)
                            if score > best_score:
                                best_score = score
                                best_candidate = candidate

                    if best_candidate and best_score > 0:
                        logger.debug(f"        ✓ Search found (DB match, score={best_score}): {best_candidate.get('artistName')} - {best_candidate.get('collectionName')}")
                        return best_candidate

                # Return first candidate
                result = candidates[0]
                logger.debug(f"        ✓ Search found: {result.get('artistName')} - {result.get('collectionName')}")
                return result
            else:
                logger.debug(f"        iTunes search returned no results")
                return None

        except Exception as e:
            logger.warning(f"Error searching iTunes: {e}")
            return None

    def _album_db_match_score(self, album_name: str, artist_name: str) -> int:
        """
        Score how well an album matches releases in our DB for this song.
        Returns a score based on keyword overlap (higher is better).
        """
        if not hasattr(self, 'current_song_title'):
            return 0

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    song_title = self.current_song_title.lower()

                    # Get all releases linked to recordings of this song
                    cur.execute("""
                        SELECT rel.title, rel.artist_credit
                        FROM releases rel
                        JOIN recording_releases rr ON rel.id = rr.release_id
                        JOIN recordings r ON rr.recording_id = r.id
                        JOIN songs s ON r.song_id = s.id
                        WHERE LOWER(s.title) = %s
                    """, (song_title,))
                    releases = cur.fetchall()

                    # Compare iTunes album with DB releases using keyword matching
                    album_words = set(self._extract_keywords(album_name))
                    artist_words = set(self._extract_keywords(artist_name))

                    best_score = 0
                    best_release = None

                    for rel in releases:
                        db_title_words = set(self._extract_keywords(rel['title']))
                        db_artist_words = set(self._extract_keywords(rel['artist_credit'] or ''))

                        # Check for significant word overlap
                        title_overlap = album_words & db_title_words
                        artist_overlap = artist_words & db_artist_words

                        # Score = title matches * 3 + artist matches
                        # Bonus: +5 if ALL db title words appear in iTunes album (full containment)
                        if len(artist_overlap) >= 1:
                            score = len(title_overlap) * 3 + len(artist_overlap)
                            # Bonus for full containment
                            if db_title_words and db_title_words <= album_words:
                                score += 5
                            if score > best_score:
                                best_score = score
                                best_release = rel['title']

                    if best_score > 0:
                        logger.debug(f"        DB match score={best_score} for '{best_release}'")

                    return best_score
        except Exception as e:
            logger.debug(f"        DB check error: {e}")
            return 0

    def _extract_keywords(self, text: str) -> list:
        """Extract significant keywords from text (skip common words)."""
        if not text:
            return []
        # Common words to skip - note: 'live' is NOT skipped as it's meaningful for album titles
        stop_words = {'the', 'a', 'an', 'at', 'in', 'on', 'of', 'to', 'and', 'for', 'with', 'by', 'feat', 'featuring'}
        words = text.lower().split()
        # Remove punctuation and filter stop words, keep words 3+ chars
        import re
        return [re.sub(r'[^\w]', '', w) for w in words
                if len(w) >= 3 and re.sub(r'[^\w]', '', w).lower() not in stop_words]

    def extract_js_music_data(self, html_content: str) -> Dict[str, Dict]:
        """
        Extract music data from JavaScript arrays embedded in the page.

        The page contains JS data in this format:
        "mp3_url##" +
        "ogg_url##" +
        "Song Title ##" +
        "Album Title##" +
        "Artist Name ##" +
        "image_url##" +
        "amazon_url##" +
        ...###

        Returns:
            Dict mapping artist names (lowercased) to {'artist': str, 'album': str}
        """
        js_data = {}

        try:
            # Find all record blocks (ending with ###)
            # Pattern: starts after ### or beginning, ends with ###
            # Each field is "value##" concatenated with +

            # Extract all ## separated values between string quotes
            # Look for patterns like: "Song##" + "Album##" + "Artist ##"
            pattern = r'"([^"]*?)##"\s*\+\s*"([^"]*?)##"\s*\+\s*"([^"]*?)\s*##"'

            # Find sequences that look like: Song ## Album ## Artist ##
            # We need to find: OGG_URL## + Song## + Album## + Artist##
            ogg_pattern = r'"https://www\.jazzstandards\.com/games/OGGFiles[^"]+\.ogg##"\s*\+\s*"([^"]+?)\s*##"\s*\+\s*"([^"]+?)##"\s*\+\s*"([^"]+?)\s*##"'

            matches = re.findall(ogg_pattern, html_content)

            for match in matches:
                song_title, album_title, artist_name = match
                # Unescape HTML entities (e.g., &amp; -> &, &#39; -> ')
                song_title = html.unescape(song_title.strip())
                album_title = html.unescape(album_title.strip())
                artist_name = html.unescape(artist_name.strip())

                if artist_name and album_title:
                    # Use lowercase artist for matching, but store original
                    key = artist_name.lower()
                    js_data[key] = {
                        'artist': artist_name,
                        'album': album_title,
                        'song': song_title
                    }
                    logger.debug(f"    JS data: {artist_name} - {album_title}")

            if js_data:
                logger.debug(f"  Extracted {len(js_data)} records from JavaScript data")

        except Exception as e:
            logger.debug(f"  Error extracting JS data: {e}")

        return js_data

    def parse_itunes_panel(self, table_elem, js_music_data: Dict = None) -> List[Dict]:
        """
        Parse iTunes panel format recommendations.

        Structure:
        <table class="JSRecommendationInset">
          <tr class="JSiTunesPanelRow">
            <td class="JSiTunesPanelColumn">
              <a href="https://itunes.apple.com/us/album/id286173678?i=286174089">
                <img alt="A Child Is Born - Benny Carter & Hank Jones" />
              </a>
              <br>Artist Name</br>
            </td>
            <td class="JSiTunesPanelColumn">...</td>
          </tr>
        </table>

        Args:
            table_elem: BeautifulSoup table element
            js_music_data: Optional dict of JavaScript-extracted music data for enrichment
        """
        recommendations = []
        js_music_data = js_music_data or {}

        try:
            # Find all iTunes panel columns
            columns = table_elem.find_all('td', class_='JSiTunesPanelColumn')
            
            logger.debug(f"    Found {len(columns)} iTunes panel columns")
            
            for i, column in enumerate(columns, 1):
                # Extract iTunes link
                link = column.find('a', href=re.compile(r'itunes\.apple\.com'))
                if not link:
                    continue
                
                href = link.get('href', '')
                
                # Extract iTunes IDs
                album_id = None
                track_id = None
                
                album_match = re.search(r'/album/id(\d+)', href)
                if album_match:
                    album_id = int(album_match.group(1))
                
                track_match = re.search(r'[?&]i=(\d+)', href)
                if track_match:
                    track_id = int(track_match.group(1))
                
                # Extract artist name from <br>text</br> or after <br/>
                artist_name = None
                br_tag = column.find('br')
                if br_tag:
                    # Check if text is inside <br></br>
                    if br_tag.string:
                        artist_name = br_tag.string.strip()
                    else:
                        # Check next sibling after <br/>
                        next_text = br_tag.next_sibling
                        if next_text and isinstance(next_text, str):
                            artist_name = next_text.strip()

                if not artist_name or not album_id:
                    logger.debug(f"      Column {i}: Missing artist or album ID")
                    continue

                # Check if artist name is truncated (ends with "...")
                # If so, try to find full name from JS data
                original_artist = artist_name
                if artist_name.endswith('...') and js_music_data:
                    # Try to find a matching artist in JS data
                    truncated_prefix = artist_name[:-3].lower()  # Remove "..."
                    for js_key, js_info in js_music_data.items():
                        if js_key.startswith(truncated_prefix):
                            artist_name = js_info['artist']
                            logger.debug(f"      Fixed truncated artist: '{original_artist}' -> '{artist_name}'")
                            break
                
                logger.debug(f"      Column {i}: {artist_name} (iTunes: {album_id})")
                
                # Fetch album metadata from iTunes API
                album_title = None
                recording_year = None

                itunes_data = self.fetch_itunes_metadata(album_id, lookup_type='album')

                # If album lookup failed but we have a track ID, try looking up the track
                if not itunes_data and track_id:
                    logger.debug(f"        Album lookup failed, trying track ID: {track_id}")
                    itunes_data = self.fetch_itunes_metadata(track_id, lookup_type='track')

                # If both lookups failed, try searching by artist + song title (stale ID fallback)
                if not itunes_data and hasattr(self, 'current_song_title'):
                    itunes_data = self.search_itunes(artist_name, self.current_song_title)

                if itunes_data:
                    # Prefer iTunes artist name over HTML-parsed name
                    # HTML names can be truncated or incorrectly formatted (e.g., "Hendricks & Ross Lambert"
                    # instead of "Lambert, Hendricks & Ross")
                    itunes_artist = itunes_data.get('artistName')
                    if itunes_artist and itunes_artist != artist_name:
                        logger.debug(f"        Using iTunes artist: '{itunes_artist}' (HTML had: '{artist_name}')")
                        artist_name = itunes_artist

                    album_title = itunes_data.get('collectionName')

                    # Extract year from releaseDate
                    if itunes_data.get('releaseDate'):
                        year_match = re.search(r'(\d{4})', itunes_data['releaseDate'])
                        if year_match:
                            recording_year = int(year_match.group(1))

                    self.stats['itunes_enriched'] += 1
                    logger.debug(f"        ✓ Artist (iTunes): {artist_name}")
                    logger.debug(f"        ✓ Album (iTunes): {album_title}")
                    logger.debug(f"        ✓ Year: {recording_year}")

                # If no album title yet, try to get it from JS data
                if not album_title and js_music_data:
                    artist_lower = artist_name.lower()
                    if artist_lower in js_music_data:
                        album_title = js_music_data[artist_lower]['album']
                        logger.debug(f"        ✓ Album (JS data): {album_title}")

                # Build recommendation text
                rec_text = artist_name
                if album_title:
                    rec_text = f"{artist_name} - {album_title}"
                if recording_year:
                    rec_text += f" ({recording_year})"
                
                rec = {
                    'artist_name': artist_name[:255],
                    'album_title': album_title[:255] if album_title else None,
                    'recording_year': recording_year,
                    'itunes_album_id': album_id,
                    'itunes_track_id': track_id,
                    'recommendation_text': rec_text
                }
                
                recommendations.append(rec)
            
        except Exception as e:
            logger.error(f"Error parsing iTunes panel: {e}", exc_info=True)
        
        return recommendations
    
    def parse_recommendation_html(self, td_elem, table_elem) -> Optional[Dict]:
        """
        Parse recommendation from HTML structure.
        
        HTML structure:
        <td>
          <b>
          Artist Name<br/>
          <span style="font-size: smaller;"><i>
          <a>Album Title</a></i><br/>
          Label Catalog<br/>
          Original recording YYYY<br/>  OR  YYYY, Label<br/>
          </span></b>
        </td>
        """
        try:
            # Extract iTunes IDs first
            itunes_ids = self.extract_itunes_ids(table_elem)
            
            # Find the <b> tag containing metadata
            b_tag = td_elem.find('b')
            if not b_tag:
                return None
            
            # Get all text, then split by <br/> tags
            # Artist is before first <br/>
            artist_name = None
            for content in b_tag.contents:
                if content.name == 'br':
                    break
                if isinstance(content, str):
                    text = content.strip()
                    if text:
                        artist_name = text
                        break
            
            if not artist_name:
                return None
            
            # Find album title in <i> or <a> tag
            album_title = None
            i_tag = b_tag.find('i')
            if i_tag:
                a_tag = i_tag.find('a')
                if a_tag:
                    album_title = a_tag.get_text(strip=True)
                else:
                    album_title = i_tag.get_text(strip=True)
            
            # Find year - look for "Original recording YYYY" or "YYYY,"
            recording_year = None
            span_tag = b_tag.find('span')
            if span_tag:
                span_text = span_tag.get_text()
                
                # Try "Original recording YYYY"
                match = re.search(r'Original recording[,\s]+(\d{4})', span_text, re.IGNORECASE)
                if match:
                    recording_year = int(match.group(1))
                else:
                    # Try "YYYY," pattern
                    match = re.search(r'\b(\d{4}),', span_text)
                    if match:
                        recording_year = int(match.group(1))
            
            # If album_title is missing but we have iTunes ID, fetch from iTunes API
            if not album_title and itunes_ids['itunes_album_id']:
                logger.debug(f"    Missing album_title, fetching from iTunes API...")
                itunes_data = self.fetch_itunes_metadata(
                    itunes_ids['itunes_album_id'], 
                    lookup_type='album'
                )
                
                if itunes_data:
                    # Prefer iTunes artist name over HTML-parsed name
                    # HTML names can be truncated or incorrectly formatted
                    itunes_artist = itunes_data.get('artistName')
                    if itunes_artist:
                        if artist_name and itunes_artist != artist_name:
                            logger.debug(f"    Using iTunes artist: '{itunes_artist}' (HTML had: '{artist_name}')")
                        artist_name = itunes_artist

                    album_title = itunes_data.get('collectionName')

                    # Extract year from releaseDate if not already found
                    if not recording_year and itunes_data.get('releaseDate'):
                        release_date = itunes_data['releaseDate']
                        year_match = re.search(r'(\d{4})', release_date)
                        if year_match:
                            recording_year = int(year_match.group(1))

                    # Track successful enrichment
                    self.stats['itunes_enriched'] += 1
            
            return {
                'artist_name': artist_name[:255] if artist_name else None,  # Truncate to DB limit
                'album_title': album_title[:255] if album_title else None,  # Truncate to DB limit
                'recording_year': recording_year,
                'itunes_album_id': itunes_ids['itunes_album_id'],
                'itunes_track_id': itunes_ids['itunes_track_id'],
                'recommendation_text': ''  # Will be filled in by caller
            }
            
        except Exception as e:
            logger.debug(f"      Error parsing HTML: {e}")
            return None
    
    def parse_recommendation_text(self, text: str) -> Optional[Dict]:
        """
        Parse a single recommendation text to extract artist, album, and year.
        
        Examples:
            "Miles Davis - Kind of Blue (1959)"
            "Bill Evans Trio, Portrait in Jazz, 1960"
            "Chet Baker (1953)"
        """
        if not text or len(text) < 5:
            return None
        
        rec = {
            'artist_name': None,
            'album_title': None,
            'recording_year': None,
            'recommendation_text': text
        }
        
        # Extract year (4 digits, typically in parentheses or at the end)
        year_match = re.search(r'\(?(\d{4})\)?', text)
        if year_match:
            try:
                year = int(year_match.group(1))
                if 1900 <= year <= 2030:  # Reasonable year range
                    rec['recording_year'] = year
            except ValueError:
                pass
        
        # Remove year from text for further parsing
        text_without_year = re.sub(r'\(?\d{4}\)?', '', text).strip()
        
        # Try to split on common delimiters
        # Pattern: "Artist - Album" or "Artist, Album"
        if ' - ' in text_without_year:
            parts = text_without_year.split(' - ', 1)
            rec['artist_name'] = parts[0].strip()
            if len(parts) > 1:
                rec['album_title'] = parts[1].strip()
        elif ', ' in text_without_year:
            parts = text_without_year.split(', ')
            if len(parts) >= 2:
                rec['artist_name'] = parts[0].strip()
                rec['album_title'] = parts[1].strip()
        else:
            # Just artist name, possibly
            rec['artist_name'] = text_without_year.strip()
        
        # Clean up any remaining punctuation
        if rec['artist_name']:
            rec['artist_name'] = rec['artist_name'].rstrip(',-:')
        if rec['album_title']:
            rec['album_title'] = rec['album_title'].rstrip(',-:')
        
        # Only return if we have at least an artist name
        if rec['artist_name'] and len(rec['artist_name']) > 2:
            return rec
        
        return None
    
    def get_songs_with_jazzstandards_url(self, name_filter: Optional[str] = None) -> List[Dict]:
        """Fetch songs that have a JazzStandards URL in external_references"""
        if name_filter:
            logger.info(f"Fetching songs matching '{name_filter}'...")
        else:
            logger.info("Fetching songs with JazzStandards URLs...")
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if name_filter:
                        cur.execute("""
                            SELECT 
                                id,
                                title,
                                external_references
                            FROM songs
                            WHERE external_references ? 'jazzstandards'
                              AND title ILIKE %s
                            ORDER BY title
                        """, (f'%{name_filter}%',))
                    else:
                        cur.execute("""
                            SELECT 
                                id,
                                title,
                                external_references
                            FROM songs
                            WHERE external_references ? 'jazzstandards'
                            ORDER BY title
                        """)
                    
                    songs = []
                    for row in cur.fetchall():
                        ext_refs = row['external_references'] or {}
                        js_url = ext_refs.get('jazzstandards')
                        
                        if js_url:
                            songs.append({
                                'id': row['id'],
                                'title': row['title'],
                                'jazzstandards_url': js_url
                            })
                    
                    logger.info(f"✓ Found {len(songs)} songs with JazzStandards URLs")
                    
                    # Pre-fetch existing recommendation counts to avoid per-song DB queries
                    logger.debug("Pre-fetching existing recommendation counts...")
                    cur.execute("""
                        SELECT song_id, COUNT(*) as rec_count
                        FROM song_authority_recommendations
                        WHERE source = 'jazzstandards.com'
                        GROUP BY song_id
                    """)
                    self.existing_rec_counts = {row['song_id']: row['rec_count'] for row in cur.fetchall()}
                    logger.debug(f"  Found existing recommendations for {len(self.existing_rec_counts)} songs")
                    
                    return songs
                    
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            return []
    
    def store_recommendations(self, song_id: str, song_title: str, source_url: str, recommendations: List[Dict]) -> int:
        """Store recommendations in the database"""
        if not recommendations:
            return 0
        
        stored_count = 0
        
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would store {len(recommendations)} recommendations for: {song_title}")
            for rec in recommendations:
                logger.info(f"    - {rec['artist_name']}" + 
                          (f" - {rec['album_title']}" if rec['album_title'] else "") +
                          (f" ({rec['recording_year']})" if rec['recording_year'] else ""))
            return len(recommendations)
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # If force_refresh, delete existing recommendations first
                    existing_count = self.existing_rec_counts.get(song_id, 0)
                    if self.force_refresh and existing_count > 0:
                        cur.execute("""
                            DELETE FROM song_authority_recommendations
                            WHERE song_id = %s
                              AND source = 'jazzstandards.com'
                              AND source_url = %s
                        """, (song_id, source_url))
                        logger.debug(f"  Deleted {existing_count} existing recommendations")
                    
                    # Insert new recommendations
                    for rec in recommendations:
                        cur.execute("""
                            INSERT INTO song_authority_recommendations (
                                song_id,
                                source,
                                source_url,
                                recommendation_text,
                                artist_name,
                                album_title,
                                recording_year,
                                itunes_album_id,
                                itunes_track_id,
                                captured_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """, (
                            song_id,
                            'jazzstandards.com',
                            source_url,
                            rec['recommendation_text'],
                            rec['artist_name'],
                            rec['album_title'],
                            rec['recording_year'],
                            rec.get('itunes_album_id'),
                            rec.get('itunes_track_id')
                        ))
                        stored_count += 1
                    
                    conn.commit()
                    
                    # Update cached count
                    self.existing_rec_counts[song_id] = stored_count
                    
                    logger.info(f"  ✓ Stored {stored_count} recommendations for: {song_title}")
                    
        except Exception as e:
            logger.error(f"Error storing recommendations: {e}", exc_info=True)
            self.stats['errors'] += 1
        
        return stored_count
    
    def process_song(self, song: Dict) -> bool:
        """Process a single song: fetch page, extract recommendations, store"""
        song_id = song['id']
        song_title = song['title']
        js_url = song['jazzstandards_url']

        # Store song_title for use in iTunes search fallback
        self.current_song_title = song_title

        logger.info(f"\nProcessing: {song_title}")
        logger.debug(f"  URL: {js_url}")
        
        # Quick check: do we already have recommendations? (avoids extraction + iTunes calls)
        existing_count = self.existing_rec_counts.get(song_id, 0)
        if existing_count > 0 and not self.force_refresh:
            logger.info(f"  Already have {existing_count} recommendations for: {song_title}")
            self.stats['already_stored'] += existing_count
            # Still count as "found" for stats accuracy
            self.stats['recommendations_found'] += existing_count
            return True
        
        # Check cache first (returns HTML string if cached)
        html_content = self.get_cached_page(js_url)
        
        if not html_content:
            # Fetch page from web
            html_content = self.fetch_page(js_url)
            if not html_content:
                return False
            
            # Cache the raw HTML
            self.cache_page(js_url, html_content)
        
        # Parse HTML and extract recommendations (may call iTunes API)
        recommendations = self.extract_recommendations(html_content)
        
        self.stats['recommendations_found'] += len(recommendations)
        
        # Store recommendations
        if recommendations:
            stored = self.store_recommendations(song_id, song_title, js_url, recommendations)
            self.stats['recommendations_stored'] += stored
        else:
            logger.info(f"  No recommendations found on page")
        
        return True
    
    def run(self, limit: Optional[int] = None, name_filter: Optional[str] = None) -> bool:
        """Main execution method"""
        logger.info("="*80)
        logger.info("EXTRACT JAZZSTANDARDS.COM RECOMMENDATIONS")
        logger.info("="*80)
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"Force refresh: {self.force_refresh}")
        logger.info(f"Cache expiration: {self.cache_days} days")
        if name_filter:
            logger.info(f"Name filter: {name_filter}")
        logger.info("")
        
        # Get songs with JazzStandards URLs
        songs = self.get_songs_with_jazzstandards_url(name_filter=name_filter)
        if not songs:
            logger.info("No songs with JazzStandards URLs found")
            return True
        
        # Apply limit if specified
        if limit:
            songs = songs[:limit]
            logger.info(f"Processing first {limit} songs")
        
        # Process each song
        for i, song in enumerate(songs, 1):
            logger.debug(f"[{i}/{len(songs)}]")
            self.stats['songs_processed'] += 1
            self.process_song(song)
        
        # Print summary
        self.print_summary()
        
        return True
    
    def print_summary(self):
        """Print statistics summary"""
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Songs processed:            {self.stats['songs_processed']}")
        logger.info(f"Pages fetched:              {self.stats['pages_fetched']}")
        logger.info(f"Cache hits:                 {self.stats['cache_hits']}")
        logger.info(f"Recommendations found:      {self.stats['recommendations_found']}")
        logger.info(f"Recommendations stored:     {self.stats['recommendations_stored']}")
        logger.info(f"Already stored:             {self.stats['already_stored']}")
        if self.stats['itunes_api_calls'] > 0:
            logger.info(f"iTunes API calls:           {self.stats['itunes_api_calls']}")
            logger.info(f"iTunes enriched:            {self.stats['itunes_enriched']}")
        logger.info(f"Errors:                     {self.stats['errors']}")
        logger.info("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Extract recording recommendations from JazzStandards.com pages',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview extraction without storing
  python jazzs_extract.py --dry-run
  
  # Extract and store recommendations
  python jazzs_extract.py
  
  # Process only first 10 songs
  python jazzs_extract.py --limit 10
  
  # Process a single song by name (for debugging)
  python jazzs_extract.py --name "Autumn Leaves"
  python jazzs_extract.py --name "autumn" --debug
  
  # Force refresh cached pages
  python jazzs_extract.py --force-refresh
  
  # Custom cache expiration (60 days)
  python jazzs_extract.py --cache-days 60
  
  # With debug logging
  python jazzs_extract.py --debug
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Force refresh cached pages and overwrite existing recommendations'
    )
    
    parser.add_argument(
        '--cache-days',
        type=int,
        default=CACHE_DAYS,
        help=f'Number of days to cache pages (default: {CACHE_DAYS})'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of songs to process'
    )
    
    parser.add_argument(
        '--name',
        type=str,
        help='Filter to a single song by name (case-insensitive partial match)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create extractor and run
    extractor = JazzStandardsRecommendationExtractor(
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        cache_days=args.cache_days
    )
    
    try:
        success = extractor.run(limit=args.limit, name_filter=args.name)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()