#!/usr/bin/env python3
"""
Match Authority Recommendations to Recordings

Matches recommendations from song_authority_recommendations table to existing recordings
in the recordings table by comparing artist names, album titles, and years.
Updates recording_id when a confident match is found.

Match Criteria:
- Song ID must match (required)
- Artist name fuzzy match (≥85% similarity)
- Album title fuzzy match (≥85% similarity)
- Recording year exact or ±1 year tolerance

Confidence Levels:
- High: Artist ≥90% + album ≥90% + year match
- Medium: Artist ≥90% + album ≥85%, OR artist ≥80% + album ≥85% + year match
- Low: Artist ≥80% but weak album/year
"""

import sys
import argparse
import logging
import unicodedata
import re
import requests
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from urllib.parse import quote_plus

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from the backend .env before importing db_utils,
# which reads DB_* connection settings from the environment at import time.
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / '.env'
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass  # python-dotenv not installed, skip

from db_utils import get_db_connection
from rapidfuzz import fuzz

# Ensure log directory exists BEFORE logging configuration
(Path(__file__).parent / 'log').mkdir(exist_ok=True)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / 'log' / 'match_authority_recommendations.log')
    ]
)
logger = logging.getLogger(__name__)


def strip_accents(text: str) -> str:
    """Remove accents from text for fuzzy matching (e.g., 'Antônio' -> 'Antonio')"""
    if not text:
        return text
    # Normalize to NFD (decomposed form), then remove combining characters
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')


class AuthorityRecommendationMatcher:
    """Matches authority recommendations to recordings in the database"""
    
    def __init__(self, dry_run: bool = False, min_confidence: str = 'medium',
                 song_name: Optional[str] = None, strategy: str = 'performer'):
        self.dry_run = dry_run
        self.min_confidence = min_confidence
        self.song_name = song_name
        self.strategy = strategy  # 'performer' or 'release'
        self.stats = {
            'recommendations_processed': 0,
            'high_confidence_matches': 0,
            'medium_confidence_matches': 0,
            'low_confidence_matches': 0,
            'no_matches': 0,
            'multiple_matches': 0,
            'updated': 0,
            'errors': 0,
            'itunes_lookups': 0,
            'itunes_enriched_matches': 0
        }

        # Setup HTTP session for iTunes API calls
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)'
        })
        
        # Matching thresholds
        self.thresholds = {
            'artist_high': 85,
            'artist_medium': 90,
            'artist_low': 80,
            'album_high': 85,
            'album_medium': 90,
            'year_tolerance': 1
        }
    
    def normalize_string(self, s: Optional[str]) -> str:
        """Normalize string for comparison"""
        if not s:
            return ""
        # Strip accents (e.g., "Antônio" -> "Antonio")
        s = strip_accents(s)
        # Remove common variations
        s = s.lower().strip()
        # Remove "the" prefix
        if s.startswith("the "):
            s = s[4:]
        # Remove punctuation (helps token_set_ratio: "Band," vs "Band")
        s = re.sub(r'[^\w\s]', ' ', s)
        # Collapse multiple spaces
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def fetch_itunes_metadata(self, itunes_album_id: int) -> Optional[Dict]:
        """
        Fetch album metadata from iTunes API.

        Args:
            itunes_album_id: The iTunes collection/album ID

        Returns:
            Dict with artistName, collectionName, releaseDate, etc. or None if lookup fails
        """
        try:
            url = f"https://itunes.apple.com/lookup?id={itunes_album_id}&country=US"
            logger.debug(f"  Fetching iTunes metadata for album ID: {itunes_album_id}")

            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            self.stats['itunes_lookups'] += 1

            if data.get('resultCount', 0) > 0:
                metadata = data['results'][0]
                logger.debug(f"    iTunes: {metadata.get('artistName')} - {metadata.get('collectionName')}")
                return metadata
            else:
                logger.debug(f"    iTunes API returned no results for album ID: {itunes_album_id}")
                return None

        except Exception as e:
            logger.warning(f"Error fetching iTunes metadata: {e}")
            return None

    def enrich_recommendation_from_itunes(self, recommendation: Dict) -> Optional[Dict]:
        """
        Create an enriched copy of a recommendation using iTunes metadata.

        If the recommendation has an itunes_album_id, fetch metadata from iTunes
        and return a new dict with the iTunes artist/album names.

        Returns:
            Enriched recommendation dict, or None if no iTunes data available
        """
        itunes_album_id = recommendation.get('itunes_album_id')
        if not itunes_album_id:
            return None

        itunes_data = self.fetch_itunes_metadata(itunes_album_id)
        if not itunes_data:
            return None

        # Create enriched copy with iTunes metadata
        enriched = dict(recommendation)
        enriched['artist_name'] = itunes_data.get('artistName', recommendation.get('artist_name'))
        enriched['album_title'] = itunes_data.get('collectionName', recommendation.get('album_title'))

        # Extract year from releaseDate if available
        release_date = itunes_data.get('releaseDate', '')
        if release_date:
            year_match = re.search(r'(\d{4})', release_date)
            if year_match:
                enriched['recording_year'] = int(year_match.group(1))

        logger.info(f"  iTunes enrichment: {enriched['artist_name']} - {enriched['album_title']}")

        return enriched

    def compare_artists(self, artist1: Optional[str], artist2: Optional[str]) -> float:
        """Compare artist names with fuzzy matching"""
        if not artist1 or not artist2:
            return 0.0

        norm1 = self.normalize_string(artist1)
        norm2 = self.normalize_string(artist2)

        # Try multiple fuzzy matching approaches
        ratio = fuzz.ratio(norm1, norm2)
        token_sort = fuzz.token_sort_ratio(norm1, norm2)
        partial = fuzz.partial_ratio(norm1, norm2)

        # Return the best score
        # partial_ratio helps match "J.J. Johnson" against "Kai Winding / J.J. Johnson"
        return max(ratio, token_sort, partial)
    
    def extract_year_patterns(self, text: str) -> set:
        """
        Extract year patterns from album titles.
        Handles formats like: '47, '48, 47-48, 1947, 1947-48, etc.
        Returns a set of normalized 2-digit year strings.
        """
        if not text:
            return set()

        years = set()

        # Match 4-digit years (1947, 1948, etc.)
        for match in re.finditer(r'\b(19\d{2}|20\d{2})\b', text):
            years.add(match.group(1)[-2:])  # Keep last 2 digits

        # Match 2-digit years with apostrophe ('47, '48, etc.)
        for match in re.finditer(r"'(\d{2})\b", text):
            years.add(match.group(1))

        # Match year ranges (47-48, 1947-48, etc.)
        for match in re.finditer(r"(\d{2,4})[-–](\d{2})\b", text):
            first = match.group(1)
            second = match.group(2)
            years.add(first[-2:])  # Last 2 digits of first year
            years.add(second)       # Second year is already 2 digits

        return years

    def compare_albums(self, album1: Optional[str], album2: Optional[str]) -> float:
        """Compare album titles with fuzzy matching"""
        if not album1 or not album2:
            return 0.0

        norm1 = self.normalize_string(album1)
        norm2 = self.normalize_string(album2)

        # Try multiple fuzzy matching approaches
        ratio = fuzz.ratio(norm1, norm2)
        token_sort = fuzz.token_sort_ratio(norm1, norm2)
        partial = fuzz.partial_ratio(norm1, norm2)
        # token_set_ratio handles word reordering well, e.g.:
        # "The (Be)Witching Hour: Midnight Blue" vs "Midnight Blue, the (Be)witching Hour"
        token_set = fuzz.token_set_ratio(norm1, norm2)

        base_score = max(ratio, token_sort, partial, token_set)

        # Boost score if both albums contain matching year patterns
        # e.g., "Buddy Rich '47 '48" vs "The Legendary '47-'48 Orchestra"
        years1 = self.extract_year_patterns(album1)
        years2 = self.extract_year_patterns(album2)

        if years1 and years2:
            # Check if years match (at least 2 matching years, or all years from smaller set match)
            matching_years = years1 & years2
            min_years = min(len(years1), len(years2))

            if len(matching_years) >= 2 or (min_years > 0 and len(matching_years) == min_years):
                # Significant year overlap - boost score to meet medium confidence threshold (85%)
                # e.g., "Buddy Rich '47 '48" vs "The Legendary '47-'48 Orchestra"
                if base_score < 85:
                    boosted = 86.0  # Just above the 85% threshold for album_high
                    logger.debug(f"    Year pattern boost: {base_score:.1f}% -> {boosted:.1f}% (matching years: {matching_years})")
                    return boosted

        return base_score
    
    def compare_years(self, year1: Optional[int], year2: Optional[int]) -> bool:
        """Check if years match within tolerance"""
        if not year1 or not year2:
            return False
        
        return abs(year1 - year2) <= self.thresholds['year_tolerance']
    
    def calculate_match_confidence(
        self,
        recommendation: Dict,
        recording: Dict
    ) -> Tuple[str, float, Dict]:
        """
        Calculate match confidence level and detailed scores.

        Returns:
            (confidence_level, overall_score, details_dict)
            confidence_level: 'high', 'medium', 'low', or 'none'
        """
        details = {
            'artist_score': 0.0,
            'album_score': 0.0,
            'year_match': False,
            'release_year_match': False,  # Matches a linked release year
            'itunes_match': False  # Keep for future but always False for now
        }

        # Note: recordings table doesn't store iTunes IDs separately
        # Future enhancement: could extract from apple_music_url

        # Compare artist names
        details['artist_score'] = self.compare_artists(
            recommendation.get('artist_name'),
            recording.get('artist_name')
        )

        # Compare album titles
        details['album_score'] = self.compare_albums(
            recommendation.get('album_title'),
            recording.get('album_title')
        )

        # Compare years - check both recording year and linked release years
        details['year_match'] = self.compare_years(
            recommendation.get('recording_year'),
            recording.get('recording_year')
        )

        # Also check if recommendation year matches any linked release year
        rec_year = recommendation.get('recording_year')
        linked_years = recording.get('linked_release_years', [])
        if rec_year and linked_years:
            for release_year in linked_years:
                if self.compare_years(rec_year, release_year):
                    details['release_year_match'] = True
                    break

        # Determine confidence level
        # Note: iTunes matching not currently available (recordings don't store iTunes IDs)
        year_matches = details['year_match'] or details['release_year_match']

        # Check if album lengths are similar (to avoid substring false positives from partial_ratio)
        # e.g., "Louis Armstrong" would give 100% match for "The Essential Louis Armstrong"
        rec_album = self.normalize_string(recommendation.get('album_title', ''))
        db_album = self.normalize_string(recording.get('album_title', ''))
        albums_similar_length = (
            min(len(rec_album), len(db_album)) >= 0.7 * max(len(rec_album), len(db_album))
            if rec_album and db_album else False
        )

        if (details['artist_score'] >= self.thresholds['artist_medium'] and
              details['album_score'] >= self.thresholds['album_medium'] and
              year_matches):
            confidence = 'high'
            overall_score = 90.0
        elif (details['artist_score'] >= self.thresholds['artist_medium'] and
              details['album_score'] >= self.thresholds['album_high']):
            confidence = 'medium'
            overall_score = 85.0
        elif (details['artist_score'] >= self.thresholds['artist_low'] and
              details['album_score'] >= self.thresholds['album_high'] and
              year_matches):
            confidence = 'medium'
            overall_score = 80.0
        # New rule: Very high album match (>=95%) + release year match = medium confidence
        # This handles cases where album is unmistakably correct but artist format differs
        elif (details['album_score'] >= 95 and year_matches):
            confidence = 'medium'
            overall_score = 75.0
        # New rule: Exact album match (100%) with artist >= 60% = medium confidence
        # This handles cases like "Louis Armstrong & His Orchestra" vs "Louis Armstrong"
        # where the album title is unmistakably correct despite artist name variations
        # An exact album match is a strong enough signal to warrant medium confidence
        elif (details['album_score'] >= 100 and details['artist_score'] >= 60 and albums_similar_length):
            confidence = 'medium'
            # Weight album heavily since exact match is strong signal
            overall_score = 0.7 * details['album_score'] + 0.3 * details['artist_score']
        elif details['artist_score'] >= self.thresholds['artist_low']:
            confidence = 'low'
            overall_score = details['artist_score']
        else:
            confidence = 'none'
            overall_score = details['artist_score']

        return confidence, overall_score, details
    
    def find_matching_recordings(self, recommendation: Dict) -> List[Tuple[Dict, str, float, Dict]]:
        """
        Find all potential recording matches for a recommendation.
        
        Returns:
            List of (recording, confidence_level, overall_score, details)
        """
        song_id = recommendation['song_id']
        artist_name = recommendation.get('artist_name', '')
        
        # If no artist name, can't filter effectively
        if not artist_name:
            logger.debug("No artist name in recommendation, skipping")
            return []
        
        try:
            with get_db_connection() as db:
                with db.cursor() as cur:
                    # First, find performers whose names are similar to the recommendation artist
                    # Use PostgreSQL's similarity function or just get all and filter in Python
                    # For now, get performers with similar names (case-insensitive, contains)
                    normalized_artist = artist_name.lower()

                    # Also create accent-stripped version for matching (e.g., "Antônio" -> "Antonio")
                    stripped_artist = strip_accents(artist_name).lower()

                    # Extract last word (usually surname) for broader matching
                    # This helps with names like "Antonio Carlos Jobim" matching "Antônio Carlos Jobim"
                    name_parts = artist_name.split()
                    last_name = name_parts[-1].lower() if name_parts else normalized_artist

                    # Split combined artist names (e.g., "Warren Vache & Brian Lemon" -> ["Warren Vache", "Brian Lemon"])
                    # This helps match recordings with multiple performers
                    import re
                    artist_parts = re.split(r'\s*[&/,]\s*', artist_name)
                    artist_parts = [p.strip() for p in artist_parts if p.strip()]

                    # Build dynamic WHERE clause for each artist part
                    # Use unaccent() to handle accented characters (e.g., "Mel Tormé" matches "Mel Torme")
                    where_conditions = [
                        "unaccent(LOWER(p.name)) LIKE %s",
                        "LOWER(%s) LIKE '%%' || unaccent(LOWER(p.name)) || '%%'",
                        "unaccent(LOWER(p.name)) LIKE '%%' || LOWER(%s) || '%%'",
                        "unaccent(LOWER(p.name)) LIKE %s",
                        "unaccent(LOWER(p.name)) LIKE %s"
                    ]
                    params = [f'%{normalized_artist}%', artist_name, artist_name,
                              f'%{stripped_artist}%', f'%{last_name}%']

                    # Add conditions for each artist part (for combined names like "A & B")
                    for part in artist_parts:
                        if len(part) > 3:  # Skip very short parts
                            stripped_part = strip_accents(part).lower()
                            part_last_name = part.split()[-1].lower() if part.split() else part.lower()
                            where_conditions.append("unaccent(LOWER(p.name)) LIKE %s")
                            params.append(f'%{stripped_part}%')
                            if len(part_last_name) > 3:
                                where_conditions.append("unaccent(LOWER(p.name)) LIKE %s")
                                params.append(f'%{part_last_name}%')

                    where_clause = " OR ".join(where_conditions)

                    # Get recordings with their primary performer (usually the leader)
                    # Filter to only those where at least one performer name is somewhat similar
                    # Search by: full name, accent-stripped name, last name, or individual artist parts
                    query = f"""
                        WITH matching_performers AS (
                            SELECT DISTINCT p.id, p.name
                            FROM performers p
                            WHERE {where_clause}
                        )
                        SELECT
                            r.id,
                            def_rel.title as album_title,
                            r.recording_year,
                            -- Get ALL performers on the recording (not just matched ones)
                            (SELECT STRING_AGG(DISTINCT p2.name, ' / ' ORDER BY p2.name)
                             FROM recording_performers rp2
                             JOIN performers p2 ON rp2.performer_id = p2.id
                             WHERE rp2.recording_id = r.id
                            ) as artist_names,
                            -- Get primary performer as main artist (leader role)
                            (SELECT p2.name
                             FROM recording_performers rp2
                             JOIN performers p2 ON rp2.performer_id = p2.id
                             WHERE rp2.recording_id = r.id
                             AND (rp2.role = 'leader' OR rp2.role IS NULL)
                             LIMIT 1
                            ) as primary_artist
                        FROM recordings r
                        LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                        WHERE r.id IN (
                            SELECT DISTINCT rp.recording_id
                            FROM recording_performers rp
                            JOIN matching_performers mp ON rp.performer_id = mp.id
                            JOIN recordings rec ON rp.recording_id = rec.id
                            WHERE rec.song_id = %s
                        )
                        LIMIT 50
                    """
                    params.append(song_id)
                    cur.execute(query, params)
                    
                    recordings = cur.fetchall()

                    if recordings:
                        logger.debug(f"Found {len(recordings)} recordings via performer match")
                        for rec in recordings:
                            logger.debug(f"  → {rec.get('artist_names', 'No artist')} - {rec.get('album_title', 'No album')} ({rec.get('recording_year', 'No year')})")
                    else:
                        logger.debug(f"No recordings found via performer match for: {artist_name}")

                        # Fallback: Search by release.artist_credit when performer match fails
                        # This handles cases where the main artist isn't in recording_performers
                        logger.debug(f"Trying fallback: searching by release.artist_credit...")
                        cur.execute("""
                            SELECT
                                r.id,
                                def_rel.title as album_title,
                                r.recording_year,
                                def_rel.artist_credit as artist_names,
                                def_rel.artist_credit as primary_artist
                            FROM recordings r
                            JOIN releases def_rel ON r.default_release_id = def_rel.id
                            WHERE r.song_id = %s
                              AND (
                                  unaccent(LOWER(def_rel.artist_credit)) LIKE %s
                                  OR unaccent(LOWER(def_rel.artist_credit)) LIKE %s
                                  OR LOWER(%s) LIKE '%%' || unaccent(LOWER(def_rel.artist_credit)) || '%%'
                              )
                            LIMIT 50
                        """, (song_id, f'%{normalized_artist}%', f'%{stripped_artist}%', artist_name))

                        recordings = cur.fetchall()

                        if recordings:
                            logger.debug(f"Found {len(recordings)} recordings via release.artist_credit fallback")
                            for rec in recordings:
                                logger.debug(f"  → {rec.get('artist_names', 'No artist')} - {rec.get('album_title', 'No album')} ({rec.get('recording_year', 'No year')})")
                        else:
                            logger.debug(f"No recordings found via release.artist_credit fallback either")

                    # Additional search: Find recordings by album title match
                    # This ensures we don't miss recordings with matching albums due to LIMIT 50
                    album_title = recommendation.get('album_title', '')
                    if album_title and len(album_title) > 5:
                        album_search = strip_accents(album_title).lower()
                        cur.execute("""
                            SELECT DISTINCT
                                r.id,
                                def_rel.title as album_title,
                                r.recording_year,
                                def_rel.artist_credit as artist_names,
                                def_rel.artist_credit as primary_artist
                            FROM recordings r
                            JOIN songs s ON r.song_id = s.id
                            LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                            LEFT JOIN recording_releases rr ON r.id = rr.recording_id
                            LEFT JOIN releases rel2 ON rr.release_id = rel2.id
                            WHERE s.id = %s
                              AND (
                                  unaccent(LOWER(def_rel.title)) LIKE %s
                                  OR unaccent(LOWER(rel2.title)) LIKE %s
                              )
                            LIMIT 20
                        """, (song_id, f'%{album_search}%', f'%{album_search}%'))

                        album_matches = cur.fetchall()
                        if album_matches:
                            # Add any new recordings not already in the list
                            existing_ids = {r['id'] for r in recordings}
                            new_matches = [r for r in album_matches if r['id'] not in existing_ids]
                            if new_matches:
                                logger.debug(f"Found {len(new_matches)} additional recordings via album title match")
                                for rec in new_matches:
                                    logger.debug(f"  → {rec.get('artist_names', 'No artist')} - {rec.get('album_title', 'No album')} ({rec.get('recording_year', 'No year')})")
                                recordings = list(recordings) + new_matches

                    matches = []
                    for recording in recordings:
                        # Create a dict with artist_name for matching
                        rec_dict = dict(recording)

                        # Compare against both primary artist and combined artist names,
                        # use whichever gives the better match. This handles cases like
                        # "Warren Vache & Brian Lemon" matching "Brian Lemon / Warren Vaché"
                        primary_artist = recording.get('primary_artist', '')
                        all_artists = recording.get('artist_names', '')

                        primary_score = self.compare_artists(
                            recommendation.get('artist_name'),
                            primary_artist
                        ) if primary_artist else 0

                        all_artists_score = self.compare_artists(
                            recommendation.get('artist_name'),
                            all_artists
                        ) if all_artists else 0

                        if all_artists_score > primary_score:
                            rec_dict['artist_name'] = all_artists
                            if all_artists_score > primary_score + 10:  # Significant improvement
                                logger.debug(f"  Using combined artists: '{all_artists}' (score: {all_artists_score:.1f}%) instead of '{primary_artist}' ({primary_score:.1f}%)")
                        else:
                            rec_dict['artist_name'] = primary_artist or all_artists

                        # Get all linked releases for this recording to find best album match
                        # This handles cases where a recording appears on multiple releases
                        cur.execute("""
                            SELECT rel.title, rel.artist_credit, rel.release_year
                            FROM recording_releases rr
                            JOIN releases rel ON rr.release_id = rel.id
                            WHERE rr.recording_id = %s
                        """, (recording['id'],))
                        linked_releases = cur.fetchall()

                        # Find the best matching album title from all linked releases
                        # Also collect all linked release years for year matching
                        best_album_title = rec_dict.get('album_title', '')
                        best_album_score = self.compare_albums(
                            recommendation.get('album_title'),
                            best_album_title
                        )
                        linked_release_years = [rel['release_year'] for rel in linked_releases if rel.get('release_year')]

                        for linked_rel in linked_releases:
                            linked_album_score = self.compare_albums(
                                recommendation.get('album_title'),
                                linked_rel['title']
                            )
                            if linked_album_score > best_album_score:
                                best_album_score = linked_album_score
                                best_album_title = linked_rel['title']
                                # Also use artist_credit from matching release if better
                                linked_artist_score = self.compare_artists(
                                    recommendation.get('artist_name'),
                                    linked_rel['artist_credit']
                                )
                                if linked_artist_score > self.compare_artists(
                                    recommendation.get('artist_name'),
                                    rec_dict['artist_name']
                                ):
                                    rec_dict['artist_name'] = linked_rel['artist_credit']

                        # Use the best matching album title for comparison
                        if best_album_title != rec_dict.get('album_title'):
                            logger.debug(f"  Using linked release album: '{best_album_title}' (score: {best_album_score:.1f}%) instead of '{rec_dict.get('album_title')}'")
                            rec_dict['album_title'] = best_album_title

                        # Store linked release years for year matching
                        rec_dict['linked_release_years'] = linked_release_years

                        confidence, score, details = self.calculate_match_confidence(
                            recommendation,
                            rec_dict
                        )
                        
                        # Log detailed comparison
                        logger.debug(f"\nComparing with recording: {rec_dict['artist_name']} - {rec_dict.get('album_title', 'No album')}")
                        logger.debug(f"  Artist: {details['artist_score']:.1f}% similarity")
                        logger.debug(f"    Recommend: '{recommendation.get('artist_name')}'")
                        logger.debug(f"    Recording: '{rec_dict['artist_name']}'")
                        logger.debug(f"  Album: {details['album_score']:.1f}% similarity")
                        logger.debug(f"    Recommend: '{recommendation.get('album_title')}'")
                        logger.debug(f"    Recording: '{rec_dict.get('album_title')}'")
                        year_status = 'MATCH ✓' if details['year_match'] else ('RELEASE MATCH ✓' if details.get('release_year_match') else 'NO MATCH ✗')
                        logger.debug(f"  Year: {year_status}")
                        logger.debug(f"    Recommend: {recommendation.get('recording_year')}")
                        logger.debug(f"    Recording: {rec_dict.get('recording_year')}")
                        if rec_dict.get('linked_release_years'):
                            logger.debug(f"    Linked releases: {rec_dict.get('linked_release_years')}")
                        logger.debug(f"  → RESULT: {confidence.upper()} confidence ({score:.1f}%)")
                        
                        if confidence != 'none':
                            matches.append((rec_dict, confidence, score, details))
                    
                    # Sort by confidence level first (high > medium > low), then by score
                    confidence_order = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}
                    matches.sort(key=lambda x: (confidence_order.get(x[1], 0), x[2]), reverse=True)
                    return matches
                
        except Exception as e:
            logger.error(f"Error finding matches: {e}", exc_info=True)
            return []
    
    def find_matching_recordings_via_release(self, recommendation: Dict) -> List[Tuple[Dict, str, float, Dict]]:
        """
        Find matching recordings by first matching to releases, then finding recordings.

        This is a release-first approach:
        1. Find releases matching album_title + artist_credit + year
        2. Find recordings of this song that are on those releases
        3. Return matches with confidence scores

        Returns:
            List of (recording, confidence_level, overall_score, details)
        """
        song_id = recommendation['song_id']
        artist_name = recommendation.get('artist_name', '')
        album_title = recommendation.get('album_title', '')
        rec_year = recommendation.get('recording_year')

        if not album_title:
            logger.debug("No album title in recommendation, skipping release-based matching")
            return []

        try:
            with get_db_connection() as db:
                with db.cursor() as cur:
                    # Normalize for matching
                    normalized_album = album_title.lower()
                    stripped_album = strip_accents(album_title).lower()
                    normalized_artist = artist_name.lower() if artist_name else ''
                    stripped_artist = strip_accents(artist_name).lower() if artist_name else ''

                    # Find releases that match the album title
                    # Then join to find recordings of this song on those releases
                    # Use unaccent() to handle accented characters in database
                    cur.execute("""
                        SELECT DISTINCT
                            r.id as recording_id,
                            def_rel.title as recording_album,
                            r.recording_year,
                            rel.id as release_id,
                            rel.title as release_title,
                            rel.artist_credit,
                            rel.release_year
                        FROM releases rel
                        JOIN recording_releases rr ON rel.id = rr.release_id
                        JOIN recordings r ON rr.recording_id = r.id
                        LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                        WHERE r.song_id = %s
                          AND (
                              unaccent(LOWER(rel.title)) LIKE %s
                              OR unaccent(LOWER(rel.title)) LIKE %s
                          )
                        LIMIT 50
                    """, (song_id, f'%{normalized_album}%', f'%{stripped_album}%'))

                    results = cur.fetchall()

                    if results:
                        logger.debug(f"Found {len(results)} recordings via release match")
                        for res in results:
                            logger.debug(f"  → {res['artist_credit']} - {res['release_title']} ({res['release_year']})")
                    else:
                        logger.debug(f"No recordings found via release match for album: {album_title}")
                        return []

                    matches = []
                    for result in results:
                        # Build a recording dict for comparison
                        rec_dict = {
                            'id': result['recording_id'],
                            'album_title': result['release_title'],  # Use release title
                            'recording_year': result['recording_year'],
                            'artist_name': result['artist_credit'],  # Use release artist credit
                            'linked_release_years': [result['release_year']] if result['release_year'] else []
                        }

                        # Get all linked release years for this recording
                        cur.execute("""
                            SELECT DISTINCT rel.release_year
                            FROM recording_releases rr
                            JOIN releases rel ON rr.release_id = rel.id
                            WHERE rr.recording_id = %s AND rel.release_year IS NOT NULL
                        """, (result['recording_id'],))
                        all_years = [row['release_year'] for row in cur.fetchall()]
                        rec_dict['linked_release_years'] = all_years

                        confidence, score, details = self.calculate_match_confidence(
                            recommendation,
                            rec_dict
                        )

                        # Log detailed comparison
                        logger.debug(f"\nComparing with release: {rec_dict['artist_name']} - {rec_dict['album_title']}")
                        logger.debug(f"  Artist: {details['artist_score']:.1f}% similarity")
                        logger.debug(f"    Recommend: '{recommendation.get('artist_name')}'")
                        logger.debug(f"    Release: '{rec_dict['artist_name']}'")
                        logger.debug(f"  Album: {details['album_score']:.1f}% similarity")
                        logger.debug(f"    Recommend: '{recommendation.get('album_title')}'")
                        logger.debug(f"    Release: '{rec_dict['album_title']}'")
                        year_status = 'MATCH ✓' if details['year_match'] else ('RELEASE MATCH ✓' if details.get('release_year_match') else 'NO MATCH ✗')
                        logger.debug(f"  Year: {year_status}")
                        logger.debug(f"    Recommend: {recommendation.get('recording_year')}")
                        logger.debug(f"    Release: {result['release_year']}")
                        if rec_dict.get('linked_release_years'):
                            logger.debug(f"    All release years: {rec_dict['linked_release_years']}")
                        logger.debug(f"  → RESULT: {confidence.upper()} confidence ({score:.1f}%)")

                        if confidence != 'none':
                            matches.append((rec_dict, confidence, score, details))

                    # Sort by confidence level first, then by score
                    confidence_order = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}
                    matches.sort(key=lambda x: (confidence_order.get(x[1], 0), x[2]), reverse=True)
                    return matches

        except Exception as e:
            logger.error(f"Error finding matches via release: {e}", exc_info=True)
            return []

    def update_recommendation_recording_id(self, recommendation_id: str, recording_id: str) -> bool:
        """Update recording_id in song_authority_recommendations"""
        if self.dry_run:
            return True
        
        try:
            with get_db_connection() as db:
                with db.cursor() as cur:
                    cur.execute("""
                        UPDATE song_authority_recommendations
                        SET recording_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (recording_id, recommendation_id))
                    
                    return True
                
        except Exception as e:
            logger.error(f"Error updating recommendation: {e}", exc_info=True)
            return False
    
    def process_recommendation(self, recommendation: Dict) -> bool:
        """Process a single recommendation and try to match it"""
        self.stats['recommendations_processed'] += 1

        rec_id = recommendation['id']
        song_title = recommendation['song_title']
        artist_name = recommendation.get('artist_name', 'Unknown')
        album_title = recommendation.get('album_title', '')

        logger.info(f"Song: {song_title}")
        logger.info(f"Recommendation: {artist_name}" +
                    (f" - {album_title}" if album_title else ""))
        logger.debug(f"  Details: artist='{artist_name}', album='{album_title}', year={recommendation.get('recording_year')}")

        # Find matching recordings using selected strategy
        if self.strategy == 'release':
            matches = self.find_matching_recordings_via_release(recommendation)
        else:
            matches = self.find_matching_recordings(recommendation)

        # Track if we used iTunes enrichment for the match
        used_itunes_enrichment = False

        # If no matches found, try iTunes enrichment as fallback
        if not matches and recommendation.get('itunes_album_id'):
            logger.info("  No matches with parsed data, trying iTunes metadata...")
            enriched = self.enrich_recommendation_from_itunes(recommendation)

            if enriched:
                # Retry matching with iTunes-enriched data
                if self.strategy == 'release':
                    matches = self.find_matching_recordings_via_release(enriched)
                else:
                    matches = self.find_matching_recordings(enriched)

                if matches:
                    used_itunes_enrichment = True
                    logger.info(f"  ✓ iTunes enrichment found {len(matches)} potential match(es)")

        if not matches:
            logger.info("  No matches found with sufficient confidence")
            self.stats['no_matches'] += 1
            return False

        if len(matches) > 1:
            self.stats['multiple_matches'] += 1
            # Count by confidence level
            confidence_counts = {}
            for _, conf, _, _ in matches:
                confidence_counts[conf] = confidence_counts.get(conf, 0) + 1
            
            summary = ", ".join([f"{count} {conf}" for conf, count in sorted(confidence_counts.items(), reverse=True)])
            logger.info(f"  Found {len(matches)} potential matches: {summary}")
        
        # Get best match
        best_recording, confidence, score, details = matches[0]
        
        # Track by confidence level
        if confidence == 'high':
            self.stats['high_confidence_matches'] += 1
        elif confidence == 'medium':
            self.stats['medium_confidence_matches'] += 1
        elif confidence == 'low':
            self.stats['low_confidence_matches'] += 1
        
        # Check if confidence meets minimum threshold
        confidence_levels = {'high': 2, 'medium': 1, 'low': 0}
        min_level = confidence_levels.get(self.min_confidence, 1)
        current_level = confidence_levels.get(confidence, 0)
        
        if current_level < min_level:
            logger.info(f"  ⚠ Best match is '{confidence}' confidence but minimum is '{self.min_confidence}'")
            logger.info(f"    Recording: {best_recording.get('artist_name', 'Unknown')} - {best_recording.get('album_title', 'No album')} ({best_recording.get('recording_year', 'No year')})")
            logger.debug(f"    Scores: Artist {details['artist_score']:.1f}%, Album {details['album_score']:.1f}%, Year {'✓' if details['year_match'] else '✗'}")
            return False
        
        # Log match details
        mode = "[DRY RUN] " if self.dry_run else ""
        itunes_note = " via iTunes" if used_itunes_enrichment else ""
        logger.info(f"  ✓ {mode}MATCHED ({confidence.upper()}, {score:.1f}%){itunes_note}")
        logger.info(f"    Recording: {best_recording.get('artist_name', 'Unknown')} - {best_recording.get('album_title', 'No album')} ({best_recording.get('recording_year', 'No year')})")
        logger.debug(f"    Scores: Artist {details['artist_score']:.1f}%, Album {details['album_score']:.1f}%, Year {'✓' if details['year_match'] else '✗'}")

        # Track iTunes-enriched matches
        if used_itunes_enrichment:
            self.stats['itunes_enriched_matches'] += 1

        # Update recording_id
        if self.update_recommendation_recording_id(rec_id, best_recording['id']):
            self.stats['updated'] += 1
            return True
        else:
            self.stats['errors'] += 1
            return False
    
    def get_unmatched_recommendations(self, limit: Optional[int] = None) -> List[Dict]:
        """Fetch recommendations that don't have recording_id set"""
        if self.song_name:
            logger.info(f"Fetching unmatched recommendations for song: '{self.song_name}'...")
        else:
            logger.info("Fetching unmatched recommendations...")
        
        try:
            with get_db_connection() as db:
                with db.cursor() as cur:
                    query = """
                        SELECT 
                            sar.id,
                            sar.song_id,
                            sar.artist_name,
                            sar.album_title,
                            sar.recording_year,
                            sar.itunes_album_id,
                            sar.itunes_track_id,
                            sar.source,
                            s.title as song_title
                        FROM song_authority_recommendations sar
                        JOIN songs s ON sar.song_id = s.id
                        WHERE sar.recording_id IS NULL
                    """
                    params = []
                    
                    # Filter by song name if provided
                    if self.song_name:
                        query += " AND LOWER(s.title) = LOWER(%s)"
                        params.append(self.song_name)
                    
                    query += " ORDER BY s.title, sar.artist_name"
                    
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cur.execute(query, params)
                    recommendations = cur.fetchall()
                    
                    if self.song_name and not recommendations:
                        # Check if song exists at all
                        cur.execute("""
                            SELECT s.title, COUNT(sar.id) as total_recs
                            FROM songs s
                            LEFT JOIN song_authority_recommendations sar ON s.id = sar.song_id
                            WHERE LOWER(s.title) = LOWER(%s)
                            GROUP BY s.id
                        """, (self.song_name,))
                        song_info = cur.fetchone()
                        
                        if song_info:
                            logger.info(f"✓ Song '{song_info['title']}' exists with {song_info['total_recs']} total recommendations (all may be matched)")
                        else:
                            logger.warning(f"⚠ No song found with title: '{self.song_name}'")
                    
                    logger.info(f"✓ Found {len(recommendations)} unmatched recommendations")
                    return recommendations
                
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            return []
    
    def run(self, limit: Optional[int] = None) -> bool:
        """Main execution method"""
        logger.info("="*80)
        logger.info("MATCH AUTHORITY RECOMMENDATIONS TO RECORDINGS")
        logger.info("="*80)
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"Strategy: {self.strategy}")
        logger.info(f"Minimum confidence: {self.min_confidence}")
        if self.song_name:
            logger.info(f"Song filter: '{self.song_name}'")
        logger.info("")
        
        # Get unmatched recommendations
        recommendations = self.get_unmatched_recommendations(limit)
        if not recommendations:
            logger.info("No unmatched recommendations found")
            return True
        
        # Process each recommendation
        for i, rec in enumerate(recommendations, 1):
            logger.info(f"\n[{i}/{len(recommendations)}] ============================================")
            try:
                self.process_recommendation(rec)
            except Exception as e:
                logger.error(f"Error processing recommendation: {e}", exc_info=True)
                self.stats['errors'] += 1
        
        # Print summary
        self.print_summary()
        
        return True
    
    def print_summary(self):
        """Print statistics summary"""
        logger.info("\n" + "="*80)
        logger.info("MATCHING SUMMARY")
        logger.info("="*80)
        logger.info(f"Recommendations processed:   {self.stats['recommendations_processed']}")
        logger.info(f"High confidence matches:     {self.stats['high_confidence_matches']}")
        logger.info(f"Medium confidence matches:   {self.stats['medium_confidence_matches']}")
        logger.info(f"Low confidence matches:      {self.stats['low_confidence_matches']}")
        logger.info(f"No matches found:            {self.stats['no_matches']}")
        logger.info(f"Multiple matches found:      {self.stats['multiple_matches']}")
        logger.info(f"Recommendations updated:     {self.stats['updated']}")
        if self.stats['itunes_lookups'] > 0:
            logger.info(f"iTunes API lookups:          {self.stats['itunes_lookups']}")
            logger.info(f"iTunes-enriched matches:     {self.stats['itunes_enriched_matches']}")
        logger.info(f"Errors:                      {self.stats['errors']}")
        logger.info("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Match authority recommendations to existing recordings',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Match and update with medium confidence (default)
  python match_authority_recommendations.py
  
  # Preview matches without updating
  python match_authority_recommendations.py --dry-run
  
  # Match and update with high confidence threshold
  python match_authority_recommendations.py --min-confidence high
  
  # Process only first 10 recommendations
  python match_authority_recommendations.py --limit 10
  
  # Process recommendations for a specific song only
  python match_authority_recommendations.py --name "Body and Soul" --debug
  
  # With debug logging
  python match_authority_recommendations.py --debug

Confidence Levels:
  high   - Artist ≥90% + album ≥90% + year match
  medium - Artist ≥90% + album ≥85%, OR artist ≥80% + album ≥85% + year match
  low    - Artist ≥80% but weak album/year match
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview matches without updating database'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--min-confidence',
        choices=['high', 'medium', 'low'],
        default='medium',
        help='Minimum confidence level for matching (default: medium)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of recommendations to process'
    )
    
    parser.add_argument(
        '--name',
        type=str,
        metavar='SONG_NAME',
        help='Limit to recommendations for a specific song title (case-insensitive)'
    )

    parser.add_argument(
        '--strategy',
        choices=['performer', 'release'],
        default='performer',
        help='Matching strategy: "performer" searches by artist first, "release" searches by album first (default: performer)'
    )

    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine dry_run mode
    dry_run = args.dry_run
    
    # Create matcher and run
    matcher = AuthorityRecommendationMatcher(
        dry_run=dry_run,
        min_confidence=args.min_confidence,
        song_name=args.name,
        strategy=args.strategy
    )
    
    try:
        success = matcher.run(limit=args.limit)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()