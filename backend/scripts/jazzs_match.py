#!/usr/bin/env python3
"""
Match Songs to JazzStandards.com
Fetches the JazzStandards.com top 1000 list and matches songs in the database to their URLs,
storing the matches in the external_references JSONB field.
"""

import sys
import argparse
import logging
import json
import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from db_utils import get_db_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/match_jazzstandards.log')
    ]
)
logger = logging.getLogger(__name__)

JAZZSTANDARDS_INDEX_URL = 'https://www.jazzstandards.com/compositions/index.htm'
JAZZSTANDARDS_BASE_URL = 'https://www.jazzstandards.com'


class JazzStandardsMatcher:
    """Matches database songs to JazzStandards.com pages"""
    
    def __init__(self, dry_run: bool = False, force_refresh: bool = False, show_unmatched: bool = False):
        self.dry_run = dry_run
        self.force_refresh = force_refresh
        self.show_unmatched = show_unmatched
        self.stats = {
            'songs_in_db': 0,
            'jazzstandards_songs': 0,
            'exact_matches': 0,
            'fuzzy_matches': 0,
            'no_match': 0,
            'already_had_url': 0,
            'updated': 0,
            'errors': 0
        }
        self.unmatched_songs = []
        
        # Setup session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ApproachNote/1.0 (+support@approachnote.com)'
        })
    
    def normalize_title(self, title: str, strip_parentheses: bool = False) -> str:
        """
        Normalize song title for matching.
        Handles apostrophes, quotes, articles, and punctuation.
        
        Args:
            title: The title to normalize
            strip_parentheses: If True, remove content in parentheses (for alternate titles)
        
        Examples:
            "'Round Midnight" -> "round midnight"
            "Take the 'A' Train" -> "take a train"
            "Body and Soul" -> "body and soul"
            "Autumn Leaves (Les Feuilles Mortes)" -> "autumn leaves" (if strip_parentheses=True)
        """
        if not title:
            return ""
        
        normalized = title
        
        # Strip parenthetical content if requested
        if strip_parentheses:
            # Remove content in parentheses/brackets
            normalized = re.sub(r'\([^)]*\)', '', normalized)
            normalized = re.sub(r'\[[^\]]*\]', '', normalized)
        
        # Convert to lowercase
        normalized = normalized.lower()
        
        # Remove various quote styles and apostrophes at word boundaries
        # \u escapes used so editor smart-quote autocorrect can't rewrite
        # the literal characters back to ASCII (which is exactly how this
        # regex got broken before — see normalize_title in
        # integrations/musicbrainz/client.py for the same fix).
        normalized = re.sub("[\u2018\u2019\u02bc\u0060']", "", normalized)
        normalized = re.sub("[\u201c\u201d\u201e\"]", "", normalized)
        
        # Remove "the" at the beginning
        normalized = re.sub(r'^the\s+', '', normalized)
        
        # Remove most punctuation except spaces and letters
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Collapse multiple spaces
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized.strip()
    
    def get_title_variations(self, title: str) -> List[str]:
        """
        Generate multiple normalized variations of a title for matching.
        
        Returns list of normalized variations to try, in order of preference:
        1. Full title normalized
        2. Title with parenthetical content stripped
        3. Title before first comma (if present)
        4. Title with all spaces removed (for compound words like Stardust/Star Dust)
        """
        variations = []
        
        # Full title
        full_normalized = self.normalize_title(title)
        if full_normalized:
            variations.append(full_normalized)
        
        # Without parentheses (for alternate titles like "Autumn Leaves (Les Feuilles Mortes)")
        no_parens = self.normalize_title(title, strip_parentheses=True)
        if no_parens and no_parens != full_normalized:
            variations.append(no_parens)
        
        # Before first comma (for titles like "All of Me, or...")
        if ',' in title:
            before_comma = title.split(',')[0].strip()
            comma_normalized = self.normalize_title(before_comma)
            if comma_normalized and comma_normalized not in variations:
                variations.append(comma_normalized)
        
        # Without spaces (for compound words like "Stardust" vs "Star Dust")
        # Only add if the title has spaces or could benefit from this
        if full_normalized:
            no_spaces = full_normalized.replace(' ', '')
            if no_spaces and no_spaces != full_normalized and no_spaces not in variations:
                variations.append(no_spaces)
        
        return variations
    
    def fetch_jazzstandards_index(self) -> List[Dict[str, str]]:
        """
        Fetch and parse all JazzStandards.com index pages (paginated 1-10).
        Returns a list of dicts with 'title' and 'url' keys.
        """
        logger.info("Fetching JazzStandards.com index pages...")
        
        all_songs = []
        
        # Fetch all 10 index pages
        # index.htm (1-100), index2.htm (101-200), ... index10.htm (901-1000)
        index_urls = [
            'https://www.jazzstandards.com/compositions/index.htm',
        ] + [
            f'https://www.jazzstandards.com/compositions/index{i}.htm' 
            for i in range(2, 11)
        ]
        
        for page_num, index_url in enumerate(index_urls, 1):
            logger.info(f"  Fetching page {page_num}/10: {index_url}")
            
            try:
                response = self.session.get(index_url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all composition links
                # The index page has links in the format: /compositions-0/songname.htm
                page_songs = []
                
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    
                    # Look for composition links
                    if '/compositions' in href and href.endswith('.htm'):
                        title = link.get_text(strip=True)
                        
                        # Skip empty titles or navigation links
                        if not title or len(title) < 2:
                            continue
                        
                        # Build full URL if needed
                        if href.startswith('http'):
                            url = href
                        else:
                            url = f"{JAZZSTANDARDS_BASE_URL}{href}" if href.startswith('/') else f"{JAZZSTANDARDS_BASE_URL}/{href}"
                        
                        # Store multiple normalized variations for matching
                        variations = self.get_title_variations(title)
                        
                        page_songs.append({
                            'title': title,
                            'url': url,
                            'normalized': variations[0] if variations else '',
                            'variations': variations  # All possible normalized forms
                        })
                
                logger.info(f"    Found {len(page_songs)} songs on page {page_num}")
                all_songs.extend(page_songs)
                
            except requests.RequestException as e:
                logger.warning(f"  Failed to fetch page {page_num}: {e}")
                continue
            except Exception as e:
                logger.warning(f"  Error parsing page {page_num}: {e}")
                continue
        
        # Remove duplicates (same URL)
        seen_urls = set()
        unique_songs = []
        for song in all_songs:
            if song['url'] not in seen_urls:
                seen_urls.add(song['url'])
                unique_songs.append(song)
        
        logger.info(f"✓ Found {len(unique_songs)} unique songs across all pages")
        return unique_songs
    
    def get_database_songs(self) -> List[Dict]:
        """Fetch all songs from the database"""
        logger.info("Fetching songs from database...")
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            id,
                            title,
                            external_references
                        FROM songs
                        ORDER BY title
                    """)
                    
                    songs = []
                    for row in cur.fetchall():
                        variations = self.get_title_variations(row['title'])
                        songs.append({
                            'id': row['id'],
                            'title': row['title'],
                            'normalized': variations[0] if variations else '',
                            'variations': variations,
                            'external_references': row['external_references'] or {}
                        })
                    
                    logger.info(f"✓ Found {len(songs)} songs in database")
                    return songs
                    
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            return []
    
    def match_songs(self, db_songs: List[Dict], js_songs: List[Dict]) -> List[Tuple[Dict, Dict, int, str]]:
        """
        Match database songs to JazzStandards songs.
        Returns list of tuples: (db_song, js_song, score, match_type)
        """
        matches = []
        
        for db_song in db_songs:
            best_match = None
            best_score = 0
            match_type = 'none'
            
            # Check if already has jazzstandards URL
            ext_refs = db_song['external_references']
            if 'jazzstandards' in ext_refs and not self.force_refresh:
                logger.debug(f"  {db_song['title']} - already has JazzStandards URL")
                self.stats['already_had_url'] += 1
                continue
            
            # Try exact match with all variations
            for db_var in db_song['variations']:
                for js_song in js_songs:
                    for js_var in js_song['variations']:
                        if db_var == js_var:
                            best_match = js_song
                            best_score = 100
                            match_type = 'exact'
                            break
                    if best_match:
                        break
                if best_match:
                    break
            
            # If no exact match, try fuzzy matching with all variations
            if not best_match:
                for db_var in db_song['variations']:
                    for js_song in js_songs:
                        for js_var in js_song['variations']:
                            # Use token_sort_ratio for better matching with word order variations
                            score = fuzz.token_sort_ratio(db_var, js_var)
                            
                            if score > best_score:
                                best_score = score
                                best_match = js_song
                
                # Only accept fuzzy matches above 85% similarity
                if best_score >= 85:
                    match_type = 'fuzzy'
                else:
                    match_type = 'none'
            
            if best_match and match_type != 'none':
                matches.append((db_song, best_match, best_score, match_type))
                
                if match_type == 'exact':
                    self.stats['exact_matches'] += 1
                else:
                    self.stats['fuzzy_matches'] += 1
            else:
                self.stats['no_match'] += 1
                self.unmatched_songs.append((db_song, best_score))
                logger.debug(f"  {db_song['title']} - no match found (best score: {best_score})")
        
        return matches
    
    def update_song_references(self, matches: List[Tuple[Dict, Dict, int, str]]) -> int:
        """
        Update songs' external_references with JazzStandards URLs.
        Returns count of successful updates.
        """
        updated_count = 0
        
        if self.dry_run:
            logger.info("\n[DRY RUN] Would update the following songs:")
        else:
            logger.info("\nUpdating songs with JazzStandards URLs...")
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    for db_song, js_song, score, match_type in matches:
                        # Get current external_references
                        ext_refs = db_song['external_references'].copy()
                        
                        # Add or update jazzstandards URL
                        ext_refs['jazzstandards'] = js_song['url']
                        
                        if self.dry_run:
                            logger.info(f"  [{match_type.upper()} {score}%] {db_song['title']}")
                            logger.info(f"    -> {js_song['title']}")
                            logger.info(f"    -> {js_song['url']}")
                        else:
                            # Update the database
                            cur.execute("""
                                UPDATE songs
                                SET external_references = %s,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (json.dumps(ext_refs), db_song['id']))
                            
                            updated_count += 1
                            logger.info(f"  ✓ [{match_type.upper()} {score}%] {db_song['title']} -> {js_song['title']}")
                            logger.debug(f"    -> {js_song['url']}")
                    
                    if not self.dry_run:
                        conn.commit()
                        logger.info(f"\n✓ Updated {updated_count} songs")
        
        except Exception as e:
            logger.error(f"Error updating database: {e}", exc_info=True)
            self.stats['errors'] += 1
        
        return updated_count
    
    def print_unmatched_songs(self):
        """Print list of songs that couldn't be matched"""
        if not self.unmatched_songs:
            return
        
        logger.info("\n" + "="*80)
        logger.info("UNMATCHED SONGS")
        logger.info("="*80)
        logger.info(f"The following {len(self.unmatched_songs)} songs could not be matched:")
        logger.info("")
        
        # Sort by title for easier reading
        sorted_unmatched = sorted(self.unmatched_songs, key=lambda x: x[0]['title'])
        
        for db_song, best_score in sorted_unmatched:
            logger.info(f"  • {db_song['title']}")
            logger.info(f"    Best match score: {best_score}%")
            if db_song['variations']:
                logger.info(f"    Normalized: {db_song['variations'][0]}")
        
        logger.info("="*80)
    
    def run(self) -> bool:
        """Main execution method"""
        logger.info("="*80)
        logger.info("MATCH SONGS TO JAZZSTANDARDS.COM")
        logger.info("="*80)
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        logger.info(f"Force refresh: {self.force_refresh}")
        logger.info(f"Show unmatched: {self.show_unmatched}")
        logger.info("")
        
        # Fetch JazzStandards index
        js_songs = self.fetch_jazzstandards_index()
        if not js_songs:
            logger.error("Failed to fetch JazzStandards.com index")
            return False
        
        self.stats['jazzstandards_songs'] = len(js_songs)
        
        # Fetch database songs
        db_songs = self.get_database_songs()
        if not db_songs:
            logger.error("Failed to fetch songs from database")
            return False
        
        self.stats['songs_in_db'] = len(db_songs)
        
        # Match songs
        logger.info("\nMatching songs...")
        matches = self.match_songs(db_songs, js_songs)
        
        # Update database
        if matches:
            updated = self.update_song_references(matches)
            self.stats['updated'] = updated
        else:
            logger.info("No matches found to update")
        
        # Print summary
        self.print_summary()
        
        # Show unmatched songs if requested
        if self.show_unmatched and self.unmatched_songs:
            self.print_unmatched_songs()
        
        return True
    
    def print_summary(self):
        """Print statistics summary"""
        logger.info("\n" + "="*80)
        logger.info("MATCHING SUMMARY")
        logger.info("="*80)
        logger.info(f"Songs in database:          {self.stats['songs_in_db']}")
        logger.info(f"Songs on JazzStandards:     {self.stats['jazzstandards_songs']}")
        logger.info(f"Already had URL:            {self.stats['already_had_url']}")
        logger.info(f"Exact matches:              {self.stats['exact_matches']}")
        logger.info(f"Fuzzy matches (≥85%):       {self.stats['fuzzy_matches']}")
        logger.info(f"No match found:             {self.stats['no_match']}")
        logger.info(f"Updated in database:        {self.stats['updated']}")
        logger.info(f"Errors:                     {self.stats['errors']}")
        logger.info("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Match database songs to JazzStandards.com and store URLs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview matches without updating database
  python match_jazz_standards.py --dry-run
  
  # Update database with matches
  python match_jazz_standards.py
  
  # Show list of unmatched songs
  python match_jazz_standards.py --show-unmatched
  
  # Force refresh existing JazzStandards URLs
  python match_jazz_standards.py --force-refresh
  
  # With debug logging
  python match_jazz_standards.py --debug
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
        help='Update songs even if they already have a JazzStandards URL'
    )
    
    parser.add_argument(
        '--show-unmatched',
        action='store_true',
        help='Display list of songs that could not be matched'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create matcher and run
    matcher = JazzStandardsMatcher(
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        show_unmatched=args.show_unmatched
    )
    
    try:
        success = matcher.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()