#!/usr/bin/env python3
"""
Fix Unknown Lead Artists
Identifies and fixes recordings showing "Unknown" as lead artist by re-parsing MusicBrainz data
"""

# Standard library imports
import sys
import argparse
import logging
import time
from datetime import datetime

# Third-party imports
import requests

from db_utils import get_db_connection
from core.http_client import make_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/fix_unknown_leaders.log')
    ]
)
logger = logging.getLogger(__name__)


class LeaderFixer:
    """Fix recordings with Unknown lead artists by re-parsing MusicBrainz data"""
    
    def __init__(self, dry_run=False, request_delay=1.5):
        """
        Initialize fixer
        
        Args:
            dry_run: If True, show what would be done without making changes
            request_delay: Delay between API requests in seconds
        """
        self.dry_run = dry_run
        self.request_delay = request_delay
        self.session = make_session()
        self.stats = {
            'recordings_checked': 0,
            'recordings_no_leader': 0,
            'recordings_fixed': 0,
            'recordings_failed': 0,
            'errors': 0
        }
    
    def get_recordings_without_leaders(self):
        """Get recordings that have no leader performer"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Find recordings where no performer has role='leader'
                    cur.execute("""
                        SELECT DISTINCT
                            r.id,
                            r.musicbrainz_id,
                            r.album_title,
                            r.recording_year,
                            s.title as song_title,
                            (
                                SELECT COUNT(*) 
                                FROM recording_performers rp 
                                WHERE rp.recording_id = r.id
                            ) as performer_count,
                            (
                                SELECT COUNT(*) 
                                FROM recording_performers rp 
                                WHERE rp.recording_id = r.id AND rp.role = 'leader'
                            ) as leader_count
                        FROM recordings r
                        JOIN songs s ON r.song_id = s.id
                        WHERE r.musicbrainz_id IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 
                            FROM recording_performers rp 
                            WHERE rp.recording_id = r.id AND rp.role = 'leader'
                        )
                        ORDER BY s.title, r.recording_year DESC
                    """)
                    
                    recordings = cur.fetchall()
                    return recordings
                    
        except Exception as e:
            logger.error(f"Error fetching recordings without leaders: {e}", exc_info=True)
            return []
    
    def fetch_musicbrainz_recording(self, mb_id, max_retries=3):
        """
        Fetch recording details from MusicBrainz API with retry logic
        
        Args:
            mb_id: MusicBrainz recording ID
            max_retries: Maximum number of retry attempts
            
        Returns:
            Recording data dict or None
        """
        url = f"https://musicbrainz.org/ws/2/recording/{mb_id}"
        params = {
            'inc': 'artist-credits+releases+recording-rels+artist-rels',
            'fmt': 'json'
        }
        
        for attempt in range(max_retries):
            try:
                # Progressive delay based on configured rate and attempt number
                sleep_time = self.request_delay + (attempt * 1.5)
                time.sleep(sleep_time)
                
                if attempt > 0:
                    logger.info(f"  Retry attempt {attempt + 1}/{max_retries}...")
                
                response = self.session.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    logger.warning(f"Recording not found in MusicBrainz: {mb_id}")
                    return None
                elif response.status_code == 503:
                    logger.warning(f"MusicBrainz service unavailable (503), will retry...")
                    continue
                else:
                    logger.error(f"MusicBrainz API error {response.status_code}")
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error("All retry attempts failed")
                return None
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                logger.error("All retry attempts failed")
                return None
            except Exception as e:
                logger.error(f"Unexpected error fetching MusicBrainz recording: {e}")
                if attempt < max_retries - 1:
                    continue
                return None
        
        return None
    
    def extract_artist_from_recording(self, mb_data):
        """
        Extract artist information from MusicBrainz recording data
        
        Priority:
        1. artist-credit field (most reliable)
        2. First release's artist-credit
        3. Recording relationships
        
        Args:
            mb_data: MusicBrainz recording data
            
        Returns:
            Dict with 'name' and 'mbid' or None
        """
        # Try artist-credit first (this is the main artist)
        artist_credits = mb_data.get('artist-credit', [])
        if artist_credits:
            for credit in artist_credits:
                if isinstance(credit, dict) and 'artist' in credit:
                    artist = credit['artist']
                    return {
                        'name': artist.get('name'),
                        'mbid': artist.get('id'),
                        'source': 'recording artist-credit'
                    }
        
        # Try first release artist-credit
        releases = mb_data.get('releases', [])
        if releases:
            first_release = releases[0]
            release_artist_credits = first_release.get('artist-credit', [])
            if release_artist_credits:
                for credit in release_artist_credits:
                    if isinstance(credit, dict) and 'artist' in credit:
                        artist = credit['artist']
                        return {
                            'name': artist.get('name'),
                            'mbid': artist.get('id'),
                            'source': 'release artist-credit'
                        }
        
        # Try recording relationships (look for 'instrument' or 'performance' types)
        relations = mb_data.get('relations', [])
        for relation in relations:
            if relation.get('target-type') == 'artist':
                rel_type = relation.get('type', '')
                # Prioritize actual performance relationships
                if rel_type in ['instrument', 'vocal', 'performer']:
                    artist = relation.get('artist', {})
                    if artist:
                        return {
                            'name': artist.get('name'),
                            'mbid': artist.get('id'),
                            'source': f'recording relationship ({rel_type})'
                        }
        
        logger.warning("Could not extract artist from any source")
        return None
    
    def get_or_create_performer(self, conn, artist_name, artist_mbid=None):
        """
        Get existing performer or create new one
        
        Args:
            conn: Database connection
            artist_name: Performer name
            artist_mbid: MusicBrainz ID (optional)
            
        Returns:
            Performer ID or None
        """
        if not artist_name:
            return None
        
        with conn.cursor() as cur:
            # Check if performer exists
            if artist_mbid:
                cur.execute("""
                    SELECT id FROM performers
                    WHERE musicbrainz_id = %s
                """, (artist_mbid,))
                result = cur.fetchone()
                if result:
                    return result['id']
            
            # Check by name
            cur.execute("""
                SELECT id FROM performers
                WHERE name = %s
            """, (artist_name,))
            result = cur.fetchone()
            if result:
                return result['id']
            
            # Create new performer
            if self.dry_run:
                logger.info(f"    [DRY RUN] Would create performer: {artist_name}")
                return None
            
            cur.execute("""
                INSERT INTO performers (name, musicbrainz_id)
                VALUES (%s, %s)
                RETURNING id
            """, (artist_name, artist_mbid))
            
            result = cur.fetchone()
            conn.commit()
            return result['id']
    
    def fix_recording_leader(self, recording):
        """
        Fix a recording that has no leader
        
        Args:
            recording: Recording dict from database
            
        Returns:
            True if fixed, False otherwise
        """
        try:
            logger.info(f"Processing: {recording['song_title']} - {recording['album_title']} ({recording['recording_year'] or 'N/A'})")
            logger.info(f"  Recording ID: {recording['id']}")
            logger.info(f"  MusicBrainz ID: {recording['musicbrainz_id']}")
            logger.info(f"  Current performers: {recording['performer_count']}, Leaders: {recording['leader_count']}")
            
            # Fetch MusicBrainz data
            mb_data = self.fetch_musicbrainz_recording(recording['musicbrainz_id'])
            if not mb_data:
                logger.warning("  ✗ Could not fetch MusicBrainz data")
                return False
            
            # Extract artist
            artist_info = self.extract_artist_from_recording(mb_data)
            if not artist_info:
                logger.warning("  ✗ Could not determine lead artist from MusicBrainz data")
                return False
            
            logger.info(f"  ✓ Found artist: {artist_info['name']} (from {artist_info['source']})")
            
            # Get or create performer
            with get_db_connection() as conn:
                performer_id = self.get_or_create_performer(
                    conn, 
                    artist_info['name'], 
                    artist_info.get('mbid')
                )
                
                if not performer_id and not self.dry_run:
                    logger.error("  ✗ Failed to get/create performer")
                    return False
                
                with conn.cursor() as cur:
                    if self.dry_run:
                        logger.info(f"    [DRY RUN] Would link {artist_info['name']} as leader to recording")
                        return True
                    
                    # Check if this performer is already linked to this recording
                    cur.execute("""
                        SELECT id, role FROM recording_performers
                        WHERE recording_id = %s AND performer_id = %s
                    """, (recording['id'], performer_id))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        # Update role to leader
                        logger.info(f"    Updating existing performer to 'leader' role")
                        cur.execute("""
                            UPDATE recording_performers
                            SET role = 'leader'
                            WHERE id = %s
                        """, (existing['id'],))
                    else:
                        # Link as new leader
                        logger.info(f"    Linking as new leader performer")
                        cur.execute("""
                            INSERT INTO recording_performers (recording_id, performer_id, role)
                            VALUES (%s, %s, 'leader')
                        """, (recording['id'], performer_id))
                    
                    conn.commit()
                    logger.info(f"  ✓ Successfully fixed recording")
                    return True
                    
        except Exception as e:
            logger.error(f"Error fixing recording: {e}", exc_info=True)
            self.stats['errors'] += 1
            return False
    
    def run(self, limit=None, specific_mbids=None):
        """
        Main processing method
        
        Args:
            limit: Maximum number of recordings to process (None for all)
            specific_mbids: List of specific MusicBrainz IDs to process (None for all)
        """
        logger.info("="*80)
        logger.info("FIX UNKNOWN LEAD ARTISTS")
        logger.info("="*80)
        
        if self.dry_run:
            logger.info("*** DRY RUN MODE - No database changes will be made ***")
            logger.info("")
        
        # Get recordings to fix
        if specific_mbids:
            logger.info(f"Processing {len(specific_mbids)} specific recording(s)")
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    placeholders = ','.join(['%s'] * len(specific_mbids))
                    cur.execute(f"""
                        SELECT DISTINCT
                            r.id,
                            r.musicbrainz_id,
                            r.album_title,
                            r.recording_year,
                            s.title as song_title,
                            (
                                SELECT COUNT(*) 
                                FROM recording_performers rp 
                                WHERE rp.recording_id = r.id
                            ) as performer_count,
                            (
                                SELECT COUNT(*) 
                                FROM recording_performers rp 
                                WHERE rp.recording_id = r.id AND rp.role = 'leader'
                            ) as leader_count
                        FROM recordings r
                        JOIN songs s ON r.song_id = s.id
                        WHERE r.musicbrainz_id IN ({placeholders})
                    """, specific_mbids)
                    
                    recordings = cur.fetchall()
        else:
            logger.info("Finding recordings without leaders...")
            recordings = self.get_recordings_without_leaders()
        
        if not recordings:
            logger.info("No recordings to process!")
            return True
        
        logger.info(f"Found {len(recordings)} recording(s) to process")
        
        if limit:
            recordings = recordings[:limit]
            logger.info(f"Processing first {limit} recordings")
        
        logger.info("")
        
        # Process each recording
        for recording in recordings:
            self.stats['recordings_checked'] += 1
            self.stats['recordings_no_leader'] += 1
            
            success = self.fix_recording_leader(recording)
            
            if success:
                self.stats['recordings_fixed'] += 1
            else:
                self.stats['recordings_failed'] += 1
            
            logger.info("")
        
        # Print summary
        self.print_summary()
        
        return True
    
    def print_summary(self):
        """Print processing summary"""
        logger.info("="*80)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*80)
        logger.info(f"Recordings checked:    {self.stats['recordings_checked']}")
        logger.info(f"Recordings w/o leader: {self.stats['recordings_no_leader']}")
        logger.info(f"Recordings fixed:      {self.stats['recordings_fixed']}")
        logger.info(f"Recordings failed:     {self.stats['recordings_failed']}")
        logger.info(f"Errors:                {self.stats['errors']}")
        logger.info("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Fix recordings showing "Unknown" as lead artist',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan all recordings and fix those without leaders
  python fix_unknown_leaders.py
  
  # Dry run to see what would be done
  python fix_unknown_leaders.py --dry-run
  
  # Process only first 10 recordings
  python fix_unknown_leaders.py --limit 10
  
  # Fix specific MusicBrainz recording IDs
  python fix_unknown_leaders.py --mbids a9227d9c-1262-455f-91b0-af403d50447a 6df1bb1e-2372-4804-b042-c35d2650e00f
  
  # Use longer delay for more conservative rate limiting (helps avoid connection errors)
  python fix_unknown_leaders.py --delay 3.0 --mbids 6df1bb1e-2372-4804-b042-c35d2650e00f
  
  # Enable debug logging
  python fix_unknown_leaders.py --debug
  
  # Combination
  python fix_unknown_leaders.py --dry-run --debug --limit 5 --delay 2.0
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
        '--limit',
        type=int,
        help='Maximum number of recordings to process'
    )
    
    parser.add_argument(
        '--mbids',
        nargs='+',
        help='Specific MusicBrainz recording IDs to process'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=1.5,
        help='Delay between API requests in seconds (default: 1.5)'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create fixer and run
    fixer = LeaderFixer(dry_run=args.dry_run, request_delay=args.delay)
    
    try:
        success = fixer.run(limit=args.limit, specific_mbids=args.mbids)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nProcess cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()