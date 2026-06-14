#!/usr/bin/env python3
"""
Gather Performers from MusicBrainz for a Specific Recording
Fetches performer and instrument data for a single MusicBrainz recording and imports into database
"""

import sys
import json
import time
import argparse
import logging
import os
from datetime import datetime
import requests

# Import shared database utilities
sys.path.insert(0, '/mnt/project/scripts')
from db_utils import get_db_connection
from integrations.musicbrainz.performer_importer import PerformerImporter
from core.http_client import make_session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('log/mb_performer_import.log')
    ]
)
logger = logging.getLogger(__name__)

class SingleRecordingImporter:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.session = make_session()
        self.performer_importer = PerformerImporter(dry_run=dry_run)
        self.stats = {
            'recordings_created': 0,
            'recordings_updated': 0,
            'performers_linked': 0,
            'errors': 0
        }
    
    def find_song_by_name(self, song_name):
        """Find a song in the database by name"""
        logger.info(f"Searching for song: {song_name}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title, composer, musicbrainz_id
                    FROM songs
                    WHERE title ILIKE %s
                    ORDER BY title
                """, (f'%{song_name}%',))
                
                results = cur.fetchall()
                
                if not results:
                    logger.warning(f"No songs found matching: {song_name}")
                    return None
                
                if len(results) > 1:
                    logger.info(f"Found {len(results)} matching songs:")
                    for i, song in enumerate(results, 1):
                        logger.info(f"  {i}. {song['title']} by {song['composer']}")
                        logger.info(f"     ID: {song['id']}")
                    
                    logger.info(f"Using first result: {results[0]['title']}")
                
                return results[0]
    
    def find_song_by_id(self, song_id):
        """Find a song in the database by ID"""
        logger.info(f"Looking up song ID: {song_id}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title, composer, musicbrainz_id
                    FROM songs
                    WHERE id = %s
                """, (song_id,))
                
                result = cur.fetchone()
                
                if not result:
                    logger.error(f"Song not found with ID: {song_id}")
                    return None
                
                return result
    
    def fetch_recording_from_musicbrainz(self, recording_id):
        """
        Fetch detailed information about a specific recording from MusicBrainz
        
        Args:
            recording_id: MusicBrainz recording MBID
            
        Returns:
            Recording data dict or None if error
        """
        url = f"https://musicbrainz.org/ws/2/recording/{recording_id}"
        params = {
            'inc': 'releases+artist-credits+artist-rels',
            'fmt': 'json'
        }
        
        try:
            logger.info(f"Fetching MusicBrainz recording: {recording_id}")
            time.sleep(1.0)  # Rate limiting - MusicBrainz requires 1 req/sec
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Log key information
            title = data.get('title', 'Unknown')
            artist_credits = data.get('artist-credit', [])
            artists = [ac.get('artist', {}).get('name', 'Unknown') for ac in artist_credits]
            artist_str = ', '.join(artists) if artists else 'Unknown'
            
            releases = data.get('releases', [])
            release_count = len(releases)
            
            logger.info(f"✓ Recording: '{title}' by {artist_str}")
            logger.info(f"  Total releases: {release_count}")
            
            if releases:
                first_release = releases[0]
                logger.info(f"  First release: '{first_release.get('title', 'Unknown')}' ({first_release.get('date', 'Unknown date')})")
            
            return data
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error fetching recording: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            self.stats['errors'] += 1
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching recording: {e}")
            self.stats['errors'] += 1
            return None
    
    def get_or_create_recording(self, conn, song_id, recording_data):
        """
        Get existing recording or create new one
        
        Args:
            conn: Database connection
            song_id: Song UUID
            recording_data: MusicBrainz recording data
            
        Returns:
            Recording ID or None if error
        """
        releases = recording_data.get('releases', [])
        if not releases:
            logger.warning("Recording has no releases")
            return None
        
        # Use the first release
        release = releases[0]
        album_title = release.get('title', 'Unknown Album')
        
        # Extract date
        release_date = release.get('date', '')
        release_year = None
        formatted_date = None
        
        if release_date:
            try:
                release_year = int(release_date.split('-')[0])
                parts = release_date.split('-')
                if len(parts) == 3:
                    formatted_date = release_date
                elif len(parts) == 2:
                    formatted_date = f"{parts[0]}-{parts[1]}-01"
                elif len(parts) == 1:
                    formatted_date = f"{parts[0]}-01-01"
            except (ValueError, IndexError):
                logger.debug(f"Could not parse date: {release_date}")
        
        with conn.cursor() as cur:
            # Check if recording exists
            cur.execute("""
                SELECT id FROM recordings
                WHERE song_id = %s AND album_title = %s
            """, (song_id, album_title))
            
            result = cur.fetchone()
            
            if result:
                recording_id = result['id']
                logger.info(f"Found existing recording: {album_title} (ID: {recording_id})")
                return recording_id
            
            # Create new recording
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create recording: {album_title} ({release_year or 'unknown year'})")
                logger.info(f"[DRY RUN]   Date: {formatted_date or 'None'}")
                return None
            
            cur.execute("""
                INSERT INTO recordings (
                    song_id, album_title, recording_year, recording_date,
                    is_canonical
                )
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (
                song_id,
                album_title,
                release_year,
                formatted_date,
                False
            ))
            
            recording_id = cur.fetchone()['id']
            logger.info(f"✓ Created recording: {album_title} (ID: {recording_id})")
            self.stats['recordings_created'] += 1
            
            return recording_id
    
    def import_recording(self, song_identifier, mb_recording_id):
        """
        Main method to import a recording with performers
        
        Args:
            song_identifier: Song name or database ID
            mb_recording_id: MusicBrainz recording MBID
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("="*80)
        logger.info("MusicBrainz Performer Import - Single Recording")
        logger.info("="*80)
        
        if self.dry_run:
            logger.info("*** DRY RUN MODE - No database changes will be made ***")
            logger.info("")
        
        # Find the song
        if song_identifier.startswith('song-') or len(song_identifier) == 36:
            song = self.find_song_by_id(song_identifier)
        else:
            song = self.find_song_by_name(song_identifier)
        
        if not song:
            logger.error("Song not found. Exiting.")
            return False
        
        logger.info("")
        logger.info(f"Song: {song['title']}")
        logger.info(f"Composer: {song['composer']}")
        logger.info(f"Database ID: {song['id']}")
        logger.info(f"MusicBrainz Recording ID: {mb_recording_id}")
        logger.info("")
        
        # Fetch recording from MusicBrainz
        recording_data = self.fetch_recording_from_musicbrainz(mb_recording_id)
        
        if not recording_data:
            logger.error("Failed to fetch recording from MusicBrainz")
            return False
        
        logger.info("")
        
        # Import into database
        with get_db_connection() as conn:
            try:
                # Get or create recording
                recording_id = self.get_or_create_recording(conn, song['id'], recording_data)
                
                if not recording_id:
                    if self.dry_run:
                        # In dry run, we don't have a real ID, but we can still show what would happen
                        logger.info("")
                        logger.info("Performer information that would be imported:")
                        self.performer_importer.link_performers_to_recording(conn, None, recording_data)
                    else:
                        logger.error("Failed to get/create recording")
                        return False
                else:
                    # Link performers
                    logger.info("")
                    logger.info("Linking performers...")
                    performers_linked = self.performer_importer.link_performers_to_recording(
                        conn, recording_id, recording_data
                    )
                    
                    if not self.dry_run:
                        conn.commit()
                        logger.info(f"✓ Linked {performers_linked} performer entries")
                        self.stats['performers_linked'] = performers_linked
                
                # Print summary
                logger.info("")
                logger.info("="*80)
                logger.info("IMPORT SUMMARY")
                logger.info("="*80)
                logger.info(f"Recordings created:  {self.stats['recordings_created']}")
                logger.info(f"Performers linked:   {self.stats['performers_linked']}")
                logger.info(f"Performers created:  {self.performer_importer.stats['performers_created']}")
                logger.info(f"Instruments created: {self.performer_importer.stats['instruments_created']}")
                logger.info(f"Errors:              {self.stats['errors']}")
                logger.info("="*80)
                
                return True
                
            except Exception as e:
                logger.error(f"Error during import: {e}", exc_info=True)
                if not self.dry_run:
                    conn.rollback()
                self.stats['errors'] += 1
                return False

def main():
    parser = argparse.ArgumentParser(
        description='Import performer data from a specific MusicBrainz recording',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import by song name and MusicBrainz recording ID
  python gather_performers_from_mb_for_release.py --name "Take Five" --recording-id abc123def-4567-8901-abcd-ef1234567890
  
  # Import by song database ID
  python gather_performers_from_mb_for_release.py --id a1b2c3d4-e5f6-7890-abcd-ef1234567890 --recording-id abc123def-4567-8901-abcd-ef1234567890
  
  # Dry run to see what would be imported
  python gather_performers_from_mb_for_release.py --name "Blue in Green" --recording-id abc123 --dry-run
  
  # Enable debug logging
  python gather_performers_from_mb_for_release.py --name "Autumn Leaves" --recording-id abc123 --debug

Note: The recording ID is the MusicBrainz Recording MBID (not release ID or work ID).
You can find this by searching on musicbrainz.org and copying the ID from a specific recording's URL.
        """
    )
    
    # Song selection arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--name',
        help='Song name'
    )
    group.add_argument(
        '--id',
        help='Song database ID'
    )
    
    # Recording ID argument
    parser.add_argument(
        '--recording-id',
        required=True,
        help='MusicBrainz recording MBID (the ID from a specific recording, not release or work)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be imported without making changes'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create importer and run
    importer = SingleRecordingImporter(dry_run=args.dry_run)
    
    # Determine song identifier
    song_identifier = args.name if args.name else args.id
    
    try:
        success = importer.import_recording(song_identifier, args.recording_id)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()