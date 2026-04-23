"""
MusicBrainz song-metadata updaters.

Domain orchestrations that combine a MusicBrainz lookup with a write to
the songs table: pull composer / Wikipedia URL / composed year off the
MB work and patch our row if it's missing. Each function is idempotent —
if the song already has the field populated, it returns without touching
the DB.

These aren't pure data access (see db.py) or pure HTTP (see client.py);
they're the glue that ties the two together for a single song-level
background task.
"""

import logging

from integrations.musicbrainz.client import MusicBrainzSearcher

logger = logging.getLogger(__name__)


def update_song_composer(song_id: str, mb_searcher: MusicBrainzSearcher = None) -> bool:
    """
    Update song composer from MusicBrainz if not already set
    
    Checks for composer, writer, and lyricist relationships in MusicBrainz work data.
    
    Args:
        song_id: UUID of the song
        mb_searcher: Optional MusicBrainzSearcher instance (creates new one if not provided)
        
    Returns:
        bool: True if composer was updated, False otherwise
    """
    logger.debug("in update_song_composer")
    from db_utils import get_db_connection
    
    try:
        # Check if song has musicbrainz_id and no composer
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT musicbrainz_id, composer FROM songs WHERE id = %s",
                    (song_id,)
                )
                row = cur.fetchone()
                
                if not row:
                    return False
                
                mb_id = row['musicbrainz_id']
                composer = row['composer']                
                # Skip if no MusicBrainz ID or already has composer
                if not mb_id or composer:
                    return False

        logger.debug("song is missing composer")        
        # Create MusicBrainzSearcher if not provided
        if mb_searcher is None:
            mb_searcher = MusicBrainzSearcher()
        
        # Fetch work details from MusicBrainz
        work_data = mb_searcher.get_work_recordings(mb_id)
        
        if not work_data:
            logger.debug("No MusicBrainz work data found")
            return False
        
        # Extract composer/writer from artist relationships
        # Check multiple relationship types: composer, writer, lyricist
        creators = []
        creator_types_found = set()
        
        for relation in work_data.get('relations', []):
            rel_type = relation.get('type')
            
            # Check for any creator relationship type
            if rel_type in ['composer', 'writer', 'lyricist']:
                artist = relation.get('artist', {})
                creator_name = artist.get('name')
                
                if creator_name and creator_name not in creators:
                    creators.append(creator_name)
                    creator_types_found.add(rel_type)
        
        if not creators:
            logger.debug("No composer, writer, or lyricist found in MusicBrainz work data")
            return False
        
        # Join multiple creators with comma
        composer_name = ', '.join(creators)
        types_str = ', '.join(sorted(creator_types_found))
        
        # Update song with composer
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE songs SET composer = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                    (composer_name, song_id)
                )
                conn.commit()
        
        logger.info(f"✓ Updated composer to '{composer_name}' (from {types_str})")
        return True        

    except Exception as e:
        logger.error(f"Error updating composer: {e}")
        return False


def update_song_wikipedia_url(song_id: str, mb_searcher: MusicBrainzSearcher = None, dry_run: bool = False) -> bool:
    """
    Update song Wikipedia URL from MusicBrainz if not already set
    
    Checks for Wikipedia URL in MusicBrainz work data URL relationships.
    
    Args:
        song_id: UUID of the song
        mb_searcher: Optional MusicBrainzSearcher instance (creates new one if not provided)
        dry_run: If True, show what would be done without making changes
        
    Returns:
        bool: True if Wikipedia URL was updated (or would be in dry-run), False otherwise
    """
    from db_utils import get_db_connection
    
    try:
        # Check if song has musicbrainz_id and current wikipedia_url
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT musicbrainz_id, wikipedia_url, title FROM songs WHERE id = %s",
                    (song_id,)
                )
                row = cur.fetchone()
                
                if not row:
                    return False
                
                mb_id = row['musicbrainz_id']
                current_wikipedia_url = row['wikipedia_url']
                song_title = row['title']
                
                # Skip if no MusicBrainz ID
                if not mb_id:
                    logger.debug("Song has no MusicBrainz ID, skipping Wikipedia URL update")
                    return False
        
        # Create MusicBrainzSearcher if not provided
        if mb_searcher is None:
            mb_searcher = MusicBrainzSearcher()
        
        # Fetch work details from MusicBrainz
        work_data = mb_searcher.get_work_recordings(mb_id)
        
        if not work_data:
            logger.debug("No MusicBrainz work data found")
            return False
        
        # Extract Wikipedia URL from URL relationships
        wikipedia_url = None
        wikidata_id = None
        
        for relation in work_data.get('relations', []):
            rel_type = relation.get('type')
            
            # Check for Wikipedia URL relationship (preferred - direct link)
            if rel_type == 'wikipedia':
                url_data = relation.get('url', {})
                resource = url_data.get('resource')
                
                if resource:
                    wikipedia_url = resource
                    logger.debug(f"Found direct Wikipedia URL: {wikipedia_url}")
                    break
            
            # Also collect Wikidata ID as fallback
            elif rel_type == 'wikidata' and not wikidata_id:
                url_data = relation.get('url', {})
                resource = url_data.get('resource')
                
                if resource:
                    # Extract Wikidata ID from URL (e.g., https://www.wikidata.org/wiki/Q12345 -> Q12345)
                    if '/wiki/' in resource:
                        wikidata_id = resource.split('/wiki/')[-1]
                        logger.debug(f"Found Wikidata ID: {wikidata_id}")
        
        # If no direct Wikipedia URL, try to get it from Wikidata
        if not wikipedia_url and wikidata_id:
            logger.debug(f"No direct Wikipedia URL, trying Wikidata lookup for {wikidata_id}")
            wikipedia_url = mb_searcher.get_wikipedia_from_wikidata(wikidata_id)
            
            if wikipedia_url:
                logger.debug(f"Got Wikipedia URL from Wikidata: {wikipedia_url}")
        
        if not wikipedia_url:
            logger.debug("No Wikipedia URL found (checked direct link and Wikidata)")
            return False
        
        # Check if song already has this Wikipedia URL
        if current_wikipedia_url:
            if current_wikipedia_url == wikipedia_url:
                logger.debug(f"Song already has this Wikipedia URL: {wikipedia_url}")
            else:
                logger.info(f"Song '{song_title}' already has a different Wikipedia URL (existing: {current_wikipedia_url}, found: {wikipedia_url})")
            return False
        
        # Update song with Wikipedia URL
        if dry_run:
            logger.info(f"[DRY RUN] Would update Wikipedia URL for '{song_title}': {wikipedia_url}")
            return True
        else:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE songs SET wikipedia_url = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (wikipedia_url, song_id)
                    )
                    conn.commit()
            
            logger.info(f"✓ Updated Wikipedia URL for '{song_title}': {wikipedia_url}")
            return True
    
    except Exception as e:
        logger.error(f"Error updating Wikipedia URL: {e}")
        return False


def update_song_composed_year(song_id: str, mb_searcher: MusicBrainzSearcher = None, dry_run: bool = False) -> bool:
    """
    Update song composed_year from MusicBrainz if not already set

    Extracts the earliest recording date from MusicBrainz work data as an
    approximation of the composition year.

    Args:
        song_id: UUID of the song
        mb_searcher: Optional MusicBrainzSearcher instance (creates new one if not provided)
        dry_run: If True, show what would be done without making changes

    Returns:
        bool: True if composed_year was updated (or would be in dry-run), False otherwise
    """
    from db_utils import get_db_connection

    def extract_year_from_date(date_str):
        """Extract year from a date string (YYYY, YYYY-MM, or YYYY-MM-DD)"""
        if not date_str:
            return None
        try:
            return int(date_str[:4])
        except (ValueError, TypeError):
            return None

    try:
        # Check if song has musicbrainz_id and current composed_year
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT musicbrainz_id, second_mb_id, composed_year, title FROM songs WHERE id = %s",
                    (song_id,)
                )
                row = cur.fetchone()

                if not row:
                    return False

                mb_id = row['musicbrainz_id']
                second_mb_id = row['second_mb_id']
                current_year = row['composed_year']
                song_title = row['title']

                # Skip if no MusicBrainz ID or already has composed_year
                if not mb_id:
                    logger.debug("Song has no MusicBrainz ID, skipping composed_year update")
                    return False

                if current_year:
                    logger.debug(f"Song already has composed_year: {current_year}")
                    return False

        # Create MusicBrainzSearcher if not provided
        if mb_searcher is None:
            mb_searcher = MusicBrainzSearcher()

        def get_year_from_work_data(work_data):
            """Extract composition year from MusicBrainz work data"""
            if not work_data:
                return None

            # Strategy 1: Check composer/lyricist/writer relations for begin date
            composer_year = None
            for relation in work_data.get('relations', []):
                rel_type = relation.get('type')
                if rel_type in ('composer', 'lyricist', 'writer'):
                    begin_date = relation.get('begin')
                    if begin_date:
                        year = extract_year_from_date(begin_date)
                        if year and (composer_year is None or year < composer_year):
                            composer_year = year

            if composer_year:
                return composer_year

            # Strategy 2: Fall back to earliest recording date
            earliest_year = None
            for relation in work_data.get('relations', []):
                if relation.get('type') == 'performance':
                    recording = relation.get('recording', {})
                    first_release = recording.get('first-release-date')
                    if first_release:
                        year = extract_year_from_date(first_release)
                        if year and (earliest_year is None or year < earliest_year):
                            earliest_year = year

                    begin_date = relation.get('begin')
                    if begin_date:
                        year = extract_year_from_date(begin_date)
                        if year and (earliest_year is None or year < earliest_year):
                            earliest_year = year

            return earliest_year

        # Get year from primary MusicBrainz ID
        work_data = mb_searcher.get_work_recordings(mb_id)
        earliest_year = get_year_from_work_data(work_data)

        # Also check second_mb_id if present, use earlier year
        if second_mb_id:
            second_work_data = mb_searcher.get_work_recordings(second_mb_id)
            second_year = get_year_from_work_data(second_work_data)
            if second_year:
                if earliest_year is None or second_year < earliest_year:
                    earliest_year = second_year

        if not earliest_year:
            logger.debug("No composition year found in MusicBrainz work data")
            return False

        # Update song with composed_year
        if dry_run:
            logger.info(f"[DRY RUN] Would update composed_year for '{song_title}': {earliest_year}")
            return True
        else:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE songs SET composed_year = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s",
                        (earliest_year, song_id)
                    )
                    conn.commit()

            logger.info(f"✓ Updated composed_year for '{song_title}': {earliest_year}")
            return True

    except Exception as e:
        logger.error(f"Error updating composed_year: {e}")
        return False
