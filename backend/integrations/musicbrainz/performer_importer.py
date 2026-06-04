#!/usr/bin/env python3
"""
Shared utilities for importing performer and instrument data from MusicBrainz

UPDATED: Recording-Centric Performer Architecture
- Performers are now associated with RECORDINGS (primary)
- release_performers is kept for release-specific credits only (producers, engineers)
- New add_performers_to_recording() method aggregates performers from all releases
- Special logging when adding performers to recordings that already have performers

PERFORMANCE OPTIMIZATION (2025-12):
- Batch fetches performer lookups (by MBID and name) in single queries
- Batch fetches existing performer links in single query
- Eliminates per-performer database round trips for "already linked" checks
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from db_utils import get_db_connection
from integrations.musicbrainz.utils import MusicBrainzSearcher


def normalize_group_name(group_name):
    """
    Remove common group suffixes to get the core artist name
    
    Examples:
        "Ahmad Jamal Quintet" → "ahmad jamal"
        "Gene Krupa and His Orchestra" → "gene krupa"
    """
    if not group_name:
        return ""

    name = group_name.lower().strip()

    # Strip leading articles (the, a, an)
    name = re.sub(r'^(?:the|a|an)\s+', '', name)

    # Patterns to remove (order matters - try longest first)
    # Note: (?:and|&) handles both "and" and "&" which MusicBrainz uses interchangeably
    patterns = [
        r'\s+(?:and|&)\s+his\s+orchestra\b.*$',
        r'\s+(?:and|&)\s+his\s+band\b.*$',
        r'\s+(?:and|&)\s+his\s+quintet\b.*$',
        r'\s+(?:and|&)\s+his\s+quartet\b.*$',
        r'\s+(?:and|&)\s+his\s+trio\b.*$',
        r'\s+(?:and|&)\s+her\s+orchestra\b.*$',
        r'\s+(?:and|&)\s+her\s+band\b.*$',
        r'\s+(?:and|&)\s+her\s+quintet\b.*$',
        r'\s+(?:and|&)\s+her\s+quartet\b.*$',
        r'\s+(?:and|&)\s+her\s+trio\b.*$',
        r'\s+orchestra\b.*$',
        r'\s+big\s+band\b.*$',
        r'\s+band\b.*$',
        r'\s+ensemble\b.*$',
        r'\s+trio\b.*$',
        r'\s+quartet\b.*$',
        r'\s+quintet\b.*$',
        r'\s+sextet\b.*$',
        r'\s+septet\b.*$',
        r'\s+octet\b.*$',
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, '', name)
    
    return name.strip()


def is_performer_leader_of_group(performer_name, group_name):
    """
    Check if a performer is likely the leader of a group
    
    Args:
        performer_name: Individual performer name (e.g., "Ahmad Jamal")
        group_name: Group/ensemble name (e.g., "Ahmad Jamal Quintet")
        
    Returns:
        bool: True if the performer is likely the group leader
    """
    if not performer_name or not group_name:
        return False
    
    performer_normalized = performer_name.lower().strip()
    group_normalized = normalize_group_name(group_name)
    
    return performer_normalized == group_normalized


logger = logging.getLogger(__name__)

# Special logger for performer additions to existing recordings
# This creates a separate log file for review
_additions_logger = None

def get_additions_logger():
    """Get or create the special additions logger"""
    global _additions_logger
    if _additions_logger is None:
        _additions_logger = logging.getLogger('performer_additions')
        _additions_logger.setLevel(logging.INFO)
        
        # Create log directory if needed
        log_dir = Path(__file__).parent / 'scripts' / 'log'
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create file handler
        log_file = log_dir / 'performer_additions.log'
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        _additions_logger.addHandler(handler)
        
        # Don't propagate to root logger
        _additions_logger.propagate = False
    
    return _additions_logger


# MusicBrainz instrument relationships carry an `attributes` array that mixes
# actual instrument names with credit/qualifier attributes. The qualifiers
# below describe *how* an instrument was played (e.g. as a guest, a solo, an
# additional part) rather than naming an instrument, so they must not be stored
# as instruments. See https://musicbrainz.org/relationship-attributes
# These are matched case-insensitively against the attribute name.
INSTRUMENT_QUALIFIER_ATTRIBUTES = frozenset({
    'additional',
    'minor',
    'solo',
    'guest',
})


class PerformerImporter:
    """
    Handles importing performers and instruments from MusicBrainz data
    
    UPDATED: Recording-Centric Architecture
    - Primary method is now add_performers_to_recording()
    - link_performers_to_release() kept for release-specific credits only
    
    PERFORMANCE OPTIMIZED (2025-12):
    - Batch lookup for performers (by MBID and name)
    - Batch lookup for existing performer links
    - Minimal database round-trips per recording
    """
    
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.mb_searcher = MusicBrainzSearcher()
        self.stats = {
            'performers_created': 0,
            'instruments_created': 0,
            'performer_links_created': 0,
            'recordings_with_new_performers': 0,  # Recordings that got new performers added
        }
    
    # ========================================================================
    # DATA QUALITY CHECKS: Determine if MusicBrainz has better data
    # ========================================================================

    def has_better_performer_data(self, conn, recording_id, mb_recording_data):
        """
        Check if MusicBrainz has better performer data than what's in our database.

        "Better" means:
        - MusicBrainz has instrument-type relations (specific instruments)
        - Our database has performers but WITHOUT instrument info

        Args:
            conn: Database connection
            recording_id: Our database recording ID
            mb_recording_data: MusicBrainz recording data with relations

        Returns:
            bool: True if MusicBrainz has better data and we should re-import
        """
        if not recording_id or not mb_recording_data:
            return False

        # Check if MusicBrainz has instrument-type relations
        relations = mb_recording_data.get('relations') or []
        mb_has_instruments = any(
            r.get('type') == 'instrument' and r.get('attributes')
            for r in relations
            if r.get('target-type') == 'artist'
        )

        if not mb_has_instruments:
            return False

        # Check if our database lacks instrument info
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total_performers,
                    COUNT(instrument_id) as performers_with_instruments
                FROM recording_performers
                WHERE recording_id = %s
            """, (recording_id,))
            row = cur.fetchone()

            total = row['total_performers']
            with_instruments = row['performers_with_instruments']

            # If we have performers but none have instruments, MB data is better
            if total > 0 and with_instruments == 0:
                logger.info(f"  MusicBrainz has better performer data (instruments available)")
                return True

        return False

    def clear_recording_performers(self, conn, recording_id):
        """
        Remove all performer links for a recording to allow fresh import.

        Args:
            conn: Database connection
            recording_id: Recording ID to clear

        Returns:
            int: Number of performer links removed
        """
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would clear performers for recording")
            return 0

        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM recording_performers
                WHERE recording_id = %s
                RETURNING performer_id
            """, (recording_id,))
            deleted = cur.rowcount
            if deleted > 0:
                logger.info(f"  Cleared {deleted} old performer links (will re-import with better data)")
            return deleted

    # ========================================================================
    # PRIMARY METHOD: Add performers to recordings (OPTIMIZED)
    # ========================================================================

    def add_performers_to_recording(self, conn, recording_id, recording_data,
                                     source_release_title=None):
        """
        Add performers to a recording, aggregating from release data.
        
        This is the PRIMARY method for associating performers with recordings.
        It implements the maximalist approach: any performer found on any release
        gets added to the recording.
        
        PERFORMANCE OPTIMIZED (2025-12):
        - Batch fetches all performer lookups in 2 queries (by MBID, by name)
        - Batch fetches all existing performer-recording links in 1 query
        - Per-performer queries only for NEW performers that need creation
        
        Args:
            conn: Database connection
            recording_id: Our database recording ID
            recording_data: MusicBrainz recording data
            source_release_title: Title of the release being processed (for logging)
            
        Returns:
            Number of NEW performers added (0 if all already existed)
        """
        if not recording_data:
            logger.debug("  No recording data provided")
            return 0

        # Extract performers from recording data
        performers_to_add = self._extract_performers_from_recording(recording_data)

        if not performers_to_add:
            logger.debug("  No performers found in recording data")
            # Even when MB has no performer relations for this recording, the
            # artist-credit still tells us who the leader is. Try the fallback
            # below so we don't leak another leaderless recording into the DB.
            return self._ensure_leader_from_artist_credit(
                conn, recording_id, recording_data
            ) if (conn and recording_id) else 0

        # Get leader information from artist credits
        leader_mbids, leader_names = self._extract_leader_info_from_recording(recording_data)

        if self.dry_run:
            self._log_performers_dry_run(performers_to_add, leader_mbids, leader_names)
            return len(performers_to_add)

        if not conn or not recording_id:
            return 0

        # Check how many performers already exist for this recording
        existing_performer_count = self._get_recording_performer_count(conn, recording_id)

        # =======================================================================
        # PERFORMANCE OPTIMIZATION: Batch lookup all performers at once
        # =======================================================================

        # Collect all MBIDs and names to look up
        mbids_to_lookup = [p.get('mbid') for p in performers_to_add if p.get('mbid')]
        names_to_lookup = [p.get('name') for p in performers_to_add if p.get('name')]

        # Batch fetch existing performers (single query for MBIDs, single for names)
        performer_cache = self._batch_get_performers(conn, mbids_to_lookup, names_to_lookup)

        # Batch fetch existing performer links for this recording (single query)
        existing_links = self._batch_get_recording_performer_links(
            conn, recording_id, list(performer_cache.values())
        )

        # =======================================================================
        # Now process performers using cached data
        # =======================================================================

        new_performers_added = 0
        new_performer_names = []

        with conn.cursor() as cur:
            for performer_data in performers_to_add:
                performer_mbid = performer_data.get('mbid')
                performer_name = performer_data.get('name')

                # Try to get from cache first
                performer_id = None
                cache_key = None

                if performer_mbid and f"mbid:{performer_mbid}" in performer_cache:
                    cache_key = f"mbid:{performer_mbid}"
                    performer_id = performer_cache[cache_key]
                elif performer_name and f"name:{performer_name.lower()}" in performer_cache:
                    cache_key = f"name:{performer_name.lower()}"
                    performer_id = performer_cache[cache_key]

                if not performer_id:
                    # Need to create this performer (not in DB)
                    performer_id = self._create_performer(
                        conn,
                        performer_name,
                        performer_mbid,
                        sort_name=performer_data.get('sort_name'),
                        artist_type=performer_data.get('artist_type'),
                        disambiguation=performer_data.get('disambiguation')
                    )
                    if performer_id:
                        # Add to cache for future lookups in this batch
                        if performer_mbid:
                            performer_cache[f"mbid:{performer_mbid}"] = performer_id
                        if performer_name:
                            performer_cache[f"name:{performer_name.lower()}"] = performer_id

                if not performer_id:
                    continue

                # Check if already linked (using pre-fetched data)
                if performer_id in existing_links:
                    logger.debug(f"  Skipping {performer_name} - already linked")
                    continue

                # This is a NEW performer for this recording
                db_role = self._determine_role(performer_data, leader_mbids, leader_names)
                instruments = performer_data.get('instruments') or []

                if instruments:
                    for instrument_name in instruments:
                        instrument_id = self.get_or_create_instrument(conn, instrument_name)

                        if instrument_id:
                            cur.execute("""
                                INSERT INTO recording_performers (
                                    recording_id, performer_id, instrument_id, role
                                )
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (recording_id, performer_id, instrument_id, db_role))

                            self.link_performer_instrument(conn, performer_id, instrument_id)
                            new_performers_added += 1
                            new_performer_names.append(f"{performer_name} ({instrument_name})")
                            # Add to existing_links to prevent duplicate inserts
                            existing_links.add(performer_id)
                else:
                    cur.execute("""
                        INSERT INTO recording_performers (
                            recording_id, performer_id, role
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (recording_id, performer_id, db_role))
                    new_performers_added += 1
                    new_performer_names.append(performer_name)
                    existing_links.add(performer_id)

            # Ensure at least one leader
            if new_performers_added > 0:
                self._ensure_leader_exists(cur, recording_id, 'recording_performers')

        # Artist-credit leader fallback (#93 follow-up). If the ingest above
        # produced only role='other' rows (common for pop/crossover tracks
        # where MB has producer/engineer credits but no performance
        # relations), `_ensure_leader_exists` leaves the recording with no
        # leader — it explicitly won't promote 'other' rows. Synthesize one
        # from artist-credit in that case. No-op when a leader already
        # exists, so safe to call unconditionally.
        leaders_from_credit = self._ensure_leader_from_artist_credit(
            conn, recording_id, recording_data
        )
        if leaders_from_credit:
            new_performers_added += leaders_from_credit

        # SPECIAL LOGGING: If we added performers to a recording that already had some
        if new_performers_added > 0 and existing_performer_count > 0:
            self._log_performer_addition(
                conn, recording_id, existing_performer_count, 
                new_performer_names, source_release_title
            )
            self.stats['recordings_with_new_performers'] += 1
        
        return new_performers_added
    
    # ========================================================================
    # BATCH LOOKUP METHODS (Performance optimization)
    # ========================================================================
    
    def _batch_get_performers(self, conn, mbids: list, names: list) -> dict:
        """
        Batch fetch performers by MBIDs and names.
        
        OPTIMIZATION: Two queries total instead of 2 per performer.
        
        Args:
            conn: Database connection
            mbids: List of MusicBrainz IDs to look up
            names: List of performer names to look up
            
        Returns:
            Dict mapping cache keys to performer IDs:
            - "mbid:{mbid}" -> performer_id
            - "name:{lowercase_name}" -> performer_id
        """
        result = {}
        
        with conn.cursor() as cur:
            # Batch lookup by MBID (most reliable)
            if mbids:
                cur.execute("""
                    SELECT id, musicbrainz_id 
                    FROM performers 
                    WHERE musicbrainz_id = ANY(%s)
                """, (mbids,))
                for row in cur.fetchall():
                    result[f"mbid:{row['musicbrainz_id']}"] = row['id']
            
            # Batch lookup by name (case-insensitive) - look up all names
            # Some performers might be found by name but not MBID
            if names:
                cur.execute("""
                    SELECT id, LOWER(name) as name_lower
                    FROM performers 
                    WHERE LOWER(name) = ANY(%s)
                """, ([n.lower() for n in names if n],))
                for row in cur.fetchall():
                    result[f"name:{row['name_lower']}"] = row['id']
        
        return result
    
    def _batch_get_recording_performer_links(self, conn, recording_id: str, 
                                              performer_ids: list) -> set:
        """
        Batch fetch existing performer links for a recording.
        
        OPTIMIZATION: Single query instead of one per performer.
        
        Args:
            conn: Database connection
            recording_id: Recording ID to check
            performer_ids: List of performer IDs to check
            
        Returns:
            Set of performer IDs that are already linked to this recording
        """
        if not performer_ids or not recording_id:
            return set()
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT performer_id
                FROM recording_performers
                WHERE recording_id = %s AND performer_id = ANY(%s)
            """, (recording_id, performer_ids))
            
            return {row['performer_id'] for row in cur.fetchall()}
    
    def _create_performer(self, conn, name: str, mbid: str = None,
                          sort_name: str = None, artist_type: str = None,
                          disambiguation: str = None) -> str:
        """
        Create a new performer in the database.

        Called only when batch lookup confirms performer doesn't exist.
        Uses savepoint to handle potential conflicts gracefully.

        Args:
            conn: Database connection
            name: Performer name
            mbid: Optional MusicBrainz ID
            sort_name: Optional MusicBrainz sort name (e.g., "Davis, Miles")
            artist_type: Optional MusicBrainz artist type (Person, Group, etc.)
            disambiguation: Optional MusicBrainz disambiguation text

        Returns:
            New performer ID, or None on failure
        """
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would create performer: {name}")
            return None

        with conn.cursor() as cur:
            # Use savepoint for atomic insert attempt
            cur.execute("SAVEPOINT create_performer")
            try:
                cur.execute("""
                    INSERT INTO performers (name, musicbrainz_id, sort_name, artist_type, disambiguation)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (name, mbid, sort_name, artist_type, disambiguation))

                result = cur.fetchone()
                cur.execute("RELEASE SAVEPOINT create_performer")
                if result:
                    logger.info(f"  Created performer: {name}")
                    self.stats['performers_created'] += 1
                    return result['id']
            except Exception as e:
                # Rollback to savepoint and try to find existing performer
                cur.execute("ROLLBACK TO SAVEPOINT create_performer")
                logger.debug(f"  INSERT failed for {name}, looking up: {e}")

                # Try to find by MBID first
                if mbid:
                    cur.execute("""
                        SELECT id FROM performers WHERE musicbrainz_id = %s
                    """, (mbid,))
                    result = cur.fetchone()
                    if result:
                        return result['id']

                # Try to find by name
                cur.execute("""
                    SELECT id FROM performers WHERE LOWER(name) = LOWER(%s)
                """, (name,))
                result = cur.fetchone()
                if result:
                    return result['id']

        return None    
    # ========================================================================
    # Helper methods for performer counts and logging
    # ========================================================================
    
    def _get_recording_performer_count(self, conn, recording_id):
        """Get the current count of performers for a recording"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT performer_id) as count
                FROM recording_performers
                WHERE recording_id = %s
            """, (recording_id,))
            return cur.fetchone()['count']
    
    def _log_performer_addition(self, conn, recording_id, existing_count, 
                                 new_performer_names, source_release_title):
        """Log when performers are added to a recording that already has performers"""
        additions_log = get_additions_logger()
        
        # Get recording info for logging
        with conn.cursor() as cur:
            cur.execute("""
                SELECT def_rel.title as album_title, r.recording_year, s.title as song_title
                FROM recordings r
                JOIN songs s ON r.song_id = s.id
                LEFT JOIN releases def_rel ON r.default_release_id = def_rel.id
                WHERE r.id = %s
            """, (recording_id,))
            recording_info = cur.fetchone()

        if recording_info:
            album = recording_info['album_title'] or 'Unknown Album'
            year = recording_info['recording_year'] or 'N/A'
            song = recording_info['song_title'] or 'Unknown Song'
        else:
            album = 'Unknown'
            year = 'N/A'
            song = 'Unknown'
        
        additions_log.info(
            f"ADDITION - Recording: {album} ({year}) - Song: {song}\n"
            f"  Recording ID: {recording_id}\n"
            f"  Existing performers: {existing_count}\n"
            f"  Source release: {source_release_title or 'Unknown'}\n"
            f"  New performers added: {', '.join(new_performer_names)}\n"
        )
    
    # ========================================================================
    # RELEASE-SPECIFIC CREDITS: For producers, engineers, etc.
    # ========================================================================
    
    def link_release_credits(self, conn, release_id, mb_recording, release_details):
        """
        Link release-specific credits (producers, engineers, etc.) to a release.
        
        This is for NON-PERFORMER credits only. Performing musicians should use
        add_performers_to_recording() instead.
        
        Args:
            conn: Database connection
            release_id: Our database release ID
            mb_recording: MusicBrainz recording data
            release_details: Full MusicBrainz release details
            
        Returns:
            Number of credits linked
        """
        if not release_details:
            return 0
        
        # Extract only non-performer credits (producers, engineers, etc.)
        credits_to_add = self._extract_release_credits(release_details)
        
        if not credits_to_add:
            return 0
        
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would add {len(credits_to_add)} release credits")
            for credit in credits_to_add:
                logger.info(f"    - {credit['name']} ({credit['role']})")
            return len(credits_to_add)
        
        if not conn or not release_id:
            return 0
        
        credits_linked = 0
        
        with conn.cursor() as cur:
            for credit in credits_to_add:
                performer_id = self.get_or_create_performer(
                    conn,
                    credit['name'],
                    credit.get('mbid'),
                    sort_name=credit.get('sort_name'),
                    artist_type=credit.get('artist_type'),
                    disambiguation=credit.get('disambiguation')
                )

                if not performer_id:
                    continue
                
                # Check if already linked
                cur.execute("""
                    SELECT COUNT(*) as count
                    FROM release_performers
                    WHERE release_id = %s AND performer_id = %s
                """, (release_id, performer_id))
                
                if cur.fetchone()['count'] > 0:
                    continue
                
                # Add the credit
                cur.execute("""
                    INSERT INTO release_performers (release_id, performer_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (release_id, performer_id, credit['role']))
                
                credits_linked += 1
        
        return credits_linked
    
    def _extract_release_credits(self, release_details):
        """Extract non-performer credits from release data"""
        credits = []

        relations = release_details.get('relations') or []

        for relation in relations:
            if not relation or not isinstance(relation, dict):
                continue

            rel_type = relation.get('type', '')
            target_type = relation.get('target-type', '')

            # Only process artist relationships
            if target_type != 'artist':
                continue

            # Only process non-performer roles
            if rel_type not in ['producer', 'engineer', 'mix', 'mastering',
                               'recording', 'programming']:
                continue

            artist = relation.get('artist')
            if not artist or not isinstance(artist, dict):
                continue

            credits.append({
                'name': artist.get('name'),
                'mbid': artist.get('id'),
                'sort_name': artist.get('sort-name'),
                'artist_type': artist.get('type'),
                'disambiguation': artist.get('disambiguation'),
                'role': rel_type
            })

        return credits
    
    # ========================================================================
    # LEGACY METHODS (kept for backwards compatibility)
    # ========================================================================
    
    def link_performers_to_release(self, conn, release_id, mb_recording, release_details):
        """
        DEPRECATED: Use add_performers_to_recording() for performers,
        and link_release_credits() for producers/engineers.
        
        This method is kept for backwards compatibility but now just
        extracts release credits (non-performers).
        """
        return self.link_release_credits(conn, release_id, mb_recording, release_details)
    
    def link_performers_to_recording(self, conn, recording_id, recording_data):
        """
        LEGACY METHOD - Now wraps add_performers_to_recording()
        
        Args:
            conn: Database connection
            recording_id: Our database recording ID
            recording_data: MusicBrainz recording data
            
        Returns:
            Number of performers linked
        """
        return self.add_performers_to_recording(
            conn, recording_id, recording_data, source_release_title=None
        )
    
    # ========================================================================
    # Data extraction methods
    # ========================================================================
    
    def _extract_performers_from_recording(self, recording_data):
        """Extract performers from recording data"""
        performers = []
        
        # Get artist credits
        artist_credits = recording_data.get('artist-credit') or []
        artists_from_credits = self.parse_artist_credits(artist_credits)
        
        # Try to get relationships with instruments
        relations = recording_data.get('relations') or []
        performers_with_instruments = self.parse_artist_relationships(relations)
        
        # If no relationships, try to get from first release
        if not performers_with_instruments:
            releases = recording_data.get('releases') or []
            if releases:
                first_release_id = releases[0].get('id')
                release_data = self.fetch_release_credits(first_release_id)
                if release_data:
                    mb_recording_id = recording_data.get('id')
                    release_performers = self.parse_release_artist_credits(
                        release_data, mb_recording_id
                    )
                    if release_performers:
                        performers_with_instruments = release_performers
        
        # Use relationships if available, otherwise credits
        if performers_with_instruments:
            return performers_with_instruments
        else:
            return [
                {
                    'name': a['name'],
                    'mbid': a['mbid'],
                    'sort_name': a.get('sort_name'),
                    'artist_type': a.get('artist_type'),
                    'disambiguation': a.get('disambiguation'),
                    'instruments': [],
                    'role': 'performer'
                }
                for a in artists_from_credits
            ]
    
    def _extract_leader_info_from_recording(self, recording_data):
        """Extract leader info from recording data"""
        leader_mbids = set()
        leader_names = set()
        
        artist_credits = recording_data.get('artist-credit') or []
        for credit in artist_credits:
            if isinstance(credit, dict) and 'artist' in credit:
                artist = credit['artist']
                if artist.get('id'):
                    leader_mbids.add(artist['id'])
                if artist.get('name'):
                    leader_names.add(artist['name'].lower())
        
        return leader_mbids, leader_names
    
    def _determine_role(self, performer_data, leader_mbids, leader_names):
        """
        Determine the role (leader/sideman/other) for a performer
        """
        performer_role = performer_data.get('role', 'performer')
        performer_mbid = performer_data.get('mbid')
        performer_name = performer_data.get('name', '')
        
        # Check if technical role
        if performer_role in ['engineer', 'producer', 'mix', 'mastering']:
            return 'other'
        
        # Check if leader by MBID
        if performer_mbid and performer_mbid in leader_mbids:
            return 'leader'
        
        # Check if leader by name
        if performer_name and performer_name.lower() in leader_names:
            return 'leader'
        
        # Check if leader of a group
        for leader_name in leader_names:
            if is_performer_leader_of_group(performer_name, leader_name):
                return 'leader'
        
        return 'sideman'
    
    def _ensure_leader_exists(self, cur, entity_id, table_name):
        """
        Ensure at least one leader exists for a recording
        """
        id_column = 'recording_id' if table_name == 'recording_performers' else 'release_id'

        cur.execute(f"""
            SELECT COUNT(*) as leader_count
            FROM {table_name}
            WHERE {id_column} = %s AND role = 'leader'
        """, (entity_id,))

        leader_count = cur.fetchone()['leader_count']

        if leader_count == 0:
            logger.debug(f"      No leaders assigned - marking first performer as leader")
            cur.execute(f"""
                UPDATE {table_name}
                SET role = 'leader'
                WHERE id = (
                    SELECT id FROM {table_name}
                    WHERE {id_column} = %s
                    AND role != 'other'
                    ORDER BY id
                    LIMIT 1
                )
            """, (entity_id,))

    def _ensure_leader_from_artist_credit(self, conn, recording_id, recording_data):
        """
        Synthesize a leader row from the recording's artist-credit when the
        normal performer ingest didn't produce one.

        Background: MusicBrainz stores the billed artist in the recording's
        ``artist-credit`` field (e.g. ``[{artist: {id: ..., name: "Ray
        Charles"}}]``). Performance credits (instrument/vocal relations) are
        stored separately in ``relations``. For jazz sessions these usually
        line up — the leader shows up in both places — and ``_determine_role``
        picks them out correctly. For a lot of pop/crossover recordings,
        though, only production credits (producer, engineer, arranger, mix)
        made it onto MB, with no performance relation for the billed artist.
        Those recordings end up with only ``role='other'`` rows and no
        leader at all, which is what #93's backfill diagnosis surfaced.

        This fallback fixes that specific case: when no leader row exists,
        insert one per entry in ``artist-credit`` with no instrument (since
        artist-credit doesn't carry instrument info). Idempotent — both the
        up-front leader check and ``ON CONFLICT DO NOTHING`` keep re-runs a
        no-op.

        Returns:
            Number of leader rows inserted (0 if a leader already existed or
            the recording had no artist-credit).
        """
        if not recording_data or not conn or not recording_id:
            return 0

        artist_credits = recording_data.get('artist-credit') or []
        if not artist_credits:
            return 0

        artists = self.parse_artist_credits(artist_credits)
        if not artists:
            return 0

        if self.dry_run:
            names = ', '.join(a['name'] for a in artists if a.get('name'))
            logger.info(f"  [DRY RUN] Would synthesize leader from artist-credit: {names}")
            return 0

        with conn.cursor() as cur:
            # Bail if a leader is already present (handles both cases:
            # normal ingest just created one, or a prior run of this
            # fallback did).
            cur.execute("""
                SELECT 1 FROM recording_performers
                WHERE recording_id = %s AND role = 'leader'
                LIMIT 1
            """, (recording_id,))
            if cur.fetchone():
                return 0

            inserted = 0
            for artist in artists:
                name = artist.get('name')
                if not name:
                    continue
                mbid = artist.get('mbid')

                performer_id = None
                if mbid:
                    cur.execute(
                        "SELECT id FROM performers WHERE musicbrainz_id = %s",
                        (mbid,),
                    )
                    row = cur.fetchone()
                    if row:
                        performer_id = row['id']

                if not performer_id:
                    cur.execute(
                        "SELECT id FROM performers WHERE LOWER(name) = LOWER(%s) LIMIT 1",
                        (name,),
                    )
                    row = cur.fetchone()
                    if row:
                        performer_id = row['id']

                if not performer_id:
                    performer_id = self._create_performer(
                        conn,
                        name,
                        mbid,
                        sort_name=artist.get('sort_name'),
                        artist_type=artist.get('artist_type'),
                        disambiguation=artist.get('disambiguation'),
                    )

                if not performer_id:
                    continue

                # If this performer is already linked to the recording — even
                # with role='other' (e.g. they also produced the date, as Ron
                # Carter famously does on his own records) — promote that
                # existing row to 'leader' rather than inserting a second row
                # for the same person. Prefer the row that carries an
                # instrument so we keep the credit info; otherwise just take
                # the lowest-id row.
                cur.execute("""
                    SELECT id, role, instrument_id
                    FROM recording_performers
                    WHERE recording_id = %s AND performer_id = %s
                    ORDER BY (instrument_id IS NULL), id
                """, (recording_id, performer_id))
                existing_rows = cur.fetchall()

                if existing_rows:
                    target = existing_rows[0]
                    if target['role'] != 'leader':
                        cur.execute("""
                            UPDATE recording_performers
                            SET role = 'leader'
                            WHERE id = %s
                        """, (target['id'],))
                        inserted += 1
                        logger.debug(
                            f"      Promoted existing row to leader: {name}"
                        )
                    # If target['role'] is already 'leader', nothing to do.
                    continue

                cur.execute("""
                    INSERT INTO recording_performers (recording_id, performer_id, role)
                    VALUES (%s, %s, 'leader')
                    ON CONFLICT DO NOTHING
                """, (recording_id, performer_id))
                if cur.rowcount > 0:
                    inserted += 1
                    logger.debug(
                        f"      Added leader from artist-credit: {name}"
                    )

            return inserted
    
    def _log_performers_dry_run(self, performers, leader_mbids, leader_names):
        """Log performer info in dry-run mode"""
        logger.info(f"  [DRY RUN] Would add {len(performers)} performers:")
        for p in performers:
            role = self._determine_role(p, leader_mbids, leader_names)
            instruments = ', '.join(p.get('instruments', [])) or 'no instrument'
            logger.info(f"    - {p['name']} ({role}) - {instruments}")
    
    # ========================================================================
    # Parsing methods
    # ========================================================================
    
    def fetch_release_credits(self, release_id):
        """Fetch release credits from MusicBrainz"""
        try:
            return self.mb_searcher.get_release_details(release_id)
        except Exception as e:
            logger.warning(f"Error fetching release credits: {e}")
            return None
    
    def parse_artist_credits(self, artist_credits):
        """Parse artist credits to extract artist info"""
        if not artist_credits:
            return []

        artists = []
        for credit in artist_credits:
            if isinstance(credit, dict) and 'artist' in credit:
                artist = credit['artist']
                artists.append({
                    'name': artist.get('name'),
                    'mbid': artist.get('id'),
                    'sort_name': artist.get('sort-name'),
                    'artist_type': artist.get('type'),
                    'disambiguation': artist.get('disambiguation')
                })

        return artists
    
    def parse_artist_relationships(self, relations):
        """Parse MusicBrainz artist relationships to extract performer info"""
        if not relations:
            return []

        performers = []

        for relation in relations:
            if not relation or not isinstance(relation, dict):
                continue

            rel_type = relation.get('type', 'unknown')
            target_type = relation.get('target-type', 'unknown')

            # Only process artist relationships
            if target_type != 'artist':
                continue

            if 'artist' not in relation:
                continue

            artist = relation.get('artist')
            if not artist or not isinstance(artist, dict):
                continue

            artist_name = artist.get('name')
            artist_mbid = artist.get('id')

            if not artist_name:
                continue

            instruments = []
            role = rel_type

            if rel_type == 'instrument':
                # Extract instrument names from attributes, skipping credit
                # qualifiers like "guest"/"solo" that describe how an
                # instrument was played rather than naming an instrument.
                attributes = relation.get('attributes') or []
                for attr in attributes:
                    if isinstance(attr, str):
                        attr_name = attr
                    elif isinstance(attr, dict) and 'name' in attr:
                        attr_name = attr['name']
                    else:
                        continue
                    if attr_name.strip().lower() in INSTRUMENT_QUALIFIER_ATTRIBUTES:
                        continue
                    instruments.append(attr_name)
            elif rel_type == 'vocal':
                instruments.append('vocals')
            elif rel_type not in ['engineer', 'producer', 'mix', 'mastering']:
                continue

            performers.append({
                'name': artist_name,
                'mbid': artist_mbid,
                'sort_name': artist.get('sort-name'),
                'artist_type': artist.get('type'),
                'disambiguation': artist.get('disambiguation'),
                'instruments': instruments,
                'role': role
            })

        return performers
    
    def parse_release_artist_credits(self, release_data, target_recording_id=None):
        """Parse artist credits from a release"""
        performers = []
        
        # Check release-level relationships if not looking for specific recording
        if not target_recording_id:
            relations = release_data.get('relations') or []
            if relations:
                release_performers = self.parse_artist_relationships(relations)
                if release_performers:
                    return release_performers
        
        # Check track-level credits
        media = release_data.get('media') or []
        for medium in media:
            if not medium or not isinstance(medium, dict):
                continue
            tracks = medium.get('tracks') or []
            for track in tracks:
                if not track or not isinstance(track, dict):
                    continue
                recording = track.get('recording') or {}
                recording_id = recording.get('id') if isinstance(recording, dict) else None
                
                if target_recording_id and recording_id != target_recording_id:
                    continue
                
                # Check recording relationships on this track
                recording_relations = recording.get('relations') or [] if isinstance(recording, dict) else []
                if recording_relations:
                    track_performers = self.parse_artist_relationships(recording_relations)
                    if track_performers:
                        performers.extend(track_performers)
                
                # Check track artist credits
                track_artist_credits = track.get('artist-credit') or []
                if track_artist_credits:
                    for credit in track_artist_credits:
                        if isinstance(credit, dict) and 'artist' in credit:
                            artist = credit.get('artist')
                            if isinstance(artist, dict):
                                performers.append({
                                    'name': artist.get('name'),
                                    'mbid': artist.get('id'),
                                    'sort_name': artist.get('sort-name'),
                                    'artist_type': artist.get('type'),
                                    'disambiguation': artist.get('disambiguation'),
                                    'instruments': [],
                                    'role': 'performer'
                                })
                
                if target_recording_id and recording_id == target_recording_id:
                    break
        
        return performers
    
    # ========================================================================
    # Database helper methods
    # ========================================================================
    
    def get_or_create_performer(self, conn, artist_name, artist_mbid=None,
                                sort_name=None, artist_type=None, disambiguation=None):
        """Get existing performer or create new one"""
        with conn.cursor() as cur:
            # Try to find by MusicBrainz ID first
            if artist_mbid:
                cur.execute("""
                    SELECT id FROM performers
                    WHERE musicbrainz_id = %s
                """, (artist_mbid,))
                result = cur.fetchone()

                if result:
                    return result['id']

            # Try to find by name
            cur.execute("""
                SELECT id FROM performers
                WHERE name ILIKE %s
            """, (artist_name,))
            result = cur.fetchone()

            if result:
                return result['id']

            # Create new performer
            if self.dry_run:
                logger.info(f"  [DRY RUN] Would create performer: {artist_name}")
                return None

            cur.execute("""
                INSERT INTO performers (name, musicbrainz_id, sort_name, artist_type, disambiguation)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (artist_name, artist_mbid, sort_name, artist_type, disambiguation))

            performer_id = cur.fetchone()['id']
            logger.info(f"  Created performer: {artist_name}")
            self.stats['performers_created'] += 1

            return performer_id
    
    def get_or_create_instrument(self, conn, instrument_name):
        """Get existing instrument or create new one"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM instruments
                WHERE name ILIKE %s
            """, (instrument_name,))
            result = cur.fetchone()
            
            if result:
                return result['id']
            
            if self.dry_run:
                logger.info(f"  [DRY RUN] Would create instrument: {instrument_name}")
                return None
            
            cur.execute("""
                INSERT INTO instruments (name)
                VALUES (%s)
                RETURNING id
            """, (instrument_name,))
            
            instrument_id = cur.fetchone()['id']
            logger.info(f"  Created instrument: {instrument_name}")
            self.stats['instruments_created'] += 1
            
            return instrument_id
    
    def link_performer_instrument(self, conn, performer_id, instrument_id):
        """Link a performer to an instrument in performer_instruments table"""
        if self.dry_run:
            return
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO performer_instruments (performer_id, instrument_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (performer_id, instrument_id))