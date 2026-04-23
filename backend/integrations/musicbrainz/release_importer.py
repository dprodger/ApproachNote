"""
MusicBrainz Release Import Module - OPTIMIZED VERSION

PERFORMANCE OPTIMIZATIONS:
1. Single database connection per recording (not per release)
2. Pre-check if releases exist BEFORE fetching from MusicBrainz API
3. Only fetch release details for releases that don't exist in DB
4. Batch all release operations for a recording in one transaction
5. Song-level pre-fetch of recordings with performers
   - Single query to get all recordings that already have performers
   - Skips add_performers_to_recording() entirely for these recordings
   - Reduces "everything cached" case from 4 queries/recording to 1 query total

KEY ARCHITECTURE:
- Recording = a specific sound recording (same audio across all releases)
- Release = a product (album, CD, digital release, etc.)
- A recording can appear on multiple releases
- Performers are associated with RECORDINGS (the audio), not releases
- Releases store Spotify/album art data and release-specific credits (producers, engineers)

COVER ART ARCHIVE INTEGRATION (2025-12):
- When import_cover_art=True (default), CAA is queried for each new release
- Uses shared save_release_imagery() function from caa_release_importer module
- CAA responses are cached to avoid repeated API calls
- Cover art import failures are non-fatal (logged as warnings)
"""

import logging
from datetime import datetime
from typing import Optional, Dict, List, Any, Set, Tuple

from db_utils import get_db_connection
from integrations.musicbrainz.db import (
    create_recording,
    find_song_by_id,
    find_song_by_name,
    get_all_recording_release_links,
    get_existing_recording_release_links,
    get_existing_recordings_batch,
    get_existing_release_ids,
    get_or_create_recording,
    get_recordings_with_performers,
    get_release_id_by_mb_id,
    link_recording_to_release,
    maybe_set_default_release,
    update_recording_date_if_better,
)
from integrations.musicbrainz.parsing import (
    extract_recording_date_from_mb,
    log_release_info,
    parse_mb_date,
    parse_release_data,
)
from integrations.musicbrainz.performer_importer import PerformerImporter
from integrations.musicbrainz.utils import MusicBrainzSearcher
from integrations.coverart.utils import CoverArtArchiveClient
from integrations.coverart.release_importer import save_release_imagery

# JazzBot configuration for auto-generated contributions
JAZZBOT_EMAIL = "jazzbot@approachnote.com"
JAZZBOT_DISPLAY_NAME = "JazzBot"

# Re-exported for backward compatibility (external callers import from here):
# - extract_recording_date_from_mb — used by scripts/onetime_scripts/backfill_recording_dates.py
# - parse_mb_date — smaller helper, not currently imported externally but kept public
__all__ = [
    'MBReleaseImporter',
    'extract_recording_date_from_mb',
    'parse_mb_date',
]


class MBReleaseImporter:
    """
    Handles MusicBrainz recording and release import operations
    
    OPTIMIZED FOR:
    - Minimal database connections (one per recording batch)
    - Skip API calls for existing releases
    - Efficient lookup table caching
    - Song-level pre-fetch to skip performer checks for existing recordings
    
    UPDATED: Recording-Centric Performer Architecture
    - Performers are now added to recordings (aggregated from all releases)
    - Releases only get release-specific credits (producers, engineers)
    """
    
    def __init__(self, dry_run: bool = False, force_refresh: bool = False,
                 logger: Optional[logging.Logger] = None,
                 progress_callback: Optional[callable] = None,
                 import_cover_art: bool = True):
        """
        Initialize the importer

        Args:
            dry_run: If True, don't make database changes
            force_refresh: If True, bypass MusicBrainz cache
            logger: Optional logger instance (creates one if not provided)
            progress_callback: Optional callback(phase, current, total) for progress tracking
            import_cover_art: If True, fetch cover art from CAA for new releases
        """
        self.dry_run = dry_run
        self.force_refresh = force_refresh
        self.logger = logger or logging.getLogger(__name__)
        self.progress_callback = progress_callback
        self.mb_searcher = MusicBrainzSearcher(force_refresh=force_refresh)
        self.performer_importer = PerformerImporter(dry_run=dry_run)

        # Cover Art Archive integration
        self.import_cover_art = import_cover_art
        if import_cover_art:
            self.caa_client = CoverArtArchiveClient(force_refresh=force_refresh)
        else:
            self.caa_client = None

        self.stats = {
            'recordings_found': 0,
            'recordings_created': 0,
            'recordings_existing': 0,
            'releases_found': 0,
            'releases_created': 0,
            'releases_existing': 0,
            'releases_skipped_api': 0,  # Count of API calls skipped
            'links_created': 0,
            'performers_linked': 0,
            'performers_added_to_recordings': 0,  # NEW: Performers added to recordings
            'release_credits_linked': 0,  # NEW: Release-specific credits (producers, etc.)
            'performers_skipped_existing': 0,  # NEW: Recordings skipped because they already have performers
            'errors': 0,  # Error count
            # Cover Art Archive stats
            'caa_releases_checked': 0,
            'caa_releases_with_art': 0,
            'caa_images_created': 0,
        }

        # Cache for lookup table IDs (populated once per import)
        self._format_cache = {}
        self._status_cache = {}
        self._packaging_cache = {}

        # Cache for JazzBot user ID (for auto-generated vocal/instrumental contributions)
        self._jazzbot_user_id = None

        self.logger.info(f"MBReleaseImporter initialized (optimized version, force_refresh={force_refresh}, import_cover_art={import_cover_art})")
    
    def find_song(self, song_identifier: str) -> Optional[Dict[str, Any]]:
        """
        Find a song by name or ID
        
        Args:
            song_identifier: Song name or UUID
            
        Returns:
            Song dict with keys: id, title, composer, musicbrainz_id
            Returns None if song not found
        """
        song_identifier = str(song_identifier)
        
        # Check if it looks like a UUID
        if len(song_identifier) == 36 and '-' in song_identifier:
            return find_song_by_id(song_identifier, log=self.logger)
        else:
            return find_song_by_name(song_identifier, log=self.logger)
    
    def import_releases(self, song_identifier: str, limit: int = 200) -> Dict[str, Any]:
        """
        Main method to import recordings and releases for a song

        This method:
        1. Finds the song by name or ID
        2. Fetches recordings from MusicBrainz (via the work, and second_mb_id if present)
        3. Pre-fetches which recordings already have performers (OPTIMIZATION)
        4. For each recording:
           - Creates the recording if it doesn't exist
           - Adds performers to the RECORDING (only if not already done)
           - Checks which releases already exist (OPTIMIZATION)
           - Only fetches details for NEW releases
           - Creates release-specific credits (producers, engineers)
        5. Links recordings to releases

        Args:
            song_identifier: Song name or UUID to find recordings for
            limit: Maximum number of recordings to process

        Returns:
            Dict with import statistics
        """
        # Find the song
        song = self.find_song(song_identifier)

        if not song:
            return {'success': False, 'error': 'Song not found', 'stats': self.stats}

        self.logger.info(f"Found song: {song['title']} (ID: {song['id']})")

        # Get MusicBrainz work IDs (primary and optional secondary)
        mb_work_id = song.get('musicbrainz_id')
        second_mb_id = song.get('second_mb_id')

        if not mb_work_id:
            return {'success': False, 'error': 'Song has no MusicBrainz ID', 'stats': self.stats}

        # Fetch recordings from primary MusicBrainz work
        recordings = self._fetch_musicbrainz_recordings(mb_work_id, limit)

        # Tag recordings with their source work ID
        for rec in recordings:
            rec['_source_mb_work_id'] = mb_work_id

        # Fetch recordings from secondary MusicBrainz work if present
        if second_mb_id:
            self.logger.info(f"Song has secondary MusicBrainz work ID: {second_mb_id}")
            secondary_recordings = self._fetch_musicbrainz_recordings(second_mb_id, limit)

            # Tag secondary recordings with their source work ID
            for rec in secondary_recordings:
                rec['_source_mb_work_id'] = second_mb_id

            if secondary_recordings:
                self.logger.info(f"Found {len(secondary_recordings)} additional recordings from secondary MB work")
                recordings.extend(secondary_recordings)

        if not recordings:
            return {'success': False, 'error': 'No recordings found on MusicBrainz', 'stats': self.stats}
        
        self.logger.info(f"Found {len(recordings)} recordings to process")
        
        # Process each recording with a SINGLE connection
        with get_db_connection() as conn:
            # Pre-load lookup table caches (one-time cost)
            self._load_lookup_caches(conn)
            
            # =======================================================================
            # OPTIMIZATION: Song-level pre-fetch of ALL data needed
            # This replaces per-recording queries with batch queries upfront
            # =======================================================================
            
            # Collect all MB IDs from recordings
            mb_recording_ids = [r.get('id') for r in recordings if r.get('id')]
            
            # Collect all MB release IDs from all recordings
            all_mb_release_ids = []
            for rec in recordings:
                for rel in (rec.get('releases') or []):
                    if rel.get('id'):
                        all_mb_release_ids.append(rel.get('id'))
            
            # 1. Batch fetch: recordings with performers for THIS song (skip performer import)
            recordings_with_performers = get_recordings_with_performers(
                conn, mb_recording_ids, song['id']
            )
            self.logger.debug(f"  Pre-fetched {len(recordings_with_performers)} recordings with performers")

            # 2. Batch fetch: existing recordings by MB ID for THIS song
            # NOTE: We filter by song_id to handle medley recordings correctly.
            # A medley in MusicBrainz is one recording linked to multiple works,
            # but we create separate recording entries for each song.
            existing_recordings = get_existing_recordings_batch(
                conn, mb_recording_ids, song['id']
            )
            self.logger.debug(f"  Pre-fetched {len(existing_recordings)} existing recordings for this song")

            # 3. Batch fetch: existing releases by MB ID
            existing_releases_all = get_existing_release_ids(
                conn, all_mb_release_ids
            )
            self.logger.debug(f"  Pre-fetched {len(existing_releases_all)} existing releases")

            # 4. Batch fetch: all recording-release links for existing recordings
            existing_recording_db_ids = list(existing_recordings.values())
            all_existing_links = get_all_recording_release_links(
                conn, existing_recording_db_ids
            )
            self.logger.debug(f"  Pre-fetched {len(all_existing_links)} existing links")
            
            for i, mb_recording in enumerate(recordings, 1):
                recording_title = mb_recording.get('title', 'Unknown')
                source_work_id = mb_recording.get('_source_mb_work_id')
                is_secondary = source_work_id == second_mb_id if second_mb_id else False
                source_label = " [from secondary MB work]" if is_secondary else ""
                self.logger.info(f"\n[{i}/{len(recordings)}] Processing: {recording_title}{source_label}")

                # Report progress via callback
                if self.progress_callback:
                    self.progress_callback('musicbrainz_recording_import', i, len(recordings))

                try:
                    self._process_recording_fast(
                        conn, song['id'], mb_recording,
                        recordings_with_performers,
                        existing_recordings,
                        existing_releases_all,
                        all_existing_links,
                        source_mb_work_id=source_work_id
                    )
                    self.stats['recordings_found'] += 1
                except Exception as e:
                    self.logger.error(f"  Error processing recording: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    # Rollback the failed transaction so subsequent operations can proceed
                    try:
                        conn.rollback()
                    except Exception:
                        pass  # Connection might already be closed
                    # Continue with next recording
                    continue
        return {
            'success': True,
            'song': song,
            'recordings_processed': len(recordings),
            'stats': self.stats
        }
    
    def _load_lookup_caches(self, conn) -> None:
        """
        Pre-load lookup table caches for efficient access
        
        OPTIMIZATION: Single query each for formats, statuses, packaging
        """
        self.logger.debug("Pre-loading lookup table caches...")
        
        with conn.cursor() as cur:
            # Load formats
            cur.execute("SELECT id, name FROM release_formats")
            for row in cur.fetchall():
                self._format_cache[row['name']] = row['id']
            
            # Load statuses (by lowercase name)
            cur.execute("SELECT id, LOWER(name) as name FROM release_statuses")
            for row in cur.fetchall():
                self._status_cache[row['name']] = row['id']
            
            # Load packaging
            cur.execute("SELECT id, name FROM release_packaging")
            for row in cur.fetchall():
                self._packaging_cache[row['name']] = row['id']
        
        self.logger.debug(f"  Loaded {len(self._format_cache)} formats, "
                         f"{len(self._status_cache)} statuses, "
                         f"{len(self._packaging_cache)} packaging types")
    
    def _process_recording(self, conn, song_id: str, mb_recording: Dict[str, Any],
                           recordings_with_performers: Set[str]) -> None:
        """
        Process a single MusicBrainz recording (ORIGINAL VERSION)
        
        Kept for backward compatibility. Use _process_recording_fast for better performance.
        """
        # Delegate to fast version with empty caches (will do per-recording queries)
        self._process_recording_fast(
            conn, song_id, mb_recording,
            recordings_with_performers,
            {},  # existing_recordings
            {},  # existing_releases_all
            {}   # all_existing_links
        )
    
    def _process_recording_fast(self, conn, song_id: str, mb_recording: Dict[str, Any],
                                 recordings_with_performers: Set[str],
                                 existing_recordings: Dict[str, str],
                                 existing_releases_all: Dict[str, str],
                                 all_existing_links: Dict[str, Set[str]],
                                 source_mb_work_id: Optional[str] = None) -> None:
        """
        Process a single MusicBrainz recording (OPTIMIZED VERSION)

        FULLY OPTIMIZED: Uses pre-fetched data for ALL lookups.
        For "everything exists" case: ZERO database queries per recording.

        Args:
            conn: Database connection (reused from caller)
            song_id: Our database song ID
            mb_recording: MusicBrainz recording data
            recordings_with_performers: Set of MB recording IDs that already have performers
            existing_recordings: Dict of MB recording ID -> our recording ID
            existing_releases_all: Dict of MB release ID -> our release ID
            all_existing_links: Dict of our recording ID -> set of linked release IDs
            source_mb_work_id: MusicBrainz work ID this recording was imported from (for tracking)
        """
        mb_recording_id = mb_recording.get('id')

        # Get the releases from the recording data
        mb_releases = mb_recording.get('releases') or []
        if not mb_releases:
            self.logger.info("  No releases found for this recording")
            return

        # Use the first release title for logging
        first_release = mb_releases[0]
        first_release_title = first_release.get('title', 'Unknown Album')

        # Extract recording date from MusicBrainz data (performer relations or first-release-date)
        date_info = extract_recording_date_from_mb(mb_recording, logger=self.logger)

        # STEP 1: Get or create the recording (use cache first - NO QUERY if exists)
        recording_id = existing_recordings.get(mb_recording_id)
        if recording_id:
            self.logger.debug(f"  Recording exists (by MB ID)")
            self.stats['recordings_existing'] += 1
        else:
            # Recording doesn't exist - need to create it
            recording_id, inserted = create_recording(
                conn, song_id, mb_recording_id, date_info,
                source_mb_work_id=source_mb_work_id,
                title=mb_recording.get('title'),
                duration_ms=mb_recording.get('length'),
                dry_run=self.dry_run,
                log=self.logger,
            )
            if inserted:
                self.stats['recordings_created'] += 1
            elif recording_id:
                self.stats['recordings_existing'] += 1
            if recording_id:
                # Add to cache for future reference
                existing_recordings[mb_recording_id] = recording_id
        
        if not recording_id and not self.dry_run:
            self.logger.error("  Failed to get/create recording")
            return
        
        # STEP 2: Add performers
        # Check if we should skip, or if MusicBrainz has better data than what we have
        should_import_performers = True
        if mb_recording_id in recordings_with_performers:
            # Recording already has performers - check if MusicBrainz has better data
            if self.performer_importer.has_better_performer_data(conn, recording_id, mb_recording):
                # MusicBrainz has better data (instruments) - clear old and re-import
                self.performer_importer.clear_recording_performers(conn, recording_id)
                # Also update recording date if MB has better date info
                update_recording_date_if_better(
                    conn, recording_id, date_info,
                    dry_run=self.dry_run, log=self.logger,
                )
            else:
                self.logger.debug(f"  Skipping performer check - recording already has performers")
                self.stats['performers_skipped_existing'] += 1
                should_import_performers = False

        if should_import_performers:
            performers_added = self.performer_importer.add_performers_to_recording(
                conn, recording_id, mb_recording,
                source_release_title=first_release_title
            )
            if performers_added > 0:
                self.stats['performers_added_to_recordings'] += performers_added
                self.logger.info(f"  Added {performers_added} performers to recording")
                recordings_with_performers.add(mb_recording_id)

                # STEP 2b: Auto-create vocal/instrumental contribution based on credits
                self._create_vocal_instrumental_contribution(conn, recording_id)

        # STEP 3: Check releases using pre-fetched cache (NO QUERY)
        # Filter existing_releases_all to just this recording's releases
        existing_releases = {
            mb_id: db_id 
            for mb_id, db_id in existing_releases_all.items()
            if mb_id in {r.get('id') for r in mb_releases}
        }
        
        # STEP 4: Get existing links from pre-fetched cache (NO QUERY)
        existing_links = all_existing_links.get(recording_id, set()) if recording_id else set()
        
        # Count statistics
        fully_linked_count = len(existing_links & set(existing_releases.values()))
        new_releases_count = len(mb_releases) - len(existing_releases)
        needs_linking_count = len(existing_releases) - fully_linked_count
        
        self.logger.info(f"  Processing {len(mb_releases)} releases "
                       f"({len(existing_releases)} in DB, {fully_linked_count} already linked, "
                       f"{needs_linking_count} need linking, {new_releases_count} new)...")
        
        # STEP 5: Process each release (only does work for new/unlinked releases)
        for mb_release in mb_releases:
            self._process_release_in_transaction(
                conn, recording_id, mb_recording_id, mb_recording, 
                mb_release, existing_releases, existing_links
            )
    
    def _process_release_in_transaction(
        self, conn, recording_id: Optional[str], mb_recording_id: str,
        mb_recording: Dict[str, Any], mb_release: Dict[str, Any],
        existing_releases: Dict[str, str], existing_links: Set[str]
    ) -> None:
        """
        Process a single release within an existing transaction
        
        OPTIMIZED:
        - Uses pre-fetched existing_releases mapping (MB ID -> our ID)
        - Uses pre-fetched existing_links set (our release IDs already linked)
        - Skips entirely for releases that are already fully linked
        - No individual DB queries for existing releases
        
        UPDATED: Recording-Centric Performer Architecture
        - Performers are added to RECORDINGS, not releases
        - Releases only get release-specific credits (producers, engineers)
        
        Args:
            conn: Database connection (reused)
            recording_id: Our database recording ID (may be None in dry-run)
            mb_recording_id: MusicBrainz recording ID
            mb_recording: MusicBrainz recording data
            mb_release: Basic MusicBrainz release data
            existing_releases: Dict mapping MB release ID -> our release ID
            existing_links: Set of our release IDs already linked to this recording
        """
        mb_release_id = mb_release.get('id')
        release_title = mb_release.get('title', 'Unknown')
        
        # OPTIMIZATION: Check if release exists using pre-fetched data
        if mb_release_id in existing_releases:
            release_id = existing_releases[mb_release_id]
            self.stats['releases_existing'] += 1

            # Check if already linked using pre-fetched data (no DB query!)
            if release_id in existing_links:
                self.logger.debug(f"    Skipping fully-linked release: {release_title[:40]}")
                self.stats['releases_skipped_api'] += 1
                return

            # Need to create link (but release exists)
            if recording_id:
                self.logger.debug(f"    Creating link for existing release: {release_title[:40]}")
                # Fetch full release details to get track positions
                # (will use cache if available, so not as slow as it sounds)
                release_details = self.mb_searcher.get_release_details(mb_release_id)
                link_recording_to_release(
                    conn, recording_id, release_id, mb_recording_id,
                    release_details or mb_release,
                    log=self.logger,
                )
                self.stats['links_created'] += 1
            return
        
        # Release doesn't exist - fetch full details from MusicBrainz
        release_details = self.mb_searcher.get_release_details(mb_release_id)
        
        if not release_details:
            self.logger.warning(f"    Could not fetch details for release: {release_title[:40]}")
            return
        
        self.stats['releases_found'] += 1
        
        # Parse release data
        release_data = parse_release_data(release_details)

        if self.dry_run:
            log_release_info(release_data, self.logger)
            return
        
        # Create the release
        release_id = self._create_release(conn, release_data)

        if release_id:
            self.stats['releases_created'] += 1
            self.logger.info(f"    ✓ Created release: {release_title[:40]}")

            # Link recording to release (use release_details which has full track info)
            if recording_id:
                link_recording_to_release(
                    conn, recording_id, release_id, mb_recording_id, release_details,
                    log=self.logger,
                )
                self.stats['links_created'] += 1

            # Link release-specific credits (producers, engineers, etc.)
            # These go to the RELEASE, not the recording
            credits_linked = self.performer_importer.link_release_credits(
                conn, release_id, mb_recording, release_details
            )
            if credits_linked > 0:
                self.stats['release_credits_linked'] += credits_linked

            # Import cover art from Cover Art Archive
            self._import_cover_art_for_release(conn, release_id, mb_release_id)
    
    def _create_release(self, conn, release_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a new release in the database, or return existing ID if duplicate
        
        Args:
            conn: Database connection
            release_data: Parsed release data dict
            
        Returns:
            Release ID (new or existing) or None
        """
        # Get foreign key IDs
        format_id = self._get_or_create_format(conn, release_data.get('format_name'))
        status_id = self._get_status_id(release_data.get('status_name'))
        packaging_id = self._get_or_create_packaging(conn, release_data.get('packaging_name'))
        
        mb_release_id = release_data.get('musicbrainz_release_id')
        
        with conn.cursor() as cur:
            # Use ON CONFLICT to handle race conditions where release was created
            # between our pre-fetch check and now
            cur.execute("""
                INSERT INTO releases (
                    musicbrainz_release_id, musicbrainz_release_group_id,
                    title, artist_credit, disambiguation,
                    release_date, release_year, country,
                    label, catalog_number, barcode,
                    format_id, packaging_id, status_id,
                    language, script, total_tracks, total_discs,
                    data_quality
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (musicbrainz_release_id) DO UPDATE SET
                    musicbrainz_release_id = EXCLUDED.musicbrainz_release_id
                RETURNING id
            """, (
                mb_release_id,
                release_data.get('musicbrainz_release_group_id'),
                release_data.get('title'),
                release_data.get('artist_credit'),
                release_data.get('disambiguation'),
                release_data.get('release_date'),
                release_data.get('release_year'),
                release_data.get('country'),
                release_data.get('label'),
                release_data.get('catalog_number'),
                release_data.get('barcode'),
                format_id,
                packaging_id,
                status_id,
                release_data.get('language'),
                release_data.get('script'),
                release_data.get('total_tracks'),
                release_data.get('total_discs'),
                release_data.get('data_quality'),
            ))
            
            result = cur.fetchone()
            return result['id'] if result else None

    def _import_cover_art_for_release(self, conn, release_id: str,
                                       mb_release_id: str) -> None:
        """
        Import cover art for a newly created release from Cover Art Archive.

        Called automatically after release creation if import_cover_art=True.
        Uses the shared save_release_imagery() function for database operations.

        Args:
            conn: Database connection (caller manages transaction)
            release_id: Our database release UUID
            mb_release_id: MusicBrainz release ID
        """
        if not self.import_cover_art or not self.caa_client:
            return

        if self.dry_run:
            self.logger.debug(f"      [DRY RUN] Would check CAA for cover art")
            return

        try:
            # Get imagery data from CAA (uses cache)
            imagery_data = self.caa_client.extract_imagery_data(mb_release_id)

            # Dedupe to one Front, one Back (CAA may return multiple of each type)
            images_to_store = []
            stored_types = set()
            for img in (imagery_data or []):
                if img['type'] not in stored_types:
                    images_to_store.append(img)
                    stored_types.add(img['type'])

            # Save using shared function (doesn't commit - caller does)
            result = save_release_imagery(
                conn, release_id, images_to_store,
                logger=self.logger,
                update_checked_timestamp=True
            )

            # Update stats
            self.stats['caa_releases_checked'] += 1
            if images_to_store:
                self.stats['caa_releases_with_art'] += 1
                self.stats['caa_images_created'] += result.get('created', 0)
                front_count = sum(1 for img in images_to_store if img['type'] == 'Front')
                back_count = sum(1 for img in images_to_store if img['type'] == 'Back')
                self.logger.debug(f"      CAA: {front_count} front, {back_count} back image(s)")
            else:
                self.logger.debug(f"      CAA: no cover art available")

        except Exception as e:
            self.logger.warning(f"      CAA error (non-fatal): {e}")
            # Don't increment error count - CAA failures shouldn't fail the release import

    # ========================================================================
    # Lookup table helpers
    # ========================================================================
    
    def _get_or_create_format(self, conn, format_name: str) -> Optional[int]:
        """Get format ID, creating if needed"""
        if not format_name:
            return None
        
        # Check cache first
        if format_name in self._format_cache:
            return self._format_cache[format_name]
        
        # Not in cache - need to create it
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO release_formats (name, category)
                VALUES (%s, 'other')
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (format_name,))
            result = cur.fetchone()
            self._format_cache[format_name] = result['id']
            return result['id']
    
    def _get_status_id(self, status_name: str) -> Optional[int]:
        """Get status ID (don't create - use predefined values only)"""
        if not status_name:
            return None
        
        status_name = status_name.lower()
        return self._status_cache.get(status_name)
    
    def _get_or_create_packaging(self, conn, packaging_name: str) -> Optional[int]:
        """Get packaging ID, creating if needed"""
        if not packaging_name:
            return None
        
        # Check cache first
        if packaging_name in self._packaging_cache:
            return self._packaging_cache[packaging_name]
        
        # Not in cache - need to create it
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO release_packaging (name)
                VALUES (%s)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (packaging_name,))
            result = cur.fetchone()
            self._packaging_cache[packaging_name] = result['id']
            return result['id']

    # ========================================================================
    # JazzBot auto-contribution helpers
    # ========================================================================

    def _get_jazzbot_user_id(self, conn) -> Optional[str]:
        """
        Get the JazzBot user ID, creating the user if needed.
        Cached for the duration of the import session.

        Returns:
            JazzBot user ID, or None if in dry-run mode
        """
        if self._jazzbot_user_id:
            return self._jazzbot_user_id

        if self.dry_run:
            return None

        with conn.cursor() as cur:
            # Try to find existing bot user
            cur.execute("SELECT id FROM users WHERE email = %s", (JAZZBOT_EMAIL,))
            row = cur.fetchone()

            if row:
                self._jazzbot_user_id = str(row['id'])
                return self._jazzbot_user_id

            # Create the bot user with a placeholder password hash (can't log in)
            fake_password_hash = "$2b$12$BOTUSER.CANNOT.LOGIN.PLACEHOLDER"
            cur.execute("""
                INSERT INTO users (email, email_verified, display_name, is_active, password_hash)
                VALUES (%s, true, %s, true, %s)
                RETURNING id
            """, (JAZZBOT_EMAIL, JAZZBOT_DISPLAY_NAME, fake_password_hash))
            self._jazzbot_user_id = str(cur.fetchone()['id'])
            self.logger.info(f"Created JazzBot user: {JAZZBOT_EMAIL}")
            return self._jazzbot_user_id

    def _create_vocal_instrumental_contribution(self, conn, recording_id: str) -> bool:
        """
        Create a JazzBot contribution for vocal/instrumental based on performer credits.

        Logic:
        - If recording has multiple performers (band credits)
        - Check if any performer has "Vocals" as their instrument
        - If vocals found: is_instrumental = False (vocal)
        - If no vocals: is_instrumental = True (instrumental)

        Args:
            conn: Database connection
            recording_id: Recording ID to check and create contribution for

        Returns:
            True if contribution was created/updated, False otherwise
        """
        if self.dry_run or not recording_id:
            return False

        with conn.cursor() as cur:
            # Check if recording has multiple performers (band credits)
            cur.execute("""
                SELECT COUNT(DISTINCT performer_id) as performer_count
                FROM recording_performers
                WHERE recording_id = %s
            """, (recording_id,))
            row = cur.fetchone()
            performer_count = row['performer_count'] if row else 0

            if performer_count <= 1:
                # Single performer or no performers - don't auto-set
                return False

            # Check if any performer has "Vocals" instrument
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM recording_performers rp
                    JOIN instruments i ON rp.instrument_id = i.id
                    WHERE rp.recording_id = %s AND i.name = 'Vocals'
                ) as has_vocals
            """, (recording_id,))
            has_vocals = cur.fetchone()['has_vocals']

            # Get JazzBot user ID
            jazzbot_id = self._get_jazzbot_user_id(conn)
            if not jazzbot_id:
                return False

            # Determine is_instrumental value
            is_instrumental = not has_vocals

            # Create or update the contribution
            cur.execute("""
                INSERT INTO recording_contributions (recording_id, user_id, is_instrumental)
                VALUES (%s, %s, %s)
                ON CONFLICT (recording_id, user_id)
                DO UPDATE SET is_instrumental = EXCLUDED.is_instrumental,
                              updated_at = CURRENT_TIMESTAMP
            """, (recording_id, jazzbot_id, is_instrumental))

            label = "Instrumental" if is_instrumental else "Vocal"
            self.logger.debug(f"  JazzBot: marked as {label} ({performer_count} performers)")
            return True

    def _fetch_musicbrainz_recordings(self, work_id: str, limit: int) -> List[Dict[str, Any]]:
        """
        Fetch recordings for a MusicBrainz work
        
        Args:
            work_id: MusicBrainz work ID
            limit: Maximum recordings to fetch
            
        Returns:
            List of recording data dicts
        """
        self.logger.info(f"Fetching recordings for MusicBrainz work: {work_id}")
        
        try:
            # Get work with recording relationships
            data = self.mb_searcher.get_work_recordings(work_id)
            
            if not data:
                self.logger.error("Could not fetch work from MusicBrainz")
                return []
            
            # Extract recordings from relations
            recordings = []
            relations = data.get('relations') or []
            
            self.logger.info(f"Found {len(relations)} related items in work")

            # Count performance relations for accurate progress tracking
            performance_relations = [r for r in relations if isinstance(r, dict) and r.get('type') == 'performance' and 'recording' in r]
            total_performances = min(len(performance_relations), limit)

            for relation in relations:
                if not isinstance(relation, dict):
                    continue
                if relation.get('type') == 'performance' and 'recording' in relation:
                    recording = relation.get('recording')
                    if not isinstance(recording, dict):
                        continue
                    recording_id = recording.get('id')
                    
                    # Fetch detailed recording information (CACHED by mb_utils)
                    recording_details = self.mb_searcher.get_recording_details(recording_id)

                    if recording_details:
                        recordings.append(recording_details)

                        # Report progress during fetch phase
                        if self.progress_callback:
                            self.progress_callback('musicbrainz_fetch', len(recordings), total_performances)

                        # Log progress every 25 recordings to show activity
                        if len(recordings) % 25 == 0:
                            self.logger.info(f"  Fetched {len(recordings)}/{total_performances} recording details...")

                        if len(recordings) >= limit:
                            self.logger.info(f"Reached limit of {limit} recordings")
                            break
            
            self.logger.info(f"Successfully fetched {len(recordings)} recording details")
            return recordings
            
        except Exception as e:
            self.logger.error(f"Error fetching recordings: {e}", exc_info=True)
            return []