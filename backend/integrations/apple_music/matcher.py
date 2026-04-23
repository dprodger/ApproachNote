"""
Apple Music Matching Utilities

Core business logic for matching releases to Apple Music albums.

This module provides the AppleMusicMatcher class which handles:
- Apple Music API search and lookup
- Fuzzy matching and validation of albums and tracks
- Database updates for streaming links and artwork
- Caching of API responses to minimize rate limiting

Supports two data sources:
1. Local catalog (via Apple Music Feed) - preferred, no rate limits
2. iTunes Search API - fallback, has aggressive rate limiting

Uses the normalized streaming_links tables for storage.

Used by:
- scripts/match_apple_tracks.py (CLI interface)
- song_research.py (background worker)
"""

import logging
from typing import Any, Dict

from db_utils import get_db_connection

from integrations.apple_music.client import (
    AppleMusicClient,
    build_apple_music_album_url,
    build_apple_music_track_url,
)
from integrations.apple_music.db import (
    find_song_by_name,
    find_song_by_id,
    get_releases_for_song,
    get_recordings_for_release,
    mark_release_searched,
    upsert_release_streaming_link,
    upsert_track_streaming_link,
    upsert_release_imagery,
)
from integrations.apple_music.matching import find_matching_track
from integrations.apple_music.search import search_and_validate_album

# Optional: Apple Music Feed catalog (much faster, no rate limits)
try:
    from integrations.apple_music.feed import AppleMusicCatalog, PYARROW_AVAILABLE
    FEED_AVAILABLE = PYARROW_AVAILABLE
except ImportError:
    FEED_AVAILABLE = False
    AppleMusicCatalog = None

# Used only by the "Best candidate" debug log in _match_track_on_release
from integrations.spotify.matching import (
    calculate_similarity,
    normalize_for_comparison,
)

logger = logging.getLogger(__name__)


class AppleMusicMatcher:
    """
    Handles matching releases to Apple Music albums with fuzzy validation and caching.
    """

    def __init__(
        self,
        dry_run: bool = False,
        strict_mode: bool = True,
        force_refresh: bool = False,
        artist_filter: str = None,
        cache_days: int = 30,
        rate_limit_delay: float = 0.2,
        max_retries: int = 3,
        rematch: bool = False,
        rematch_failures: bool = False,
        logger: logging.Logger = None,
        progress_callback=None,
        use_local_catalog: bool = True,
        local_catalog_only: bool = False,
    ):
        """
        Initialize Apple Music Matcher

        Args:
            dry_run: If True, show what would be matched without making changes
            strict_mode: If True, use stricter validation thresholds
            force_refresh: If True, ignore cache and fetch fresh data
            artist_filter: Filter releases by specific artist
            cache_days: Days before cache expires
            rate_limit_delay: Delay between API calls
            max_retries: Max retries for rate-limited requests
            rematch: If True, re-evaluate releases that already have Apple Music
            rematch_failures: If True, re-evaluate releases that had no match (keeps existing matches)
            logger: Optional logger instance
            progress_callback: Optional callback(phase, current, total)
            use_local_catalog: If True, use downloaded Apple Music catalog first
            local_catalog_only: If True, don't fall back to API (avoids rate limits)
        """
        self.dry_run = dry_run
        self.strict_mode = strict_mode
        self.artist_filter = artist_filter
        self.rematch = rematch
        self.rematch_failures = rematch_failures
        self.logger = logger or logging.getLogger(__name__)
        self.progress_callback = progress_callback
        self.use_local_catalog = use_local_catalog
        self.local_catalog_only = local_catalog_only

        # Initialize local catalog if available and requested
        self.catalog = None
        if use_local_catalog and FEED_AVAILABLE:
            try:
                self.catalog = AppleMusicCatalog(logger=self.logger)
                # Try to verify catalog has data
                stats = self.catalog.get_catalog_stats()
                if stats.get('albums'):
                    self.logger.info("Using local Apple Music catalog for matching")
                else:
                    self.logger.debug("Local catalog exists but has no album data")
                    self.catalog = None
            except FileNotFoundError:
                self.logger.debug("Local Apple Music catalog not found, will use API")
                self.catalog = None
            except Exception as e:
                self.logger.warning(f"Failed to load local catalog: {e}")
                self.catalog = None

        # Initialize API client (used as fallback or primary if no catalog)
        self.client = AppleMusicClient(
            cache_days=cache_days,
            force_refresh=force_refresh,
            rate_limit_delay=rate_limit_delay,
            max_retries=max_retries,
            logger=self.logger,
        )

        # Stats tracking
        self.stats = {
            'releases_processed': 0,
            'releases_with_apple_music': 0,
            'releases_matched': 0,
            'releases_no_match': 0,
            'releases_skipped': 0,
            'tracks_matched': 0,
            'tracks_no_match': 0,
            'artwork_added': 0,
            'errors': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'local_catalog_hits': 0,
            'catalog_queries': 0,
        }

        # Validation thresholds
        if strict_mode:
            self.min_artist_similarity = 75
            self.min_album_similarity = 65
            self.min_track_similarity = 85
        else:
            self.min_artist_similarity = 65
            self.min_album_similarity = 55
            self.min_track_similarity = 75

    def match_releases(self, song_identifier: str) -> Dict[str, Any]:
        """
        Match all releases for a song to Apple Music.

        This is the main entry point for matching.

        Args:
            song_identifier: Song title or ID (with optional 'song-' prefix)

        Returns:
            Dict with 'success', 'song', 'stats', and 'message' keys
        """
        # Look up the song
        if song_identifier.startswith('song-') or self._is_uuid(song_identifier):
            song = find_song_by_id(song_identifier)
        else:
            song = find_song_by_name(song_identifier)

        if not song:
            return {
                'success': False,
                'song': None,
                'stats': self.stats,
                'message': f"Song not found: {song_identifier}"
            }

        song_id = str(song['id'])
        song_title = song['title']

        self.logger.info(f"Matching Apple Music for: {song_title}")

        # Get all releases for this song
        releases = get_releases_for_song(song_id, self.artist_filter)

        if not releases:
            return {
                'success': True,
                'song': song,
                'stats': self.stats,
                'message': f"No releases found for song: {song_title}"
            }

        self.logger.info(f"Found {len(releases)} releases to process")

        # Process each release with a fresh connection to avoid timeouts
        # (catalog searches can take a long time for some releases)
        for i, release in enumerate(releases):
            if self.progress_callback:
                self.progress_callback('matching', i + 1, len(releases))

            try:
                with get_db_connection() as conn:
                    self._process_release(conn, song_id, song_title, release, i + 1, len(releases))
            except Exception as e:
                # Log error but continue with next release
                release_title = release.get('title', 'Unknown')
                self.logger.error(f"  Error processing release {i + 1}/{len(releases)} ({release_title}): {e}")
                self.stats['errors'] += 1
                # Refresh the catalog connection if available
                if self.catalog:
                    try:
                        self.catalog._refresh_conn()
                    except Exception:
                        pass

        # Aggregate client stats
        self.stats['cache_hits'] = self.client.stats.get('cache_hits', 0)
        self.stats['api_calls'] = self.client.stats.get('api_calls', 0)
        if self.catalog:
            self.stats['catalog_queries'] = self.catalog.get_query_count()

        return {
            'success': True,
            'song': song,
            'stats': self.stats,
            'message': f"Processed {len(releases)} releases"
        }

    def _process_release(
        self,
        conn,
        song_id: str,
        song_title: str,
        release: Dict,
        current: int = 0,
        total: int = 0
    ) -> None:
        """
        Process a single release, matching it to Apple Music.

        Args:
            conn: Database connection
            song_id: Our song ID
            song_title: Song title for track matching
            release: Release dict from database
            current: Current release number (1-indexed)
            total: Total number of releases
        """
        release_id = str(release['id'])
        release_title = release['title']
        artist_credit = release['artist_credit'] or ''
        release_year = release.get('release_year')

        self.stats['releases_processed'] += 1

        progress = f"[{current}/{total}] " if current and total else ""

        # Check if already matched
        if release.get('has_apple_music') and not self.rematch:
            self.logger.debug(f"  {progress}Skipping album search (already has Apple Music): {release_title}")
            self.stats['releases_skipped'] += 1
            self.stats['releases_with_apple_music'] += 1

            # Still try to match tracks if album link exists but tracks might be missing
            apple_album_id = release.get('apple_music_album_id')
            if apple_album_id:
                self._match_track_on_release(
                    conn, song_id, song_title, release_id, apple_album_id,
                    from_local_catalog=True  # Assume local catalog to avoid API calls
                )
            return

        # Check if already searched with no match (cached negative result)
        # rematch_failures allows re-processing these while keeping existing matches
        if release.get('apple_music_searched_at') and not self.rematch and not self.rematch_failures:
            self.logger.debug(f"  {progress}Skipping (previously searched, no match): {release_title}")
            self.stats['releases_skipped'] += 1
            return

        self.logger.info(f"  {progress}Processing: {artist_credit} - {release_title}")

        # Search Apple Music for this album
        apple_album = search_and_validate_album(
            self, artist_credit, release_title, release_year
        )

        if not apple_album:
            self.logger.info(f"    No match found")
            self.stats['releases_no_match'] += 1
            # Cache negative result so we don't re-search
            mark_release_searched(conn, release_id, self.dry_run, self.logger)
            return

        # Found a match - update the database
        album_id = apple_album['id']
        album_url = build_apple_music_album_url(album_id)
        match_confidence = apple_album.get('_match_confidence', 0.8)

        self.logger.info(f"    Matched: {apple_album['artist']} - {apple_album['name']}")

        # If matched via local catalog, fetch artwork from iTunes API
        # (local catalog doesn't include artwork URLs)
        if apple_album.get('_source') == 'local_catalog' and not apple_album.get('artwork'):
            self.logger.debug(f"    Fetching artwork from iTunes API for album {album_id}")
            try:
                api_album = self.client.lookup_album(album_id)
                if api_album and api_album.get('artwork'):
                    apple_album['artwork'] = api_album['artwork']
                    self.logger.debug(f"    Got artwork from iTunes API")
            except Exception as e:
                self.logger.warning(f"    Failed to fetch artwork from iTunes API: {e}")

        # Save album link
        upsert_release_streaming_link(
            conn,
            release_id=release_id,
            service_id=album_id,
            service_url=album_url,
            match_confidence=match_confidence,
            match_method='fuzzy_search',
            dry_run=self.dry_run,
            log=self.logger,
        )

        # Save artwork
        if apple_album.get('artwork'):
            if upsert_release_imagery(
                conn,
                release_id=release_id,
                artwork=apple_album['artwork'],
                source_id=album_id,
                dry_run=self.dry_run,
                log=self.logger,
            ):
                self.stats['artwork_added'] += 1

        self.stats['releases_matched'] += 1

        # Mark as searched (for consistency and rematch tracking)
        mark_release_searched(conn, release_id, self.dry_run, self.logger)

        # Now try to match the specific track
        from_local = apple_album.get('_source') == 'local_catalog'
        self._match_track_on_release(
            conn, song_id, song_title, release_id, album_id, from_local
        )

    def _match_track_on_release(
        self,
        conn,
        song_id: str,
        song_title: str,
        release_id: str,
        apple_album_id: str,
        from_local_catalog: bool = False
    ) -> None:
        """
        Match a specific track on a release to an Apple Music track.

        Args:
            conn: Database connection
            song_id: Our song ID
            song_title: Song title to match
            release_id: Our release ID
            apple_album_id: Apple Music album ID we matched
            from_local_catalog: If True, album was matched from local catalog
        """
        # Get our recordings on this release (use existing connection to avoid idle timeout)
        recordings = get_recordings_for_release(song_id, release_id, conn=conn)

        if not recordings:
            self.logger.debug(f"    No recordings found for this release")
            return

        # Get tracks from Apple Music - prefer local catalog if available
        am_tracks = None

        if from_local_catalog and self.catalog:
            try:
                catalog_songs = self.catalog.get_songs_for_album(apple_album_id)
                if catalog_songs:
                    # Convert to our expected format
                    am_tracks = []
                    for song in catalog_songs:
                        am_tracks.append({
                            'id': str(song.get('id', '')),
                            'name': song.get('name', ''),
                            'disc_number': song.get('discNumber', 1),
                            'track_number': song.get('trackNumber', 0),
                            'duration_ms': song.get('durationInMillis'),
                            'preview_url': song.get('previewUrl'),
                            'isrc': song.get('isrc'),
                        })
            except Exception as e:
                self.logger.debug(f"Local catalog track lookup failed: {e}")

        # Fall back to API if needed
        if not am_tracks:
            am_tracks = self.client.lookup_album_tracks(apple_album_id)

        if not am_tracks:
            self.logger.debug(f"    Could not fetch tracks from Apple Music")
            return

        # Try to match each of our recordings
        for rec in recordings:
            recording_release_id = str(rec['recording_release_id'])
            our_title = rec['song_title']
            our_disc = rec.get('disc_number', 1)
            our_track = rec.get('track_number')

            # Skip if already matched
            if rec.get('apple_music_track_id') and not self.rematch:
                continue

            # Find matching track
            matched_track = find_matching_track(
                self, our_title, am_tracks, our_disc, our_track
            )

            if matched_track:
                track_id = matched_track['id']
                track_url = build_apple_music_track_url(track_id)
                track_name = matched_track.get('name')

                upsert_track_streaming_link(
                    conn,
                    recording_release_id=recording_release_id,
                    service_id=track_id,
                    service_url=track_url,
                    duration_ms=matched_track.get('duration_ms'),
                    preview_url=matched_track.get('preview_url'),
                    isrc=matched_track.get('isrc'),
                    service_title=track_name,
                    match_confidence=matched_track.get('_match_confidence', 0.8),
                    match_method='fuzzy_search',
                    dry_run=self.dry_run,
                    log=self.logger,
                )

                self.stats['tracks_matched'] += 1
                self.logger.debug(f"      Matched track: {track_name}")
            else:
                self.stats['tracks_no_match'] += 1
                # Log track match failure with details
                self.logger.debug(f"      Track no match: \"{our_title}\"")
                # Find best candidate for debugging
                if am_tracks:
                    norm_our = normalize_for_comparison(our_title)
                    best_sim = 0
                    best_candidate = None
                    for t in am_tracks:
                        norm_am = normalize_for_comparison(t.get('name', ''))
                        sim = calculate_similarity(norm_our, norm_am)
                        if sim > best_sim:
                            best_sim = sim
                            best_candidate = t.get('name', '')
                    if best_candidate:
                        self.logger.debug(f"        Best candidate: \"{best_candidate}\" ({best_sim:.1f}% < {self.min_track_similarity}%)")

    def _is_uuid(self, s: str) -> bool:
        """Check if string looks like a UUID"""
        import re
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, s.lower()))

    def print_stats(self) -> None:
        """Print matching statistics"""
        self.logger.info("=" * 60)
        self.logger.info("Apple Music Matching Statistics")
        self.logger.info("=" * 60)
        self.logger.info(f"Releases processed:     {self.stats['releases_processed']}")
        self.logger.info(f"  Already had Apple:    {self.stats['releases_with_apple_music']}")
        self.logger.info(f"  Matched:              {self.stats['releases_matched']}")
        self.logger.info(f"  No match:             {self.stats['releases_no_match']}")
        self.logger.info(f"  Skipped:              {self.stats['releases_skipped']}")
        self.logger.info(f"Tracks matched:         {self.stats['tracks_matched']}")
        self.logger.info(f"Tracks no match:        {self.stats['tracks_no_match']}")
        self.logger.info(f"Artwork added:          {self.stats['artwork_added']}")
        self.logger.info(f"Local catalog hits:     {self.stats['local_catalog_hits']}")
        self.logger.info(f"Catalog queries:        {self.stats['catalog_queries']}")
        self.logger.info(f"Cache hits:             {self.stats['cache_hits']}")
        self.logger.info(f"API calls:              {self.stats['api_calls']}")
        self.logger.info(f"Errors:                 {self.stats['errors']}")
        self.logger.info("=" * 60)
