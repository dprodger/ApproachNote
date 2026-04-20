"""
Spotify Track Matching Utilities
Core business logic for matching releases to Spotify albums

UPDATED: Recording-Centric Performer Architecture
- Spotify data (album art, URLs) is stored on RELEASES, not recordings
- Recordings have a default_release_id pointing to the best release for display
- The match_releases() method is the primary entry point

This module provides the SpotifyMatcher class which handles:
- Spotify API authentication and token management
- Fuzzy matching and validation of albums and tracks
- Album artwork extraction (stored on releases)
- Database updates for releases and recording_releases
- Setting default_release_id on recordings
- Caching of API responses to minimize rate limiting
- Intelligent rate limit handling with exponential backoff

Used by:
- scripts/match_spotify_releases.py (CLI interface)
- song_research.py (background worker)
"""

import logging
from typing import Dict, Any, Optional, List

from db_utils import get_db_connection

from integrations.spotify.client import SpotifyClient
from integrations.spotify.matching import (
    normalize_for_comparison,
    calculate_similarity,
    is_substring_title_match,
    extract_primary_artist,
    duration_confidence,
    check_album_context_via_tracklist,
    match_track_to_recording,
)
from integrations.spotify.search import search_spotify_album
from integrations.spotify.diagnostics import (
    is_track_match_cached_failure,
    cache_track_match_failure,
    log_duration_rejection,
    log_orphaned_track,
    log_album_context_audit,
)
from integrations.spotify.db import (
    find_song_by_name,
    find_song_by_id,
    get_recordings_for_song,
    get_releases_for_song,
    get_releases_with_duration_mismatches,
    get_releases_without_artwork,
    get_recordings_for_release,
    update_release_spotify_data,
    update_release_artwork,
    clear_release_spotify_data,
    clear_recording_release_track,
    update_recording_release_track_id,
    update_recording_default_release,
    is_track_blocked,
    is_album_blocked,
)

logger = logging.getLogger(__name__)


class SpotifyMatcher:
    """
    Handles matching recordings to Spotify tracks with fuzzy validation and caching
    """
    
    def __init__(self, dry_run=False, strict_mode=False, force_refresh=False,
                 artist_filter=False, cache_days=30, logger=None,
                 rate_limit_delay=0.2, max_retries=3,
                 progress_callback=None, rematch=False, rematch_tracks=False,
                 rematch_all=False, duration_mismatch_threshold=None,
                 album_context=None):
        """
        Initialize Spotify Matcher

        Args:
            dry_run: If True, show what would be matched without making changes
            artist_filter: Filter to recordings by specific artist
            strict_mode: If True, use stricter validation thresholds (recommended)
            logger: Optional logger instance (uses module logger if not provided)
            cache_days: Number of days before cache is considered stale
            force_refresh: If True, always fetch fresh data ignoring cache
            rate_limit_delay: Base delay between API calls (seconds)
            max_retries: Maximum number of retries for rate-limited requests
            progress_callback: Optional callback(phase, current, total) for progress tracking
            rematch: If True, re-evaluate releases that already have Spotify URLs
            rematch_tracks: If True, re-run track matching for releases with album IDs
            rematch_all: If True, full re-match from scratch - ignores existing track IDs too
            duration_mismatch_threshold: If set (in ms), only process releases with
                duration mismatches above this threshold. Implies rematch-all behavior.
            album_context: None (default), 'audit', or 'rescue'. When set, tracks
                that would be rejected for low duration confidence are evaluated
                against album-wide match context. 'audit' logs what would be rescued
                without changing behavior. 'rescue' accepts them with match_method
                'album_context'.
        """
        self.dry_run = dry_run
        self.artist_filter = artist_filter
        self.strict_mode = strict_mode
        self.duration_mismatch_threshold = duration_mismatch_threshold
        self.album_context = album_context
        # duration-mismatches mode implies full rematch
        if duration_mismatch_threshold is not None:
            rematch = True
            rematch_tracks = True
            rematch_all = True
        self.rematch = rematch
        self.rematch_tracks = rematch_tracks
        self.rematch_all = rematch_all
        self.logger = logger or logging.getLogger(__name__)
        self.progress_callback = progress_callback
        
        # Initialize the API client
        self.client = SpotifyClient(
            cache_days=cache_days,
            force_refresh=force_refresh,
            rate_limit_delay=rate_limit_delay,
            max_retries=max_retries,
            logger=self.logger
        )
        
        # Stats - updated for releases and tracks
        self.stats = {
            'recordings_processed': 0,
            'recordings_with_spotify': 0,
            'recordings_updated': 0,
            'recordings_no_match': 0,
            'recordings_skipped': 0,
            'recordings_rejected': 0,
            'releases_processed': 0,
            'releases_with_spotify': 0,
            'releases_updated': 0,
            'releases_no_match': 0,
            'releases_skipped': 0,
            'releases_blocked': 0,  # Albums blocked via bad_streaming_matches
            'releases_cleared': 0,  # Releases where stale Spotify data was cleared on rematch
            'tracks_matched': 0,
            'tracks_skipped': 0,
            'tracks_no_match': 0,
            'tracks_had_previous': 0,  # Tracks that had a match before but failed rematch
            'tracks_blocked': 0,  # Tracks blocked via bad_streaming_matches
            'tracks_album_context_rescued': 0,  # Tracks rescued by album context
            'tracks_album_context_would_rescue': 0,  # Tracks that would be rescued (audit mode)
            'errors': 0,
            'cache_hits': 0,
            'api_calls': 0,
            'rate_limit_hits': 0,
            'rate_limit_waits': 0
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
    
    def _aggregate_client_stats(self):
        """
        Aggregate statistics from the SpotifyClient into the matcher's stats.

        The client owns cache_hits, api_calls, and rate-limit counters — matcher
        pulls them forward before returning results so callers see one combined
        stats dict on the matcher.
        """
        self.stats['cache_hits'] = self.client.stats.get('cache_hits', 0)
        self.stats['api_calls'] = self.client.stats.get('api_calls', 0)
        self.stats['rate_limit_hits'] = self.client.stats.get('rate_limit_hits', 0)
        self.stats['rate_limit_waits'] = self.client.stats.get('rate_limit_waits', 0)
    
    # ========================================================================
    # DELEGATED PROPERTIES (for backwards compatibility)
    # ========================================================================
    
    @property
    def last_made_api_call(self):
        return self.client.last_made_api_call
    
    @last_made_api_call.setter
    def last_made_api_call(self, value):
        self.client.last_made_api_call = value
    
    # ========================================================================
    # MATCHING HELPER METHODS
    # ========================================================================
    
    def normalize_for_comparison(self, text: str) -> str:
        """Normalize text for fuzzy comparison"""
        return normalize_for_comparison(text)
    
    def calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two strings"""
        return calculate_similarity(text1, text2)
    
    def is_substring_title_match(self, title1: str, title2: str) -> bool:
        """Check if one normalized title is a complete substring of the other"""
        return is_substring_title_match(title1, title2)
    
    def extract_primary_artist(self, artist_credit: str) -> str:
        """Extract the primary artist from a MusicBrainz artist_credit string"""
        return extract_primary_artist(artist_credit)

    # ========================================================================
    # DATABASE METHODS (delegated)
    # ========================================================================
    
    def find_song_by_name(self, song_name: str) -> Optional[dict]:
        """Look up song by name"""
        return find_song_by_name(song_name)
    
    def find_song_by_id(self, song_id: str) -> Optional[dict]:
        """Look up song by ID"""
        return find_song_by_id(song_id)
    
    def get_recordings_for_song(self, song_id: str) -> List[dict]:
        """Get all recordings for a song, optionally filtered by artist"""
        return get_recordings_for_song(song_id, self.artist_filter)
    
    def get_releases_for_song(self, song_id: str) -> List[dict]:
        """Get all releases for a song, optionally filtered by artist"""
        return get_releases_for_song(song_id, self.artist_filter)
    
    def get_releases_without_artwork(self) -> List[dict]:
        """Get releases with Spotify URL but no cover artwork"""
        return get_releases_without_artwork()
    
    def get_recordings_for_release(self, song_id: str, release_id: str, conn=None) -> List[dict]:
        """Get recordings linked to a specific release for a specific song"""
        return get_recordings_for_release(song_id, release_id, conn=conn)
    
    def update_release_spotify_data(self, conn, release_id: str, spotify_data: dict,
                                    release_title: str = None, artist: str = None,
                                    year: int = None, index: int = None, total: int = None):
        """Update release with Spotify album URL, ID, and cover artwork"""
        update_release_spotify_data(conn, release_id, spotify_data, 
                                   dry_run=self.dry_run, log=self.logger)
        
        if not self.dry_run:
            if index and total and release_title:
                self.logger.info(f"[{index}/{total}] {release_title} ({artist or 'Unknown'}, {year or 'Unknown'}) - ✓ Updated with Spotify URL and cover artwork")
            else:
                self.logger.info(f"    ✓ Updated with Spotify URL and cover artwork")
            
            self.stats['releases_updated'] += 1
    
    def update_release_artwork(self, conn, release_id: str, album_art: dict):
        """Update release with cover artwork only"""
        update_release_artwork(conn, release_id, album_art, 
                              dry_run=self.dry_run, log=self.logger)
        if not self.dry_run:
            self.logger.info(f"    ✓ Updated with cover artwork")
            self.stats['releases_updated'] += 1
    
    def update_recording_release_track_id(self, conn, recording_id: str, release_id: str,
                                          track_id: str, track_url: str,
                                          disc_number: int = None, track_number: int = None,
                                          track_title: str = None, duration_ms: int = None,
                                          match_confidence: float = None,
                                          match_method: str = 'fuzzy_search'):
        """Update the recording_releases junction table with Spotify track info"""
        update_recording_release_track_id(conn, recording_id, release_id, track_id, track_url,
                                         disc_number=disc_number, track_number=track_number,
                                         track_title=track_title, duration_ms=duration_ms,
                                         match_confidence=match_confidence,
                                         match_method=match_method,
                                         dry_run=self.dry_run, log=self.logger)
    
    def update_recording_default_release(self, conn, song_id: str, release_id: str):
        """Update recordings linked to a release to set it as their default_release"""
        update_recording_default_release(conn, song_id, release_id,
                                        dry_run=self.dry_run, log=self.logger)

    def match_recordings(self, song_identifier: str) -> Dict[str, Any]:
        """
        Main method to match Spotify tracks for a song's recordings
        
        Args:
            song_identifier: Song name or database ID
            
        Returns:
            dict: {
                'success': bool,
                'song': dict (if found),
                'stats': dict,
                'error': str (if failed)
            }
        """
        # DEPRECATED: Redirect to match_releases
        self.logger.warning("match_recordings() is deprecated - redirecting to match_releases()")
        self.logger.info("Spotify data is now stored on releases, not recordings.")
        self.logger.info("Use match_releases() directly for better results.")
        self.logger.info("")
        
        return self.match_releases(song_identifier)

    # ========================================================================
    # MAIN ORCHESTRATION METHODS
    # ========================================================================
    
    def match_releases(self, song_identifier: str, start_from: int = 1) -> Dict[str, Any]:
        """
        Main method to match Spotify albums for a song's releases

        Args:
            song_identifier: Song name or database ID
            start_from: Release number to start from (1-indexed). Use this to resume
                       after a previous run was interrupted. Releases before this
                       number will be skipped.

        Returns:
            dict: {
                'success': bool,
                'song': dict (if found),
                'stats': dict,
                'error': str (if failed)
            }
        """
        try:
            # Find the song
            if song_identifier.startswith('song-') or len(song_identifier) == 36:
                song = self.find_song_by_id(song_identifier)
            else:
                song = self.find_song_by_name(song_identifier)
            
            if not song:
                return {
                    'success': False,
                    'error': 'Song not found'
                }
            
            self.logger.info(f"Song: {song['title']}")
            self.logger.info(f"Composer: {song['composer']}")
            self.logger.info(f"Database ID: {song['id']}")
            if song.get('alt_titles'):
                self.logger.info(f"Alt titles: {song['alt_titles']}")
            if self.artist_filter:
                self.logger.info(f"Filtering to releases by: {self.artist_filter}")
            self.logger.info("")
            
            # Get releases
            if self.duration_mismatch_threshold is not None:
                releases = get_releases_with_duration_mismatches(
                    song['id'], self.duration_mismatch_threshold, self.artist_filter)
            else:
                releases = self.get_releases_for_song(song['id'])
            
            if not releases:
                return {
                    'success': False,
                    'song': song,
                    'error': 'No releases found for this song'
                }
            
            self.logger.info(f"Found {len(releases)} releases to process")
            if start_from > 1:
                self.logger.info(f"Resuming from release #{start_from} (skipping first {start_from - 1})")
            self.logger.info("")

            # Process each release
            for i, release in enumerate(releases, 1):
                # Skip releases before start_from (for resuming interrupted runs)
                if i < start_from:
                    continue

                self.stats['releases_processed'] += 1

                # Report progress via callback
                if self.progress_callback:
                    self.progress_callback('spotify_track_match', i, len(releases))
                
                title = release['title'] or 'Unknown Album'
                year = release['release_year']
                
                # Get artist - prefer artist_credit (full credit from MusicBrainz release)
                # This preserves ensemble names like "Gene Krupa & His Orchestra"
                # which would otherwise be truncated by extract_primary_artist
                artist_credit = release.get('artist_credit')
                artist_name = artist_credit

                if not artist_name:
                    performers = release.get('performers') or []
                    leaders = [p['name'] for p in performers if p.get('role') == 'leader']
                    artist_name = leaders[0] if leaders else (
                        performers[0]['name'] if performers else None
                    )
                
                self.logger.debug(f"[{i}/{len(releases)}] {title}")
                self.logger.debug(f"    Artist: {artist_name or 'Unknown'}")
                self.logger.debug(f"    Year: {year or 'Unknown'}")
                
                # Check if already has Spotify ID (skip unless rematch or rematch_tracks mode)
                if release.get('spotify_album_id') and not self.rematch and not self.rematch_tracks:
                    self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ⊙ Already has Spotify ID, skipping")
                    self.stats['releases_skipped'] += 1
                    continue
                elif release.get('spotify_album_id') and self.rematch_tracks and not self.rematch_all:
                    # rematch_tracks mode (not rematch_all): Re-run track matching for releases with album IDs
                    # but only if there are recordings missing track IDs
                    existing_album_id = release.get('spotify_album_id')
                    recordings = self.get_recordings_for_release(song['id'], release['id'])
                    needs_track_match = any(not r.get('spotify_track_id') for r in recordings)

                    if not needs_track_match:
                        self.logger.debug(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ⊙ All tracks already matched, skipping")
                        self.stats['releases_skipped'] += 1
                        continue

                    self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ↻ Re-matching tracks...")
                    # Fetch Spotify tracks BEFORE opening DB connection
                    # to avoid holding the connection idle during API calls
                    spotify_tracks = self.client.get_album_tracks(existing_album_id)
                    if not spotify_tracks:
                        self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ✗ Could not fetch Spotify album tracks")
                        self.stats['releases_no_match'] += 1
                        continue

                    with get_db_connection() as conn:
                        track_matched = self.match_tracks_for_release(
                            conn,
                            song['id'],
                            release['id'],
                            existing_album_id,
                            song['title'],
                            alt_titles=song.get('alt_titles'),
                            spotify_tracks=spotify_tracks
                        )
                        if track_matched:
                            self.stats['releases_with_spotify'] += 1
                        else:
                            self.stats['releases_no_match'] += 1
                    continue
                elif release.get('spotify_album_id') and self.rematch_all:
                    # rematch_all mode: Re-search for album AND re-match all tracks
                    self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ↻ Full re-match...")
                    # Fall through to album search below
                elif release.get('spotify_album_id') and self.rematch:
                    self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ↻ Re-matching...")
                elif self.rematch_tracks and not self.rematch_all and not release.get('spotify_album_id'):
                    # In rematch_tracks mode (not rematch_all), skip releases without album IDs
                    self.logger.debug(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ⊙ No album ID, skipping (rematch-tracks mode)")
                    self.stats['releases_skipped'] += 1
                    continue

                # Track whether this release had previous Spotify data (for cleanup on rematch failure)
                had_previous_spotify = bool(release.get('spotify_album_id'))

                # Search Spotify for album (with song title for track verification fallback)
                spotify_match = search_spotify_album(self, title, artist_name, song['title'])

                if spotify_match:
                    # Check if this album is blocked for this song
                    if is_album_blocked(song['id'], spotify_match['id']):
                        self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ⊘ Album blocked (in blocklist)")
                        self.stats['releases_blocked'] += 1
                        if had_previous_spotify:
                            with get_db_connection() as conn:
                                clear_release_spotify_data(conn, release['id'],
                                                          dry_run=self.dry_run, log=self.logger)
                            self.logger.info(f"    ✓ Cleared stale Spotify data")
                            self.stats['releases_cleared'] += 1
                        continue
                    # Check if we already know track matching fails for this combination
                    # This avoids opening a DB connection just to reach the same "no match" conclusion
                    # Skip this cache check in rematch_all mode
                    if not self.rematch_all and is_track_match_cached_failure(self.client, self.logger, song['id'], release['id'], spotify_match['id']):
                        self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ✗ Album matched but track not found (cached)")
                        self.stats['releases_no_match'] += 1
                        continue

                    # IMPORTANT: Fetch Spotify tracks BEFORE opening DB connection
                    # to avoid holding the connection idle during API calls
                    # (Supabase's PgBouncer has ~6 min idle timeout)
                    spotify_tracks = self.client.get_album_tracks(spotify_match['id'])
                    if not spotify_tracks:
                        self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ✗ Could not fetch Spotify album tracks")
                        self.stats['releases_no_match'] += 1
                        continue

                    with get_db_connection() as conn:
                        # Match tracks using pre-fetched data (no API calls inside DB transaction)
                        track_matched = self.match_tracks_for_release(
                            conn,
                            song['id'],
                            release['id'],
                            spotify_match['id'],
                            song['title'],
                            alt_titles=song.get('alt_titles'),
                            spotify_tracks=spotify_tracks
                        )

                        if track_matched:
                            # Only store album data if track was found (validates album match)
                            self.stats['releases_with_spotify'] += 1
                            self.update_release_spotify_data(
                                conn,
                                release['id'],
                                spotify_match,
                                title,
                                artist_name,
                                year,
                                i,
                                len(releases)
                            )

                            # NEW: Set this as the default release for linked recordings
                            # (only if they don't already have a better default)
                            self.update_recording_default_release(
                                conn,
                                song['id'],
                                release['id']
                            )
                        else:
                            # Album matched but no track found - cache this for future runs
                            cache_track_match_failure(
                                self.client, self.logger,
                                song['id'], release['id'], spotify_match['id'], song['title']
                            )
                            self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ✗ Album matched but track not found (possible false positive)")
                            self.stats['releases_no_match'] += 1
                            # Clear stale Spotify data if this was a rematch
                            if had_previous_spotify:
                                clear_release_spotify_data(conn, release['id'],
                                                          dry_run=self.dry_run, log=self.logger)
                                self.logger.info(f"    ✓ Cleared stale Spotify data")
                                self.stats['releases_cleared'] += 1
                else:
                    self.logger.info(f"[{i}/{len(releases)}] {title} ({artist_name or 'Unknown'}, {year or 'Unknown'}) - ✗ No valid Spotify match found")
                    self.stats['releases_no_match'] += 1
                    # Clear stale data if this was a rematch or duration-mismatches mode
                    if had_previous_spotify or self.duration_mismatch_threshold is not None:
                        with get_db_connection() as conn:
                            if had_previous_spotify:
                                clear_release_spotify_data(conn, release['id'],
                                                          dry_run=self.dry_run, log=self.logger)
                                self.logger.info(f"    ✓ Cleared stale Spotify data")
                                self.stats['releases_cleared'] += 1
                            # Also clear track-level links (may exist even if release-level was already cleared)
                            recordings = self.get_recordings_for_release(song['id'], release['id'], conn=conn)
                            for recording in recordings:
                                if recording.get('spotify_track_id'):
                                    clear_recording_release_track(
                                        conn, recording['recording_id'], release['id'],
                                        dry_run=self.dry_run, log=self.logger)
            
            self._aggregate_client_stats()
            return {
                'success': True,
                'song': song,
                'stats': self.stats
            }
            
        except Exception as e:
            self.logger.error(f"Error matching releases: {e}", exc_info=True)
            self._aggregate_client_stats()
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats
            }
    
    def match_tracks_for_release(self, conn, song_id: str, release_id: str,
                                  spotify_album_id: str, song_title: str,
                                  alt_titles: List[str] = None,
                                  spotify_tracks: List[dict] = None) -> bool:
        """
        Match Spotify tracks to recordings for a release

        After we've matched a release to a Spotify album, this method:
        1. Fetches all tracks from the Spotify album (or uses pre-fetched tracks)
        2. Gets our recordings linked to this release
        3. Fuzzy matches the song title to find the right track
        4. Updates the recording_releases junction table with the track ID

        Args:
            conn: Database connection
            song_id: Our song ID
            release_id: Our release ID
            spotify_album_id: Spotify album ID we matched to
            song_title: The song title to search for
            alt_titles: Alternative titles to try if primary doesn't match
            spotify_tracks: Pre-fetched Spotify tracks (optional). If provided, skips
                           the API call. IMPORTANT: Pass this when calling from within
                           a DB transaction to avoid holding the connection idle during
                           API calls (which can cause connection timeouts).

        Returns:
            bool: True if at least one track was matched, False otherwise
        """
        # Get tracks from Spotify album (use pre-fetched if provided)
        if spotify_tracks is None:
            spotify_tracks = self.client.get_album_tracks(spotify_album_id)
        if not spotify_tracks:
            self.logger.debug(f"    Could not fetch tracks for album {spotify_album_id}")
            return False
        
        self.logger.debug(f"    Matching tracks ({len(spotify_tracks)} tracks in album)...")
        
        # Get our recordings for this release (use existing connection to avoid idle timeout)
        recordings = self.get_recordings_for_release(song_id, release_id, conn=conn)

        any_matched = False
        for recording in recordings:
            # Skip if already has a track ID (unless rematch_all mode)
            if recording['spotify_track_id'] and not self.rematch_all:
                self.logger.debug(f"      Recording already has track ID, skipping")
                self.stats['tracks_skipped'] += 1
                any_matched = True  # Consider already-matched as success
                continue
            elif recording['spotify_track_id'] and self.rematch_all:
                self.logger.debug(f"      Recording has track ID but rematch_all mode, re-matching...")
            
            # Match song title to a track, passing position info for fallback matching
            # Pass conn to avoid nested connections and idle timeout issues
            recording_duration_ms = recording.get('recording_duration_ms')
            matched_track = match_track_to_recording(
                self.logger,
                self.stats,
                self.min_track_similarity,
                song_title,
                spotify_tracks,
                expected_disc=recording.get('disc_number'),
                expected_track=recording.get('track_number'),
                alt_titles=alt_titles,
                song_id=song_id,
                conn=conn,
                expected_duration_ms=recording_duration_ms
            )

            if matched_track:
                # Calculate match confidence from duration proximity
                confidence = None
                if recording_duration_ms and matched_track.get('duration_ms'):
                    confidence = duration_confidence(
                        recording_duration_ms, matched_track['duration_ms'])

                # Hard reject and log if confidence is too low
                rescued = False
                if confidence is not None and confidence <= 0.4:
                    title_score = self.calculate_similarity(song_title, matched_track['name'])
                    duration_diff = abs(recording_duration_ms - matched_track['duration_ms'])
                    self.logger.info(
                        f"      Rejecting low-confidence match: '{song_title}' → '{matched_track['name']}' "
                        f"(title {title_score}%, duration diff {duration_diff/1000:.0f}s, confidence {confidence})")

                    # Album context rescue: compare full MB vs Spotify tracklists
                    if self.album_context and title_score >= 90:
                        album_ctx = check_album_context_via_tracklist(
                            conn, release_id, spotify_tracks)
                        would_rescue = (
                            album_ctx['match_ratio'] >= 0.7
                            and album_ctx['matched_count'] >= 3
                        )
                        self.logger.info(
                            f"      Album context: {album_ctx['matched_count']}/{album_ctx['mb_track_count']} "
                            f"MB tracks match Spotify ({album_ctx['match_ratio']:.0%}) → "
                            f"{'RESCUE' if would_rescue else 'still reject'}")
                        log_album_context_audit(
                            self.logger,
                            song_title=song_title,
                            recording_id=recording['recording_id'],
                            release_id=release_id,
                            spotify_track_id=matched_track['id'],
                            spotify_track_name=matched_track['name'],
                            expected_ms=recording_duration_ms,
                            actual_ms=matched_track['duration_ms'],
                            confidence=confidence,
                            title_score=title_score,
                            album_context=album_ctx,
                            would_rescue=would_rescue,
                        )
                        if would_rescue:
                            self.stats['tracks_album_context_would_rescue'] += 1
                            if self.album_context == 'rescue':
                                rescued = True

                    if not rescued:
                        log_duration_rejection(
                            self.logger,
                            song_title=song_title,
                            recording_id=recording['recording_id'],
                            release_id=release_id,
                            spotify_track_id=matched_track['id'],
                            spotify_track_name=matched_track['name'],
                            expected_ms=recording_duration_ms,
                            actual_ms=matched_track['duration_ms'],
                            confidence=confidence,
                            title_score=title_score,
                        )
                        # Clear existing bad link if rematching
                        if recording.get('spotify_track_id'):
                            clear_recording_release_track(
                                conn, recording['recording_id'], release_id,
                                dry_run=self.dry_run, log=self.logger)
                        self.stats['tracks_no_match'] += 1
                        continue

                    # Rescued by album context — accept with low confidence
                    self.logger.info(
                        f"      ✓ Rescued by album context (match_method='album_context')")
                    self.stats['tracks_album_context_rescued'] += 1
                    rescued = True

                match_method = 'album_context' if rescued else 'fuzzy_search'
                self.update_recording_release_track_id(
                    conn,
                    recording['recording_id'],
                    release_id,
                    matched_track['id'],
                    matched_track['url'],
                    disc_number=matched_track.get('disc_number'),
                    track_number=matched_track.get('track_number'),
                    track_title=matched_track.get('name'),
                    duration_ms=matched_track.get('duration_ms'),
                    match_confidence=confidence,
                    match_method=match_method,
                )
                self.stats['tracks_matched'] += 1
                any_matched = True
            else:
                # Show what tracks are on the album to help debug
                track_names = [t['name'] for t in spotify_tracks[:8]]
                more = f"... (+{len(spotify_tracks) - 8} more)" if len(spotify_tracks) > 8 else ""
                self.logger.debug(f"      No track match for '{song_title}'")
                if alt_titles:
                    self.logger.debug(f"      Also tried alt titles: {alt_titles}")
                self.logger.debug(f"      Album tracks: {track_names}{more}")
                self.stats['tracks_no_match'] += 1

                # Clear existing bad link if rematching
                if recording.get('spotify_track_id'):
                    self.stats['tracks_had_previous'] += 1
                    previous_track_id = recording['spotify_track_id']
                    previous_url = f"https://open.spotify.com/track/{previous_track_id}"
                    self.logger.warning(f"      ⚠ Had previous track ID: {previous_track_id} — clearing stale link")

                    clear_recording_release_track(
                        conn, recording['recording_id'], release_id,
                        dry_run=self.dry_run, log=self.logger)

                    # Log to file for later investigation
                    log_orphaned_track(
                        self.logger,
                        release_id=release_id,
                        recording_id=recording['recording_id'],
                        spotify_track_url=previous_url
                    )
        
        return any_matched
    
    def backfill_images(self):
        """
        UPDATED: Backfill cover artwork for releases (not recordings).
        
        Album artwork is now stored on releases, not recordings.
        This method fetches artwork for releases that have a Spotify album ID
        but are missing cover art.
        """
        self.logger.info("="*80)
        self.logger.info("Spotify Cover Artwork Backfill (Releases)")
        self.logger.info("="*80)
        
        if self.dry_run:
            self.logger.info("*** DRY RUN MODE - No database changes will be made ***")
        
        self.logger.info("")
        
        # Get releases without images
        releases = self.get_releases_without_artwork()
        
        if not releases:
            self.logger.info("No releases found that need cover artwork")
            return True
        
        self.logger.info(f"Found {len(releases)} releases to process")
        self.logger.info("")
        
        # Process each release
        with get_db_connection() as conn:
            for i, release in enumerate(releases, 1):
                self.stats['releases_processed'] += 1
                
                title = release['title'] or 'Unknown Album'
                album_id = release['spotify_album_id']
                
                self.logger.info(f"[{i}/{len(releases)}] {title}")
                self.logger.info(f"    Album ID: {album_id}")
                
                if not album_id:
                    self.logger.warning(f"    ✗ No Spotify album ID")
                    self.stats['errors'] += 1
                    continue
                
                # Get album details (with caching)
                album_data = self.client.get_album_details(album_id)
                
                if not album_data:
                    self.logger.warning(f"    ✗ Could not fetch album details from Spotify")
                    self.stats['errors'] += 1
                    continue
                
                # Extract album artwork
                album_art = {}
                images = album_data.get('images', [])
                
                for image in images:
                    height = image.get('height', 0)
                    if height >= 600:
                        album_art['large'] = image['url']
                    elif height >= 300:
                        album_art['medium'] = image['url']
                    elif height >= 64:
                        album_art['small'] = image['url']
                
                if not album_art:
                    self.logger.warning(f"    ✗ No cover artwork found in album data")
                    self.stats['errors'] += 1
                    continue
                
                # Update release
                self.update_release_artwork(conn, release['id'], album_art)
        
        # Aggregate client stats before printing summary
        self._aggregate_client_stats()
        
        # Print summary
        self.logger.info("")
        self.logger.info("="*80)
        self.logger.info("BACKFILL SUMMARY")
        self.logger.info("="*80)
        self.logger.info(f"Releases processed: {self.stats['releases_processed']}")
        self.logger.info(f"Releases updated:   {self.stats['releases_updated']}")
        self.logger.info(f"Errors:             {self.stats['errors']}")
        self.logger.info(f"Cache hits:         {self.stats['cache_hits']}")
        self.logger.info(f"API calls:          {self.stats['api_calls']}")
        self.logger.info("="*80)
        
        return True
    
    def print_summary(self):
        """Print summary of matching statistics"""
        # Aggregate client stats before printing
        self._aggregate_client_stats()

        self.logger.info("\n" + "=" * 70)
        self.logger.info("SPOTIFY MATCHING SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Recordings processed:      {self.stats['recordings_processed']}")
        self.logger.info(f"Already had Spotify URL:   {self.stats['recordings_skipped']}")
        self.logger.info(f"Newly matched:             {self.stats['recordings_updated']}")
        self.logger.info(f"No match found:            {self.stats['recordings_no_match']}")
        self.logger.info(f"Errors:                    {self.stats['errors']}")
        self.logger.info("-" * 70)
        self.logger.info(f"Total with Spotify:        {self.stats['recordings_with_spotify']}")
        self.logger.info("-" * 70)
        # Show blocklist stats if any were encountered
        if self.stats['tracks_blocked'] > 0 or self.stats['releases_blocked'] > 0:
            self.logger.info(f"Tracks blocked:            {self.stats['tracks_blocked']}")
            self.logger.info(f"Albums blocked:            {self.stats['releases_blocked']}")
            self.logger.info("-" * 70)
        self.logger.info(f"API calls made:            {self.stats['api_calls']}")
        self.logger.info(f"Cache hits:                {self.stats['cache_hits']}")
        self.logger.info(f"Rate limit hits:           {self.stats['rate_limit_hits']}")
        self.logger.info(f"Rate limit waits:          {self.stats['rate_limit_waits']}")
        cache_hit_rate = (self.stats['cache_hits'] / (self.stats['api_calls'] + self.stats['cache_hits']) * 100) if (self.stats['api_calls'] + self.stats['cache_hits']) > 0 else 0
        self.logger.info(f"Cache hit rate:            {cache_hit_rate:.1f}%")
        self.logger.info("=" * 70)