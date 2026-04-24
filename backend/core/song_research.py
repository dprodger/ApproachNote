"""
Song Research Module
Coordinates background research tasks for songs

In-process responsibilities (this module):
- MBReleaseImporter for MusicBrainz releases and performer data
- Song-metadata updates (composer, wikipedia url, composed year)

After MusicBrainz import succeeds, _enqueue_downstream_jobs fans out
durable-queue jobs for the matchers that have been migrated:
- Spotify  -> research_worker/handlers/spotify.py
- Apple    -> research_worker/handlers/apple.py
- YouTube  -> research_worker/handlers/youtube.py (one per recording)

The worker service (research_worker/run.py) drains those jobs in
parallel with the in-process song-metadata updates.
"""

import logging
import os
from typing import Dict, Any

from integrations.musicbrainz.release_importer import MBReleaseImporter
from db_utils import get_db_connection, execute_query
from integrations.musicbrainz.utils import MusicBrainzSearcher, update_song_composer, update_song_wikipedia_url, update_song_composed_year
from core import research_queue, research_jobs
logger = logging.getLogger(__name__)


def _enqueue_downstream_jobs(song_id: str, force_refresh: bool) -> None:
    """Queue Spotify + per-recording YouTube jobs on the durable queue.

    Called from research_song() AFTER MusicBrainz import completes so the
    jobs see the freshly-created recordings + releases. Failures here are
    logged but don't abort the in-process flow — the refresh still has
    value even if the queue is temporarily unavailable.

    Priority 50 so these user-initiated jobs jump ahead of any bulk
    backfill work the worker might be chewing on. payload.rematch mirrors
    the refresh's force_refresh flag so handlers know whether to honor
    existing matches or re-evaluate them.
    """
    # Per-song jobs (Spotify + Apple Music).
    for source in (research_jobs.SOURCE_SPOTIFY, research_jobs.SOURCE_APPLE):
        try:
            research_jobs.enqueue(
                source=source,
                job_type='match_song',
                target_type=research_jobs.TARGET_SONG,
                target_id=song_id,
                payload={'rematch': force_refresh},
                priority=50,
            )
        except Exception:
            logger.exception("failed to enqueue %s job for song %s", source, song_id)

    # YouTube: one job per recording. Empty set is fine — nothing to match.
    try:
        recordings = execute_query(
            "SELECT id FROM recordings WHERE song_id = %s", (song_id,),
        ) or []
    except Exception:
        logger.exception("failed to query recordings for youtube enqueue")
        return

    queued = 0
    for rec in recordings:
        try:
            if research_jobs.enqueue(
                source=research_jobs.SOURCE_YOUTUBE,
                job_type='match_recording',
                target_type=research_jobs.TARGET_RECORDING,
                target_id=rec['id'],
                payload={'rematch': force_refresh},
                priority=50,
            ) is not None:
                queued += 1
        except Exception:
            logger.exception(
                "failed to enqueue youtube job for recording %s", rec['id'],
            )
    logger.info(
        "Enqueued downstream research jobs for song %s: "
        "spotify=1, apple=1, youtube=%d/%d recordings",
        song_id, queued, len(recordings),
    )


def research_song(song_id: str, song_name: str, force_refresh: bool = True) -> Dict[str, Any]:
    """
    Research a song and update its data

    This is the main entry point called by the in-process background worker
    thread. It imports MusicBrainz releases and performer credits, updates
    song-level metadata, and enqueues the per-source matching jobs onto
    the durable research queue. Spotify, Apple Music, and YouTube matching
    all run on the worker service.

    The function is designed to be fault-tolerant and will not raise exceptions
    to the caller - all errors are logged and returned in the result dict.

    Cache behavior is controlled by the force_refresh parameter:
    - force_refresh=True (default): "Deep refresh" - bypass all caches
    - force_refresh=False: "Simple refresh" - use cached data (30-day expiration)

    Args:
        song_id: UUID of the song to research
        song_name: Name of the song (for logging)
        force_refresh: If True (default), bypass cache and re-fetch all data.
                      If False, use cached data where available.

    Returns:
        dict: {
            'success': bool,
            'song_id': str,
            'song_name': str,
            'stats': dict (if successful),
            'error': str (if failed)
        }
    """
    refresh_mode = "deep" if force_refresh else "simple"
    logger.info(f"Starting research for song {song_id} / {song_name} ({refresh_mode} refresh)")
    
    # Create a progress callback that updates the research_queue progress state
    def progress_callback(phase: str, current: int, total: int):
        research_queue.update_progress(phase, current, total)
    
    try:
        # Step 1: Import MusicBrainz releases
        # MBReleaseImporter uses MusicBrainzSearcher internally which has caching
        importer = MBReleaseImporter(
            dry_run=False,
            force_refresh=force_refresh,
            logger=logger,
            progress_callback=progress_callback
        )
        
        # Get import limit from environment variable, default to 100
        mb_import_limit = int(os.environ.get('MB_IMPORT_LIMIT', 100))
        logger.info(f"Importing MusicBrainz releases...; limiting to {mb_import_limit}")
        mb_result = importer.import_releases(str(song_id), mb_import_limit)
        
        if not mb_result['success']:
            error = mb_result.get('error', 'Unknown error')
            logger.error(f"✗ Failed to import MusicBrainz releases: {error}")
            return {
                'success': False,
                'song_id': song_id,
                'song_name': song_name,
                'error': f"MusicBrainz import failed: {error}"
            }
        
        mb_stats = mb_result['stats']
        logger.info(f"✓ MusicBrainz import complete")
        logger.info(f"  Recordings found: {mb_stats['recordings_found']}")
        logger.info(f"  Recordings created: {mb_stats['recordings_created']}")
        logger.info(f"  Releases created: {mb_stats['releases_created']}")
        logger.info(f"  Releases existing: {mb_stats['releases_existing']}")
        logger.info(f"  Performers linked: {mb_stats['performers_linked']}")
        # Cover Art Archive stats (integrated into MBReleaseImporter)
        if mb_stats.get('caa_releases_checked', 0) > 0:
            logger.info(f"  CAA releases checked: {mb_stats['caa_releases_checked']}")
            logger.info(f"  CAA releases with art: {mb_stats['caa_releases_with_art']}")
            logger.info(f"  CAA images created: {mb_stats['caa_images_created']}")
        if mb_stats['errors'] > 0:
            logger.info(f"  Errors: {mb_stats['errors']}")

        # Step 1.9: Enqueue downstream research jobs on the durable queue.
        #
        # These MUST happen after MB import so they see the freshly-created
        # recordings + releases. Enqueueing here (not in the HTTP handler)
        # also means a brand-new song with no recordings yet still gets
        # YouTube jobs once MB creates them — something the old route-side
        # enqueue couldn't do.
        #
        # Spotify, Apple Music, and YouTube matching all run in the
        # worker service in parallel with the song-metadata updates below.
        _enqueue_downstream_jobs(str(song_id), force_refresh)

        # Step 1.5: Update composer from MusicBrainz if needed
        logger.info("Checking for composer update...")
        composer_updated = update_song_composer(str(song_id))
        if not composer_updated:
            logger.debug("Composer not updated (already set or not found)")
        
        # Step 1.6: Update Wikipedia URL from MusicBrainz if needed
        logger.info("Checking for Wikipedia URL update...")
        wikipedia_updated = update_song_wikipedia_url(str(song_id))
        if not wikipedia_updated:
            logger.debug("Wikipedia URL not updated (already set or not found)")

        # Step 1.7: Update composed_year from MusicBrainz if needed
        logger.info("Checking for composed_year update...")
        composed_year_updated = update_song_composed_year(str(song_id))
        if not composed_year_updated:
            logger.debug("Composed year not updated (already set or not found)")

        # Spotify, Apple Music, and YouTube matching all run on the
        # durable research queue (research_worker/handlers/*). Their
        # per-job stats live on the research_jobs row's `result` field —
        # see the admin research dashboard for live state.
        combined_stats = {
            'musicbrainz': mb_stats,
        }

        logger.info(f"✓ Successfully researched {song_name}")

        return {
            'success': True,
            'song_id': song_id,
            'song_name': song_name,
            'stats': combined_stats
        }
            
    except Exception as e:
        # Catch any unexpected errors so they don't crash the worker thread
        error_msg = f"Unexpected error researching song {song_id}: {e}"
        logger.error(error_msg, exc_info=True)
        
        return {
            'success': False,
            'song_id': song_id,
            'song_name': song_name,
            'error': error_msg
        }


# Future expansion: Additional research functions can be added here
# For example:
# - research_song_wikipedia(song_id, song_name)
# - update_song_images(song_id, song_name)