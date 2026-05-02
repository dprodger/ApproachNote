#!/usr/bin/env python3
"""
Backfill Spotify Track Links

Finds releases that have a spotify_album_id but are missing track-level
streaming links in recording_release_streaming_links, and runs the Spotify
track matcher for each affected song.

Features:
- Processes one song at a time, committing after each
- Tracks progress in a JSON file so it can be resumed after interruption
- Skips songs already completed in previous runs
- Rate-limited to avoid Spotify API throttling

Usage:
    python scripts/backfill_spotify_track_links.py
    python scripts/backfill_spotify_track_links.py --dry-run
    python scripts/backfill_spotify_track_links.py --reset   # clear progress and start over
    python scripts/backfill_spotify_track_links.py --limit 10  # process only 10 songs
"""

import sys
import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from db_utils import get_db_connection

PROGRESS_FILE = Path(__file__).parent / 'backfill_spotify_track_links_progress.json'


def load_progress() -> dict:
    """Load progress from previous runs."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        'completed_songs': [],
        'failed_songs': [],
        'started_at': None,
        'last_updated': None,
        'total_tracks_matched': 0,
        'total_tracks_no_match': 0,
    }


def save_progress(progress: dict):
    """Save progress to disk."""
    progress['last_updated'] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_affected_songs() -> list:
    """
    Find all songs that have releases with spotify_album_id but no Spotify
    track link in recording_release_streaming_links.

    Returns list of (song_id, song_title, stuck_release_count).
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.title, COUNT(DISTINCT r.id) as stuck_releases
                FROM songs s
                JOIN recordings rec ON rec.song_id = s.id
                JOIN recording_releases rr ON rr.recording_id = rec.id
                JOIN releases r ON r.id = rr.release_id
                JOIN release_streaming_links rsl
                    ON rsl.release_id = r.id AND rsl.service = 'spotify'
                WHERE NOT EXISTS (
                      SELECT 1 FROM recording_release_streaming_links rrsl
                      WHERE rrsl.recording_release_id = rr.id
                        AND rrsl.service = 'spotify'
                  )
                GROUP BY s.id, s.title
                ORDER BY COUNT(DISTINCT r.id) DESC
            """)
            return cur.fetchall()


def main():
    parser = argparse.ArgumentParser(description='Backfill Spotify track links for releases with album IDs')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--reset', action='store_true', help='Clear progress and start from scratch')
    parser.add_argument('--limit', type=int, default=0, help='Max number of songs to process (0 = all)')
    parser.add_argument('--song', type=str, help='Process a specific song by name or ID')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%H:%M:%S'
    )
    logger = logging.getLogger('backfill_spotify_tracks')
    # Quiet down noisy loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('spotify_client').setLevel(logging.INFO)

    # Handle reset
    if args.reset:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
            logger.info(f"Cleared progress file: {PROGRESS_FILE}")
        else:
            logger.info("No progress file to clear")
        return

    # Load progress
    progress = load_progress()
    completed_set = set(progress['completed_songs'])
    failed_set = set(progress['failed_songs'])

    if not progress['started_at']:
        progress['started_at'] = datetime.now().isoformat()

    # Find affected songs
    logger.info("Finding songs with missing Spotify track links...")
    affected_songs = get_affected_songs()
    total_songs = len(affected_songs)
    total_stuck = sum(row['stuck_releases'] for row in affected_songs)

    logger.info(f"Found {total_songs} songs with {total_stuck} stuck releases")
    logger.info(f"Already completed: {len(completed_set)} songs")
    logger.info(f"Previously failed: {len(failed_set)} songs (will retry)")

    # Filter to songs not yet completed
    pending = [s for s in affected_songs if str(s['id']) not in completed_set]

    # Filter to specific song if requested
    if args.song:
        song_filter = args.song.lower()
        pending = [s for s in pending
                   if song_filter in s['title'].lower() or song_filter == str(s['id'])]
        if not pending:
            logger.error(f"No pending songs matching '{args.song}'")
            return

    logger.info(f"Pending: {len(pending)} songs")

    if not pending:
        logger.info("Nothing to do — all songs already processed!")
        return

    if args.limit:
        pending = pending[:args.limit]
        logger.info(f"Limited to {args.limit} songs")

    if args.dry_run:
        logger.info("[DRY RUN] Would process:")
        for s in pending[:20]:
            logger.info(f"  {s['title']} ({s['stuck_releases']} stuck releases)")
        if len(pending) > 20:
            logger.info(f"  ... and {len(pending) - 20} more")
        return

    # Import helper here (after arg parsing, so --help is fast)
    from core.spotify_rematch import run_spotify_rematch_for_song

    logger.info("")
    logger.info("=" * 70)
    logger.info("Starting backfill")
    logger.info("=" * 70)

    songs_processed = 0
    songs_matched = 0
    songs_failed = 0
    total_tracks_matched = 0
    total_tracks_no_match = 0

    for i, song in enumerate(pending, 1):
        song_id = str(song['id'])
        song_title = song['title']
        stuck_count = song['stuck_releases']

        logger.info("")
        logger.info(f"[{i}/{len(pending)}] {song_title} ({stuck_count} stuck releases)")
        logger.info("-" * 50)

        try:
            run = run_spotify_rematch_for_song(song_id, logger=logger)

            if run['matcher_success']:
                tracks_matched = run['stats'].get('tracks_matched', 0)
                tracks_no_match = run['stats'].get('tracks_no_match', 0)
                tracks_skipped = run['stats'].get('tracks_skipped', 0)
                releases_with_spotify = run['stats'].get('releases_with_spotify', 0)

                logger.info(f"  Done: {tracks_matched} matched, {tracks_no_match} no match, "
                            f"{tracks_skipped} skipped, {releases_with_spotify} releases updated "
                            f"({len(run['changes'])} total changes)")

                total_tracks_matched += tracks_matched
                total_tracks_no_match += tracks_no_match
                songs_matched += 1
            else:
                logger.warning(f"  Failed: {run.get('matcher_error', 'Unknown error')}")
                songs_failed += 1

            # Mark as completed regardless of match outcome
            # (we don't want to retry songs that simply had no matches)
            progress['completed_songs'].append(song_id)
            completed_set.add(song_id)

            # Remove from failed list if it was there from a previous run
            if song_id in failed_set:
                progress['failed_songs'] = [s for s in progress['failed_songs'] if s != song_id]
                failed_set.discard(song_id)

        except Exception as e:
            logger.error(f"  Error processing {song_title}: {e}", exc_info=True)
            songs_failed += 1

            if song_id not in failed_set:
                progress['failed_songs'].append(song_id)
                failed_set.add(song_id)

        songs_processed += 1

        # Save progress after every song
        progress['total_tracks_matched'] += total_tracks_matched
        progress['total_tracks_no_match'] += total_tracks_no_match
        total_tracks_matched = 0
        total_tracks_no_match = 0
        save_progress(progress)

    # Final summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("Backfill complete")
    logger.info("=" * 70)
    logger.info(f"Songs processed:    {songs_processed}")
    logger.info(f"Songs with matches: {songs_matched}")
    logger.info(f"Songs failed:       {songs_failed}")
    logger.info(f"Total completed:    {len(progress['completed_songs'])}/{total_songs}")
    logger.info(f"Cumulative tracks matched:  {progress['total_tracks_matched']}")
    logger.info(f"Cumulative tracks no match: {progress['total_tracks_no_match']}")
    logger.info(f"Progress saved to: {PROGRESS_FILE}")


if __name__ == '__main__':
    main()
