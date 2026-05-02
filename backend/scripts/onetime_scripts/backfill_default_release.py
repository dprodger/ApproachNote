#!/usr/bin/env python3
"""
Backfill default_release_id for recordings that have linked releases but no default set.

Priority for selecting the default release:
1. Release with Spotify track URL + cover art (best match)
2. Release with Spotify album URL + cover art
3. Release with CAA imagery (release_imagery table)
4. Release with any cover art
5. Release with Spotify track URL (even without art)
6. Any linked release (fallback)

Usage:
    python scripts/backfill_default_release.py [--dry-run]
    python scripts/backfill_default_release.py --execute
    python scripts/backfill_default_release.py --execute --batch-size 500
"""

import os
import sys
import argparse
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['DB_USE_POOLING'] = 'true'

from db_utils import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 1000  # Commit every N updates


def find_recordings_without_default_release():
    """Find recordings that have linked releases but no default_release_id."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.album_title, r.recording_year
                FROM recordings r
                WHERE r.default_release_id IS NULL
                  AND EXISTS (SELECT 1 FROM recording_releases rr WHERE rr.recording_id = r.id)
                ORDER BY r.recording_year
            """)
            return cur.fetchall()


def find_best_release_for_recording(cur, recording_id):
    """
    Find the best release to use as default for a recording.

    Returns the release_id of the best match, or None if no releases linked.
    """
    cur.execute("""
        SELECT
            rel.id,
            rel.title,
            rel.release_year,
            (
                CASE WHEN rr.spotify_track_id IS NOT NULL AND rel.cover_art_small IS NOT NULL THEN 100
                     WHEN rsl_sp.service_id IS NOT NULL AND rel.cover_art_small IS NOT NULL THEN 90
                     WHEN EXISTS (SELECT 1 FROM release_imagery ri WHERE ri.release_id = rel.id AND ri.type = 'Front') THEN 80
                     WHEN rel.cover_art_small IS NOT NULL THEN 70
                     WHEN rr.spotify_track_id IS NOT NULL THEN 60
                     WHEN rsl_sp.service_id IS NOT NULL THEN 50
                     ELSE 10
                END
            ) as score
        FROM recording_releases rr
        JOIN releases rel ON rr.release_id = rel.id
        LEFT JOIN release_streaming_links rsl_sp
            ON rsl_sp.release_id = rel.id AND rsl_sp.service = 'spotify'
        WHERE rr.recording_id = %s
        ORDER BY score DESC, rel.release_year ASC NULLS LAST
        LIMIT 1
    """, (recording_id,))

    return cur.fetchone()


def backfill_default_releases(dry_run=True, batch_size=BATCH_SIZE):
    """Main function to backfill default_release_id."""

    # First, get all recordings needing update (separate connection)
    logger.info("Finding recordings without default_release_id...")
    recordings = find_recordings_without_default_release()
    total = len(recordings)

    logger.info(f"Found {total} recordings without default_release_id")

    if not recordings:
        logger.info("Nothing to do!")
        return

    updated_count = 0
    skipped_count = 0

    # Process in batches
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch = recordings[batch_start:batch_end]

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for i, rec in enumerate(batch):
                    global_idx = batch_start + i
                    recording_id = rec['id']

                    # Find best release
                    best_release = find_best_release_for_recording(cur, recording_id)

                    if not best_release:
                        skipped_count += 1
                        continue

                    release_id = best_release['id']
                    release_title = best_release['title']
                    score = best_release['score']

                    if (global_idx + 1) % 1000 == 0 or global_idx < 10:
                        logger.info(f"[{global_idx+1}/{total}] '{rec['album_title'][:40] if rec['album_title'] else '?'}' ({rec['recording_year']}) -> "
                                   f"'{release_title[:40]}' (score={score})")

                    if not dry_run:
                        cur.execute("""
                            UPDATE recordings
                            SET default_release_id = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (release_id, recording_id))
                        updated_count += 1

                if not dry_run:
                    conn.commit()
                    logger.info(f"  Committed batch {batch_start//batch_size + 1} ({updated_count} total updates so far)")

    if dry_run:
        logger.info(f"DRY RUN: Would update {total - skipped_count} recordings, skip {skipped_count}")
    else:
        logger.info(f"DONE: Updated {updated_count} recordings, skipped {skipped_count}")


def main():
    parser = argparse.ArgumentParser(description='Backfill default_release_id for recordings')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Show what would be done without making changes (default: True)')
    parser.add_argument('--execute', action='store_true',
                        help='Actually execute the changes')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f'Commit every N updates (default: {BATCH_SIZE})')

    args = parser.parse_args()

    dry_run = not args.execute

    if dry_run:
        logger.info("=== DRY RUN MODE (use --execute to apply changes) ===")
    else:
        logger.info("=== EXECUTING CHANGES ===")

    backfill_default_releases(dry_run=dry_run, batch_size=args.batch_size)


if __name__ == '__main__':
    main()
