#!/usr/bin/env python3
"""
Backfill Spotify Track Durations — issue #100.

Enqueues one ('spotify', 'backfill_durations') job per song that owns at
least one recording_release_streaming_links row with service='spotify'
and duration_ms IS NULL. The actual API + DB work happens on the durable
research-queue worker (see research_worker/handlers/spotify.py).

This script used to do the API calls itself in-process. That mode was
retired when the worker queue grew strong enough to host the same logic
with retry, dedup, and admin visibility — see the issue for context.

Usage:
    python backfill_spotify_durations.py
    python backfill_spotify_durations.py --limit 500
    python backfill_spotify_durations.py --dry-run
"""

from script_base import ScriptBase, run_script
from core.spotify_duration_backfill import enqueue_sweep, find_candidate_song_ids


def main():
    script = ScriptBase(
        name="backfill_spotify_durations",
        description=(
            "Enqueue per-song Spotify duration-backfill jobs onto the "
            "research-queue worker."
        ),
        epilog="""
Examples:
  python backfill_spotify_durations.py
  python backfill_spotify_durations.py --limit 500
  python backfill_spotify_durations.py --dry-run
        """
    )

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)

    args = script.parse_args()

    script.print_header({
        "DRY RUN": args.dry_run,
        "LIMIT": args.limit if args.limit is not None else 'all candidates',
    })

    if args.dry_run:
        song_ids = find_candidate_song_ids(limit=args.limit)
        script.logger.info(
            "Would enqueue %d song(s) for Spotify duration backfill", len(song_ids),
        )
        for sid in song_ids[:25]:
            script.logger.debug("  candidate: %s", sid)
        if len(song_ids) > 25:
            script.logger.debug("  ... %d more", len(song_ids) - 25)
        script.print_summary({
            'candidates': len(song_ids),
            'enqueued': 0,
            'errors': 0,
        })
        return True

    stats = enqueue_sweep(limit=args.limit)
    script.print_summary(stats)
    return stats['errors'] == 0


if __name__ == "__main__":
    run_script(main)
