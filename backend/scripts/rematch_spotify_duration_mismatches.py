#!/usr/bin/env python3
"""
Rematch Spotify Duration Mismatches — issue #100, second phase.

Enqueues one ('spotify', 'rematch_duration_mismatches') job per song that
owns at least one Spotify streaming link whose duration differs from the
linked recording's canonical duration by more than a threshold (default
60s). The handler — see research_worker/handlers/spotify.py — re-runs
the SpotifyMatcher narrowly on the mismatched releases.

The matcher swaps the link to a better track if one is found; otherwise
the existing match stays in place for human review via
/admin/duration-mismatches. There is no auto-unlinking.

For ad-hoc one-shot work the in-process
`match_spotify_tracks.py --duration-mismatches <seconds>` flag still
exists; this CLI is the production path that hands the work to the
durable worker queue.

Usage:
    python rematch_spotify_duration_mismatches.py
    python rematch_spotify_duration_mismatches.py --threshold-seconds 30
    python rematch_spotify_duration_mismatches.py --limit 100
    python rematch_spotify_duration_mismatches.py --dry-run
"""

from script_base import ScriptBase, run_script
from core.spotify_rematch_mismatches import (
    DEFAULT_THRESHOLD_MS,
    enqueue_sweep,
    find_candidate_song_ids,
)


def main():
    script = ScriptBase(
        name="rematch_spotify_duration_mismatches",
        description=(
            "Enqueue per-song Spotify rematch jobs for releases whose "
            "linked Spotify track duration differs from the recording's "
            "canonical duration by more than a threshold."
        ),
        epilog="""
Examples:
  python rematch_spotify_duration_mismatches.py
  python rematch_spotify_duration_mismatches.py --threshold-seconds 30
  python rematch_spotify_duration_mismatches.py --limit 100
  python rematch_spotify_duration_mismatches.py --dry-run
        """
    )

    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)

    # Threshold knob — seconds-keyed to match the existing match_spotify_tracks
    # CLI's --duration-mismatches flag and the admin review page UX.
    script.parser.add_argument(
        '--threshold-seconds',
        type=int,
        default=DEFAULT_THRESHOLD_MS // 1000,
        help=(
            f'Mismatch threshold in seconds (default: '
            f'{DEFAULT_THRESHOLD_MS // 1000}). Songs whose Spotify track '
            f'duration differs from the recording\'s duration by more '
            f'than this are eligible.'
        ),
    )

    args = script.parse_args()

    threshold_ms = int(args.threshold_seconds) * 1000

    script.print_header({
        "DRY RUN": args.dry_run,
        "THRESHOLD": f"{args.threshold_seconds}s",
        "LIMIT": args.limit if args.limit is not None else 'all candidates',
    })

    if args.dry_run:
        song_ids = find_candidate_song_ids(
            threshold_ms=threshold_ms, limit=args.limit,
        )
        script.logger.info(
            "Would enqueue %d song(s) for Spotify duration-mismatch rematch "
            "(threshold %ds)",
            len(song_ids), args.threshold_seconds,
        )
        for sid in song_ids[:25]:
            script.logger.debug("  candidate: %s", sid)
        if len(song_ids) > 25:
            script.logger.debug("  ... %d more", len(song_ids) - 25)
        script.print_summary({
            'candidates': len(song_ids),
            'enqueued': 0,
            'errors': 0,
            'threshold_ms': threshold_ms,
        })
        return True

    stats = enqueue_sweep(threshold_ms=threshold_ms, limit=args.limit)
    script.print_summary(stats)
    return stats['errors'] == 0


if __name__ == "__main__":
    run_script(main)
