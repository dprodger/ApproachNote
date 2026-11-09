#!/usr/bin/env python3
"""
Backfill Spotify links lost to the version-keyword asymmetry rejection on
live albums.

Spotify suffixes every track on a live album " - Live" and titles the
album "<Name> (Live)"; MusicBrainz annotates neither. Until the
album-wide escape landed in integrations/spotify/matcher.py, that
asymmetry made the matcher reject otherwise-perfect track matches — and,
because the song's only track on the release had failed, discard the
album match with it. This script finds the affected (song, release) pairs
and re-runs the matcher against only those releases.

Use this instead of queueing ('spotify', 'match_song') per song:
match_song walks every release of the song (~92x the API calls to reach
the same rows) and its payload cannot set rematch_all, so it re-reaches
the cached track-match failure and changes nothing.

Candidate selection and the two matcher modes (and why neither uses
rematch_all) are documented in core/spotify_live_suffix_backfill.py.

Examples:
    # What would change, across the whole catalog
    python backfill_spotify_live_suffix_links.py --dry-run

    # Smoke test on 20 pairs first
    python backfill_spotify_live_suffix_links.py --dry-run --limit 20 --debug

    # One song, to check a known case
    python backfill_spotify_live_suffix_links.py \\
        --song bfbe30ec-846b-443e-a3c8-000045d1e40f --dry-run --debug

    # Write the links
    python backfill_spotify_live_suffix_links.py

    # List the affected songs/releases without touching Spotify
    python backfill_spotify_live_suffix_links.py --list-only
"""

from __future__ import annotations

import collections

from script_base import ScriptBase, run_script
from core.spotify_live_suffix_backfill import (
    find_candidate_pairs,
    plan_from_pairs,
    run_backfill,
)


def main():
    script = ScriptBase(
        name='backfill_spotify_live_suffix_links',
        description=(
            'Re-run the Spotify matcher over releases whose track links '
            'were rejected because Spotify labels the whole live album '
            '"- Live" and MusicBrainz does not.'
        ),
        epilog="""
Examples:
  python backfill_spotify_live_suffix_links.py --dry-run
  python backfill_spotify_live_suffix_links.py --dry-run --limit 20 --debug
  python backfill_spotify_live_suffix_links.py --list-only
  python backfill_spotify_live_suffix_links.py
        """,
    )
    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)

    script.parser.add_argument(
        '--song',
        help='Restrict to one song by UUID (for spot-checking a known case)',
    )
    script.parser.add_argument(
        '--list-only',
        action='store_true',
        help='Print the affected songs and releases and exit — no Spotify '
             'calls, no writes',
    )
    script.add_force_refresh_arg()
    script.parser.add_argument(
        '--album-context',
        action='store_true',
        help="Also enable the matcher's album-context rescue for "
             'duration-rejected tracks. Off by default so this run only '
             'reflects the version-asymmetry fix.',
    )

    args = script.parse_args()

    script.print_header({
        'DRY RUN':       args.dry_run,
        'LIMIT':         args.limit if args.limit is not None else 'all pairs',
        'SONG':          args.song or 'all songs',
        'LIST ONLY':     args.list_only,
        'ALBUM CONTEXT': args.album_context,
        'FORCE REFRESH': args.force_refresh,
    })

    pairs = find_candidate_pairs(limit=args.limit, song_id=args.song)
    if not pairs:
        script.logger.info('No candidate (song, release) pairs found.')
        script.print_summary({'pairs': 0, 'tracks_matched': 0})
        return True

    plan = plan_from_pairs(pairs)
    per_song = collections.Counter(p['song_id'] for p in pairs)
    script.logger.info(
        'Found %d candidate pair(s) across %d song(s) and %d release(s)',
        len(pairs), len(per_song),
        len({p['release_id'] for p in pairs}),
    )
    script.logger.info(
        '  known album, fill missing tracks: %d pair(s) in %d song(s)',
        sum(len(v) for v in plan['tracks_only'].values()),
        len(plan['tracks_only']),
    )
    script.logger.info(
        '  no album link, full search:       %d pair(s) in %d song(s)',
        sum(len(v) for v in plan['full_search'].values()),
        len(plan['full_search']),
    )

    if args.list_only:
        for song_id, count in per_song.most_common():
            script.logger.info('  %s  %d release(s)', song_id, count)
        script.print_summary({
            'pairs':    len(pairs),
            'songs':    len(per_song),
            'releases': len({p['release_id'] for p in pairs}),
        })
        return True

    def progress(mode, done, total):
        script.logger.info('[%s] %d/%d songs', mode, done, total)

    stats = run_backfill(
        pairs,
        dry_run=args.dry_run,
        album_context='rescue' if args.album_context else None,
        force_refresh=args.force_refresh,
        logger_=script.logger,
        progress_callback=progress,
    )

    script.print_summary(stats)
    # A run that couldn't complete any song is a failure worth a non-zero
    # exit; partial progress is not.
    return stats['song_runs_ok'] > 0 or stats['song_runs_failed'] == 0


if __name__ == '__main__':
    run_script(main)
