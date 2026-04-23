#!/usr/bin/env python3
"""
YouTube Matcher — CLI.

Match a single recording or every recording of a song against YouTube.
Designed for iteration (quota is tight: 10k units/day, search is 100
units/call, videos.list is 1 unit). Defaults are conservative; bump with
--exhaustive or --no-strict only when needed.

Examples:

    # Match the reference recording (known to have a YT link already)
    python match_youtube_videos.py --recording-id 183273bf-c0bc-4fb1-ae9f-8710638cdd6e --dry-run

    # Every recording of a song by song ID
    python match_youtube_videos.py --id a1b2c3d4-e5f6-... --dry-run

    # By partial song name
    python match_youtube_videos.py --name "Once I Loved" --dry-run

    # Re-evaluate recordings that already have a non-manual YouTube link
    python match_youtube_videos.py --name "Once I Loved" --rematch --dry-run

Quota stats are printed in the summary. Hard-stops at 9500 units used.
"""

import os
import sys
from pathlib import Path

# Make `integrations`, `db_utils`, etc importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parent.parent / '.env')

from script_base import ScriptBase, run_script  # noqa: E402
from integrations.youtube.db import find_song_by_name  # noqa: E402
from integrations.youtube.matcher import YouTubeMatcher  # noqa: E402


def main() -> bool:
    script = ScriptBase(
        name='match_youtube_videos',
        description='Match recordings to YouTube videos via YouTube Data API v3',
    )

    # Allow --recording-id alone (no song id needed), so make --name/--id optional.
    script.add_song_args(required=False)
    script.add_common_args()

    script.parser.add_argument(
        '--recording-id',
        help='Match a single recording by UUID (overrides --name / --id).',
    )
    script.parser.add_argument(
        '--rematch',
        action='store_true',
        help='Re-evaluate recordings that already have a non-manual YouTube link',
    )
    script.parser.add_argument(
        '--search-results',
        type=int,
        default=8,
        help='How many results to request per search.list (default: 8)',
    )
    script.parser.add_argument(
        '--cache-days',
        type=int,
        default=30,
        help='Days before cache is stale (default: 30)',
    )
    script.parser.add_argument(
        '--max-quota',
        type=int,
        default=9500,
        help='Hard-stop quota budget for this run (default: 9500)',
    )

    args = script.parse_args()

    if not os.environ.get('YOUTUBE_API_KEY'):
        script.logger.error("YOUTUBE_API_KEY is not set. Add it to backend/.env.")
        return False

    matcher = YouTubeMatcher(
        dry_run=args.dry_run,
        rematch=args.rematch,
        search_results=args.search_results,
        logger=script.logger,
        cache_days=args.cache_days,
        force_refresh=args.force_refresh,
        max_units=args.max_quota,
    )

    script.print_header({
        'DRY RUN': args.dry_run,
        'REMATCH': args.rematch,
        'FORCE REFRESH': args.force_refresh,
    })

    if args.recording_id:
        result = matcher.match_recording(args.recording_id)
        if not result.get('success', True):
            script.logger.error(result.get('message', 'Unknown error'))
            return False

        if result.get('matched'):
            script.logger.info("")
            script.logger.info(f"MATCHED: {result['video_url']}")
            script.logger.info(f"  title: {result['video_title']}")
            script.logger.info(f"  channel: {result['channel']}")
            script.logger.info(f"  confidence: {result['confidence']:.2f}")
            script.logger.info(f"  rows written: {result.get('rows_written', 0)}")
        elif result.get('skipped'):
            script.logger.info(f"SKIPPED ({result['skipped']})")
        else:
            script.logger.info("NO MATCH")
            for r in (result.get('rejected') or [])[:5]:
                script.logger.info(
                    f"  - {(r.get('video_title') or '')[:60]} "
                    f"(channel={r.get('channel')}) → {r.get('rejected_reason')}"
                )

        matcher._aggregate_client_stats()
        script.print_summary(matcher.stats, title='YOUTUBE MATCHING SUMMARY')
        return True

    # Song-level run: resolve song_id from --name or --id.
    song_id = args.id
    if not song_id and args.name:
        song = find_song_by_name(args.name)
        if not song:
            script.logger.error(f"No song found matching {args.name!r}")
            return False
        song_id = str(song['id'])
        script.logger.info(f"Resolved song {args.name!r} → {song['title']} ({song_id})")

    if not song_id:
        script.logger.error('Provide --recording-id, --id, or --name')
        return False

    result = matcher.match_song(song_id)
    if not result.get('success'):
        script.logger.error(result.get('message', 'Unknown error'))
        return False

    script.print_summary(result['stats'], title='YOUTUBE MATCHING SUMMARY')
    return True


if __name__ == '__main__':
    run_script(main)
