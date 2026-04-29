#!/usr/bin/env python3
"""
Research a single song end-to-end — Python equivalent of
``reloadSongFromScratch.sh``.

Runs the same five children the shell script runs, in the same order,
but accepts EITHER ``--id`` or ``--name``. The shell script only takes
a name, which gets in the way for one-off junction-correction runs
like the My Heart Stood Still case where only the song UUID was on
hand. The Python version also resolves whichever one was provided to
the OTHER form, so children that only accept ``--name``
(``jazzs_extract.py``, ``jazzs_match_authorityrecs.py``) get fed
correctly when the user supplied an ID.

Children, in order:
  1. import_mb_releases.py     — MB releases + recording_releases junction
  2. match_spotify_tracks.py   — Spotify album + track matching
  3. match_apple_tracks.py     — Apple Music matching
  4. jazzs_extract.py          — JazzStandards.com extraction
  5. jazzs_match_authorityrecs.py — Authority recommendation matching

Stops on first child failure. Re-run a specific child manually if
needed; idempotent at every step.

Examples:
    python research_song.py --id 1a9897bc-0194-4611-a7d5-0396003a29b3
    python research_song.py --name "My Heart Stood Still"
    python research_song.py --id <uuid> --mb-only      # just step 1
    python research_song.py --id <uuid> --skip jazzs_extract
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from script_base import ScriptBase, run_script
from db_utils import get_db_connection


# Children to run, in order. Each entry: (script_filename, accepts_id_flag).
# The "accepts_id_flag" controls whether we pass --id or --name when both
# are available — scripts that only support --name still get the song's
# resolved name regardless of how the user invoked us.
_CHILDREN = [
    ('import_mb_releases.py',         True,  ['--force-refresh', '--limit', '2000']),
    ('match_spotify_tracks.py',       True,  ['--force-refresh', '--rematch-all']),
    ('match_apple_tracks.py',         True,  ['--force-refresh']),
    ('jazzs_extract.py',              False, ['--force-refresh']),
    ('jazzs_match_authorityrecs.py',  False, []),
]


def _resolve_song(song_id: str | None, song_name: str | None) -> tuple[str, str]:
    """Return (song_id, song_name), querying the DB to fill in the
    one the caller didn't provide. Raises SystemExit if no row found."""
    if song_id and song_name:
        return song_id, song_name

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if song_id:
                cur.execute(
                    "SELECT id, title FROM songs WHERE id = %s", (song_id,),
                )
            else:
                # Case-insensitive lookup so "my heart stood still" works the
                # same as "My Heart Stood Still". Tie-break on shortest title
                # (so we don't pick a parenthetical variant when both exist).
                cur.execute(
                    """
                    SELECT id, title
                    FROM songs
                    WHERE LOWER(title) = LOWER(%s)
                    ORDER BY length(title) ASC
                    LIMIT 1
                    """,
                    (song_name,),
                )
            row = cur.fetchone()

    if not row:
        ident = f"id={song_id}" if song_id else f"name={song_name!r}"
        print(f"ERROR: no song found matching {ident}", file=sys.stderr)
        raise SystemExit(2)

    return str(row['id']), row['title']


def _run_child(script_name: str, args: list[str], logger) -> bool:
    """Run a child script via subprocess, inheriting stdout/stderr so
    the caller sees progress in real time. Returns True on success."""
    scripts_dir = Path(__file__).resolve().parent
    cmd = [sys.executable, str(scripts_dir / script_name), *args]
    logger.info("=" * 80)
    logger.info("→ %s", ' '.join(cmd))
    logger.info("=" * 80)
    completed = subprocess.run(cmd, cwd=str(scripts_dir))
    if completed.returncode != 0:
        logger.error(
            "Child %s exited with status %d — aborting",
            script_name, completed.returncode,
        )
        return False
    return True


def main():
    script = ScriptBase(
        name='research_song',
        description=(
            'Run the full song-research pipeline (MB import, Spotify, '
            'Apple, JazzStandards.com, authority recs) for one song.'
        ),
        epilog="""
Examples:
  python research_song.py --id 1a9897bc-0194-4611-a7d5-0396003a29b3
  python research_song.py --name "My Heart Stood Still"
  python research_song.py --id <uuid> --mb-only
  python research_song.py --name "Take Five" --skip jazzs_extract --skip jazzs_match_authorityrecs
        """,
    )
    script.add_song_args(required=True)
    script.add_debug_arg()
    script.parser.add_argument(
        '--mb-only',
        action='store_true',
        help='Run only the MB importer (step 1) — quick way to fix '
             'recording_releases junction state without waiting for '
             'the matchers',
    )
    script.parser.add_argument(
        '--skip',
        action='append',
        default=[],
        help='Skip a child by script filename (without .py). Can be '
             'passed multiple times. Useful when re-running after a '
             'mid-pipeline failure.',
    )

    args = script.parse_args()

    song_id, song_name = _resolve_song(args.id, args.name)

    script.print_header({
        'SONG ID':   song_id,
        'SONG NAME': song_name,
        'MB ONLY':   args.mb_only,
        'SKIPS':     ', '.join(args.skip) or '(none)',
    })

    skip_set = {s.removesuffix('.py') for s in args.skip}

    children = _CHILDREN[:1] if args.mb_only else _CHILDREN

    for script_name, accepts_id, extra_args in children:
        if script_name.removesuffix('.py') in skip_set:
            script.logger.info("⊙ Skipping %s (per --skip)", script_name)
            continue

        # Pass --id when the child supports it (cheaper / unambiguous),
        # otherwise pass --name (which we resolved above).
        if accepts_id:
            child_args = ['--id', song_id, *extra_args]
        else:
            child_args = ['--name', song_name, *extra_args]
        if args.debug:
            child_args.append('--debug')

        ok = _run_child(script_name, child_args, script.logger)
        if not ok:
            return False

    script.logger.info("")
    script.logger.info("All steps completed for %s (%s)", song_name, song_id)
    return True


if __name__ == '__main__':
    run_script(main)
