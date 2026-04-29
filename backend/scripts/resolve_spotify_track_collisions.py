#!/usr/bin/env python3
"""
Resolve Spotify track-collision links across the whole catalog.

A "collision" is a single Spotify track ID linked to two-or-more
recording_release junctions on the same release — typical case: MB has
two recordings of the same song on a 2-disc release (e.g. one 5:50
studio cut and one 7:39 live cut), Spotify reissued only one of them
on a single-disc compilation, and the per-recording matcher linked
both MB recordings to the surviving Spotify track.

The same logic that runs at the tail of
SpotifyMatcher.match_tracks_for_release on every rematch is invoked
release-by-release here so we can clean up the catalog without
re-running the full matcher (which is slow and pays Spotify API cost
per release). For each collision: keep the link with the highest
duration_confidence between the recording's effective duration and the
Spotify track's; clear the losers. Manual-override links
(match_method='manual') always win — admin assertion beats heuristic.

Run after a sweep that may have produced collisions (or any time the
catalog-wide diagnostic SQL turns up >0 rows). Idempotent: a second
run with no collisions produces zero-cleared output.

Examples:
    python resolve_spotify_track_collisions.py --dry-run
    python resolve_spotify_track_collisions.py
    python resolve_spotify_track_collisions.py --limit 10        # do first 10 releases
"""

from __future__ import annotations

from script_base import ScriptBase, run_script
from db_utils import get_db_connection
from integrations.spotify.matcher import SpotifyMatcher


def _find_releases_with_collisions(limit: int | None = None) -> list[str]:
    """Return release UUIDs that have at least one (spotify_track_id) linked
    to two or more recording_releases on that release. Newest-collision-first
    isn't a useful sort here — order by release_id for stable output."""
    sql = """
        SELECT DISTINCT rr.release_id
        FROM recording_release_streaming_links rrsl
        JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
        WHERE rrsl.service = 'spotify'
          AND rr.release_id IN (
              SELECT rr2.release_id
              FROM recording_release_streaming_links rrsl2
              JOIN recording_releases rr2 ON rr2.id = rrsl2.recording_release_id
              WHERE rrsl2.service = 'spotify'
              GROUP BY rr2.release_id, rrsl2.service_id
              HAVING COUNT(*) > 1
          )
        ORDER BY rr.release_id
    """
    if limit is not None:
        sql += "\n        LIMIT %s"
        params: tuple = (limit,)
    else:
        params = ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(r['release_id']) for r in rows]


def main():
    script = ScriptBase(
        name='resolve_spotify_track_collisions',
        description=(
            'Scan all releases for Spotify track collisions '
            '(one track linked to multiple junctions on the same release) '
            'and clear the lower-confidence links.'
        ),
        epilog="""
Examples:
  python resolve_spotify_track_collisions.py --dry-run
  python resolve_spotify_track_collisions.py
  python resolve_spotify_track_collisions.py --limit 10
        """,
    )
    script.add_dry_run_arg()
    script.add_debug_arg()
    script.add_limit_arg(default=None)
    args = script.parse_args()

    script.print_header({
        'DRY RUN': args.dry_run,
        'LIMIT':   args.limit if args.limit is not None else 'all releases',
    })

    release_ids = _find_releases_with_collisions(limit=args.limit)
    script.logger.info("Found %d release(s) with track collisions", len(release_ids))

    if not release_ids:
        script.print_summary({
            'releases_scanned': 0,
            'links_cleared': 0,
        })
        return True

    matcher = SpotifyMatcher(dry_run=args.dry_run, logger=script.logger)

    total_cleared = 0
    for i, release_id in enumerate(release_ids, 1):
        script.logger.info(
            "[%d/%d] Resolving release %s",
            i, len(release_ids), release_id,
        )
        try:
            with get_db_connection() as conn:
                # Underscore-prefixed by convention but used here as the
                # public entry-point for the standalone resolver — same
                # logic the matcher's tail invocation runs.
                cleared = matcher._resolve_release_track_collisions(
                    conn, release_id,
                )
                conn.commit()
        except Exception:
            script.logger.exception(
                "Failed to resolve collisions for release %s — continuing",
                release_id,
            )
            continue

        total_cleared += cleared

    script.print_summary({
        'releases_scanned': len(release_ids),
        'links_cleared':    total_cleared,
    })
    return True


if __name__ == '__main__':
    run_script(main)
