#!/usr/bin/env python3
"""
Audit existing Spotify album matches in `release_streaming_links` against
the same tracklist gate the live matcher now uses (issue #184).

Read-only: computes the MB-vs-Spotify tracklist match ratio for every
spotify link in `release_streaming_links` and writes a CSV. Use it to
identify rows that the old matcher accepted but the new gate would
reject — i.e. the stale-match cleanup list.

Output columns:
    release_id, mb_release_id, release_title, release_artist_credit,
    spotify_album_id, spotify_album_name,
    mb_track_count, spotify_track_count, matched_count, match_ratio,
    is_stale, error

`is_stale` = TRUE when match_ratio < threshold OR matched_count < min,
using the same defaults as the live gate. Rows where MB tracklist or
Spotify tracklist couldn't be fetched (`error` set) are skipped from
the stale verdict — manual review.

Usage:
    python scripts/audit_spotify_release_links.py
    python scripts/audit_spotify_release_links.py --limit 50
    python scripts/audit_spotify_release_links.py --only-stale -o stale.csv
    python scripts/audit_spotify_release_links.py --ratio 0.5 --matches 2
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

from db_utils import get_db_connection
from integrations.spotify.client import SpotifyClient
from integrations.spotify.matching import (
    compare_mb_to_spotify_tracks,
    fetch_mb_tracks_for_release,
)


DEFAULT_RATIO = 0.6
DEFAULT_MATCHES = 3


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Audit release_streaming_links Spotify rows against the "
            "tracklist gate from issue #184."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        '-o', '--output',
        default=f'spotify_release_link_audit_{datetime.now():%Y%m%d_%H%M%S}.csv',
        help='Output CSV path (default: timestamped file in cwd)',
    )
    p.add_argument(
        '--limit', type=int, default=None,
        help='Only process the first N rows (for sanity-checking)',
    )
    p.add_argument(
        '--only-stale', action='store_true',
        help='Skip rows that pass the gate; only write suspect rows',
    )
    p.add_argument(
        '--ratio', type=float, default=DEFAULT_RATIO,
        help=f'match_ratio threshold for stale verdict (default {DEFAULT_RATIO})',
    )
    p.add_argument(
        '--matches', type=int, default=DEFAULT_MATCHES,
        help=f'matched_count threshold for stale verdict (default {DEFAULT_MATCHES})',
    )
    p.add_argument(
        '--debug', action='store_true', help='Verbose logging',
    )
    return p.parse_args()


def _fetch_links(conn, limit):
    """All spotify rows in release_streaming_links, joined to release info."""
    sql = """
        SELECT
            rsl.release_id,
            rsl.service_id      AS spotify_album_id,
            rel.musicbrainz_release_id,
            rel.title           AS release_title,
            rel.artist_credit   AS release_artist_credit
        FROM release_streaming_links rsl
        JOIN releases rel ON rel.id = rsl.release_id
        WHERE rsl.service = 'spotify'
          AND rsl.service_id IS NOT NULL
        ORDER BY rel.title, rel.artist_credit
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def _audit_row(conn, client, row, log):
    """Compute the tracklist fit for one row.

    Returns a dict suitable for CSV writing. The `error` field is set
    for rows we couldn't audit (MB or Spotify side missing); those rows
    are excluded from the stale verdict.
    """
    out = {
        'release_id': str(row['release_id']),
        'mb_release_id': row.get('musicbrainz_release_id') or '',
        'release_title': row.get('release_title') or '',
        'release_artist_credit': row.get('release_artist_credit') or '',
        'spotify_album_id': row['spotify_album_id'],
        'spotify_album_name': '',
        'mb_track_count': 0,
        'spotify_track_count': 0,
        'matched_count': 0,
        'match_ratio': 0.0,
        'is_stale': '',
        'error': '',
    }

    if not row.get('musicbrainz_release_id'):
        out['error'] = 'no MB release id on releases row'
        return out

    try:
        mb_tracks = fetch_mb_tracks_for_release(conn, row['release_id'])
    except Exception as e:
        out['error'] = f'MB fetch failed: {e}'
        return out

    if not mb_tracks:
        out['error'] = 'MB returned empty tracklist'
        return out

    try:
        sp_tracks = client.get_album_tracks(row['spotify_album_id'])
    except Exception as e:
        out['error'] = f'Spotify fetch failed: {e}'
        return out

    if not sp_tracks:
        out['error'] = 'Spotify returned no tracks (album removed?)'
        return out

    # Album name + artist credit are the most useful signals when eyeballing
    # the CSV — fetch them once per album. get_album_details is cached.
    try:
        details = client.get_album_details(row['spotify_album_id'])
        if details:
            out['spotify_album_name'] = details.get('name', '')
    except Exception as e:
        log.debug(f"  album-details fetch failed for {row['spotify_album_id']}: {e}")

    info = compare_mb_to_spotify_tracks(mb_tracks, sp_tracks)
    out.update({
        'mb_track_count': info['mb_track_count'],
        'spotify_track_count': info['spotify_track_count'],
        'matched_count': info['matched_count'],
        'match_ratio': round(info['match_ratio'], 3),
    })
    return out


def main():
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    log = logging.getLogger('audit_spotify_release_links')

    client = SpotifyClient(logger=log)
    if not client.get_spotify_auth_token():
        log.error("Spotify auth failed — set SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET")
        return 1

    fields = [
        'release_id', 'mb_release_id', 'release_title', 'release_artist_credit',
        'spotify_album_id', 'spotify_album_name',
        'mb_track_count', 'spotify_track_count', 'matched_count', 'match_ratio',
        'is_stale', 'error',
    ]

    written = 0
    audited = 0
    stale_count = 0
    error_count = 0

    with get_db_connection() as conn:
        rows = _fetch_links(conn, args.limit)
        log.info(f"Auditing {len(rows)} spotify release_streaming_links rows")

        # MusicBrainz is rate-limited at 1 req/sec inside MusicBrainzSearcher;
        # release-detail responses are cached on disk per the MB client. The
        # matcher worker holds a single SpotifyClient open for the life of a
        # run; we reuse one here.
        with open(args.output, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()

            for i, row in enumerate(rows, 1):
                if i % 25 == 0:
                    log.info(
                        f"  [{i}/{len(rows)}] audited={audited} "
                        f"stale={stale_count} error={error_count}"
                    )

                out = _audit_row(conn, client, row, log)
                if out['error']:
                    error_count += 1
                else:
                    audited += 1
                    is_stale = (
                        out['match_ratio'] < args.ratio
                        or out['matched_count'] < min(args.matches, out['mb_track_count'])
                    )
                    out['is_stale'] = 'TRUE' if is_stale else 'FALSE'
                    if is_stale:
                        stale_count += 1
                        log.debug(
                            f"  STALE: '{out['release_title']}' / "
                            f"'{out['release_artist_credit']}' → spotify {out['spotify_album_id']} "
                            f"(matched {out['matched_count']}/{out['mb_track_count']}, "
                            f"ratio {out['match_ratio']})"
                        )

                if args.only_stale and out['is_stale'] != 'TRUE':
                    continue

                w.writerow(out)
                written += 1

    log.info("")
    log.info(f"Total rows: {len(rows)}")
    log.info(f"  audited (gate verdict): {audited}")
    log.info(f"    stale: {stale_count}")
    log.info(f"    ok:    {audited - stale_count}")
    log.info(f"  errors (excluded from verdict): {error_count}")
    log.info(f"Wrote {written} rows to {args.output}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
