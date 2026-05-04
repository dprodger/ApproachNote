#!/usr/bin/env python3
"""
Re-evaluate an existing audit CSV with the current (in-tree) matcher rules.

Reads each row of an audit CSV and re-runs compare_mb_to_spotify_tracks
against the cached MB + Spotify tracklists. No new API calls — rows whose
caches are missing are reported as 'cache_miss' and excluded from the
verdict math. Use this to quantify how many flagged rows the latest
matching changes would now rescue, without competing with a running audit
for Spotify rate-limit budget.

Filters: by default only re-evaluates count-equal rows (mb == sp), since
that's the gate the new fallback opens. Use --all to re-evaluate every row.

Usage:
    python scripts/reaudit_spotify_links.py
        # newest CSV in backend/data/spotify_link_audits/, count-equal only

    python scripts/reaudit_spotify_links.py path/to/audit.csv

    python scripts/reaudit_spotify_links.py --all
"""

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from integrations.spotify.matching import (
    compare_mb_to_spotify_tracks,
    normalize_for_comparison,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
MB_CACHE_DIR = REPO_ROOT / 'cache' / 'musicbrainz' / 'releases'
SP_CACHE_DIR = REPO_ROOT / 'cache' / 'spotify' / 'albums'
AUDIT_DIR = REPO_ROOT / 'data' / 'spotify_link_audits'

DEFAULT_RATIO = 0.6
DEFAULT_MATCHES = 3


def _parse_args():
    p = argparse.ArgumentParser(
        description="Re-evaluate an audit CSV with current matcher rules.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        'csv_path', nargs='?', default=None,
        help='Path to audit CSV (default: newest in backend/data/spotify_link_audits/)',
    )
    p.add_argument(
        '--all', action='store_true',
        help='Re-evaluate every row, not just count-equal ones',
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
        '-o', '--output', default=None,
        help='Optional output CSV with new columns appended',
    )
    return p.parse_args()


def _pick_default_csv():
    if not AUDIT_DIR.exists():
        return None
    csvs = sorted(AUDIT_DIR.glob('*.csv'),
                  key=lambda p: p.stat().st_mtime, reverse=True)
    return csvs[0] if csvs else None


def _load_mb_tracks(mb_release_id):
    """Read the cached MB release-tracklist response and project to the
    {title, position, normalized} shape compare_mb_to_spotify_tracks expects."""
    if not mb_release_id:
        return None
    cache_path = MB_CACHE_DIR / f'release_{mb_release_id}_tracklist.json'
    if not cache_path.exists():
        return None
    raw = json.loads(cache_path.read_text())
    data = raw.get('data') if isinstance(raw, dict) else None
    if data is None:
        return None
    tracks = []
    pos = 0
    for medium in data.get('media', []) or []:
        for track in medium.get('tracks', []) or []:
            pos += 1
            title = track.get('title', '')
            tracks.append({
                'title': title,
                'position': pos,
                'normalized': normalize_for_comparison(title),
            })
    return tracks


def _load_sp_tracks(spotify_album_id):
    """Read the cached Spotify album-tracks response. The Spotify client
    saves the list of {id, name, track_number, ...} dicts directly."""
    if not spotify_album_id:
        return None
    cache_path = SP_CACHE_DIR / f'album_{spotify_album_id}.json'
    if not cache_path.exists():
        return None
    return json.loads(cache_path.read_text())


def _verdict(matched, mb_count, ratio, args):
    if mb_count <= 0:
        return False
    return (
        ratio < args.ratio
        or matched < min(args.matches, mb_count)
    )


def main():
    args = _parse_args()
    csv_path = Path(args.csv_path) if args.csv_path else _pick_default_csv()
    if not csv_path or not csv_path.exists():
        print(f"No audit CSV found (looked in {AUDIT_DIR})", file=sys.stderr)
        return 1

    print(f"Reading {csv_path}")
    rows = list(csv.DictReader(csv_path.open()))
    print(f"  {len(rows)} rows total")

    eligible = []
    for r in rows:
        try:
            mb_n = int(r.get('mb_track_count') or 0)
            sp_n = int(r.get('spotify_track_count') or 0)
        except ValueError:
            continue
        if not args.all and mb_n != sp_n:
            continue
        if mb_n == 0 or sp_n == 0:
            continue
        eligible.append(r)
    print(f"  {len(eligible)} eligible "
          f"({'all rows' if args.all else 'count-equal only'})")

    cache_miss = 0
    rescued = 0       # was stale, now passes
    still_stale = 0
    no_change = 0     # was passing, still passes (rare for stale-only CSVs)
    new_stale = 0     # was passing, now flagged (regression)
    out_rows = []
    for r in eligible:
        mb_tracks = _load_mb_tracks(r.get('mb_release_id'))
        sp_tracks = _load_sp_tracks(r.get('spotify_album_id'))
        if mb_tracks is None or sp_tracks is None:
            cache_miss += 1
            continue

        # Project Spotify cached items into the {name, ...} shape the matcher expects.
        sp_for_matcher = [{'name': t.get('name'), 'duration_ms': t.get('duration_ms')}
                          for t in sp_tracks]

        info = compare_mb_to_spotify_tracks(mb_tracks, sp_for_matcher)
        was_stale = (r.get('is_stale') or '').strip().upper() == 'TRUE'
        now_stale = _verdict(info['matched_count'], info['mb_track_count'],
                             info['match_ratio'], args)

        if was_stale and not now_stale:
            rescued += 1
        elif was_stale and now_stale:
            still_stale += 1
        elif not was_stale and not now_stale:
            no_change += 1
        else:
            new_stale += 1

        out_rows.append({
            **r,
            'new_matched_count': info['matched_count'],
            'new_match_ratio': round(info['match_ratio'], 3),
            'new_ordering_ratio': (
                round(info['ordering_ratio'], 3)
                if info['ordering_ratio'] is not None else ''
            ),
            'new_is_stale': 'TRUE' if now_stale else 'FALSE',
        })

    evaluated = rescued + still_stale + no_change + new_stale
    print()
    print(f"Re-evaluated {evaluated} rows ({cache_miss} cache miss)")
    print(f"  Rescued (was stale, now passes):  {rescued:>4d}  "
          f"({(rescued/evaluated*100 if evaluated else 0):.1f}%)")
    print(f"  Still stale:                       {still_stale:>4d}")
    print(f"  Was passing, still passing:        {no_change:>4d}")
    print(f"  Newly stale (regression):          {new_stale:>4d}")

    if args.output and out_rows:
        out_path = Path(args.output)
        with out_path.open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
        print(f"Wrote {len(out_rows)} re-evaluated rows to {out_path}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
