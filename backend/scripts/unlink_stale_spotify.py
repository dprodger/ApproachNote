#!/usr/bin/env python3
"""
Remove stale Spotify album links from release_streaming_links + release_imagery.

Reads a still-stale CSV (output of audit_remaining.py — original audit shape
with rescued rows pruned) and, for each (release_id, spotify_album_id) row:

  1. DELETE FROM release_streaming_links
       WHERE release_id = %s AND service = 'spotify' AND service_id = %s
       AND (match_method != 'manual' OR match_method IS NULL)

  2. DELETE FROM release_imagery
       WHERE release_id = %s AND source = 'Spotify'

The album link being wrong implies any Spotify-sourced cover art on that
release was also pulled from the wrong album, so both go together. This
mirrors core.spotify.db.clear_spotify_data, which is the live matcher's
own teardown path.

NOT touched: recording_release_streaming_links (per-track Spotify URLs).
Those have a separate cleanup path; the next track-matcher run will
reconsider them once the bad album link is gone.

Default is DRY-RUN. Pass --apply to actually delete. Every row that would
be (or was) deleted is also written to an undo CSV, so the operation is
reversible by hand.

Usage:
    # Preview only (no DB changes):
    python scripts/unlink_stale_spotify.py still_stale.csv

    # Actually delete + auto-save undo CSV alongside the input:
    python scripts/unlink_stale_spotify.py still_stale.csv --apply

    # Pick a custom undo path:
    python scripts/unlink_stale_spotify.py still_stale.csv --apply \
        -o data/spotify_link_audits/undo_$(date +%Y%m%d_%H%M%S).csv
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


# Columns we write to the undo CSV. Matches the union of release_streaming_links
# and release_imagery; rows from each table get tagged with `_table`. Keeping
# the schema flat makes the undo file straightforward to inspect or re-import.
UNDO_COLUMNS = [
    '_table',           # 'release_streaming_links' | 'release_imagery'
    'id',
    'release_id',
    'service',
    'service_id',
    'service_url',
    'match_confidence',
    'match_method',
    'matched_at',
    'last_verified_at',
    'added_by_user_id',
    'notes',
    'source',           # release_imagery
    'source_id',        # release_imagery
    'source_url',       # release_imagery
    'type',             # release_imagery
    'image_url_small',
    'image_url_medium',
    'image_url_large',
    'checksum',
    'comment',
    'approved',
    'created_at',
    'updated_at',
]


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Remove stale Spotify album links + imagery flagged by the "
            "audit/reaudit pipeline."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        'csv_path',
        help='Still-stale CSV (must have release_id + spotify_album_id columns)',
    )
    p.add_argument(
        '--apply', action='store_true',
        help='Actually delete (default is dry-run — nothing changes in the DB)',
    )
    p.add_argument(
        '-o', '--undo-output', default=None,
        help=(
            'Path for the undo CSV. Default when --apply: '
            '<input>_undo_<timestamp>.csv next to the input file. Always '
            'written when --apply is set; without --apply it is written '
            'only if -o is given (preview of what would be saved).'
        ),
    )
    p.add_argument(
        '--debug', action='store_true', help='Verbose logging',
    )
    return p.parse_args()


def _coerce_undo_row(row, table):
    """Project a DB row into the flat UNDO_COLUMNS shape."""
    out = {col: '' for col in UNDO_COLUMNS}
    out['_table'] = table
    for k, v in row.items():
        if k in out:
            out[k] = '' if v is None else str(v)
    return out


def main():
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    log = logging.getLogger('unlink_stale_spotify')

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        log.error(f"CSV not found: {csv_path}")
        return 1

    rows = list(csv.DictReader(csv_path.open()))
    if not rows:
        log.warning("CSV is empty — nothing to do.")
        return 0
    if 'release_id' not in rows[0] or 'spotify_album_id' not in rows[0]:
        log.error("CSV must have release_id and spotify_album_id columns")
        return 1
    log.info(f"Read {len(rows)} rows from {csv_path}")

    # Decide where to write the undo CSV.
    undo_path = None
    if args.undo_output:
        undo_path = Path(args.undo_output)
    elif args.apply:
        undo_path = csv_path.with_name(
            f'{csv_path.stem}_undo_{datetime.now():%Y%m%d_%H%M%S}.csv'
        )
        log.info(f"Auto-generating undo file: {undo_path}")

    not_in_db = 0
    manual_skipped = 0
    deleted_streaming = 0
    deleted_imagery = 0
    undo_rows = []

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"Mode: {mode}")
    log.info("")

    with get_db_connection() as conn:
        try:
            with conn.cursor() as cur:
                for csv_row in rows:
                    release_id = (csv_row.get('release_id') or '').strip()
                    spotify_album_id = (csv_row.get('spotify_album_id') or '').strip()
                    if not release_id or not spotify_album_id:
                        continue

                    # Fetch the streaming-link row first so we know the full
                    # state for the undo file. The match_method check guards
                    # against blowing away admin manual overrides — same gate
                    # the live matcher's clear_spotify_data uses.
                    cur.execute("""
                        SELECT * FROM release_streaming_links
                        WHERE release_id = %s
                          AND service = 'spotify'
                          AND service_id = %s
                    """, (release_id, spotify_album_id))
                    link_row = cur.fetchone()

                    label = (
                        f'{csv_row.get("release_title") or release_id} '
                        f'/ {spotify_album_id}'
                    )

                    if not link_row:
                        not_in_db += 1
                        log.debug(f"  [skip not-in-DB] {label}")
                        continue
                    if (link_row.get('match_method') or '').lower() == 'manual':
                        manual_skipped += 1
                        log.warning(f"  [skip manual override] {label}")
                        continue

                    # Companion: any Spotify-sourced imagery on the same
                    # release. Deleting the link without clearing imagery
                    # leaves orphaned cover art that points at the wrong
                    # album.
                    cur.execute("""
                        SELECT * FROM release_imagery
                        WHERE release_id = %s AND source = 'Spotify'
                    """, (release_id,))
                    imagery_rows = cur.fetchall() or []

                    undo_rows.append(_coerce_undo_row(link_row, 'release_streaming_links'))
                    for ir in imagery_rows:
                        undo_rows.append(_coerce_undo_row(ir, 'release_imagery'))

                    if args.apply:
                        cur.execute("""
                            DELETE FROM release_streaming_links
                            WHERE release_id = %s
                              AND service = 'spotify'
                              AND service_id = %s
                              AND (match_method != 'manual' OR match_method IS NULL)
                        """, (release_id, spotify_album_id))
                        deleted_streaming += cur.rowcount
                        cur.execute("""
                            DELETE FROM release_imagery
                            WHERE release_id = %s AND source = 'Spotify'
                        """, (release_id,))
                        deleted_imagery += cur.rowcount
                        log.info(
                            f"  [DELETED] {label} "
                            f"(+{len(imagery_rows)} imagery)"
                        )
                    else:
                        deleted_streaming += 1
                        deleted_imagery += len(imagery_rows)
                        log.info(
                            f"  [WOULD DELETE] {label} "
                            f"(+{len(imagery_rows)} imagery)"
                        )

            if args.apply:
                conn.commit()
                log.info("Committed.")
            else:
                conn.rollback()
        except Exception:
            conn.rollback()
            log.exception("Aborted — rolled back.")
            return 1

    if undo_path and undo_rows:
        undo_path.parent.mkdir(parents=True, exist_ok=True)
        with undo_path.open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=UNDO_COLUMNS)
            w.writeheader()
            w.writerows(undo_rows)
        log.info(f"Wrote {len(undo_rows)} rows to {undo_path}")

    log.info("")
    log.info("Summary:")
    log.info(f"  CSV rows:                       {len(rows)}")
    log.info(f"  Not in DB (already gone):       {not_in_db}")
    log.info(f"  Skipped (manual override):      {manual_skipped}")
    if args.apply:
        log.info(f"  Deleted from release_streaming_links: {deleted_streaming}")
        log.info(f"  Deleted from release_imagery:         {deleted_imagery}")
    else:
        log.info(f"  Would delete from release_streaming_links: {deleted_streaming}")
        log.info(f"  Would delete from release_imagery:         {deleted_imagery}")
        log.info("  Pass --apply to actually delete.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
