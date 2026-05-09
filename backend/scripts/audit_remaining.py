#!/usr/bin/env python3
"""
Produce a filtered audit CSV with re-rescued rows removed.

Inputs: the original audit CSV (output of audit_spotify_release_links.py)
and a reaudit CSV (output of reaudit_spotify_links.py with -o).

Output: a CSV in the same column shape as the original audit, containing
only the rows that the reaudit didn't rescue. Use this as the manual-review
worklist after each round of matcher tightening.

Match key: (release_id, spotify_album_id). A row is dropped iff the reaudit
file has the same key with new_is_stale == 'FALSE'. Rows the reaudit didn't
evaluate (e.g. count-mismatch under default flags) are preserved.

Usage:
    python scripts/audit_remaining.py \
        --audit spotify_release_link_audit_20260503_024935.csv \
        --reaudit reaudit_20260509_131823.csv \
        -o audit_remaining_20260509_131823.csv

    # If -o is omitted the output is written next to the audit file with
    # `_remaining` appended to the basename.
"""

import argparse
import csv
import sys
from pathlib import Path


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Filter an audit CSV to drop rows the reaudit rescued."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--audit', required=True, help='Original audit CSV path')
    p.add_argument('--reaudit', required=True, help='Reaudit CSV path '
                   '(must have new_is_stale column)')
    p.add_argument('-o', '--output', default=None,
                   help='Output CSV path. Default: <audit>_remaining.csv')
    return p.parse_args()


def _default_output(audit_path: Path) -> Path:
    return audit_path.with_name(audit_path.stem + '_remaining' + audit_path.suffix)


def main():
    args = _parse_args()
    audit_path = Path(args.audit)
    reaudit_path = Path(args.reaudit)
    if not audit_path.exists():
        print(f"audit CSV not found: {audit_path}", file=sys.stderr)
        return 1
    if not reaudit_path.exists():
        print(f"reaudit CSV not found: {reaudit_path}", file=sys.stderr)
        return 1
    out_path = Path(args.output) if args.output else _default_output(audit_path)

    rescued = set()
    with reaudit_path.open(newline='') as f:
        rdr = csv.DictReader(f)
        if 'new_is_stale' not in (rdr.fieldnames or []):
            print(
                f"reaudit CSV missing 'new_is_stale' column. "
                f"Re-run reaudit_spotify_links.py with -o.",
                file=sys.stderr,
            )
            return 1
        for r in rdr:
            if (r.get('new_is_stale') or '').strip().upper() == 'FALSE':
                rescued.add((r['release_id'], r['spotify_album_id']))

    print(f"Rescued by reaudit: {len(rescued)} rows")

    kept = 0
    dropped = 0
    with audit_path.open(newline='') as inp, out_path.open('w', newline='') as out:
        rdr = csv.DictReader(inp)
        w = csv.DictWriter(out, fieldnames=rdr.fieldnames)
        w.writeheader()
        for r in rdr:
            key = (r.get('release_id', ''), r.get('spotify_album_id', ''))
            if key in rescued:
                dropped += 1
                continue
            w.writerow(r)
            kept += 1

    print(f"Read audit:  {kept + dropped} rows")
    print(f"Dropped:     {dropped} rescued")
    print(f"Wrote:       {kept} rows → {out_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
