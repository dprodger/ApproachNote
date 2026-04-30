#!/usr/bin/env python3
"""
Read-only SQL REPL against the Apple Music catalog DuckDB.

Run from the worker shell so /data is reachable:

    cd /opt/render/project/src/backend
    python scripts/onetime_scripts/apple_catalog_repl.py

Tables: albums, songs (artists is dropped at end of build).
Common queries:

    SELECT * FROM albums WHERE LOWER(artist_name) LIKE '%miles davis%' LIMIT 10;
    SELECT * FROM songs WHERE LOWER(name) LIKE '%kind of blue%' LIMIT 10;
    SELECT album_id, COUNT(*) FROM songs GROUP BY album_id ORDER BY 2 DESC LIMIT 5;

End-of-line semicolon is accepted but optional. Type 'exit' or Ctrl-D to quit.
"""

import os
import sys
from pathlib import Path

import duckdb


def _print_table(rows, columns):
    """Lightweight tabular print. Truncates wide cells, no extra deps."""
    if not rows:
        print("(0 rows)")
        return
    widths = [
        max(len(str(c)), max(len(str(r[i])) for r in rows))
        for i, c in enumerate(columns)
    ]
    widths = [min(w, 60) for w in widths]
    fmt = " | ".join("{:<" + str(w) + "}" for w in widths)

    def trunc(v, w):
        s = str(v)
        return s if len(s) <= w else s[: w - 1] + "…"

    print(fmt.format(*[trunc(c, w) for c, w in zip(columns, widths)]))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(fmt.format(*[trunc(v, w) for v, w in zip(r, widths)]))
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


def main() -> int:
    db_path = Path(
        os.environ.get('APPLE_MUSIC_CATALOG_DB')
        or '/data/apple_music_catalog.duckdb'
    )
    if not db_path.exists():
        print(f"ERROR: {db_path} does not exist.")
        return 1

    print(f"Connecting read-only to {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)

    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    print(f"Tables: {tables}")
    for t in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t}: {count:,} rows")
        except Exception as e:
            print(f"  {t}: error counting — {e}")

    print("\nType SQL queries (end optional ;). 'exit' or Ctrl-D to quit.\n")

    while True:
        try:
            line = input("sql> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line:
            continue
        if line.lower() in ('exit', 'quit', '\\q'):
            break
        # Allow but don't require trailing semicolon.
        if line.endswith(';'):
            line = line[:-1]
        try:
            cur = conn.execute(line)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            if cols:
                _print_table(rows, cols)
            else:
                print("(no rows / DDL)")
        except Exception as e:
            print(f"ERROR: {e}")

    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
