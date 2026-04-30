#!/usr/bin/env python3
"""
Add idx_song_album + idx_song_id to an existing Apple Music catalog DuckDB.

Use this when the rebuild_index handler succeeded loading data but failed
on the song indexes specifically. The .duckdb file is intact; we just
need to add the missing indexes without redoing the 30+ minute load.

Run from the worker shell so the file path /data/apple_music_catalog.duckdb
resolves naturally:

    cd /opt/render/project/src/backend
    python scripts/onetime_scripts/add_apple_song_indexes.py

Use a high DUCKDB_MEMORY_LIMIT for this — there are no other workers
competing inside the script's process, so DuckDB gets nearly the full
plan memory. On an 8GB worker, 7000MB is reasonable.
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    load_dotenv(os.path.join(backend_dir, '.env'))
except ImportError:
    pass

import duckdb


def main() -> int:
    db_path = Path(
        os.environ.get('APPLE_MUSIC_CATALOG_DB')
        or '/data/apple_music_catalog.duckdb'
    )
    if not db_path.exists():
        print(f"ERROR: {db_path} does not exist. Run rebuild_index first.")
        return 1

    memory_limit = os.environ.get('DUCKDB_MEMORY_LIMIT', '7000MB')
    threads = os.environ.get('DUCKDB_THREADS', '1')
    temp_dir = db_path.parent / 'duckdb_tmp'
    temp_dir.mkdir(parents=True, exist_ok=True)

    print(f"Opening {db_path}")
    print(f"  memory_limit={memory_limit} threads={threads} temp_dir={temp_dir}")
    conn = duckdb.connect(str(db_path))
    conn.execute(f"SET memory_limit='{memory_limit}'")
    conn.execute(f"SET temp_directory='{temp_dir}'")
    conn.execute(f"SET threads={threads}")
    conn.execute("SET preserve_insertion_order=false")

    # Quick sanity check.
    row = conn.execute("SELECT COUNT(*) FROM songs").fetchone()
    print(f"songs row count: {row[0]:,}")

    existing = {
        r[0] for r in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'songs'"
        ).fetchall()
    }
    print(f"existing song indexes: {sorted(existing) or '(none)'}")

    targets = [
        ('idx_song_album', 'CREATE INDEX idx_song_album ON songs(album_id)'),
        ('idx_song_id',    'CREATE INDEX idx_song_id ON songs(id)'),
    ]

    for name, sql in targets:
        if name in existing:
            print(f"  {name}: already exists, skipping")
            continue
        print(f"  {name}: building...")
        t0 = time.time()
        try:
            conn.execute(sql)
        except Exception as e:
            print(f"  {name}: FAILED in {time.time()-t0:.1f}s — {e}")
            conn.close()
            return 2
        print(f"  {name}: built in {time.time()-t0:.1f}s")

    conn.execute("CHECKPOINT")
    conn.close()
    print(f"Done. {db_path} is fully indexed.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
