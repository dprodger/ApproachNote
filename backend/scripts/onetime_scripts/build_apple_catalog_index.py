#!/usr/bin/env python3
"""
Build Indexed Apple Music Catalog Database

Loads the downloaded Apple Music parquet files into an indexed DuckDB database
for fast searching. This is a one-time operation that makes subsequent searches
nearly instant.

IMPORTANT: The Apple Music Feed has a known issue where primaryArtists[].name
contains localized names (e.g., Japanese) instead of English. To get English
artist names, you must also download the 'artists' catalog, which contains
the nameDefault field with English names.

Usage:
  python build_apple_catalog_index.py

  # Rebuild from scratch (delete existing database)
  python build_apple_catalog_index.py --rebuild

  # Show statistics about the indexed database
  python build_apple_catalog_index.py --stats
"""

import sys
import os
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from script_base import ScriptBase, run_script

from integrations.apple_music.catalog_index import (
    DUCKDB_AVAILABLE,
    build_index,
)
from integrations.apple_music.feed import _resolve_catalog_dir

try:
    import duckdb
except ImportError:
    duckdb = None


# Default paths — honor APPLE_MUSIC_CATALOG_DIR / APPLE_MUSIC_CATALOG_DB if set.
DEFAULT_CATALOG_DIR = _resolve_catalog_dir()
DEFAULT_DB_PATH = Path(
    os.environ.get('APPLE_MUSIC_CATALOG_DB')
    or DEFAULT_CATALOG_DIR.parent / "apple_music_catalog.duckdb"
)


def main() -> bool:
    script = ScriptBase(
        name="build_apple_catalog_index",
        description="Build indexed DuckDB database from Apple Music parquet files",
        epilog="""
This script loads the downloaded Apple Music catalog (parquet files) into
an indexed DuckDB database for fast searching.

The indexed database will be ~2-3GB and makes searches nearly instant
compared to scanning 150+ parquet files for each query.

Examples:
  # Build the indexed database (first time)
  python build_apple_catalog_index.py

  # Rebuild from scratch
  python build_apple_catalog_index.py --rebuild

  # Check database statistics
  python build_apple_catalog_index.py --stats
        """
    )

    script.parser.add_argument(
        '--rebuild',
        action='store_true',
        help='Delete existing database and rebuild from scratch'
    )

    script.parser.add_argument(
        '--stats',
        action='store_true',
        help='Show statistics about the indexed database'
    )

    script.parser.add_argument(
        '--albums-only',
        action='store_true',
        help='Only index albums (skip songs to save disk space)'
    )

    script.parser.add_argument(
        '--skip-song-indexes',
        action='store_true',
        help='Load songs but skip creating indexes (faster, less memory)'
    )

    script.parser.add_argument(
        '--catalog-dir',
        type=Path,
        default=DEFAULT_CATALOG_DIR,
        help=f'Directory containing parquet files (default: {DEFAULT_CATALOG_DIR})'
    )

    script.parser.add_argument(
        '--db-path',
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f'Output database path (default: {DEFAULT_DB_PATH})'
    )

    script.add_debug_arg()
    args = script.parse_args()

    if not DUCKDB_AVAILABLE:
        script.logger.error("duckdb is required. Install with: pip install duckdb")
        return False

    db_path = args.db_path
    catalog_dir = args.catalog_dir

    # Stats mode
    if args.stats:
        if not db_path.exists():
            script.logger.error(f"Database not found: {db_path}")
            script.logger.info("Run without --stats to build the database first.")
            return False

        conn = duckdb.connect(str(db_path), read_only=True)

        script.logger.info("Apple Music Catalog Index Statistics")
        script.logger.info("=" * 50)

        # Get table counts
        for table in ['albums', 'songs']:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                script.logger.info(f"  {table}: {count:,} records")
            except Exception as e:
                script.logger.info(f"  {table}: not found")

        # Get database file size
        db_size = db_path.stat().st_size / (1024 * 1024 * 1024)
        script.logger.info(f"  Database size: {db_size:.2f} GB")

        # Test search performance
        script.logger.info("\nSearch Performance Test:")
        start = time.time()
        result = conn.execute("""
            SELECT id, name, artist_name
            FROM albums
            WHERE LOWER(name) LIKE '%kind of blue%'
            AND LOWER(artist_name) LIKE '%miles davis%'
            LIMIT 5
        """).fetchall()
        elapsed = (time.time() - start) * 1000
        script.logger.info(f"  'Kind of Blue' search: {elapsed:.1f}ms ({len(result)} results)")

        conn.close()
        return True

    # Build mode — delegate to the shared catalog_index.build_index() so the
    # CLI and the apple/rebuild_index research-job handler stay in lockstep.
    try:
        result = build_index(
            catalog_dir=catalog_dir,
            db_path=db_path,
            rebuild=args.rebuild,
            albums_only=args.albums_only,
            skip_song_indexes=args.skip_song_indexes,
            logger=script.logger,
        )
    except FileNotFoundError as e:
        script.logger.error(str(e))
        script.logger.info("Run download_apple_catalog.py --feed albums first.")
        return False

    if result.get('skipped'):
        script.logger.info("Use --rebuild to recreate, or --stats to view statistics.")
        return True

    script.logger.info("")
    script.logger.info("Run with --stats to test search performance.")
    return True


if __name__ == "__main__":
    run_script(main)
