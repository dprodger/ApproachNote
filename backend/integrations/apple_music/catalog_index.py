"""
Build the indexed DuckDB database from downloaded Apple Music parquet files.

Used by:
  - The chained `apple/rebuild_index` research job (production refresh).
  - `scripts/onetime_scripts/build_apple_catalog_index.py` (manual CLI).

The script and the worker both call `build_index()` so behavior is identical
across CLI and queue paths.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None  # type: ignore


def latest_export_dir(catalog_dir: Path, feed_name: str) -> Optional[Path]:
    """Most recent dated export under `catalog_dir/<feed_name>/`, or None."""
    feed_dir = Path(catalog_dir) / feed_name
    if not feed_dir.exists():
        return None
    export_dirs = sorted(
        [d for d in feed_dir.iterdir() if d.is_dir()],
        reverse=True,
    )
    return export_dirs[0] if export_dirs else None


def build_index(
    catalog_dir: Path,
    db_path: Path,
    *,
    rebuild: bool = True,
    albums_only: bool = False,
    skip_song_indexes: bool = False,
    logger: Optional[logging.Logger] = None,
) -> dict[str, Any]:
    """Build the indexed DuckDB database from parquet files.

    Args:
        catalog_dir: directory containing per-feed dated subdirs of parquet files
        db_path: output path for the DuckDB file
        rebuild: if True and db_path exists, delete and rebuild
        albums_only: skip songs (saves disk space)
        skip_song_indexes: load songs but don't create lookup indexes
        logger: where to log progress

    Returns:
        Dict with build stats: {albums, songs?, artists?, duration_seconds, db_size_bytes, has_artists}.

    Raises:
        ImportError: duckdb not installed
        FileNotFoundError: no albums export found in catalog_dir
    """
    log = logger or logging.getLogger(__name__)

    if not DUCKDB_AVAILABLE:
        raise ImportError("duckdb is required. Install with: pip install duckdb")

    catalog_dir = Path(catalog_dir)
    db_path = Path(db_path)

    if db_path.exists():
        if rebuild:
            log.info(f"Removing existing database: {db_path}")
            db_path.unlink()
        else:
            log.info(f"Database already exists: {db_path}; skipping build")
            return {'skipped': True, 'reason': 'exists', 'db_size_bytes': db_path.stat().st_size}

    albums_dir = latest_export_dir(catalog_dir, 'albums')
    songs_dir = latest_export_dir(catalog_dir, 'songs')
    artists_dir = latest_export_dir(catalog_dir, 'artists')

    if not albums_dir:
        raise FileNotFoundError(f"No albums export found in {catalog_dir}/albums/")

    albums_glob = str(albums_dir / '*.parquet')
    songs_glob = str(songs_dir / '*.parquet') if songs_dir else None
    artists_glob = str(artists_dir / '*.parquet') if artists_dir else None

    log.info("Building indexed Apple Music catalog database...")
    log.info(f"  Albums source:  {albums_dir}")
    if songs_dir:
        log.info(f"  Songs source:   {songs_dir}")
    if artists_dir:
        log.info(f"  Artists source: {artists_dir}")
    else:
        log.warning("  Artists catalog not found - will use localized artist names!")
    log.info(f"  Output: {db_path}")

    started = time.time()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    artist_count = 0
    has_artists = False
    if artists_glob:
        log.info("Loading artists (for English name lookup)...")
        t0 = time.time()
        conn.execute(f"""
            CREATE TABLE artists AS
            SELECT
                id,
                nameDefault as name_english,
                name['default'] as name_default,
                CAST(name AS VARCHAR) as name_localized_json
            FROM read_parquet('{artists_glob}')
            WHERE nameDefault IS NOT NULL
        """)
        artist_count = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        log.info(f"  Loaded {artist_count:,} artists in {time.time()-t0:.1f}s")
        conn.execute("CREATE INDEX idx_artist_id ON artists(id)")
        has_artists = True

    log.info("Loading albums...")
    t0 = time.time()
    if has_artists:
        conn.execute(f"""
            CREATE TABLE albums AS
            SELECT
                a.id,
                a.nameDefault as name,
                COALESCE(art.name_english, a.primaryArtists[1].name) as artist_name,
                a.primaryArtists[1].id as artist_id,
                CAST(a.releaseDate AS VARCHAR) as release_date,
                len(a.songs) as track_count,
                a.upc,
                a.urlTemplate as url_template,
                CAST(a.primaryArtists AS VARCHAR) as primary_artists_json
            FROM read_parquet('{albums_glob}') a
            LEFT JOIN artists art ON a.primaryArtists[1].id = art.id
            WHERE a.nameDefault IS NOT NULL
        """)
    else:
        conn.execute(f"""
            CREATE TABLE albums AS
            SELECT
                id,
                nameDefault as name,
                primaryArtists[1].name as artist_name,
                primaryArtists[1].id as artist_id,
                CAST(releaseDate AS VARCHAR) as release_date,
                len(songs) as track_count,
                upc,
                urlTemplate as url_template,
                CAST(primaryArtists AS VARCHAR) as primary_artists_json
            FROM read_parquet('{albums_glob}')
            WHERE nameDefault IS NOT NULL
        """)
    album_count = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
    log.info(f"  Loaded {album_count:,} albums in {time.time()-t0:.1f}s")

    log.info("Creating album indexes...")
    t0 = time.time()
    conn.execute("CREATE INDEX idx_album_name ON albums(LOWER(name))")
    conn.execute("CREATE INDEX idx_album_artist ON albums(LOWER(artist_name))")
    conn.execute("CREATE INDEX idx_album_id ON albums(id)")
    log.info(f"  Created album indexes in {time.time()-t0:.1f}s")

    song_count: Optional[int] = None
    if songs_glob and not albums_only:
        log.info("Loading songs...")
        t0 = time.time()
        if has_artists:
            conn.execute(f"""
                CREATE TABLE songs AS
                SELECT
                    s.id,
                    s.nameDefault as name,
                    COALESCE(art.name_english, s.primaryArtists[1].name) as artist_name,
                    s.primaryArtists[1].id as artist_id,
                    s.album.id as album_id,
                    s.album.name as album_name,
                    s.volumeNumber as disc_number,
                    s.trackNumber as track_number,
                    s.durationInMillis as duration_ms,
                    s.isrc,
                    s.shortPreview as preview_url
                FROM read_parquet('{songs_glob}') s
                LEFT JOIN artists art ON s.primaryArtists[1].id = art.id
                WHERE s.nameDefault IS NOT NULL
            """)
        else:
            conn.execute(f"""
                CREATE TABLE songs AS
                SELECT
                    id,
                    nameDefault as name,
                    primaryArtists[1].name as artist_name,
                    primaryArtists[1].id as artist_id,
                    album.id as album_id,
                    album.name as album_name,
                    volumeNumber as disc_number,
                    trackNumber as track_number,
                    durationInMillis as duration_ms,
                    isrc,
                    shortPreview as preview_url
                FROM read_parquet('{songs_glob}')
                WHERE nameDefault IS NOT NULL
            """)
        song_count = conn.execute("SELECT COUNT(*) FROM songs").fetchone()[0]
        log.info(f"  Loaded {song_count:,} songs in {time.time()-t0:.1f}s")

        if skip_song_indexes:
            log.info("Skipping song indexes (skip_song_indexes=True)")
        else:
            log.info("Creating song indexes...")
            t0 = time.time()
            conn.execute("CREATE INDEX idx_song_name ON songs(LOWER(name))")
            conn.execute("CREATE INDEX idx_song_artist ON songs(LOWER(artist_name))")
            conn.execute("CREATE INDEX idx_song_album ON songs(album_id)")
            conn.execute("CREATE INDEX idx_song_id ON songs(id)")
            log.info(f"  Created song indexes in {time.time()-t0:.1f}s")

    if has_artists:
        log.info("Dropping artists lookup table (data is now in albums/songs)")
        conn.execute("DROP TABLE artists")

    conn.close()

    duration = time.time() - started
    db_size = db_path.stat().st_size
    log.info(f"Done in {duration:.1f}s — {db_size / (1024**3):.2f} GB at {db_path}")

    return {
        'skipped': False,
        'has_artists': has_artists,
        'artists': artist_count,
        'albums': album_count,
        'songs': song_count,
        'duration_seconds': round(duration, 1),
        'db_size_bytes': db_size,
        'db_path': str(db_path),
    }
