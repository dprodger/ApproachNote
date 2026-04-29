"""
Read-only health/status snapshot for the Apple Music DuckDB catalog.

Powers the /admin/apple-music-catalog admin page. Four sections, each
isolated so a failure in one (e.g. MotherDuck unreachable) doesn't
stop the others from rendering:

  1. Mode + configuration — local file vs MotherDuck vs parquet-only
  2. Connectivity — can we open a connection right now? round-trip ms.
  3. Freshness — last export per feed (songs / albums / artists)
  4. Row counts — COUNT(*) on each table

The catalog is opened fresh per request (no global handle). Admin page
hits are rare; correctness over latency.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any, Optional


logger = logging.getLogger(__name__)


# Tables we expect in the indexed DuckDB.
_EXPECTED_TABLES = ('songs', 'albums', 'artists')


def get_catalog_status() -> dict[str, Any]:
    """Top-level entry point used by the admin route.

    Returns a dict with five sections — `configuration`, `connectivity`,
    `freshness`, `row_counts`, `recent_refresh_jobs`. Each section's
    failure is contained within that section's `error` field rather
    than bubbling up.
    """
    return {
        'configuration':       _gather_configuration(),
        'connectivity':        _gather_connectivity(),
        'freshness':           _gather_freshness(),
        'row_counts':          _gather_row_counts(),
        'recent_refresh_jobs': _gather_recent_refresh_jobs(),
    }


# ---------------------------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------------------------

def _gather_configuration() -> dict[str, Any]:
    """Identify which backing store is in use and what env knobs are set.

    Doesn't need a DuckDB connection — purely env + filesystem checks.
    """
    motherduck_token_set = bool(os.environ.get('MOTHERDUCK_TOKEN'))
    catalog_db_override = os.environ.get('APPLE_MUSIC_CATALOG_DB')
    catalog_dir_override = os.environ.get('APPLE_MUSIC_CATALOG_DIR')

    # Resolve the "would-be" db path the same way AppleMusicCatalog does.
    try:
        from integrations.apple_music.feed import AppleMusicCatalog
        default_db_path = AppleMusicCatalog.DEFAULT_DB_PATH
    except Exception as e:
        return {
            'mode': 'unknown',
            'details': None,
            'error': f'Could not import AppleMusicCatalog: {e}',
        }

    if catalog_db_override and str(catalog_db_override).startswith('md:'):
        mode = 'motherduck'
        details = catalog_db_override
    elif catalog_db_override:
        # User-pointed local file
        mode = 'local_file'
        details = catalog_db_override
    elif Path(default_db_path).exists():
        mode = 'local_file'
        details = str(default_db_path)
    elif motherduck_token_set:
        mode = 'motherduck'
        details = 'md:apple_music_feed'
    else:
        mode = 'parquet_fallback'
        details = 'no indexed DB; will scan parquet files at query time'

    return {
        'mode': mode,
        'details': details,
        'motherduck_token_set': motherduck_token_set,
        'catalog_db_override': catalog_db_override,
        'catalog_dir_override': catalog_dir_override,
        'default_db_path': str(default_db_path),
        'default_db_path_exists': Path(default_db_path).exists(),
    }


# ---------------------------------------------------------------------------
# 2. Connectivity
# ---------------------------------------------------------------------------

def _gather_connectivity() -> dict[str, Any]:
    """Open a connection and time a `SELECT 1` round-trip.

    For MotherDuck this measures the network hop; for a local file it's
    near-zero (interesting only as a "did the file open" check).
    Catches any exception and reports it on the page rather than
    crashing the route.
    """
    try:
        from integrations.apple_music.feed import AppleMusicCatalog
    except Exception as e:
        return {'ok': False, 'error': f'import failed: {e}', 'elapsed_ms': None}

    start = time.perf_counter()
    try:
        catalog = AppleMusicCatalog()
        # _create_conn() is the public-ish entry the matcher uses too.
        # It returns None and stashes the connection on self._conn.
        catalog._create_conn()
        conn = catalog._conn
        if conn is None:
            return {
                'ok': False,
                'elapsed_ms': None,
                'error': 'connection object is None after _create_conn()',
            }
        conn.execute('SELECT 1').fetchone()
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        return {
            'ok': False,
            'elapsed_ms': elapsed_ms,
            'error': str(e),
        }
    else:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        return {
            'ok': True,
            'elapsed_ms': elapsed_ms,
            'error': None,
        }
    finally:
        try:
            conn.close()  # noqa: F821 — guarded by the try-except above
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 3. Freshness
# ---------------------------------------------------------------------------

def _gather_freshness() -> dict[str, Any]:
    """Per-feed `metadata.txt` snapshot — export date + record count.

    Reuses AppleMusicCatalog.get_catalog_stats() which already parses
    the metadata files. Surfaces the whole thing or an error string.
    """
    try:
        from integrations.apple_music.feed import AppleMusicCatalog
        catalog = AppleMusicCatalog()
        stats = catalog.get_catalog_stats()
    except Exception as e:
        return {'error': str(e), 'feeds': None}

    return {'error': None, 'feeds': stats}


# ---------------------------------------------------------------------------
# 4. Row counts
# ---------------------------------------------------------------------------

def _gather_row_counts() -> dict[str, Any]:
    """`COUNT(*)` per expected table. Each table is probed independently
    so a missing or unreadable table doesn't blank out the rest.

    Only runs when `_use_indexed_db` is True — the parquet-fallback
    path doesn't have a stable per-table COUNT story without scanning
    everything, which would defeat "cheap status page."
    """
    try:
        from integrations.apple_music.feed import AppleMusicCatalog
        catalog = AppleMusicCatalog()
    except Exception as e:
        return {'error': str(e), 'tables': None, 'mode': None}

    if not catalog._use_indexed_db:
        return {
            'error': None,
            'tables': None,
            'mode': 'parquet_fallback_skipped',
            'note': 'Row counts not gathered — no indexed DB available, '
                    'and full-parquet scans would be too slow for a status page.',
        }

    try:
        catalog._create_conn()
    except Exception as e:
        return {'error': f'connection failed: {e}', 'tables': None, 'mode': 'indexed'}

    conn = catalog._conn
    counts: dict[str, Any] = {}
    for table in _EXPECTED_TABLES:
        try:
            row = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
            counts[table] = {'count': int(row[0]), 'error': None}
        except Exception as e:
            counts[table] = {'count': None, 'error': str(e)}

    try:
        conn.close()
    except Exception:
        pass

    return {'error': None, 'tables': counts, 'mode': 'indexed'}


# ---------------------------------------------------------------------------
# 5. Recent refresh-chain activity
# ---------------------------------------------------------------------------

# job_types that make up the refresh chain. Same set the apple_catalog
# handler uses; duplicated here to avoid importing the worker module from
# the web app.
_REFRESH_JOB_TYPES = ('refresh_catalog', 'rebuild_index')


def _gather_recent_refresh_jobs(limit: int = 12) -> dict[str, Any]:
    """Return the last few apple/refresh_catalog + apple/rebuild_index rows.

    Powers the dashboard's "Recent refresh activity" panel and lets the
    operator see whether a chain is in flight, where it stalled, or when
    the last successful run finished.
    """
    try:
        from db_utils import get_db_connection
    except Exception as e:
        return {'error': f'db import failed: {e}', 'jobs': None}

    sql = """
        SELECT id, source, job_type, status, priority, attempts, max_attempts,
               payload, result, last_error,
               created_at, claimed_at, finished_at, run_after
        FROM research_jobs
        WHERE source = 'apple'
          AND job_type = ANY(%s)
        ORDER BY created_at DESC
        LIMIT %s
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (list(_REFRESH_JOB_TYPES), limit))
                rows = cur.fetchall()
    except Exception as e:
        return {'error': str(e), 'jobs': None}

    jobs = []
    for row in rows:
        jobs.append({
            'id': row['id'],
            'job_type': row['job_type'],
            'feed': (row.get('payload') or {}).get('feed'),
            'status': row['status'],
            'attempts': row['attempts'],
            'max_attempts': row['max_attempts'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'claimed_at': row['claimed_at'].isoformat() if row['claimed_at'] else None,
            'finished_at': row['finished_at'].isoformat() if row['finished_at'] else None,
            'last_error': row.get('last_error'),
            'result': row.get('result'),
        })

    return {'error': None, 'jobs': jobs}
