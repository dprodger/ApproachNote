"""
Release-label backfill sweep (issue #195).

`releases` rows imported before commit a019176 (the `+labels` `inc`
addition) have label IS NULL — ~71k as of 2026-05-27. This module is the
producer side of the fix: it scans for releases that still need
backfilling and enqueues one ('musicbrainz', 'backfill_release_label')
job per row onto the durable research queue. The handler in
`research_worker/handlers/musicbrainz.py` does the actual MB API +
parse + DB UPDATE — see that file for retry / idempotency semantics.

Per-release was chosen over a single mega-job because:

  - Each job is bounded: one MB fetch (~1s with the 1-req/sec limit)
    and one UPDATE. The worker is never holding state across thousands
    of rows.
  - The research_jobs unique index dedups cleanly on
    (musicbrainz, backfill_release_label, release, <id>), so re-running
    the producer mid-sweep is a no-op for in-flight rows.
  - A worker crash mid-row loses at most one job's worth of progress;
    the janitor reaps the stuck 'running' row after 90 min and the
    next worker picks it up. No bespoke checkpointing needed.
  - Admin can query research_jobs by (source='musicbrainz', status=...)
    to see progress, errors, and dead rows without a side table.

This module is the template for future MB walking tasks (release
country, recording ISRCs, artist aliases, etc.). Each new task adds
one helper here and one sibling handler — the queue, retry, dedup,
and admin surfaces come for free.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from db_utils import get_db_connection


logger = logging.getLogger(__name__)


# Releases needing backfill: have an MB ID (so we can fetch) but no
# label yet. ORDER BY created_at DESC so a partial sweep covers the
# most-recently-imported rows first — same convention as the Spotify
# duration backfill.
_CANDIDATE_RELEASES_SQL_TEMPLATE = """
    SELECT id
    FROM releases
    WHERE label IS NULL
      AND musicbrainz_release_id IS NOT NULL
    ORDER BY created_at DESC
    {limit_clause}
"""


def find_candidate_release_ids(limit: Optional[int] = None) -> list[str]:
    """Return release UUIDs that have an MB ID but no label yet."""
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = _CANDIDATE_RELEASES_SQL_TEMPLATE.format(limit_clause=limit_clause)
    params: tuple = (limit,) if limit is not None else ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row['id']) for row in rows]


def enqueue_sweep(limit: Optional[int] = None,
                  priority: int = 110,
                  batch_size: int = 1000) -> dict[str, int]:
    """Find candidate releases and enqueue one backfill job per row.

    Priority 110 sits behind user-driven work (50) and the normal
    research-pipeline default (100), so a 71k-row sweep won't starve
    interactive jobs.

    Uses research_jobs.enqueue_many_for_targets so 71k rows enqueue in
    ~71 round-trips (default batch_size=1000) instead of ~150k. With a
    remote pooler the wall-time difference is roughly latency × batch
    size — minutes instead of hours.

    Returns:
        {'candidates': N, 'enqueued': M, 'skipped': S}
        - candidates: how many releases the SELECT found.
        - enqueued:   how many new research_jobs rows were inserted.
        - skipped:    how many collapsed against an existing in-flight
                      job via the dedup index — safe and expected on
                      re-runs.
    """
    release_ids = find_candidate_release_ids(limit=limit)
    if not release_ids:
        return {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_MUSICBRAINZ,
        job_type='backfill_release_label',
        target_type=research_jobs.TARGET_RELEASE,
        target_ids=release_ids,
        payload={},
        priority=priority,
        batch_size=batch_size,
    )

    logger.info(
        "release_label_backfill: candidates=%d enqueued=%d skipped=%d",
        result['requested'], result['inserted'], result['skipped'],
    )
    return {
        'candidates': result['requested'],
        'enqueued': result['inserted'],
        'skipped': result['skipped'],
    }
