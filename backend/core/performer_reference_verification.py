"""
Performer reference-verification sweep (only-new mode).

Producer side of the durable-queue replacement for the old in-process
scripts/verify_performer_references.py. It scans for performers missing a
Wikipedia and/or MusicBrainz reference and enqueues one
('musicbrainz', 'verify_performer_references') job per row onto the research
queue. The handler in research_worker/handlers/musicbrainz.py does the
actual Wikipedia + MB search and DB UPDATE — see that file for the
only-new / transient-handling semantics.

Per-performer was chosen over a single mega-job for the same reasons as the
release-label backfill (see core.release_label_backfill):

  - Each job is bounded: at most one Wikipedia search + one MB search + one
    UPDATE, naturally serialised under MB's 1-req/sec limit.
  - The research_jobs unique index dedups on
    (musicbrainz, verify_performer_references, performer, <id>), so
    re-running the producer mid-sweep is a no-op for in-flight rows.
  - A worker crash mid-row loses at most one job; the janitor reaps the
    stuck 'running' row and the next worker re-claims it.
  - Admin can watch progress via research_jobs without a side table.

"Missing a reference" honours the dedicated columns AND the external_links
JSONB fallback, matching how the handler reads existing refs: a performer is
a candidate when it lacks a Wikipedia ref in BOTH wikipedia_url and
external_links->>'wikipedia', OR lacks an MB ref in BOTH musicbrainz_id and
external_links->>'musicbrainz'.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from db_utils import get_db_connection


logger = logging.getLogger(__name__)


# Candidates: performers missing at least one reference. ORDER BY created_at
# DESC so a partial sweep covers the most-recently-added performers first —
# same convention as the release-label and Spotify-duration backfills.
_CANDIDATE_PERFORMERS_SQL_TEMPLATE = """
    SELECT id
    FROM performers
    WHERE (wikipedia_url IS NULL AND external_links->>'wikipedia' IS NULL)
       OR (musicbrainz_id IS NULL AND external_links->>'musicbrainz' IS NULL)
    ORDER BY created_at DESC
    {limit_clause}
"""


def find_candidate_performer_ids(limit: Optional[int] = None) -> list[str]:
    """Return performer UUIDs missing a Wikipedia and/or MusicBrainz ref."""
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = _CANDIDATE_PERFORMERS_SQL_TEMPLATE.format(limit_clause=limit_clause)
    params: tuple = (limit,) if limit is not None else ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row['id']) for row in rows]


def enqueue_sweep(limit: Optional[int] = None,
                  priority: int = 110,
                  batch_size: int = 1000) -> dict[str, int]:
    """Find candidate performers and enqueue one verify job per row.

    Priority 110 sits behind user-driven work (50) and the normal
    research-pipeline default (100), so the sweep won't starve interactive
    jobs.

    Returns:
        {'candidates': N, 'enqueued': M, 'skipped': S}
        - candidates: how many performers the SELECT found.
        - enqueued:   how many new research_jobs rows were inserted.
        - skipped:    how many collapsed against an existing in-flight job
                      via the dedup index — safe and expected on re-runs.
    """
    performer_ids = find_candidate_performer_ids(limit=limit)
    if not performer_ids:
        return {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_MUSICBRAINZ,
        job_type='verify_performer_references',
        target_type=research_jobs.TARGET_PERFORMER,
        target_ids=performer_ids,
        payload={},
        priority=priority,
        batch_size=batch_size,
    )

    logger.info(
        "performer_reference_verification: candidates=%d enqueued=%d skipped=%d",
        result['requested'], result['inserted'], result['skipped'],
    )
    return {
        'candidates': result['requested'],
        'enqueued': result['inserted'],
        'skipped': result['skipped'],
    }
