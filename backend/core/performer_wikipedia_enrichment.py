"""
Performer Wikipedia-enrichment sweep.

Producer side of the ('wikipedia', 'enrich_performer_from_wikipedia') job.
It enqueues one job per performer that has a Wikipedia URL; the handler in
research_worker/handlers/wikipedia.py does the page fetch and the DB writes
(birth_date, death_date, biography, images). Dates are only-new; biography is
refreshed from Wikipedia on every run (overwriting any existing blurb); images
are harvested in full and linked when new (deduped by URL), so additional page
images get picked up even for performers who already have one — all of which
propagate on the next sweep.

Deliberately NOT gated on missing fields. Unlike the reference-verification
sweep (which only enqueues performers missing a ref), this walks *every*
performer that has a Wikipedia URL, every run:

  - Wikipedia content evolves — a biography or new image can appear, or a
    death date can be added, long after the row was created. Re-examining
    everyone is how we pick those up.
  - The hit rate is modest (only a minority of performers have a Wikipedia
    URL at all), so the full set is not overwhelming, and the handler
    short-circuits cheaply for anything already complete.

"Has a Wikipedia URL" honours both the dedicated wikipedia_url column and the
external_links->>'wikipedia' fallback, and treats empty strings as absent —
matching how the handler resolves the URL.

Per-performer jobs (rather than one mega-job) for the same reasons as the
other backfills: each job is bounded (one page fetch + a couple of image API
calls + one UPDATE), the research_jobs unique index dedups on
(wikipedia, enrich_performer_from_wikipedia, performer, <id>) so re-running
mid-sweep is a no-op for in-flight rows, a crash loses at most one job, and
admin can watch progress via the research_jobs table.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from db_utils import get_db_connection

logger = logging.getLogger(__name__)

JOB_TYPE = 'enrich_performer_from_wikipedia'

# A performer is a candidate iff it has a non-empty Wikipedia URL in either the
# dedicated column or external_links.
_HAS_WIKIPEDIA = (
    "COALESCE(NULLIF(wikipedia_url, ''), "
    "NULLIF(external_links->>'wikipedia', '')) IS NOT NULL"
)


def find_candidate_performer_ids(limit: Optional[int] = None) -> list[str]:
    """Return UUIDs of performers that have a Wikipedia URL.

    ORDER BY created_at DESC so a partial (limited) sweep covers the
    most-recently-added performers first — same convention as the other
    backfills.
    """
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = f"""
        SELECT id
        FROM performers
        WHERE {_HAS_WIKIPEDIA}
        ORDER BY created_at DESC
        {limit_clause}
    """
    params: tuple = (limit,) if limit is not None else ()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row['id']) for row in rows]


def enqueue_one(performer_id: str,
                force_refresh: bool = False,
                priority: int = 110) -> Optional[int]:
    """Enqueue a single performer (used by the CLI --name / --id path)."""
    payload = {'force_refresh': True} if force_refresh else {}
    return research_jobs.enqueue(
        source=research_jobs.SOURCE_WIKIPEDIA,
        job_type=JOB_TYPE,
        target_type=research_jobs.TARGET_PERFORMER,
        target_id=performer_id,
        payload=payload,
        priority=priority,
    )


def enqueue_sweep(limit: Optional[int] = None,
                  force_refresh: bool = False,
                  priority: int = 110,
                  batch_size: int = 1000) -> dict[str, int]:
    """Find candidate performers and enqueue one enrich job per row.

    Priority 110 sits behind user-driven work (50) and the normal
    research-pipeline default (100), so the sweep won't starve interactive
    jobs.

    Returns:
        {'candidates': N, 'enqueued': M, 'skipped': S}
        - candidates: how many performers have a Wikipedia URL.
        - enqueued:   how many new research_jobs rows were inserted.
        - skipped:    how many collapsed against an existing in-flight job via
                      the dedup index — expected on re-runs while jobs are
                      still draining.
    """
    performer_ids = find_candidate_performer_ids(limit=limit)
    if not performer_ids:
        return {'candidates': 0, 'enqueued': 0, 'skipped': 0}

    payload = {'force_refresh': True} if force_refresh else {}
    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_WIKIPEDIA,
        job_type=JOB_TYPE,
        target_type=research_jobs.TARGET_PERFORMER,
        target_ids=performer_ids,
        payload=payload,
        priority=priority,
        batch_size=batch_size,
    )

    logger.info(
        "performer_wikipedia_enrichment: candidates=%d enqueued=%d skipped=%d",
        result['requested'], result['inserted'], result['skipped'],
    )
    return {
        'candidates': result['requested'],
        'enqueued': result['inserted'],
        'skipped': result['skipped'],
    }
