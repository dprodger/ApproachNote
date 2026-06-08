"""
Performer Commons-imagery enrichment sweep (producer side).

Enqueues one ('commons', 'enrich_performer_imagery') job per performer that is
"due": never imagery-checked, or last checked longer ago than the staleness
window (default 90 days). The handler in
research_worker/handlers/commons.py does the gathering, visual analysis and DB
writes, and stamps performers.last_imagery_check on every completion — which is
what makes a performer stop being "due" until the window elapses again.

This covers both first-population (last_imagery_check IS NULL) and periodic
re-examination for newly-uploaded Commons photos (stale rows), in one query.

Per-performer jobs (not one mega-job) for the same reasons as the other
backfills: each job is bounded, the research_jobs unique index dedups on
(commons, enrich_performer_imagery, performer, <id>) so re-running mid-sweep is
a no-op for in-flight rows, and admin can watch progress on the dashboard.
"""

from __future__ import annotations

import logging
from typing import Optional

from core import research_jobs
from db_utils import get_db_connection

logger = logging.getLogger(__name__)

JOB_TYPE = "enrich_performer_imagery"

# Bulk sweep: sits behind user work (50), the normal pipeline (100), and the
# Wikipedia sweep (110). Imagery is expensive and low-urgency.
DEFAULT_PRIORITY = 200
DEFAULT_STALE_DAYS = 90


def find_candidate_performer_ids(stale_days: int = DEFAULT_STALE_DAYS,
                                 limit: Optional[int] = None) -> list[str]:
    """UUIDs of performers due for an imagery (re)check: never checked, or
    last checked more than `stale_days` ago. Newest performers first."""
    limit_clause = "LIMIT %s" if limit is not None else ""
    sql = f"""
        SELECT id
        FROM performers
        WHERE last_imagery_check IS NULL
           OR last_imagery_check < now() - make_interval(days => %s)
        ORDER BY created_at DESC
        {limit_clause}
    """
    params: tuple = (stale_days, limit) if limit is not None else (stale_days,)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [str(row["id"]) for row in rows]


def enqueue_one(performer_id: str, *, limit: Optional[int] = None,
                rerank_cap: Optional[int] = None,
                priority: int = DEFAULT_PRIORITY) -> Optional[int]:
    """Enqueue a single performer (the --name / --id CLI path)."""
    payload: dict = {}
    if limit is not None:
        payload["limit"] = limit
    if rerank_cap is not None:
        payload["rerank_cap"] = rerank_cap
    return research_jobs.enqueue(
        source=research_jobs.SOURCE_COMMONS,
        job_type=JOB_TYPE,
        target_type=research_jobs.TARGET_PERFORMER,
        target_id=performer_id,
        payload=payload,
        priority=priority,
    )


def enqueue_sweep(stale_days: int = DEFAULT_STALE_DAYS,
                  limit: Optional[int] = None,
                  priority: int = DEFAULT_PRIORITY,
                  batch_size: int = 1000) -> dict[str, int]:
    """Enqueue one imagery job per due performer.

    Returns {'candidates': N, 'enqueued': M, 'skipped': S} where 'skipped'
    counts dedup-index collapses against still-in-flight jobs (expected on
    re-runs while the queue drains).
    """
    performer_ids = find_candidate_performer_ids(stale_days=stale_days, limit=limit)
    if not performer_ids:
        return {"candidates": 0, "enqueued": 0, "skipped": 0}

    result = research_jobs.enqueue_many_for_targets(
        source=research_jobs.SOURCE_COMMONS,
        job_type=JOB_TYPE,
        target_type=research_jobs.TARGET_PERFORMER,
        target_ids=performer_ids,
        priority=priority,
        batch_size=batch_size,
    )
    logger.info("performer_commons_imagery: candidates=%d enqueued=%d skipped=%d",
                result["requested"], result["inserted"], result["skipped"])
    return {
        "candidates": result["requested"],
        "enqueued": result["inserted"],
        "skipped": result["skipped"],
    }
