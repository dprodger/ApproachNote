"""
Job claim/release/finalize SQL.

Each operation runs in its own short transaction. We never hold a row lock
across the handler call — `claim_next` flips status to 'running' and
commits, then the handler runs without any open transaction; on completion
one of the finalize functions writes the outcome.

That trades the safety net of "if the worker crashes mid-handler the row
auto-releases" for not pinning a Postgres connection for the whole handler
duration (handlers can take many seconds on a slow API). The janitor
covers crash recovery instead — see janitor.py.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import timedelta
from typing import Any, Optional

from db_utils import get_db_connection

from .errors import utcnow

logger = logging.getLogger(__name__)


# Backoff schedule by attempt number (1-indexed). Index 0 unused.
# attempt 1 -> 1m, 2 -> 5m, 3 -> 30m, 4 -> 2h, 5 -> 12h. Beyond 5 we cap.
_BACKOFF_SECONDS = [0, 60, 300, 1800, 7200, 43200]


def _backoff_delay(attempts: int) -> timedelta:
    """Exponential-ish backoff with ±20% jitter."""
    base = _BACKOFF_SECONDS[min(attempts, len(_BACKOFF_SECONDS) - 1)]
    jitter = random.uniform(0.8, 1.2)
    return timedelta(seconds=base * jitter)


def claim_next(source: str, worker_id: str) -> Optional[dict[str, Any]]:
    """Atomically claim the oldest eligible queued job for `source`.

    Returns the job row (with status now 'running' and attempts incremented),
    or None if nothing is eligible.

    SKIP LOCKED makes this safe to run from multiple workers / threads.
    """
    sql = """
        UPDATE research_jobs
        SET status     = 'running',
            claimed_at = now(),
            claimed_by = %s,
            attempts   = attempts + 1
        WHERE id = (
            SELECT id FROM research_jobs
            WHERE source = %s
              AND status = 'queued'
              AND run_after <= now()
            ORDER BY priority, run_after, id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING *
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (worker_id, source))
            return cur.fetchone()


def mark_done(job_id: int, result: dict[str, Any]) -> None:
    sql = """
        UPDATE research_jobs
        SET status      = 'done',
            finished_at = now(),
            last_error  = NULL,
            result      = %s::jsonb
        WHERE id = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (json.dumps(result), job_id))


def release_for_quota(job_id: int, resets_at) -> None:
    """Quota exhausted: put the job back to 'queued' for the reset time.
    Decrement attempts so the deferral doesn't count against the budget."""
    sql = """
        UPDATE research_jobs
        SET status     = 'queued',
            attempts   = GREATEST(attempts - 1, 0),
            run_after  = %s,
            claimed_at = NULL,
            claimed_by = NULL,
            last_error = 'quota_exhausted'
        WHERE id = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (resets_at, job_id))


def schedule_retry(job_id: int, attempts: int, max_attempts: int, error: str) -> str:
    """Schedule a retry, or mark dead if we're out of attempts.

    Returns the new status ('queued' or 'dead') for logging.
    """
    if attempts >= max_attempts:
        sql = """
            UPDATE research_jobs
            SET status      = 'dead',
                finished_at = now(),
                last_error  = %s
            WHERE id = %s
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (error[:2000], job_id))
        return 'dead'

    next_run = utcnow() + _backoff_delay(attempts)
    sql = """
        UPDATE research_jobs
        SET status     = 'queued',
            run_after  = %s,
            claimed_at = NULL,
            claimed_by = NULL,
            last_error = %s
        WHERE id = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (next_run, error[:2000], job_id))
    return 'queued'


def mark_dead(job_id: int, error: str) -> None:
    """Permanent failure — handler raised PermanentError, or admin cancelled."""
    sql = """
        UPDATE research_jobs
        SET status      = 'dead',
            finished_at = now(),
            last_error  = %s
        WHERE id = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (error[:2000], job_id))
