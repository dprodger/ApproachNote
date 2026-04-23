"""
research_jobs: producer-side helpers for the durable research job queue.

Anyone wanting work done by the research worker calls `enqueue(...)`. The
worker process (backend/research_worker/) claims and runs jobs out of the
same `research_jobs` table — see sql/migrations/015_research_jobs.sql.

Idempotency: a partial unique index ensures at most one queued/running job
per (source, job_type, target_type, target_id). Duplicate enqueues collapse
silently — `enqueue()` returns the existing job's id when that happens.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from db_utils import get_db_connection

logger = logging.getLogger(__name__)

# Source identifiers. Keep in sync with handler registrations in
# backend/research_worker/handlers/.
SOURCE_YOUTUBE = 'youtube'
SOURCE_SPOTIFY = 'spotify'
SOURCE_APPLE = 'apple'
SOURCE_MUSICBRAINZ = 'musicbrainz'

# Target types. These are the kinds of rows a job operates on.
TARGET_SONG = 'song'
TARGET_RECORDING = 'recording'
TARGET_PERFORMER = 'performer'
TARGET_RELEASE = 'release'


def enqueue(
    source: str,
    job_type: str,
    target_type: str,
    target_id: str | UUID,
    *,
    payload: Optional[dict[str, Any]] = None,
    priority: int = 100,
    max_attempts: int = 5,
    run_after: Optional[datetime] = None,
) -> Optional[int]:
    """Insert a job, or return the id of the existing in-flight job for the
    same (source, job_type, target_type, target_id).

    Args:
        source: 'youtube' | 'spotify' | 'apple' | 'musicbrainz'.
        job_type: handler-specific verb, e.g. 'search_song'.
        target_type: 'song' | 'recording' | 'performer' | 'release'.
        target_id: UUID of the target row.
        payload: free-form JSON the handler may need (extra context).
        priority: lower runs sooner. Defaults to 100. Use <100 for
            user-initiated jobs that should jump the queue.
        max_attempts: how many times the worker should retry on failure
            before marking the job 'dead'.
        run_after: defer eligibility until this time (rare; mostly for
            tests or manual scheduling).

    Returns:
        The job's id, or None if the insert silently collapsed and we
        couldn't read back the existing job's id (shouldn't happen in
        practice, but the caller shouldn't crash on it).
    """
    payload_json = json.dumps(payload or {})

    insert_sql = """
        INSERT INTO research_jobs
            (source, job_type, target_type, target_id, payload,
             priority, max_attempts, run_after)
        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, COALESCE(%s, now()))
        ON CONFLICT DO NOTHING
        RETURNING id
    """

    # If the unique index swallows the insert, look up the existing
    # in-flight job so callers can still reference it.
    lookup_sql = """
        SELECT id FROM research_jobs
        WHERE source = %s
          AND job_type = %s
          AND target_type = %s
          AND target_id = %s
          AND status IN ('queued', 'running')
        LIMIT 1
    """

    target_id_str = str(target_id)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (
                source, job_type, target_type, target_id_str, payload_json,
                priority, max_attempts, run_after,
            ))
            row = cur.fetchone()
            if row:
                logger.info(
                    "research_jobs: enqueued id=%s source=%s job_type=%s "
                    "target=%s/%s",
                    row['id'], source, job_type, target_type, target_id_str,
                )
                return row['id']

            # Dedup hit — fetch the existing job so the caller has an id.
            cur.execute(lookup_sql, (
                source, job_type, target_type, target_id_str,
            ))
            existing = cur.fetchone()
            if existing:
                logger.debug(
                    "research_jobs: dedup hit id=%s source=%s job_type=%s "
                    "target=%s/%s",
                    existing['id'], source, job_type, target_type, target_id_str,
                )
                return existing['id']

            logger.warning(
                "research_jobs: enqueue collapsed but no in-flight job "
                "found for source=%s job_type=%s target=%s/%s",
                source, job_type, target_type, target_id_str,
            )
            return None


def get_job(job_id: int) -> Optional[dict[str, Any]]:
    """Return the job row by id, or None."""
    sql = "SELECT * FROM research_jobs WHERE id = %s"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id,))
            return cur.fetchone()


def status_for_target(
    target_type: str,
    target_id: str | UUID,
) -> list[dict[str, Any]]:
    """Latest job per source for a given target. Powers the client-facing
    research_status endpoint (added in a later step)."""
    sql = """
        SELECT DISTINCT ON (source)
            id, source, job_type, status, attempts, max_attempts,
            run_after, claimed_at, finished_at, last_error,
            created_at, updated_at
        FROM research_jobs
        WHERE target_type = %s AND target_id = %s
        ORDER BY source, created_at DESC
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (target_type, str(target_id)))
            return cur.fetchall()
