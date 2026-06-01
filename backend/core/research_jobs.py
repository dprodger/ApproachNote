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
SOURCE_WIKIPEDIA = 'wikipedia'

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


def enqueue_many_for_targets(
    source: str,
    job_type: str,
    target_type: str,
    target_ids: list[str | UUID],
    *,
    payload: Optional[dict[str, Any]] = None,
    priority: int = 100,
    max_attempts: int = 5,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Bulk-enqueue jobs that share everything but target_id.

    Optimised producer path for backfill sweeps that enqueue tens of
    thousands of (source, job_type, target_type, *) jobs at once. Each
    batch is a single multi-row INSERT ... ON CONFLICT DO NOTHING, so
    the wire cost is ~N/batch_size round-trips instead of N. With a
    remote pooler the speedup is dominated by latency-per-row collapse.

    All rows in a sweep share payload, priority, max_attempts, and
    run_after=now(). If you need per-row variation, use enqueue() in a
    loop instead.

    Returns:
        {'requested': N, 'inserted': M, 'skipped': N-M}
        where 'skipped' counts rows that collapsed against an existing
        queued/running job (dedup index hit). The DB end-state is
        identical to N calls to enqueue(); only the round-trip count
        differs.

    Failure mode: a batch-level DB exception propagates. Any prior
    batches have already committed; the script can re-run safely
    because the unique index makes the whole operation idempotent.
    """
    if not target_ids:
        return {'requested': 0, 'inserted': 0, 'skipped': 0}

    payload_json = json.dumps(payload or {})

    # Postgres caps libpq parameter arrays at ~65535. With 7 params per
    # row (source, job_type, target_type, target_id, payload, priority,
    # max_attempts), batch_size up to ~9300 is safe. Default 1000 is
    # comfortably inside.
    PARAMS_PER_ROW = 7

    total_inserted = 0
    total_requested = len(target_ids)

    for batch_start in range(0, total_requested, batch_size):
        batch = target_ids[batch_start:batch_start + batch_size]

        # run_after is hardcoded to now() in SQL (not a parameter), saving
        # one bind per row.
        placeholders = ",".join(
            ["(%s, %s, %s, %s, %s::jsonb, %s, %s, now())"] * len(batch)
        )
        sql = f"""
            INSERT INTO research_jobs
                (source, job_type, target_type, target_id, payload,
                 priority, max_attempts, run_after)
            VALUES {placeholders}
            ON CONFLICT DO NOTHING
            RETURNING id
        """

        params: list = []
        for tid in batch:
            params.extend([
                source, job_type, target_type, str(tid), payload_json,
                priority, max_attempts,
            ])

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                inserted_rows = cur.fetchall()

        inserted = len(inserted_rows)
        total_inserted += inserted

        logger.info(
            "research_jobs.enqueue_many: source=%s job_type=%s "
            "batch=%d-%d/%d inserted=%d deduped=%d",
            source, job_type,
            batch_start + 1,
            min(batch_start + batch_size, total_requested),
            total_requested,
            inserted, len(batch) - inserted,
        )

    return {
        'requested': total_requested,
        'inserted': total_inserted,
        'skipped': total_requested - total_inserted,
    }


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
