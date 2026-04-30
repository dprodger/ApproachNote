"""
Periodic housekeeping for research_jobs.

- Reap stuck jobs: rows in 'running' for longer than the stuck threshold are
  moved back to 'queued' so another worker can try. Covers the case where
  a worker crashed mid-handler (we don't hold a row lock for handler
  duration — see claim.py).
- Prune old terminal rows: 'done' / 'dead' older than the prune horizon
  are deleted. Keeps the table from growing unboundedly without erasing
  recent history that the client status UI depends on.
"""

from __future__ import annotations

import logging
import threading
from datetime import timedelta

from db_utils import get_db_connection

logger = logging.getLogger(__name__)


# Tuning knobs. Conservative defaults; revisit once we have real load.
STUCK_RUNNING_AFTER = timedelta(minutes=15)
PRUNE_DONE_AFTER = timedelta(days=30)
PRUNE_DEAD_AFTER = timedelta(days=90)
JANITOR_INTERVAL_SECONDS = 300  # 5 minutes


_REAP_MESSAGE = 'reaped: stuck in running'


def reap_stuck_jobs() -> int:
    """Move stuck 'running' jobs back to 'queued', or to 'dead' if they've
    already exhausted their max_attempts. The latter case is what an OOM
    loop looks like: the worker process dies before it can call
    schedule_retry, so attempts can ratchet past max_attempts unchecked
    until the janitor finalizes the row.

    Returns count of jobs reaped.
    """
    sql_requeue = """
        UPDATE research_jobs
        SET status     = 'queued',
            claimed_at = NULL,
            claimed_by = NULL,
            last_error = CASE
                WHEN last_error IS NULL OR last_error = '' THEN %s
                ELSE last_error || '; ' || %s
            END
        WHERE status = 'running'
          AND claimed_at < now() - %s
          AND attempts < max_attempts
        RETURNING id, source, claimed_by
    """
    sql_dead = """
        UPDATE research_jobs
        SET status      = 'dead',
            finished_at = now(),
            claimed_at  = NULL,
            claimed_by  = NULL,
            last_error  = CASE
                WHEN last_error IS NULL OR last_error = '' THEN %s
                ELSE last_error || '; ' || %s
            END
        WHERE status = 'running'
          AND claimed_at < now() - %s
          AND attempts >= max_attempts
        RETURNING id, source, claimed_by, attempts, max_attempts
    """
    dead_msg = 'reaped: max_attempts exhausted (likely worker OOM/crash loop)'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_dead, (dead_msg, dead_msg, STUCK_RUNNING_AFTER))
            dead_rows = cur.fetchall()
            cur.execute(sql_requeue, (_REAP_MESSAGE, _REAP_MESSAGE, STUCK_RUNNING_AFTER))
            reaped = cur.fetchall()
    for row in dead_rows:
        logger.error(
            "janitor: marked dead (attempts %s/%s) id=%s source=%s claimed_by=%s",
            row['attempts'], row['max_attempts'],
            row['id'], row['source'], row['claimed_by'],
        )
    for row in reaped:
        logger.warning(
            "janitor: reaped stuck job id=%s source=%s claimed_by=%s",
            row['id'], row['source'], row['claimed_by'],
        )
    return len(dead_rows) + len(reaped)


def prune_terminal_jobs() -> int:
    """Delete old 'done' and 'dead' rows. Returns total rows deleted."""
    sql = """
        DELETE FROM research_jobs
        WHERE (status = 'done' AND finished_at < now() - %s)
           OR (status = 'dead' AND finished_at < now() - %s)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (PRUNE_DONE_AFTER, PRUNE_DEAD_AFTER))
            deleted = cur.rowcount
    if deleted:
        logger.info("janitor: pruned %d terminal job(s)", deleted)
    return deleted


def run_janitor(shutdown: threading.Event) -> None:
    """Loop: reap + prune every JANITOR_INTERVAL_SECONDS until shutdown."""
    logger.info(
        "janitor thread starting interval=%ds stuck_after=%s",
        JANITOR_INTERVAL_SECONDS, STUCK_RUNNING_AFTER,
    )
    while not shutdown.is_set():
        try:
            reap_stuck_jobs()
            prune_terminal_jobs()
        except Exception:
            logger.exception("janitor sweep failed; will retry next interval")
        shutdown.wait(JANITOR_INTERVAL_SECONDS)
    logger.info("janitor thread stopping")
