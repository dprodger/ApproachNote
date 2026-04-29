"""
Per-source worker thread loop.

One thread per source. Each tick:
1. Claim the oldest eligible queued job (status -> 'running').
2. Look up the registered handler for (source, job_type).
3. Run it inside a JobContext that tracks elapsed time and exposes
   `consume_quota` so the handler can deduct units before the API call.
4. Translate the outcome into a research_jobs row update.

Sleeps `poll_interval` seconds when there's nothing to do.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

from . import claim, quota
from .errors import (
    HandlerError,
    PermanentError,
    QuotaExhausted,
    RetryableError,
    utcnow,
)
from .registry import get_handler

logger = logging.getLogger(__name__)


class JobContext:
    """Per-job handle passed to handler functions.

    Handlers receive (payload, ctx) and use ctx to:
    - log with structured fields (job_id, source, target_id) prefilled
    - deduct quota units before making cost-bearing API calls
    """

    def __init__(self, job: dict[str, Any]):
        self.job = job
        self.job_id: int = job['id']
        self.source: str = job['source']
        self.job_type: str = job['job_type']
        self.target_type: str = job['target_type']
        self.target_id: str = str(job['target_id'])
        self.attempt: int = job['attempts']
        self.log = logger.getChild(self.source)
        self._started = time.perf_counter()

    def consume_quota(self, cost: int, window: str = 'day') -> None:
        """Deduct quota units. Raises QuotaExhausted to signal the loop."""
        quota.consume(self.source, window, cost)

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self._started) * 1000)


def run_source(
    source: str,
    job_type: str,
    shutdown: threading.Event,
    poll_interval: float = 2.0,
    worker_id: Optional[str] = None,
) -> None:
    """Drive one (source, job_type) pair until shutdown is set.

    Each thread is bound to a specific (source, job_type); claim_next filters
    on both so a thread for ('apple','match_song') never picks up an
    ('apple','refresh_catalog') job.
    """
    worker_id = worker_id or _make_worker_id(source, job_type)
    log = logger.getChild(f"{source}.{job_type}")
    handler_fn = get_handler(source, job_type)
    log.info(
        "worker thread starting source=%s job_type=%s worker_id=%s "
        "poll_interval=%.1fs",
        source, job_type, worker_id, poll_interval,
    )

    while not shutdown.is_set():
        try:
            job = claim.claim_next(source, job_type, worker_id)
        except Exception:
            log.exception("claim_next failed; backing off")
            shutdown.wait(poll_interval * 2)
            continue

        if job is None:
            shutdown.wait(poll_interval)
            continue

        _process_one(handler_fn, job, log)

    log.info("worker thread stopping source=%s", source)


def _process_one(handler_fn, job: dict[str, Any], log: logging.Logger) -> None:
    ctx = JobContext(job)
    log = log.getChild(f"job{ctx.job_id}")
    log.info(
        "claimed job_id=%s job_type=%s target=%s/%s attempt=%s/%s",
        ctx.job_id, ctx.job_type, ctx.target_type, ctx.target_id,
        ctx.attempt, job['max_attempts'],
    )

    try:
        result = handler_fn(job['payload'] or {}, ctx)
    except QuotaExhausted as e:
        log.warning(
            "quota exhausted; releasing job for resets_at=%s elapsed_ms=%d",
            e.resets_at.isoformat(), ctx.elapsed_ms(),
        )
        claim.release_for_quota(ctx.job_id, e.resets_at)
        return
    except PermanentError as e:
        log.error("permanent failure: %s elapsed_ms=%d", e, ctx.elapsed_ms())
        claim.mark_dead(ctx.job_id, str(e))
        return
    except RetryableError as e:
        log.warning(
            "retryable failure attempt=%s/%s: %s elapsed_ms=%d",
            ctx.attempt, job['max_attempts'], e, ctx.elapsed_ms(),
        )
        new_status = claim.schedule_retry(
            ctx.job_id, ctx.attempt, job['max_attempts'], str(e),
        )
        log.info("retry scheduled status=%s", new_status)
        return
    except HandlerError as e:
        # Unknown HandlerError subclass — treat as retryable to be safe.
        log.warning("handler error: %s; treating as retryable", e)
        claim.schedule_retry(
            ctx.job_id, ctx.attempt, job['max_attempts'], str(e),
        )
        return
    except Exception as e:
        log.exception("handler crashed; treating as retryable")
        claim.schedule_retry(
            ctx.job_id, ctx.attempt, job['max_attempts'],
            f"{type(e).__name__}: {e}",
        )
        return

    if not isinstance(result, dict):
        log.error(
            "handler returned non-dict %r; coercing to empty result",
            type(result).__name__,
        )
        result = {}

    claim.mark_done(ctx.job_id, result)
    log.info(
        "done job_id=%s elapsed_ms=%d result_keys=%s",
        ctx.job_id, ctx.elapsed_ms(), sorted(result.keys()),
    )


def _make_worker_id(source: str, job_type: str) -> str:
    import os
    import socket
    return f"{socket.gethostname()}:{os.getpid()}:{source}.{job_type}"
