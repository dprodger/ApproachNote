"""
Exceptions handlers raise to communicate per-job outcome.

The worker loop catches these and translates them into the appropriate
research_jobs row update (release, retry, fail, dead).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


class HandlerError(Exception):
    """Base for outcomes the worker loop knows how to act on."""


class QuotaExhausted(HandlerError):
    """Source's rate limit/quota is spent. Job is released back to 'queued'
    with run_after set to the reset time. Attempts NOT incremented (we
    didn't actually do work on the target)."""

    def __init__(self, source: str, resets_at: datetime, message: str = ''):
        self.source = source
        self.resets_at = resets_at
        super().__init__(
            message
            or f"{source} quota exhausted; resets at {resets_at.isoformat()}"
        )


class RetryableError(HandlerError):
    """Transient failure (network blip, 5xx, brief 429). Worker increments
    attempts and reschedules with backoff. If attempts hits max_attempts
    the job goes to 'dead'."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: Optional[int] = None,
    ):
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message)


class PermanentError(HandlerError):
    """Definitive failure that retrying won't fix (bad payload, target
    missing, 4xx that isn't quota/rate-limit). Job goes straight to
    'dead' — no further retries."""


def utcnow() -> datetime:
    """UTC-aware now. Centralised so tests can monkeypatch one symbol."""
    return datetime.now(timezone.utc)
