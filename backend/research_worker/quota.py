"""
source_quotas accounting.

Two operations:
- consume(source, window, cost) — atomic CAS-style decrement. Returns the
  resets_at on success; raises QuotaExhausted on failure.
- mark_exhausted(source, window) — invoked when the upstream API itself
  reports quota exhaustion (e.g. YouTube 403 quotaExceeded). Slams
  units_used to units_limit so subsequent consume() calls fail fast until
  the next reset.

Both call _maybe_reset() first to roll the window forward when resets_at
has passed.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from db_utils import get_db_connection

from .errors import QuotaExhausted, utcnow

logger = logging.getLogger(__name__)


# How to advance resets_at when a window rolls over.
# Keyed by (source, window). Default: +1 day for any 'day' window.
_RESET_RULES = {
    # YouTube quota resets at midnight Pacific. We compute the next midnight
    # PT in SQL using AT TIME ZONE so DST is handled correctly.
    ('youtube', 'day'): (
        "((date_trunc('day', now() AT TIME ZONE 'America/Los_Angeles') "
        " + interval '1 day') AT TIME ZONE 'America/Los_Angeles')"
    ),
}

_DEFAULT_RESET_SQL = {
    'day':    "(date_trunc('day', now()) + interval '1 day')",
    'minute': "(date_trunc('minute', now()) + interval '1 minute')",
    'second': "(date_trunc('second', now()) + interval '1 second')",
}


def _reset_sql(source: str, window: str) -> str:
    expr = _RESET_RULES.get((source, window))
    if expr:
        return expr
    if window in _DEFAULT_RESET_SQL:
        return _DEFAULT_RESET_SQL[window]
    raise ValueError(f"No reset rule for source={source!r} window={window!r}")


def _maybe_reset(source: str, window: str) -> None:
    """If the current window has expired, zero units_used and push resets_at."""
    sql = f"""
        UPDATE source_quotas
        SET units_used = 0,
            resets_at  = {_reset_sql(source, window)}
        WHERE source = %s AND window_name = %s AND resets_at <= now()
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source, window))
            if cur.rowcount:
                logger.info(
                    "quota: rolled over source=%s window=%s", source, window,
                )


def consume(source: str, window: str, cost: int) -> datetime:
    """Atomically deduct `cost` units. Returns the current resets_at.

    Raises QuotaExhausted if the budget would be exceeded.
    """
    _maybe_reset(source, window)

    sql = """
        UPDATE source_quotas
        SET units_used = units_used + %s
        WHERE source = %s AND window_name = %s
          AND resets_at > now()
          AND units_used + %s <= units_limit
        RETURNING resets_at, units_used, units_limit
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (cost, source, window, cost))
            row = cur.fetchone()

    if row:
        logger.debug(
            "quota: consumed source=%s window=%s cost=%s used=%s/%s",
            source, window, cost, row['units_used'], row['units_limit'],
        )
        return row['resets_at']

    # Either over budget or window not yet reset — re-read for the resets_at.
    resets = current_resets_at(source, window)
    if resets is None:
        # No row at all — this is a configuration error.
        raise RuntimeError(
            f"source_quotas missing row for source={source!r} window={window!r}"
        )
    raise QuotaExhausted(source, resets)


def refund(source: str, window: str, units: int) -> None:
    """Return reserved-but-unused units to the bucket.

    Used by handlers that pre-deduct a worst-case cost before an API call,
    then settle up afterwards based on `client.stats`. GREATEST guards
    against ever going below zero (e.g. if the window rolled over between
    consume and refund).
    """
    if units <= 0:
        return
    sql = """
        UPDATE source_quotas
        SET units_used = GREATEST(units_used - %s, 0)
        WHERE source = %s AND window_name = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (units, source, window))
    logger.debug(
        "quota: refunded source=%s window=%s units=%s", source, window, units,
    )


def mark_exhausted(source: str, window: str) -> datetime:
    """Force the bucket to empty when the upstream API reports exhaustion.

    Returns the resets_at so the caller can use it as run_after.
    """
    _maybe_reset(source, window)
    sql = """
        UPDATE source_quotas
        SET units_used = units_limit
        WHERE source = %s AND window_name = %s
        RETURNING resets_at
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source, window))
            row = cur.fetchone()
    if not row:
        raise RuntimeError(
            f"source_quotas missing row for source={source!r} window={window!r}"
        )
    logger.warning(
        "quota: marked exhausted via upstream signal source=%s window=%s "
        "resets_at=%s", source, window, row['resets_at'],
    )
    return row['resets_at']


def current_resets_at(source: str, window: str) -> Optional[datetime]:
    sql = "SELECT resets_at FROM source_quotas WHERE source = %s AND window_name = %s"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source, window))
            row = cur.fetchone()
    return row['resets_at'] if row else None


def snapshot(source: str) -> list[dict]:
    """All quota windows for a source, for the admin/status endpoints."""
    sql = """
        SELECT source, window_name, units_used, units_limit, resets_at, updated_at
        FROM source_quotas WHERE source = %s
        ORDER BY window_name
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source,))
            return cur.fetchall()
