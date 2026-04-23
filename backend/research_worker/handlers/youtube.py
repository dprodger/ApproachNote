"""
YouTube handler — first source to live on the new queue.

Job shape:
    source='youtube', job_type='match_recording',
    target_type='recording', target_id=<recording UUID>
    payload may include: {'rematch': bool}

Wraps the existing YouTubeMatcher (integrations/youtube/matcher.py) so the
matching logic stays in one place — the worker just adds queueing,
scheduling, and quota accounting around it.

Quota accounting: source_quotas is the source of truth. To avoid burning
budget on jobs that were going to no-op anyway, we:
  1. Pre-check the matcher's skip conditions (`has_youtube`, missing default
     recording_release) BEFORE consuming any units — those paths return
     immediately and cost zero quota.
  2. Reserve the worst-case (3 searches + 1 metadata = 301 units) before
     calling the matcher, then refund whatever the client didn't actually
     spend afterwards. Net cost matches reality (typically 101–301 units).

Upstream signals override local accounting: if YouTube returns 403
quotaExceeded, we slam the bucket to its limit via quota.mark_exhausted
and re-raise as QuotaExhausted. No refund happens in that case — the API
itself confirmed we're out, regardless of what our counter said.
"""

from __future__ import annotations

import logging
from typing import Any

from integrations.youtube import db as yt_db
from integrations.youtube.client import (
    QUOTA_COST_SEARCH,
    QUOTA_COST_VIDEOS,
    YouTubeAPIError,
    YouTubeClient,
    YouTubeQuotaExceededError,
)
from integrations.youtube.matcher import YouTubeMatcher

from research_worker import quota as quota_mod
from research_worker.errors import (
    PermanentError,
    QuotaExhausted,
    RetryableError,
)
from research_worker.registry import handler

logger = logging.getLogger(__name__)

# How many search.list calls one match_recording might fan out to. The
# matcher fires every query in its ladder (currently up to 3) plus one
# videos.list batch. We pre-deduct the worst case so a quota-exhausted
# state is detected before we burn API calls; if the matcher uses fewer,
# the unused units stay deducted until the next reset — that's a small,
# acceptable over-counting tradeoff for the safety it buys.
WORST_CASE_SEARCHES = 3
WORST_CASE_QUOTA = WORST_CASE_SEARCHES * QUOTA_COST_SEARCH + QUOTA_COST_VIDEOS


def _skip_result(reason: str) -> dict[str, Any]:
    return {'matched': False, 'skipped': reason, 'reason': f'skipped_{reason}'}


@handler('youtube', 'match_recording')
def match_recording(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Match a single recording to a YouTube video.

    Returns a result dict suitable for storing in research_jobs.result:
        {matched: bool, video_id?, video_url?, confidence?, channel?, ...}
    """
    recording_id = ctx.target_id

    # Fail loudly (and permanently) if the target row disappeared between
    # enqueue and claim — no point retrying.
    row = yt_db.load_recording(recording_id)
    if not row:
        raise PermanentError(f"recording not found: {recording_id}")

    rematch = bool(payload.get('rematch', False))

    # Pre-check the matcher's no-cost skip paths so we don't burn quota
    # on jobs that would never have hit the API. Mirrors the early exits
    # in YouTubeMatcher._process_recording.
    if not row.get('default_recording_release_id'):
        ctx.log.info("skip: no default recording_release row")
        return _skip_result('no_default_release')
    if row.get('has_youtube') and not rematch:
        ctx.log.info("skip: already has youtube link (rematch=false)")
        return _skip_result('has_youtube')

    # Reserve the worst-case quota before any API work; we'll refund what
    # the matcher didn't actually spend. If the bucket is empty this
    # raises QuotaExhausted and the loop reschedules for the reset time.
    ctx.consume_quota(WORST_CASE_QUOTA)
    quota_settled = False  # track whether we've already settled the bucket

    # max_units high enough that the client doesn't pre-empt us before
    # the upstream API does — quota accounting is owned by source_quotas.
    client = YouTubeClient(max_units=10**9, logger=ctx.log)
    matcher = YouTubeMatcher(
        client=client,
        rematch=rematch,
        logger=ctx.log,
    )

    try:
        try:
            result = matcher.match_recording(recording_id)
        except YouTubeQuotaExceededError as e:
            # Upstream is authoritative — slam to limit, no refund.
            resets_at = quota_mod.mark_exhausted('youtube', 'day')
            quota_settled = True
            raise QuotaExhausted('youtube', resets_at, str(e)) from e
        except YouTubeAPIError as e:
            raise PermanentError(f"YouTube API error: {e}") from e
        except Exception as e:
            raise RetryableError(f"{type(e).__name__}: {e}") from e
    finally:
        if not quota_settled:
            actual_used = client.stats.get('quota_units', 0)
            unused = WORST_CASE_QUOTA - actual_used
            if unused > 0:
                quota_mod.refund('youtube', 'day', unused)
                ctx.log.debug(
                    "quota: reserved=%s actual=%s refunded=%s",
                    WORST_CASE_QUOTA, actual_used, unused,
                )

    # Normalise matcher's internal shape into a flat, JSON-friendly dict.
    if result.get('matched'):
        return {
            'matched': True,
            'video_id': result.get('video_id'),
            'video_url': result.get('video_url'),
            'video_title': result.get('video_title'),
            'channel': result.get('channel'),
            'confidence': result.get('confidence'),
            'rows_written': result.get('rows_written'),
        }

    skipped = result.get('skipped')
    return {
        'matched': False,
        'skipped': skipped,
        'reason': 'no_match' if not skipped else f'skipped_{skipped}',
    }
