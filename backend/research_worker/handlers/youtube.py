"""
YouTube handler — first source to live on the new queue.

Job shape:
    source='youtube', job_type='match_recording',
    target_type='recording', target_id=<recording UUID>
    payload may include: {'rematch': bool}

Wraps the existing YouTubeMatcher (integrations/youtube/matcher.py) so the
matching logic stays in one place — the worker just adds queueing,
scheduling, and quota accounting around it.

Quota accounting: the matcher's underlying client also tracks per-process
quota, but the source of truth is now source_quotas. We call
ctx.consume_quota up-front for the worst-case search budget before each
matcher run, and we treat the upstream YouTubeQuotaExceededError as
authoritative — when we see it, we slam the bucket via quota.mark_exhausted
and surface QuotaExhausted to the loop. This guards against (a) our local
counter drifting and (b) the API key being shared with another process.
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


@handler('youtube', 'match_recording')
def match_recording(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Match a single recording to a YouTube video.

    Returns a result dict suitable for storing in research_jobs.result:
        {matched: bool, video_id?, video_url?, confidence?, channel?, ...}
    """
    recording_id = ctx.target_id

    # Sanity check — fail loudly (and permanently) if the target row
    # disappeared between enqueue and claim. No point retrying.
    row = yt_db.load_recording(recording_id)
    if not row:
        raise PermanentError(f"recording not found: {recording_id}")

    rematch = bool(payload.get('rematch', False))

    # Reserve the worst-case quota before doing any API work. If we're
    # already exhausted this raises QuotaExhausted and the loop will
    # release the job for the reset time without touching attempts.
    ctx.consume_quota(WORST_CASE_QUOTA)

    # max_units high enough that the client doesn't pre-empt us before
    # the upstream API does — quota accounting is owned by source_quotas.
    client = YouTubeClient(max_units=10**9, logger=ctx.log)
    matcher = YouTubeMatcher(
        client=client,
        rematch=rematch,
        logger=ctx.log,
    )

    try:
        result = matcher.match_recording(recording_id)
    except YouTubeQuotaExceededError as e:
        # Authoritative signal from upstream — empty the bucket and
        # surface to the loop so the job reschedules at reset time.
        resets_at = quota_mod.mark_exhausted('youtube', 'day')
        raise QuotaExhausted('youtube', resets_at, str(e)) from e
    except YouTubeAPIError as e:
        # 4xx that wasn't quota — don't burn retries on bad input.
        raise PermanentError(f"YouTube API error: {e}") from e
    except Exception as e:
        # Network blips, 5xx, parse errors — let backoff sort it out.
        raise RetryableError(f"{type(e).__name__}: {e}") from e

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
