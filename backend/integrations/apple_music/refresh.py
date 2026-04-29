"""
Shared chain-definition + producer for the Apple Music catalog refresh.

Imported by:
  - The admin route that enqueues a refresh on user click (web service).
  - The research_worker handler that runs each step (worker service).

Putting the chain shape here keeps both services in lockstep without the
web app having to import worker-side code.

Chain order:
    refresh_catalog/albums
    refresh_catalog/songs
    refresh_catalog/artists
    rebuild_index
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from core import research_jobs


SOURCE = 'apple'
JOB_TYPE_REFRESH = 'refresh_catalog'
JOB_TYPE_REBUILD = 'rebuild_index'

# Synthetic target_type for catalog-level jobs (no row in any business table).
TARGET_TYPE = 'catalog'

# Feeds run in this order. After the last one, rebuild_index is enqueued.
FEED_ORDER = ('albums', 'songs', 'artists')

# Fixed sentinel UUIDs — one per step. Postgres requires a UUID target_id
# and the unique-while-in-flight index uses target_id, so reusing the same
# UUID per step gives us free dedup (no concurrent refresh of the same feed).
TARGET_IDS: dict[str, UUID] = {
    'albums':        UUID('11111111-1111-1111-1111-aaaaaaaaaaaa'),
    'songs':         UUID('11111111-1111-1111-1111-bbbbbbbbbbbb'),
    'artists':       UUID('11111111-1111-1111-1111-cccccccccccc'),
    'rebuild_index': UUID('11111111-1111-1111-1111-dddddddddddd'),
}

# Refresh jumps ahead of routine match work but stays below user-initiated
# research (which uses 10–50). Tweak if needed.
DEFAULT_PRIORITY = 40


def enqueue_refresh_chain(*, priority: int = DEFAULT_PRIORITY) -> Optional[int]:
    """Kick off a refresh by enqueuing step 1 (albums). Returns the job id,
    or the id of an already-in-flight chain if one exists (dedup hit)."""
    return research_jobs.enqueue(
        source=SOURCE,
        job_type=JOB_TYPE_REFRESH,
        target_type=TARGET_TYPE,
        target_id=TARGET_IDS['albums'],
        payload={
            'feed': 'albums',
            'chain_started_at': datetime.now(timezone.utc).isoformat(),
        },
        priority=priority,
        # Refresh is long and idempotent; single attempt — admin can re-enqueue.
        max_attempts=1,
    )


def enqueue_next_step(
    *,
    current_feed: str,
    chain_started_at: Optional[str],
    priority: int = DEFAULT_PRIORITY,
) -> tuple[str, Optional[int]]:
    """Called by the refresh_catalog handler on success. Enqueues either the
    next feed download or the final rebuild_index step.

    Returns (next_step_label, next_job_id).
    """
    next_idx = FEED_ORDER.index(current_feed) + 1
    if next_idx < len(FEED_ORDER):
        next_feed = FEED_ORDER[next_idx]
        next_id = research_jobs.enqueue(
            source=SOURCE,
            job_type=JOB_TYPE_REFRESH,
            target_type=TARGET_TYPE,
            target_id=TARGET_IDS[next_feed],
            payload={'feed': next_feed, 'chain_started_at': chain_started_at},
            priority=priority,
            max_attempts=1,
        )
        return f"refresh_catalog/{next_feed}", next_id

    next_id = research_jobs.enqueue(
        source=SOURCE,
        job_type=JOB_TYPE_REBUILD,
        target_type=TARGET_TYPE,
        target_id=TARGET_IDS['rebuild_index'],
        payload={'chain_started_at': chain_started_at},
        priority=priority,
        max_attempts=1,
    )
    return 'rebuild_index', next_id
