"""
Apple Music catalog refresh handlers.

The chain (albums → songs → artists → rebuild_index) is defined in
integrations.apple_music.refresh so the web app and worker both reach
for the same source of truth. This module only registers the worker-side
behavior for each step.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from integrations.apple_music.catalog_index import build_index
from integrations.apple_music.feed import (
    AppleMusicFeedClient,
    _resolve_catalog_dir,
)
from integrations.apple_music.refresh import (
    FEED_ORDER,
    enqueue_next_step,
)
from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


@handler('apple', 'refresh_catalog')
def refresh_catalog(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Download one feed, then enqueue the next chain step."""
    feed = payload.get('feed')
    if feed not in FEED_ORDER:
        raise PermanentError(f"refresh_catalog: invalid feed '{feed}'")

    client = AppleMusicFeedClient(logger=ctx.log)
    if not client.is_configured():
        raise PermanentError(
            "Apple Music Feed not configured "
            "(APPLE_MEDIA_ID / APPLE_PRIVATE_KEY_PATH / APPLE_KEY_ID / APPLE_TEAM_ID)"
        )

    ctx.log.info(f"apple/refresh_catalog: feed='{feed}' catalog_dir={client.catalog_dir}")
    try:
        output_dir = client.download_feed(feed)
    except Exception as e:
        raise RetryableError(f"download_feed({feed}) failed: {e}") from e

    ctx.log.info(f"apple/refresh_catalog: feed='{feed}' done, output_dir={output_dir}")

    next_step, next_id = enqueue_next_step(
        current_feed=feed,
        chain_started_at=payload.get('chain_started_at'),
    )

    return {
        'feed': feed,
        'output_dir': str(output_dir),
        'next_step': next_step,
        'next_job_id': next_id,
    }


@handler('apple', 'rebuild_index')
def rebuild_index(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Final chain step: rebuild the DuckDB index from the freshly-downloaded
    parquet files. Errors out on MotherDuck (different code path, not built)."""
    catalog_dir = _resolve_catalog_dir()
    db_override = os.environ.get('APPLE_MUSIC_CATALOG_DB')

    if db_override and str(db_override).startswith('md:'):
        raise PermanentError(
            "rebuild_index does not support MotherDuck "
            "(APPLE_MUSIC_CATALOG_DB starts with 'md:'). "
            "Switch to a local DuckDB path or rebuild via the MotherDuck CLI."
        )

    db_path = Path(db_override) if db_override else (
        catalog_dir.parent / 'apple_music_catalog.duckdb'
    )

    ctx.log.info(f"apple/rebuild_index: catalog_dir={catalog_dir} db_path={db_path}")
    try:
        stats = build_index(
            catalog_dir=catalog_dir,
            db_path=db_path,
            rebuild=True,
            logger=ctx.log,
        )
    except FileNotFoundError as e:
        raise PermanentError(f"rebuild_index: {e}") from e
    except Exception as e:
        raise RetryableError(f"rebuild_index failed: {e}") from e

    return {
        'rebuilt': True,
        'stats': stats,
        'chain_started_at': payload.get('chain_started_at'),
    }
