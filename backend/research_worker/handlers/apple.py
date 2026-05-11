"""
Apple Music handler — third source on the durable queue.

Job shape:
    source='apple', job_type='match_song',
    target_type='song', target_id=<song UUID>
    payload may include: {'rematch': bool}

Wraps integrations/apple_music/matcher.AppleMusicMatcher.match_releases —
the per-song matcher that fans out across every release of the song. Same
matcher the in-process pipeline used to call; the in-process call has been
removed in favor of this handler.

Matching uses the local MotherDuck catalog (local_catalog_only=True),
matching the previous in-process configuration so behavior is identical.

Quota accounting: skipped. Apple Music matching uses the local catalog
(no rate limit) for ~all queries; the API client only kicks in as a
fallback and handles 429s with internal backoff.

Result shape mapping (note: Apple matcher differs from Spotify's):
    success=True                    -> done with normalized stats
                                       (covers both real matches and
                                       "no releases" — Apple reports the
                                       latter as success, unlike Spotify)
    "Song not found" message        -> PermanentError
    any other failure               -> RetryableError
"""

from __future__ import annotations

import logging
import uuid
from io import StringIO
from typing import Any

from db_utils import get_db_connection
from integrations.apple_music.matcher import AppleMusicMatcher
from integrations.apple_music.search import search_and_validate_album

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


# Substring of result['message'] that means the input was bad / target gone;
# retrying won't help.
_PERMANENT_ERROR_MARKERS = ('song not found',)

# Mirrors the strict-mode thresholds enforced inside AppleMusicMatcher.
# Reported back to the admin UI alongside the inputs so the operator can
# see exactly which bar the candidate had to clear.
_STRICT_MODE_THRESHOLDS = {
    'min_artist_similarity': 75,
    'min_album_similarity': 65,
    'min_track_similarity': 85,
}


@handler('apple', 'match_song')
def match_song(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Match every release of one song to Apple Music. Returns stats summary."""
    song_id = ctx.target_id
    rematch = bool(payload.get('rematch', False))

    matcher = AppleMusicMatcher(
        rematch=rematch,
        local_catalog_only=True,  # Same as the previous in-process config.
        logger=ctx.log,
    )
    result = matcher.match_releases(song_id)

    if result.get('success'):
        stats = result.get('stats') or {}
        return {
            'matched': stats.get('releases_matched', 0) > 0,
            'releases_processed': stats.get('releases_processed', 0),
            'releases_matched': stats.get('releases_matched', 0),
            'releases_with_apple_music': stats.get('releases_with_apple_music', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'tracks_no_match': stats.get('tracks_no_match', 0),
            'artwork_added': stats.get('artwork_added', 0),
            'cache_hits': stats.get('cache_hits', 0),
            'api_calls': stats.get('api_calls', 0),
            'catalog_queries': stats.get('catalog_queries', 0),
        }

    message = result.get('message') or 'unknown error'
    message_lower = message.lower()

    if any(marker in message_lower for marker in _PERMANENT_ERROR_MARKERS):
        raise PermanentError(f"Apple Music: {message}")

    # Network blips, catalog connection issues, etc. Let backoff sort it out.
    raise RetryableError(f"Apple Music match failed: {message}")


@handler('apple', 'diagnose_match')
def diagnose_match(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Read-only "what would the matcher do?" for one release.

    The web service can't reach the Apple Music catalog (the DuckDB index
    and parquet exports live on the worker's persistent disk), so the
    admin diagnose UI enqueues this job and polls research_jobs for the
    result. Same code path the live matcher uses; nothing is written.

    Captures the matcher's DEBUG log into the returned dict so the admin
    UI can show the reasoning inline. Job completes as 'done' even when
    the matcher errors out — the error string goes in the result so the
    UI can render it; only setup failures (release missing, etc.) raise.
    """
    release_id = ctx.target_id
    use_api_fallback = bool(payload.get('use_api_fallback', False))
    force_refresh = bool(payload.get('force_refresh', False))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, artist_credit, release_year
                FROM releases WHERE id = %s
                """,
                (release_id,),
            )
            release = cur.fetchone()
            if not release:
                raise PermanentError(f"Release {release_id} not found")

    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'worker.apple_diag.{uuid.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(log_handler)

    error = None
    result = None
    try:
        matcher = AppleMusicMatcher(
            dry_run=True,
            strict_mode=True,
            force_refresh=force_refresh,
            local_catalog_only=not use_api_fallback,
            logger=diag_logger,
        )
        result = search_and_validate_album(
            matcher,
            artist_name=release['artist_credit'] or '',
            album_title=release['title'],
            release_year=release['release_year'],
        )
    except Exception as e:
        ctx.log.exception(
            "Apple Music diagnosis failed for release %s", release_id,
        )
        error = str(e)
    finally:
        diag_logger.removeHandler(log_handler)
        log_handler.close()

    return {
        'input': {
            'album_title': release['title'],
            'artist_name': release['artist_credit'],
            'release_year': release['release_year'],
            'use_api_fallback': use_api_fallback,
            'force_refresh': force_refresh,
            'thresholds': _STRICT_MODE_THRESHOLDS,
        },
        'matched': result is not None,
        'result': result,
        'log': log_buffer.getvalue(),
        'error': error,
    }
