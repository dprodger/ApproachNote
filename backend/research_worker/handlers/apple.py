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

from typing import Any

from integrations.apple_music.matcher import AppleMusicMatcher

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


# Substring of result['message'] that means the input was bad / target gone;
# retrying won't help.
_PERMANENT_ERROR_MARKERS = ('song not found',)


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
