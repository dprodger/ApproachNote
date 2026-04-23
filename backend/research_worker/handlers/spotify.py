"""
Spotify handler — second source on the durable queue.

Job shape:
    source='spotify', job_type='match_song',
    target_type='song', target_id=<song UUID>
    payload may include: {'rematch': bool}

Wraps integrations/spotify/matcher.SpotifyMatcher.match_releases — a
per-song operation that walks every release of the song and matches each
against Spotify (album lookup + per-track matching). Same matcher the
existing in-process pipeline uses (core.song_research), so behavior is
identical.

Quota accounting: skipped. Spotify uses HTTP 429 rate limits, not a daily
budget like YouTube. The SpotifyClient already retries 429s internally
with exponential backoff (max_retries=3 by default). If those retries
exhaust, the matcher returns success=False with the rate-limit error in
the message; we surface that as RetryableError so the worker reschedules
with its own backoff.

Result mapping:
    success=True                    -> dict of stats, status=done
    "Song not found"                -> PermanentError (won't fix on retry)
    "No releases found"             -> dict with matched=False (nothing to do)
    any other failure               -> RetryableError (treat as transient)
"""

from __future__ import annotations

from typing import Any

from integrations.spotify.matcher import SpotifyMatcher

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


# Substrings of result['error'] that mean "the input was bad / target gone";
# retrying won't help.
_PERMANENT_ERROR_MARKERS = ('song not found',)

# Substrings that mean "nothing to match" — not a failure, just a no-op.
_NO_OP_ERROR_MARKERS = ('no releases found',)


@handler('spotify', 'match_song')
def match_song(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Match every release of one song to Spotify. Returns stats summary."""
    song_id = ctx.target_id
    rematch = bool(payload.get('rematch', False))

    matcher = SpotifyMatcher(rematch=rematch, logger=ctx.log)
    result = matcher.match_releases(song_id)

    if result.get('success'):
        stats = result.get('stats') or {}
        return {
            'matched': stats.get('releases_with_spotify', 0) > 0,
            'releases_processed': stats.get('releases_processed', 0),
            'releases_with_spotify': stats.get('releases_with_spotify', 0),
            'releases_updated': stats.get('releases_updated', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'cache_hits': stats.get('cache_hits', 0),
            'api_calls': stats.get('api_calls', 0),
            'rate_limit_hits': stats.get('rate_limit_hits', 0),
        }

    error = result.get('error') or 'unknown error'
    error_lower = error.lower()

    if any(marker in error_lower for marker in _PERMANENT_ERROR_MARKERS):
        raise PermanentError(f"Spotify: {error}")

    if any(marker in error_lower for marker in _NO_OP_ERROR_MARKERS):
        # The matcher reports this via success=False but it's not a failure.
        # Record it as a clean no-op so the job goes to 'done', not 'dead'.
        return {
            'matched': False,
            'reason': 'no_releases',
            'releases_processed': 0,
        }

    # Rate-limit-after-retries, network blips, parse errors, etc. Let the
    # worker schedule a backoff and try again.
    raise RetryableError(f"Spotify match failed: {error}")
