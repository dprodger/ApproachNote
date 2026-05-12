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

from core.apple_catalog_status import get_worker_catalog_status

from research_worker.errors import PermanentError, RetryableError
from research_worker.registry import handler


def _release_link_snapshot(cur, release_id: str) -> list:
    """Capture release-level + track-level + imagery state for a release.

    Used before and after a rematch to diff the matcher's DB writes.
    Same SQL the synchronous endpoint used to run inline — kept here so
    the rematch handler is self-contained on the worker side.
    """
    cur.execute(
        """
        SELECT 'album'  AS scope, NULL::uuid AS recording_release_id,
               service, service_id, service_url,
               match_method, match_confidence,
               NULL::text AS img_source, NULL::text AS img_type
        FROM release_streaming_links WHERE release_id = %s
        UNION ALL
        SELECT 'track'  AS scope, rrsl.recording_release_id,
               rrsl.service, rrsl.service_id, rrsl.service_url,
               rrsl.match_method, rrsl.match_confidence,
               NULL, NULL
        FROM recording_release_streaming_links rrsl
        JOIN recording_releases rr ON rr.id = rrsl.recording_release_id
        WHERE rr.release_id = %s
        UNION ALL
        SELECT 'imagery' AS scope, NULL,
               ri.source::text AS service, ri.source_id AS service_id,
               ri.source_url AS service_url,
               NULL, NULL,
               ri.source::text, ri.type::text
        FROM release_imagery ri WHERE ri.release_id = %s
        ORDER BY scope, service, recording_release_id
        """,
        (release_id, release_id, release_id),
    )
    return [dict(r) for r in cur.fetchall()]


def _diff_snapshots(before: list, after: list) -> dict:
    """Compute added / removed / changed between two snapshot lists.

    Identity key:
      - imagery rows: (scope, source, type)
      - streaming rows: (scope, service, recording_release_id)
    A service_id or match_method change shows up as a 'changed' row,
    not added+removed.
    """
    def _key(row):
        if row['scope'] == 'imagery':
            return ('imagery', row.get('img_source') or '', row.get('img_type') or '')
        return (
            row['scope'],
            row['service'],
            str(row.get('recording_release_id') or ''),
        )

    before_by = {_key(r): r for r in before}
    after_by = {_key(r): r for r in after}

    added = [after_by[k] for k in after_by.keys() - before_by.keys()]
    removed = [before_by[k] for k in before_by.keys() - after_by.keys()]
    changed = []
    for k in before_by.keys() & after_by.keys():
        b, a = before_by[k], after_by[k]
        if (
            b.get('service_id') != a.get('service_id')
            or b.get('match_method') != a.get('match_method')
        ):
            changed.append({'before': b, 'after': a})
    return {'added': added, 'removed': removed, 'changed': changed}


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
            release_id=release_id,
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


@handler('apple', 'rematch_release')
def rematch_release(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Re-run the Apple Music matcher against one release; persist results.

    The synchronous web endpoint that used to do this work ran the matcher
    in the Flask process, which can't reach the Apple Music catalog on
    the worker's persistent disk. Moved here so the live matcher actually
    sees the catalog the way the background apple/match_song handler does.

    Per-release semantics: a release can carry tracks from multiple songs,
    so we look up every distinct song on the release and call
    AppleMusicMatcher.match_releases(song_id, release_ids=[release_id]) for
    each — same shared matcher instance, so stats accumulate. force_refresh
    + rematch=True ensure the album is re-searched even when there's a
    stale "searched / no match" timestamp.

    Snapshots release_streaming_links, recording_release_streaming_links,
    and release_imagery before and after; the diff is returned so the UI
    can render added / removed / changed rows.

    Job completes as 'done' even when the matcher errors out — the error
    string goes in the result so the UI can render it; only setup
    failures (release missing, no songs linked) raise PermanentError.
    """
    release_id = ctx.target_id
    use_api_fallback = bool(payload.get('use_api_fallback', False))

    # Phase 1: validate, fetch songs, snapshot before. Releases connection
    # before the matcher run so we don't pin a pool slot for many seconds.
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM releases WHERE id = %s", (release_id,))
            if not cur.fetchone():
                raise PermanentError(f"Release {release_id} not found")

            cur.execute(
                """
                SELECT DISTINCT rec.song_id, s.title AS song_title
                FROM recording_releases rr
                JOIN recordings rec ON rec.id = rr.recording_id
                JOIN songs s ON s.id = rec.song_id
                WHERE rr.release_id = %s
                ORDER BY s.title
                """,
                (release_id,),
            )
            songs = [dict(r) for r in cur.fetchall()]
            if not songs:
                raise PermanentError(
                    f"No songs linked to release {release_id}; nothing to rematch"
                )
            before = _release_link_snapshot(cur, release_id)

    # Phase 2: run the matcher with a buffered logger. Matcher opens its
    # own DB connections via the pool as it writes.
    log_buffer = StringIO()
    diag_logger = logging.getLogger(f'worker.apple_rematch.{uuid.uuid4().hex}')
    diag_logger.setLevel(logging.DEBUG)
    diag_logger.propagate = False
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(logging.Formatter('%(message)s'))
    diag_logger.addHandler(log_handler)

    error = None
    per_song_results = []
    matcher = None
    try:
        matcher = AppleMusicMatcher(
            dry_run=False,
            strict_mode=True,
            force_refresh=True,
            rematch=True,
            local_catalog_only=not use_api_fallback,
            logger=diag_logger,
        )
        for song in songs:
            song_id_str = str(song['song_id'])
            diag_logger.info(
                f"=== Processing song {song['song_title']!r} ({song_id_str}) ==="
            )
            res = matcher.match_releases(song_id_str, release_ids=[release_id])
            per_song_results.append({
                'song_id': song_id_str,
                'song_title': song['song_title'],
                'success': res.get('success', False),
                'message': res.get('message'),
            })
    except Exception as e:
        ctx.log.exception(
            "Apple Music rematch failed for release %s", release_id,
        )
        error = str(e)
    finally:
        diag_logger.removeHandler(log_handler)
        log_handler.close()

    # Phase 3: snapshot after + compute the diff.
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            after = _release_link_snapshot(cur, release_id)

    changes = _diff_snapshots(before, after)
    stats = matcher.stats if matcher else {}

    return {
        'songs_processed': per_song_results,
        'stats': {
            'releases_processed': stats.get('releases_processed', 0),
            'releases_matched': stats.get('releases_matched', 0),
            'releases_with_apple_music': stats.get('releases_with_apple_music', 0),
            'releases_no_match': stats.get('releases_no_match', 0),
            'releases_skipped': stats.get('releases_skipped', 0),
            'tracks_matched': stats.get('tracks_matched', 0),
            'tracks_no_match': stats.get('tracks_no_match', 0),
            'artwork_added': stats.get('artwork_added', 0),
            'local_catalog_hits': stats.get('local_catalog_hits', 0),
            'api_calls': stats.get('api_calls', 0),
        },
        'changes': changes,
        'log': log_buffer.getvalue(),
        'error': error,
    }


@handler('apple', 'catalog_status')
def catalog_status(payload: dict[str, Any], ctx) -> dict[str, Any]:
    """Gather the worker's view of the Apple Music DuckDB catalog.

    Same four sections the web process used to compute itself
    (configuration, connectivity, freshness, row_counts), but now run
    in the process that actually has the catalog disk mounted. The
    admin page reads the cached result of this job and overlays a
    fresh `recent_refresh_jobs` section locally.

    Pure read; no DB writes. Errors per section are contained inside
    that section's `error` field (per get_worker_catalog_status's
    design), so this handler should essentially never raise.
    """
    return get_worker_catalog_status()
