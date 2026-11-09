"""
Targeted backfill for Spotify links lost to the version-keyword asymmetry
rejection on live albums.

Background. Until the album-wide escape landed in
integrations/spotify/matcher.py, `match_tracks_for_release` rejected any
candidate where one side's title carried a version keyword and the other
didn't. That is right when MusicBrainz says "Peace (live at Newport)" and
Spotify offers a lone studio "Peace". It is wrong in the opposite
direction: Spotify suffixes EVERY track on a live album " - Live" and
titles the album "<Name> (Live)", while MB annotates neither, so the
asymmetry is labelling convention and says nothing about which
performance a track is. Those matches were thrown away, and because the
song's only track on the release had failed, the album match was
discarded as a probable false positive — costing the release its album
link and cover art too.

This module finds the affected (song, release) pairs and re-runs the
matcher against only those releases.

Why not just queue ('spotify', 'match_song') per song? Two reasons:

  - Cost. `match_song` walks every release of the song. Across the
    affected songs that is ~195k releases to reach the ~2.1k that need
    it — about 92x the Spotify API calls.
  - The failure cache. `is_track_match_cached_failure` short-circuits a
    (song, release, album) combination the matcher already gave up on,
    and `match_song` only honours payload['rematch'], which maps to
    `rematch` — not `rematch_all`, the only flag that skips the cache.
    So the queued job would re-reach the same cached "no track match"
    and change nothing.

Two candidate signals, both requiring that the recording_release has no
Spotify link and that the MB recording title carries NO version keyword
(that asymmetry is the precondition for the old rejection):

  A. Another service's link on the same recording_release has an
     annotated title. Apple Music and YouTube label live albums the way
     Spotify does, so their title is a usable proxy for Spotify's.
  B. The release's OTHER recordings already have Spotify links whose
     titles are >=80% annotated — direct evidence that this is a live
     album the matcher reached and that Spotify labels album-wide.

Sampled dry runs put signal B around 87% precision and signal A around
47%, so B carries most of the value and A is kept for releases that have
no Spotify links at all yet.

SAFETY: the candidates are split into two groups that need different
matcher modes, and mixing them up risks destroying good data.

  - Release already has a Spotify album link -> rematch_tracks mode.
    The matcher takes its dedicated rematch-tracks path: fetch the known
    album's tracks and fill only the recordings missing a track link. It
    does not re-search for the album, so it cannot clear the release's
    existing Spotify data, and recordings that already have a track link
    are left alone. That path never consults the failure cache.
  - Release has no Spotify album link -> rematch mode. Here the matcher
    must search for the album, which is the path that checks the failure
    cache, so run_backfill purges the candidates' negative cache entries
    first (see purge_failure_cache). Clearing is gated on
    `had_previous_spotify`, which is false for these rows, so a failed
    search cannot wipe anything.

Purging beats `force_refresh=True` for getting past those cached
failures: force_refresh bypasses every cache on the client, including the
album searches and album-track fetches that repeat across the several
songs sharing one release. The negative entries live in their own
`track_failures/` directory, so they can be dropped on their own while
the positive caches stay warm.

The cache is per-host, so where this runs matters. On the host whose
cache the research worker writes, the purge is what unblocks the
candidates. On any other host there is nothing cached to block them and
the purge reports 0 — which is correct, not a failure. A dry run can't
delete anything either way, so it sets force_refresh on the searching
mode to keep the preview truthful; see run_backfill.

What we deliberately do NOT use is `rematch_all`. On a group-2 release
it would re-evaluate links that are already good and clear any the fresh
pass happened to reject — and signal B selects precisely the releases
that have good links on their other recordings.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from db_utils import get_db_connection
from integrations.spotify.matcher import SpotifyMatcher


logger = logging.getLogger(__name__)


# Mirrors _VERSION_KEYWORDS in integrations/spotify/matching.py. Kept as a
# Postgres regex so candidate selection happens in one round trip instead
# of pulling every recording_release into Python. \m and \M are Postgres's
# word-boundary operators, so 'live' does not match inside 'olive'.
VERSION_KEYWORD_REGEX = (
    r'\m(live|demo|alternate|alt take|rehearsal|instrumental|acoustic'
    r'|unplugged|session)\M'
)

# Fraction of a release's existing Spotify track titles that must be
# annotated for signal B to treat it as an album-wide-labelled live
# album. Same bar as version_keyword_is_album_wide in matching.py.
ALBUM_WIDE_RATIO = 0.8

_CANDIDATE_SQL_TEMPLATE = """
    WITH no_spotify AS (
        SELECT rr.id AS rr_id, rr.release_id, rr.recording_id
        FROM recording_releases rr
        WHERE NOT EXISTS (
            SELECT 1 FROM recording_release_streaming_links l
            WHERE l.recording_release_id = rr.id AND l.service = 'spotify')
    ),
    sig_a AS (
        SELECT DISTINCT ns.rr_id
        FROM no_spotify ns
        JOIN recording_release_streaming_links l
          ON l.recording_release_id = ns.rr_id
        WHERE l.service <> 'spotify'
          AND l.service_title ~* %(vk)s
    ),
    live_albums AS (
        SELECT rr.release_id
        FROM recording_releases rr
        JOIN recording_release_streaming_links l
          ON l.recording_release_id = rr.id AND l.service = 'spotify'
        GROUP BY rr.release_id
        HAVING count(*) FILTER (WHERE l.service_title ~* %(vk)s)::float
               / count(*) >= %(ratio)s
    ),
    sig_b AS (
        SELECT DISTINCT ns.rr_id
        FROM no_spotify ns
        JOIN live_albums la ON la.release_id = ns.release_id
    )
    SELECT DISTINCT
        rec.song_id,
        ns.release_id,
        (rsl.service_id IS NOT NULL) AS has_album_link
    FROM no_spotify ns
    JOIN recordings rec ON rec.id = ns.recording_id
    LEFT JOIN release_streaming_links rsl
      ON rsl.release_id = ns.release_id AND rsl.service = 'spotify'
    WHERE rec.title !~* %(vk)s
      AND (ns.rr_id IN (SELECT rr_id FROM sig_a)
           OR ns.rr_id IN (SELECT rr_id FROM sig_b))
      {song_clause}
    ORDER BY rec.song_id, ns.release_id
    {limit_clause}
"""


def find_candidate_pairs(limit: Optional[int] = None,
                         song_id: Optional[str] = None) -> list[dict[str, Any]]:
    """Return the (song, release) pairs that look like victims of the
    version-asymmetry rejection on an album-wide-labelled live album.

    Args:
        limit: cap the number of pairs returned (for smoke runs).
        song_id: restrict to one song, for spot-checking a single case.

    Returns:
        List of {'song_id': str, 'release_id': str, 'has_album_link': bool}.
        `has_album_link` decides which matcher mode the pair needs — see
        the module docstring.
    """
    sql = _CANDIDATE_SQL_TEMPLATE.format(
        song_clause="AND rec.song_id = %(song_id)s" if song_id else "",
        limit_clause="LIMIT %(limit)s" if limit is not None else "",
    )
    params: dict[str, Any] = {
        'vk': VERSION_KEYWORD_REGEX,
        'ratio': ALBUM_WIDE_RATIO,
    }
    if song_id:
        params['song_id'] = song_id
    if limit is not None:
        params['limit'] = limit

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        {
            'song_id': str(row['song_id']),
            'release_id': str(row['release_id']),
            'has_album_link': bool(row['has_album_link']),
        }
        for row in rows
    ]


def plan_from_pairs(pairs: list[dict[str, Any]]) -> dict[str, dict[str, list[str]]]:
    """Group candidate pairs into the two matcher modes.

    Returns:
        {
          'tracks_only': {song_id: [release_id, ...]},  # has album link
          'full_search': {song_id: [release_id, ...]},  # no album link
        }

    Grouping by song matters: the matcher's entry point is per-song with
    a release filter, so one call can cover every candidate release of
    that song in that mode.
    """
    plan: dict[str, dict[str, list[str]]] = {'tracks_only': {}, 'full_search': {}}
    for pair in pairs:
        bucket = 'tracks_only' if pair['has_album_link'] else 'full_search'
        plan[bucket].setdefault(pair['song_id'], []).append(pair['release_id'])
    return plan


def purge_failure_cache(pairs: list[dict[str, Any]], *,
                        dry_run: bool = False,
                        logger_: Optional[logging.Logger] = None,
                        cache_dir: Optional[Any] = None) -> int:
    """Delete the cached "no track match" entries for these pairs.

    `is_track_match_cached_failure` short-circuits the album-search path
    for any (song, release, album) it has already given up on, and those
    entries are exactly the ones this fix invalidates. They're keyed
    `fail_<song>_<release>_<album>.json` under `<cache>/track_failures/`,
    so the album id doesn't need to be known in advance — glob it.

    Returns the number of files removed (or that would be, under dry_run).
    """
    log = logger_ or logger
    if cache_dir is None:
        from core.cache_utils import get_cache_dir
        cache_dir = get_cache_dir('spotify')
    failure_dir = cache_dir / 'track_failures'
    if not failure_dir.is_dir():
        return 0

    removed = 0
    for p in pairs:
        pattern = f"fail_{p['song_id']}_{p['release_id']}_*.json"
        for path in failure_dir.glob(pattern):
            if dry_run:
                log.debug("would purge failure cache %s", path.name)
                removed += 1
                continue
            try:
                path.unlink()
                removed += 1
            except OSError as e:
                log.warning("could not purge %s: %s", path.name, e)

    if removed:
        log.info("purged %d cached track-match failure(s)%s",
                 removed, ' (dry run)' if dry_run else '')
    return removed


def _empty_stats() -> dict[str, int]:
    return {
        'pairs': 0,
        'songs': 0,
        'tracks_only_pairs': 0,
        'full_search_pairs': 0,
        # One "run" is one (song, mode) matcher call. A song with
        # candidates in both modes contributes two runs, so these never
        # read as a fraction of `songs`.
        'song_runs_ok': 0,
        'song_runs_failed': 0,
        'releases_processed': 0,
        'releases_with_spotify': 0,
        'tracks_matched': 0,
        'releases_cleared': 0,
        'failure_cache_purged': 0,
        'api_calls': 0,
        'cache_hits': 0,
    }


# Per-mode matcher kwargs. See the SAFETY note in the module docstring for
# why neither mode sets rematch_all.
_MODE_KWARGS: dict[str, dict[str, Any]] = {
    # Known album: fill only the missing track links. No album re-search,
    # so the release's existing Spotify data is never cleared.
    'tracks_only': {'rematch_tracks': True},
    # No album link: the matcher must search for it. The cached failure
    # that would short-circuit this path is purged up front instead of
    # being bypassed with force_refresh.
    'full_search': {'rematch': True},
}


def run_backfill(
    pairs: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    album_context: Optional[str] = None,
    force_refresh: bool = False,
    logger_: Optional[logging.Logger] = None,
    matcher_factory: Callable[..., SpotifyMatcher] = SpotifyMatcher,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
) -> dict[str, int]:
    """Re-run the matcher over the candidate pairs, one song per call.

    Args:
        pairs: output of find_candidate_pairs.
        dry_run: pass through to the matcher; no writes.
        album_context: None (default) or 'audit' / 'rescue'. Left off by
            default so this run's effects are attributable to the
            version-asymmetry fix rather than the duration rescue.
        force_refresh: bypass every Spotify cache, not just the purged
            negative entries. Off by default — the purge is enough, and
            this multiplies API calls across songs sharing a release.
        logger_: logger handed to the matcher.
        matcher_factory: injection point for tests.
        progress_callback: called as (mode, done, total) after each song.

    Returns:
        Aggregated stats; see _empty_stats for the keys.
    """
    log = logger_ or logger
    stats = _empty_stats()
    if not pairs:
        return stats

    plan = plan_from_pairs(pairs)
    stats['pairs'] = len(pairs)
    stats['songs'] = len(set(p['song_id'] for p in pairs))
    stats['tracks_only_pairs'] = sum(len(v) for v in plan['tracks_only'].values())
    stats['full_search_pairs'] = sum(len(v) for v in plan['full_search'].values())

    # Only the full_search path consults the failure cache, so purge just
    # those pairs' entries.
    full_search_pairs = [p for p in pairs if not p['has_album_link']]
    if full_search_pairs:
        stats['failure_cache_purged'] = purge_failure_cache(
            full_search_pairs, dry_run=dry_run, logger_=log)

    # A dry run leaves the cache files in place, so without help the
    # matcher would re-reach the very cached failures this backfill
    # invalidates and preview nothing. force_refresh gets the album-search
    # path past them (is_track_match_cached_failure returns False when the
    # client has it set), which costs API calls but makes the preview
    # truthful — the whole point of running dry first.
    mode_force_refresh = {
        'tracks_only': force_refresh,
        'full_search': force_refresh or dry_run,
    }

    for mode in ('tracks_only', 'full_search'):
        songs = plan[mode]
        if not songs:
            continue
        log.info("%s: %d song(s), %d release(s)",
                 mode, len(songs), sum(len(v) for v in songs.values()))

        for done, (song_id, release_ids) in enumerate(sorted(songs.items()), 1):
            matcher = matcher_factory(
                dry_run=dry_run,
                album_context=album_context,
                force_refresh=mode_force_refresh[mode],
                logger=log,
                **_MODE_KWARGS[mode],
            )
            try:
                result = matcher.match_releases(song_id, release_ids=release_ids)
            except Exception:
                log.exception("song %s (%s) raised — continuing", song_id, mode)
                stats['song_runs_failed'] += 1
                continue

            if not result.get('success'):
                log.warning("song %s (%s) failed: %s", song_id, mode,
                            result.get('error') or 'unknown error')
                stats['song_runs_failed'] += 1
            else:
                stats['song_runs_ok'] += 1

            # Roll up whatever the matcher reported, success or not — a
            # partial run still did real work worth counting.
            mstats = result.get('stats') or {}
            for key in ('releases_processed', 'releases_with_spotify',
                        'tracks_matched', 'releases_cleared',
                        'api_calls', 'cache_hits'):
                stats[key] += mstats.get(key, 0) or 0

            if progress_callback:
                progress_callback(mode, done, len(songs))

    log.info(
        "spotify_live_suffix_backfill: songs=%d runs_ok=%d runs_failed=%d "
        "tracks_matched=%d releases_with_spotify=%d cleared=%d api_calls=%d",
        stats['songs'], stats['song_runs_ok'], stats['song_runs_failed'],
        stats['tracks_matched'], stats['releases_with_spotify'],
        stats['releases_cleared'], stats['api_calls'],
    )
    return stats
