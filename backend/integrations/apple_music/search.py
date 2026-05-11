"""
Apple Music search strategies.

Top-level orchestration for finding an Apple Music album that matches a release.
Tries the local Apple Music Feed catalog first (no rate limits), falls back to
the iTunes Search API. Each source runs a ladder of progressively relaxed
search strategies (strip ensemble suffix, strip live suffix, primary artist
only, album-only, punctuation-stripped, main title only). Each candidate is
scored against the expected artist/album via matching.validate_album_match.
"""

import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Callable, Dict, List, Optional

from integrations.apple_music.matching import (
    assess_apple_album_tracklist,
    validate_album_match,
)
from integrations.spotify.matching import (
    extract_primary_artist,
    fetch_mb_tracks_for_release,
    strip_ensemble_suffix,
    strip_live_suffix,
)


def search_and_validate_album(
    matcher,
    artist_name: str,
    album_title: str,
    release_year: Optional[int] = None,
    release_id: Optional[str] = None,
    conn_factory: Optional[Callable] = None,
) -> Optional[Dict]:
    """
    Search Apple Music for an album and validate the match.

    Local catalog first when available; iTunes API as fallback. Returns the
    matched album dict (with `_match_confidence` and `_source` set) or None.

    When `release_id` is supplied, candidates that pass the title+artist
    check are additionally gated on a tracklist-coverage comparison against
    our MusicBrainz tracklist — same pattern Spotify uses (issue #184).
    Without `release_id` the gate is disabled and behavior matches the
    pre-gate code path (lenient, title+artist only).

    Args:
        matcher: AppleMusicMatcher instance.
        artist_name: Expected artist for the release we're matching.
        album_title: Expected album title.
        release_year: Optional release year for the year-bonus score.
        release_id: Internal `releases.id` of the release we're matching
            against. Required to enable the tracklist gate.
        conn_factory: Zero-arg callable returning a context-manager DB
            connection. Defaults to db_utils.get_db_connection. Allows
            the caller to thread an existing connection through if it
            wants to avoid hitting the pool again.
    """
    if matcher.catalog:
        result = search_local_catalog(
            matcher, artist_name, album_title, release_year,
            release_id=release_id, conn_factory=conn_factory,
        )
        if result:
            matcher.stats['local_catalog_hits'] += 1
            return result

    if matcher.local_catalog_only:
        matcher.logger.debug("    Skipping API fallback (local catalog only mode)")
        return None

    return search_api(
        matcher, artist_name, album_title, release_year,
        release_id=release_id, conn_factory=conn_factory,
    )


def _build_mb_tracks_getter(
    release_id: Optional[str],
    conn_factory: Optional[Callable],
    logger,
):
    """Return a zero-arg callable that returns our MB tracklist (cached).

    The MB lookup hits Postgres + the MusicBrainz API; we only want to
    pay that cost once per search call, no matter how many candidates the
    gate runs against. Returns [] (lenient) when release_id is missing.
    """
    cache: Dict[str, list] = {}

    def _get() -> list:
        if release_id is None:
            return []
        if 'tracks' not in cache:
            from db_utils import get_db_connection as _default
            _factory = conn_factory or _default
            try:
                with _factory() as _conn:
                    cache['tracks'] = fetch_mb_tracks_for_release(_conn, release_id)
            except Exception as e:
                logger.debug(f"    Tracklist gate: MB fetch failed: {e}")
                cache['tracks'] = []
        return cache['tracks']

    return _get


def _apple_tracks_for_candidate(matcher, album_id: str, source: str) -> list:
    """Fetch the candidate album's tracklist from whichever side produced it.

    Local-catalog candidates use catalog.get_songs_for_album (no API cost);
    API candidates use client.lookup_album_tracks (cached on disk). Returns
    [] on any error — the gate treats that as "no signal" and passes.
    """
    try:
        if source == 'local_catalog' and matcher.catalog:
            return matcher.catalog.get_songs_for_album(album_id) or []
        return matcher.client.lookup_album_tracks(album_id) or []
    except Exception as e:
        matcher.logger.debug(f"    Tracklist gate: track fetch failed: {e}")
        return []


def _tracklist_gate_passes(
    matcher,
    album_id: str,
    source: str,
    get_mb_tracks: Callable[[], list],
) -> bool:
    """Run the tracklist-coverage gate. Logs the verdict either way."""
    apple_tracks = _apple_tracks_for_candidate(matcher, album_id, source)
    mb_tracks = get_mb_tracks()
    accepted, reason, _info = assess_apple_album_tracklist(mb_tracks, apple_tracks)
    if accepted:
        matcher.logger.debug(f"    ✓ Tracklist gate passed: {reason}")
    else:
        matcher.logger.debug(f"    ✗ Tracklist gate rejected: {reason}")
    return accepted


def search_local_catalog(
    matcher,
    artist_name: str,
    album_title: str,
    release_year: Optional[int] = None,
    release_id: Optional[str] = None,
    conn_factory: Optional[Callable] = None,
) -> Optional[Dict]:
    """Search the local Apple Music catalog with a strategy ladder."""
    if not matcher.catalog:
        return None

    get_mb_tracks = _build_mb_tracks_getter(release_id, conn_factory, matcher.logger)

    search_strategies = []

    # Strategy 1: Full artist + album
    search_strategies.append((artist_name, album_title))

    # Strategy 2: Strip ensemble suffix
    stripped_artist = strip_ensemble_suffix(artist_name)
    if stripped_artist != artist_name:
        search_strategies.append((stripped_artist, album_title))

    # Strategy 3: Strip live suffix from album
    stripped_album = strip_live_suffix(album_title)
    if stripped_album != album_title:
        search_strategies.append((artist_name, stripped_album))

    # Strategy 4: Extract primary artist from collaborations
    primary_artist = extract_primary_artist(artist_name)
    if primary_artist != artist_name:
        search_strategies.append((primary_artist, album_title))

    # Strategy 5: Album only (fallback for name variants like David/Dave)
    search_strategies.append((None, album_title))

    # Strategy 6: Album with punctuation stripped
    punct_stripped_album = re.sub(r'[:\-\(\)\[\]]', ' ', album_title)
    punct_stripped_album = ' '.join(punct_stripped_album.split())
    if punct_stripped_album != album_title:
        search_strategies.append((None, punct_stripped_album))

    # Strategy 7: Main title only (before colon, dash, or parenthesis)
    main_title_match = re.match(r'^([^:\-\(\[]+)', album_title)
    if main_title_match:
        main_title = main_title_match.group(1).strip()
        if main_title and len(main_title) >= 5 and main_title != album_title:
            search_strategies.append((None, main_title))

    for search_artist, search_album in search_strategies:
        try:
            albums = _search_with_timeout(
                matcher.catalog, search_artist, search_album, timeout=30
            )

            if not albums:
                continue

            for album_data in albums:
                album = _convert_catalog_album(album_data, matcher.logger)
                if not album:
                    continue
                is_valid, confidence = validate_album_match(
                    matcher, album, artist_name, album_title, release_year
                )
                if not is_valid:
                    continue
                # Tracklist gate: prevent title-stem false positives
                # (e.g. "Recuerdos" matching "Recuerdos de Ti" with a
                # completely different track listing). Disabled when no
                # release_id was passed — preserves the old behaviour
                # for callers that don't yet thread one through.
                if release_id and not _tracklist_gate_passes(
                    matcher, album['id'], 'local_catalog', get_mb_tracks,
                ):
                    continue
                album['_match_confidence'] = confidence
                album['_source'] = 'local_catalog'
                return album

        except FuturesTimeoutError:
            matcher.logger.warning(
                f"    Catalog search timed out for: {search_artist} - {search_album}"
            )
            if matcher.catalog:
                matcher.catalog._refresh_conn()
            continue
        except Exception as e:
            matcher.logger.debug(f"Local catalog search error: {e}")
            continue

    return None


def search_api(
    matcher,
    artist_name: str,
    album_title: str,
    release_year: Optional[int] = None,
    release_id: Optional[str] = None,
    conn_factory: Optional[Callable] = None,
) -> Optional[Dict]:
    """Search the iTunes API with a strategy ladder (fallback when catalog misses)."""
    get_mb_tracks = _build_mb_tracks_getter(release_id, conn_factory, matcher.logger)

    search_strategies = []

    # Strategy 1: Full artist + album
    search_strategies.append((artist_name, album_title))

    # Strategy 2: Strip ensemble suffix (e.g., "Bill Evans Trio" -> "Bill Evans")
    stripped_artist = strip_ensemble_suffix(artist_name)
    if stripped_artist != artist_name:
        search_strategies.append((stripped_artist, album_title))

    # Strategy 3: Strip live suffix from album
    stripped_album = strip_live_suffix(album_title)
    if stripped_album != album_title:
        search_strategies.append((artist_name, stripped_album))

    # Strategy 4: Album only (fallback for Various Artists, etc.)
    search_strategies.append((None, album_title))

    for search_artist, search_album in search_strategies:
        albums = matcher.client.search_albums(
            artist_name=search_artist or '',
            album_title=search_album,
            limit=10,
        )

        if not albums:
            continue

        for album in albums:
            is_valid, confidence = validate_album_match(
                matcher, album, artist_name, album_title, release_year
            )
            if not is_valid:
                continue
            if release_id and not _tracklist_gate_passes(
                matcher, album['id'], 'itunes_api', get_mb_tracks,
            ):
                continue
            album['_match_confidence'] = confidence
            album['_source'] = 'itunes_api'
            return album

    return None


def _search_with_timeout(
    catalog,
    artist_name: Optional[str],
    album_title: str,
    timeout: int = 30,
) -> List[Dict]:
    """
    Run a local-catalog album search under a wall-clock timeout.

    The catalog's SQLite-backed search can occasionally hang on pathological
    inputs; the ThreadPoolExecutor guard bounds the wait so a single bad row
    can't stall a whole batch. Raises FuturesTimeoutError on timeout.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            catalog.search_albums,
            artist_name=artist_name,
            album_title=album_title,
            limit=50,
        )
        return future.result(timeout=timeout)


def _convert_catalog_album(catalog_data: Dict, logger) -> Optional[Dict]:
    """Normalize a local-catalog row into the shape the matcher/iTunes-API code expects."""
    try:
        album = {
            'id': str(catalog_data.get('id', '')),
            'name': catalog_data.get('name', ''),
            'artist': catalog_data.get('artistName', ''),
            'release_date': catalog_data.get('releaseDate', ''),
            'track_count': catalog_data.get('trackCount', 0),
        }

        artwork_url = catalog_data.get('artworkUrl')
        if artwork_url:
            album['artwork'] = {
                'small': artwork_url.replace('{w}x{h}', '100x100'),
                'medium': artwork_url.replace('{w}x{h}', '300x300'),
                'large': artwork_url.replace('{w}x{h}', '600x600'),
            }

        return album if album['id'] and album['name'] else None

    except Exception as e:
        logger.debug(f"Failed to convert catalog album: {e}")
        return None
