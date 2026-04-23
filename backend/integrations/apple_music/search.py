"""
Apple Music search strategies.

Top-level orchestration for finding an Apple Music album that matches a release.
Tries the local Apple Music Feed catalog first (no rate limits), falls back to
the iTunes Search API. Each source runs a ladder of progressively relaxed
search strategies (strip ensemble suffix, strip live suffix, primary artist
only, album-only, punctuation-stripped, main title only).

Validation still lives on the matcher (`matcher._validate_album_match`) — that
moves out in the next step.
"""

import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Dict, List, Optional

from integrations.spotify.matching import (
    extract_primary_artist,
    strip_ensemble_suffix,
    strip_live_suffix,
)


def search_and_validate_album(
    matcher,
    artist_name: str,
    album_title: str,
    release_year: Optional[int] = None,
) -> Optional[Dict]:
    """
    Search Apple Music for an album and validate the match.

    Local catalog first when available; iTunes API as fallback. Returns the
    matched album dict (with `_match_confidence` and `_source` set) or None.
    """
    if matcher.catalog:
        result = search_local_catalog(matcher, artist_name, album_title, release_year)
        if result:
            matcher.stats['local_catalog_hits'] += 1
            return result

    if matcher.local_catalog_only:
        matcher.logger.debug("    Skipping API fallback (local catalog only mode)")
        return None

    return search_api(matcher, artist_name, album_title, release_year)


def search_local_catalog(
    matcher,
    artist_name: str,
    album_title: str,
    release_year: Optional[int] = None,
) -> Optional[Dict]:
    """Search the local Apple Music catalog with a strategy ladder."""
    if not matcher.catalog:
        return None

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
                if album:
                    is_valid, confidence = matcher._validate_album_match(
                        album, artist_name, album_title, release_year
                    )
                    if is_valid:
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
) -> Optional[Dict]:
    """Search the iTunes API with a strategy ladder (fallback when catalog misses)."""
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
            is_valid, confidence = matcher._validate_album_match(
                album, artist_name, album_title, release_year
            )

            if is_valid:
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
