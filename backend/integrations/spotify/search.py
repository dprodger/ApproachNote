"""
Spotify Search

Progressive search strategies against the Spotify /v1/search endpoint, with
validation of each candidate via matching.py's validators. These live here
instead of on SpotifyClient because search is more than an HTTP call — each
response is scored and filtered before being returned.

The functions take the SpotifyMatcher instance as their first argument so
they can reach its client (cache, auth, rate-limited HTTP), its logger, and
its similarity thresholds (`min_*_similarity`). Validation and track-verify
are free functions in matching.py — search passes the thresholds + a
`verify_album_contains_track` callback through explicitly. A later refactor
(#115 step 6) can shrink this further by passing (client, logger,
thresholds) instead of the whole matcher.

API-call stats are incremented on `matcher.client.stats['api_calls']`, not
on the matcher — the client owns that counter and `_aggregate_client_stats`
pulls it forward into matcher.stats before returning to callers.
"""

import logging
from typing import Optional

import requests

from integrations.spotify.client import SpotifyRateLimitError, _CACHE_MISS
from integrations.spotify.matching import (
    AlbumMatchAssessment,
    assess_album_match,
    fetch_mb_tracks_for_release,
    strip_ensemble_suffix,
    strip_live_suffix,
    strip_mb_year_disambiguator,
    normalize_for_search,
    validate_track_match,
    validate_album_match,
    verify_album_contains_track,
)

logger = logging.getLogger(__name__)


def search_spotify_track(matcher, song_title: str, album_title: str,
                         artist_name: str = None, year: int = None) -> Optional[dict]:
    """
    Search Spotify for a track with fuzzy validation and progressive search strategy.
    Uses caching to minimize API calls.

    Args:
        matcher: SpotifyMatcher instance providing client, logger, and the
            min_*_similarity thresholds used for candidate validation.
        song_title: Song title to search for
        album_title: Album title
        artist_name: Artist name (optional, but recommended)
        year: Recording year (optional)

    Returns:
        dict with 'url', 'id', 'artists', 'album', 'album_art', 'similarity_scores'
        or None if no valid match found
    """
    client = matcher.client
    log = matcher.logger

    # Check cache first
    cache_path = client._get_search_cache_path(song_title, album_title, artist_name, year)
    cached_result = client._load_from_cache(cache_path)

    if cached_result is not _CACHE_MISS:
        # Cache hit - return cached result (which might be None for "no match found")
        return cached_result

    # Not in cache - perform search
    token = client.get_spotify_auth_token()
    if not token:
        client._save_to_cache(cache_path, None)
        return None

    # Progressive search strategy
    # Start with specific queries, fall back to broader searches
    search_strategies = []

    # Normalize search terms (convert en-dashes to hyphens, etc.)
    search_song = normalize_for_search(song_title)
    search_album = normalize_for_search(album_title)
    search_artist = normalize_for_search(artist_name) if artist_name else None

    # Check if we should try a stripped artist name as fallback
    stripped_artist = strip_ensemble_suffix(search_artist) if search_artist else None
    has_stripped_fallback = stripped_artist and stripped_artist != search_artist

    if search_artist and year:
        search_strategies.append({
            'query': f'track:"{search_song}" artist:"{search_artist}" album:"{search_album}" year:{year}',
            'description': 'exact track, artist, album, and year'
        })

    if search_artist:
        search_strategies.append({
            'query': f'track:"{search_song}" artist:"{search_artist}" album:"{search_album}"',
            'description': 'exact track, artist, and album'
        })
        search_strategies.append({
            'query': f'track:"{search_song}" artist:"{search_artist}"',
            'description': 'exact track and artist'
        })

    # Fallback: try with ensemble suffix stripped (e.g., "Bill Evans Trio" -> "Bill Evans")
    if has_stripped_fallback:
        search_strategies.append({
            'query': f'track:"{search_song}" artist:"{stripped_artist}" album:"{search_album}"',
            'description': f'exact track, stripped artist ({stripped_artist}), and album'
        })
        search_strategies.append({
            'query': f'track:"{search_song}" artist:"{stripped_artist}"',
            'description': f'exact track and stripped artist ({stripped_artist})'
        })

    search_strategies.append({
        'query': f'track:"{search_song}" album:"{search_album}"',
        'description': 'exact track and album'
    })

    search_strategies.append({
        'query': f'track:"{search_song}"',
        'description': 'exact track only'
    })

    # Try each search strategy until we get a valid match
    for strategy in search_strategies:
        try:
            log.debug(f"  → Trying: {strategy['description']}")

            response = client._make_api_request(
                'get',
                'https://api.spotify.com/v1/search',
                headers={'Authorization': f'Bearer {token}'},
                params={
                    'q': strategy['query'],
                    'type': 'track',
                    'limit': 5  # Get top 5 results for validation
                },
                timeout=10
            )

            response.raise_for_status()
            data = response.json()

            # Track API call
            client.stats['api_calls'] = client.stats.get('api_calls', 0) + 1
            client.last_made_api_call = True

            tracks = data.get('tracks', {}).get('items', [])

            if tracks:
                log.debug(f"    Found {len(tracks)} candidates")

                # Try to validate each candidate
                for i, track in enumerate(tracks):
                    is_valid, reason, scores = validate_track_match(
                        track, song_title, artist_name or '', album_title,
                        matcher.min_track_similarity,
                        matcher.min_artist_similarity,
                        matcher.min_album_similarity,
                    )

                    if is_valid:
                        # Extract album artwork URLs
                        album_art = {}
                        images = track['album'].get('images', [])

                        for image in images:
                            height = image.get('height', 0)
                            if height >= 600:
                                album_art['large'] = image['url']
                            elif height >= 300:
                                album_art['medium'] = image['url']
                            elif height >= 64:
                                album_art['small'] = image['url']

                        # Build result
                        track_artists = [a['name'] for a in track['artists']]
                        track_album = track['album']['name']

                        result = {
                            'url': track['external_urls']['spotify'],
                            'id': track['id'],
                            'artists': track_artists,
                            'album': track_album,
                            'album_art': album_art,
                            'similarity_scores': scores
                        }

                        # Cache successful result
                        client._save_to_cache(cache_path, result)

                        log.debug(f"    ✓ Valid match found (candidate #{i+1})")
                        return result
                    else:
                        log.debug(f"    ✗ Candidate #{i+1} rejected: {reason}")
                        log.debug(f"       Expected: '{song_title}' by {artist_name} on '{album_title}'")
                        log.debug(f"       Found: '{scores['spotify_song']}' by {scores['spotify_artist']} on '{scores['spotify_album']}'")
                        if scores.get('artist_best_individual'):
                            log.debug(f"       Artist match scores - Individual: {scores['artist_best_individual']}%, Full string: {scores['artist_full_string']}%")
                        if scores['album']:
                            log.debug(f"       Album similarity: {scores['album']}%")

                log.debug(f"    ✗ No valid matches with {strategy['description']}")
            else:
                log.debug(f"    ✗ No results with {strategy['description']}")

        except SpotifyRateLimitError as e:
            log.error(f"Rate limit exceeded during search: {e}")
            # Don't cache rate limit errors - might succeed later
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                client.access_token = None
                log.warning("Spotify token expired, will refresh on next request")
                # Don't cache auth failures
                return None
            log.error(f"Spotify search failed: {e}")
            # Don't cache errors
            return None
        except Exception as e:
            log.error(f"Error searching Spotify: {e}")
            # Don't cache errors
            return None

    log.debug(f"    ✗ No valid Spotify matches found after trying all strategies")

    # Cache the "no match" result
    client._save_to_cache(cache_path, None)

    return None


def search_spotify_album(matcher, album_title: str, artist_name: str = None,
                         song_title: str = None,
                         release_id: str = None,
                         conn_factory=None) -> Optional[dict]:
    """
    Search Spotify for an album with fuzzy validation.

    Args:
        matcher: SpotifyMatcher instance providing client, logger, and the
            min_*_similarity thresholds. A verify_album_contains_track
            callback is built here that binds those dependencies, so
            validate_album_match can fall back on track presence when the
            artist score is low.
        album_title: Album title to search for
        artist_name: Artist name (optional, but recommended)
        song_title: Song title for track verification fallback (optional).
                   When provided, albums with high similarity but low artist
                   match can still be accepted if they contain this track.
        release_id: Internal `releases.id` for the MB release we're matching
                   against. When provided alongside `conn_factory`, the
                   validator gates weak-artist candidates on a full-tracklist
                   comparison between MB and Spotify (issue #184). Omitting
                   either disables the gate and preserves the prior
                   substring/track-presence behavior.
        conn_factory: Zero-arg callable that returns a context-manager DB
                   connection. Lets us look up `musicbrainz_release_id` and
                   delay opening the connection until the gate actually
                   fires. Defaults to `db_utils.get_db_connection`.

    Returns:
        dict with 'url', 'id', 'artists', 'name', 'album_art', 'similarity_scores'
        or None if no valid match found
    """
    client = matcher.client
    log = matcher.logger

    # Bound callback for validate_album_match's track-presence fallback —
    # captures the matcher's client, logger, and min_track_similarity so the
    # validator itself stays free of matcher references.
    def _verify_track(album_id: str, st: str) -> bool:
        return verify_album_contains_track(
            client, log, matcher.min_track_similarity, album_id, st
        )

    # Lazy MB tracklist fetch for the unified album-identity assessment
    # (assess_album_match). Cached on first call so candidate fanout
    # multiplies neither the DB hit nor the MB API call.
    _mb_cache: dict = {}  # 'tracks' set on first lookup

    def _get_mb_tracks() -> list:
        if release_id is None:
            return []
        if 'tracks' not in _mb_cache:
            from db_utils import get_db_connection as _default_conn_factory
            _conn_factory = conn_factory or _default_conn_factory
            try:
                with _conn_factory() as _conn:
                    _mb_cache['tracks'] = fetch_mb_tracks_for_release(
                        _conn, release_id)
            except Exception as e:
                log.debug(f"      assess: MB fetch failed: {e}")
                _mb_cache['tracks'] = []
        return _mb_cache['tracks']

    def _full_assess(album: dict) -> AlbumMatchAssessment:
        """Run the unified scorer against `album`, fetching its Spotify
        tracklist (cached on the client side). Returns an assessment that
        the caller decides whether to accept."""
        sp_tracks = client.get_album_tracks(album['id']) or []
        return assess_album_match(
            mb_album_title=album_title,
            mb_artist_credit=artist_name or '',
            spotify_album_name=album['name'],
            spotify_artists=album.get('artists', []),
            mb_tracks=_get_mb_tracks(),
            spotify_tracks=sp_tracks,
            min_album_similarity=matcher.min_album_similarity,
            min_artist_similarity=matcher.min_artist_similarity,
        )

    # Check cache first (reuse search cache with 'album' prefix)
    cache_path = client._get_search_cache_path('album', album_title, artist_name)
    cached_result = client._load_from_cache(cache_path)

    if cached_result is not _CACHE_MISS:
        return cached_result

    token = client.get_spotify_auth_token()
    if not token:
        client._save_to_cache(cache_path, None)
        return None

    # Progressive search strategy
    search_strategies = []

    # Normalize album title for search (convert en-dashes to hyphens, etc.)
    search_album = normalize_for_search(album_title)
    search_artist = normalize_for_search(artist_name) if artist_name else None

    # Truncate very long artist names to avoid Spotify API 400 errors
    # (Some releases have absurdly long artist credits with full orchestra rosters)
    MAX_ARTIST_LENGTH = 100
    if search_artist and len(search_artist) > MAX_ARTIST_LENGTH:
        # Try to truncate at a natural break point (comma, hyphen, etc.)
        truncated = search_artist[:MAX_ARTIST_LENGTH]
        for sep in [', ', ' - ', ' & ', ' and ']:
            if sep in truncated:
                truncated = truncated.rsplit(sep, 1)[0]
                break
        log.debug(f"  Truncated long artist name: '{search_artist[:50]}...' -> '{truncated}'")
        search_artist = truncated

    # Check if album title has a live suffix we can strip (e.g., "Solo: Live" -> "Solo")
    stripped_album = strip_live_suffix(search_album)
    has_stripped_album = stripped_album != search_album

    # Check if album title has a MusicBrainz-style ~ YYYY ~ disambiguator we
    # can strip — common on compilation reissues like
    # "It's Up to You ~ 1946 ~ Volume 2". Validation still scores against
    # the full original title, so a candidate has to clear the same
    # thresholds; this just gives Spotify a query string it can actually
    # match exactly.
    mb_stripped_album = strip_mb_year_disambiguator(search_album)
    has_mb_stripped_album = mb_stripped_album != search_album

    if search_artist:
        search_strategies.append({
            'query': f'album:"{search_album}" artist:"{search_artist}"',
            'description': 'exact album and artist'
        })
        search_strategies.append({
            'query': f'"{search_album}" "{search_artist}"',
            'description': 'quoted album and artist'
        })

        # Try with ensemble suffix stripped (e.g., "Bill Evans Trio" -> "Bill Evans")
        stripped_artist = strip_ensemble_suffix(search_artist)
        if stripped_artist != search_artist:
            search_strategies.append({
                'query': f'album:"{search_album}" artist:"{stripped_artist}"',
                'description': f'exact album with stripped artist ({stripped_artist})'
            })
            search_strategies.append({
                'query': f'"{search_album}" "{stripped_artist}"',
                'description': f'quoted album with stripped artist ({stripped_artist})'
            })

        # Try with live suffix stripped from album (e.g., "Solo: Live" -> "Solo")
        if has_stripped_album:
            search_strategies.append({
                'query': f'album:"{stripped_album}" artist:"{search_artist}"',
                'description': f'stripped album ({stripped_album}) and artist'
            })
            search_strategies.append({
                'query': f'"{stripped_album}" "{search_artist}"',
                'description': f'quoted stripped album ({stripped_album}) and artist'
            })

        # Try with MB-style ~ YYYY ~ disambiguator stripped (e.g.,
        # "It's Up to You ~ 1946 ~ Volume 2" -> "It's Up to You").
        if has_mb_stripped_album:
            search_strategies.append({
                'query': f'album:"{mb_stripped_album}" artist:"{search_artist}"',
                'description': f'MB-stripped album ({mb_stripped_album}) and artist'
            })
            search_strategies.append({
                'query': f'"{mb_stripped_album}" "{search_artist}"',
                'description': f'quoted MB-stripped album ({mb_stripped_album}) and artist'
            })

    search_strategies.append({
        'query': f'album:"{search_album}"',
        'description': 'exact album only'
    })

    # Fallback: stripped album only
    if has_stripped_album:
        search_strategies.append({
            'query': f'album:"{stripped_album}"',
            'description': f'stripped album only ({stripped_album})'
        })

    # Fallback: MB-stripped album only
    if has_mb_stripped_album:
        search_strategies.append({
            'query': f'album:"{mb_stripped_album}"',
            'description': f'MB-stripped album only ({mb_stripped_album})'
        })

    for strategy in search_strategies:
        try:
            log.debug(f"  → Trying: {strategy['description']}")

            response = client._make_api_request(
                'get',
                'https://api.spotify.com/v1/search',
                headers={'Authorization': f'Bearer {token}'},
                params={
                    'q': strategy['query'],
                    'type': 'album',
                    'limit': 10
                },
                timeout=10
            )

            response.raise_for_status()
            data = response.json()

            client.stats['api_calls'] = client.stats.get('api_calls', 0) + 1
            client.last_made_api_call = True

            albums = data.get('albums', {}).get('items', [])

            if albums:
                log.debug(f"    Found {len(albums)} candidates")

                # Normalize expected album title for exact matching
                expected_normalized = album_title.lower().strip()

                # Two passes for accept/reject. validate_album_match is the
                # title+artist gate (preserves long-tail edge cases like
                # the "Spotify prepended artist name" rule and the track-
                # presence fallback for compilation artists). After it
                # passes, assess_album_match is the unified album-identity
                # scorer that adds tracklist coverage + ordering — same
                # function the diagnose route surfaces, so verdicts agree.
                # Both must accept; borderline assess verdicts skip per
                # the policy decided in issue #184.

                def _build_result(album, assessment, scores):
                    album_art = {}
                    for image in album.get('images', []) or []:
                        h = image.get('height', 0)
                        if h >= 600:
                            album_art['large'] = image['url']
                        elif h >= 300:
                            album_art['medium'] = image['url']
                        elif h >= 64:
                            album_art['small'] = image['url']
                    return {
                        'url': album['external_urls']['spotify'],
                        'id': album['id'],
                        'artists': [a['name'] for a in album['artists']],
                        'name': album['name'],
                        'album_art': album_art,
                        'similarity_scores': scores,
                        'assessment': assessment.as_scores_dict() if assessment else None,
                    }

                expected_normalized = album_title.lower().strip()

                # FIRST PASS: exact album-title matches.
                exact_matches = []
                for i, album in enumerate(albums):
                    if album['name'].lower().strip() != expected_normalized:
                        continue
                    is_valid, reason, scores = validate_album_match(
                        album, album_title, artist_name or '',
                        matcher.min_album_similarity,
                        matcher.min_artist_similarity,
                        song_title=song_title,
                        verify_track_callback=_verify_track,
                    )
                    exact_matches.append({
                        'index': i, 'album': album,
                        'is_valid': is_valid, 'reason': reason, 'scores': scores,
                    })

                if exact_matches:
                    log.debug(f"    Found {len(exact_matches)} exact title match(es)")
                    for em in exact_matches:
                        if not em['is_valid']:
                            continue
                        assessment = _full_assess(em['album'])
                        log.debug(
                            f"    Exact #{em['index']+1} '{em['album']['name']}' "
                            f"→ {assessment.verdict}: {assessment.reason}")
                        if assessment.verdict == 'accept':
                            log.debug(f"    ✓ Exact match found: '{em['album']['name']}' (#{em['index']+1})")
                            scores = assessment.as_scores_dict()
                            result = _build_result(em['album'], assessment, scores)
                            client._save_to_cache(cache_path, result)
                            return result
                    log.debug(f"    Exact matches did not pass full assessment, trying fuzzy matching...")

                # SECOND PASS: fuzzy matching across all candidates.
                candidate_results = []
                for i, album in enumerate(albums):
                    is_valid, reason, scores = validate_album_match(
                        album, album_title, artist_name or '',
                        matcher.min_album_similarity,
                        matcher.min_artist_similarity,
                        song_title=song_title,
                        verify_track_callback=_verify_track,
                    )
                    candidate_results.append({
                        'index': i, 'album': album,
                        'is_valid': is_valid, 'reason': reason, 'scores': scores,
                        'assessment': None,
                    })

                # Run the unified assessment for every title+artist pass —
                # the result drives the verdict and is logged so diagnose
                # users see why each candidate was kept or dropped.
                for cr in candidate_results:
                    if cr['is_valid']:
                        cr['assessment'] = _full_assess(cr['album'])

                log.debug(f"    --- Candidate Summary ---")
                for cr in candidate_results:
                    a = cr['assessment']
                    if not cr['is_valid']:
                        verdict = '✗ title/artist'
                    elif a is None:
                        verdict = '?'
                    else:
                        verdict = {
                            'accept': '✓',
                            'borderline': '~',
                            'reject': '✗',
                        }[a.verdict]
                    album_sim = cr['scores'].get('album', 0)
                    artist_sim = cr['scores'].get('artist', 0)
                    spotify_album = cr['scores'].get('spotify_album', '')
                    extra = ''
                    if a is not None:
                        cov = f"{a.coverage:.0%}" if a.coverage is not None else 'n/a'
                        order = f"{a.ordering:.0%}" if a.ordering is not None else 'n/a'
                        extra = f", cov: {cov}, ord: {order}"
                    log.debug(f"    {verdict} #{cr['index']+1}: '{spotify_album}' "
                              f"(album: {album_sim:.0f}%, artist: {artist_sim:.0f}%{extra})")
                log.debug(f"    -------------------------")

                # First candidate that the unified assessment accepts wins.
                # Borderline verdicts skip; the matcher does not write them
                # (per the issue #184 policy decision).
                for cr in candidate_results:
                    a = cr['assessment']
                    if a is None or a.verdict != 'accept':
                        if a is not None and a.verdict == 'borderline':
                            log.debug(
                                f"    ~ Skipping borderline #{cr['index']+1} "
                                f"'{cr['album']['name']}': {a.reason}")
                        continue
                    log.debug(
                        f"       Matched: '{a.spotify_album}' by {a.spotify_artist} — {a.reason}")
                    result = _build_result(cr['album'], a, a.as_scores_dict())
                    client._save_to_cache(cache_path, result)
                    log.debug(f"    ✓ Valid match found (candidate #{cr['index']+1})")
                    return result

                log.debug(f"    ✗ No valid matches with {strategy['description']}")
            else:
                log.debug(f"    ✗ No results with {strategy['description']}")

        except SpotifyRateLimitError as e:
            log.error(f"Rate limit exceeded during search: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                client.access_token = None
                return None
            log.error(f"Spotify search failed: {e}")
            return None
        except Exception as e:
            log.error(f"Error searching Spotify: {e}")
            return None

    log.debug(f"    ✗ No valid Spotify matches found after trying all strategies")
    client._save_to_cache(cache_path, None)
    return None
