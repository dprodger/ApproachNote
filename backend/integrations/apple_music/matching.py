"""
Apple Music fuzzy matching and scoring.

Match validation for Apple Music album / track candidates. The primitive
normalization + similarity functions live in integrations.spotify.matching
(service-agnostic); this module holds the Apple-Music-specific orchestration
that threads matcher thresholds through and applies compilation-artist and
name-variant rules.
"""

from typing import Any, Dict, List, Optional, Tuple

from integrations.spotify.matching import (
    calculate_similarity,
    compare_mb_to_spotify_tracks,
    is_compilation_artist,
    is_substring_title_match,
    normalize_for_comparison,
    normalize_name_variants,
)


# Tracklist-gate thresholds for the Apple album-match acceptance step.
# Mirrors the defaults used in integrations.spotify.matching.assess_album_match
# (issue #184). The Spotify experience is that 0.4 is a confident reject
# floor without false negatives on reissues. Tunable independently if Apple's
# signal turns out to be noisier (e.g. catalog songs missing for some albums).
COVERAGE_REJECT_BELOW = 0.4
ORDERING_REJECT_BELOW = 0.4
COVERAGE_RELAX_ORDERING_AT_LEAST = 0.8


def assess_apple_album_tracklist(
    mb_tracks: List[Dict[str, Any]],
    apple_tracks: List[Dict[str, Any]],
) -> Tuple[bool, str, Dict[str, Any]]:
    """Decide whether an Apple album candidate passes the tracklist gate.

    Compares our MusicBrainz tracklist for a release against the Apple
    album's tracklist; rejects candidates whose coverage is too low or
    whose track order is shuffled relative to ours (compilation signal).

    Lenient when either side is empty: returns (True, ...) so a missing
    MB tracklist or an Apple album with no song rows doesn't manufacture
    a false reject. The catalog rebuild can run in albums-only mode and
    we don't want that to silently break matching.

    Args:
        mb_tracks: rows from spotify.matching.fetch_mb_tracks_for_release
            — dicts with title/position/normalized.
        apple_tracks: Apple album tracklist dicts with at least a 'name'
            key. Either local catalog rows (get_songs_for_album) or
            iTunes API rows (client.lookup_album_tracks) — both already
            expose 'name'.

    Returns:
        (accept, reason, info) where info is the raw output of
        compare_mb_to_spotify_tracks (or {} when lenient-passed).
    """
    if not mb_tracks or not apple_tracks:
        return True, 'tracklist data unavailable; gate skipped', {}

    info = compare_mb_to_spotify_tracks(mb_tracks, apple_tracks)
    coverage = info['match_ratio']
    ordering = info['ordering_ratio']
    matched = info['matched_count']
    mb_count = info['mb_track_count']

    if coverage < COVERAGE_REJECT_BELOW:
        return False, (
            f"tracklist coverage too low ({coverage:.0%} = "
            f"{matched}/{mb_count} matched — needs ≥ "
            f"{int(COVERAGE_REJECT_BELOW * 100)}%)"
        ), info

    if (
        ordering is not None
        and ordering < ORDERING_REJECT_BELOW
        and coverage < COVERAGE_RELAX_ORDERING_AT_LEAST
    ):
        return False, (
            f"tracks shuffled (ordering {ordering:.0%}) and coverage "
            f"({coverage:.0%}) below the relaxation cutoff "
            f"({int(COVERAGE_RELAX_ORDERING_AT_LEAST * 100)}%) — "
            f"likely a compilation"
        ), info

    ordering_note = (
        f", ordering {ordering:.0%}" if ordering is not None else ""
    )
    return True, (
        f"coverage {coverage:.0%} ({matched}/{mb_count}){ordering_note}"
    ), info


def validate_album_match(
    matcher,
    apple_album: Dict,
    expected_artist: str,
    expected_album: str,
    expected_year: Optional[int] = None,
) -> Tuple[bool, float]:
    """
    Validate that an Apple Music album matches the expected release.

    Returns (is_valid, confidence). Uses matcher.min_artist_similarity /
    matcher.min_album_similarity as thresholds. Compilations are handled
    specially — a compilation vs. single-artist mismatch is a hard reject
    (prevents "Various Artists - X" matching "Some Artist - X").
    """
    am_artist = apple_album.get('artist', '')
    am_album = apple_album.get('name', '')

    norm_expected_artist = normalize_for_comparison(expected_artist)
    norm_expected_album = normalize_for_comparison(expected_album)
    norm_am_artist = normalize_for_comparison(am_artist)
    norm_am_album = normalize_for_comparison(am_album)

    artist_similarity = calculate_similarity(norm_expected_artist, norm_am_artist)
    album_similarity = calculate_similarity(norm_expected_album, norm_am_album)

    expected_is_compilation = is_compilation_artist(expected_artist)
    am_is_compilation = is_compilation_artist(am_artist)

    if expected_is_compilation != am_is_compilation:
        matcher.logger.debug(
            f"    Compilation mismatch: expected={expected_artist} "
            f"(comp={expected_is_compilation}) vs {am_artist} (comp={am_is_compilation})"
        )
        return False, 0.0

    if expected_is_compilation and am_is_compilation:
        if album_similarity >= matcher.min_album_similarity:
            return True, album_similarity / 100.0
        return False, 0.0

    if artist_similarity < matcher.min_artist_similarity:
        if not is_substring_title_match(norm_expected_artist, norm_am_artist):
            # Name variants (e.g., Dave -> David, Bill -> William)
            norm_expected_with_variants = normalize_name_variants(norm_expected_artist)
            norm_am_with_variants = normalize_name_variants(norm_am_artist)
            variant_similarity = calculate_similarity(
                norm_expected_with_variants, norm_am_with_variants
            )

            if variant_similarity >= matcher.min_artist_similarity:
                matcher.logger.debug(
                    f"    Artist match via name variants: {expected_artist} vs "
                    f"{am_artist} ({variant_similarity:.1f}%)"
                )
                artist_similarity = variant_similarity
            elif is_substring_title_match(norm_expected_with_variants, norm_am_with_variants):
                matcher.logger.debug(
                    f"    Artist match via name variant substring: "
                    f"{expected_artist} vs {am_artist}"
                )
                artist_similarity = matcher.min_artist_similarity
            else:
                matcher.logger.debug(
                    f"    Artist mismatch: {expected_artist} vs {am_artist} "
                    f"({artist_similarity}%)"
                )
                return False, 0.0

    if album_similarity < matcher.min_album_similarity:
        if not is_substring_title_match(norm_expected_album, norm_am_album):
            matcher.logger.debug(
                f"    Album mismatch: {expected_album} vs {am_album} ({album_similarity}%)"
            )
            return False, 0.0

    year_bonus = 0
    if expected_year and apple_album.get('release_date'):
        try:
            am_year = int(apple_album['release_date'][:4])
            if abs(am_year - expected_year) <= 1:
                year_bonus = 0.1
        except (ValueError, TypeError):
            pass

    confidence = (
        artist_similarity / 100.0 * 0.4
        + album_similarity / 100.0 * 0.5
        + year_bonus
    )

    return True, min(confidence, 1.0)


def find_matching_track(
    matcher,
    song_title: str,
    apple_tracks: List[Dict],
    expected_disc: Optional[int] = None,
    expected_track: Optional[int] = None,
) -> Optional[Dict]:
    """
    Pick the best Apple Music track for a song from an album's tracklist.

    Scores each candidate by title similarity, with a position bonus when
    disc/track numbers line up. Falls back to substring match at low
    confidence (0.7) if no candidate clears matcher.min_track_similarity.
    """
    norm_title = normalize_for_comparison(song_title)
    best_match = None
    best_score = 0

    for track in apple_tracks:
        am_title = track.get('name', '')
        norm_am_title = normalize_for_comparison(am_title)

        similarity = calculate_similarity(norm_title, norm_am_title)

        position_bonus = 0
        if expected_disc and expected_track:
            if (
                track.get('disc_number') == expected_disc
                and track.get('track_number') == expected_track
            ):
                position_bonus = 10

        total_score = similarity + position_bonus

        if total_score > best_score:
            best_score = total_score
            best_match = track

    if best_match and best_score >= matcher.min_track_similarity:
        best_match['_match_confidence'] = min(best_score / 100.0, 1.0)
        return best_match

    for track in apple_tracks:
        am_title = track.get('name', '')
        if is_substring_title_match(norm_title, normalize_for_comparison(am_title)):
            track['_match_confidence'] = 0.7
            return track

    return None
