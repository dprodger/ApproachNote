"""
Apple Music fuzzy matching and scoring.

Match validation for Apple Music album / track candidates. The primitive
normalization + similarity functions live in integrations.spotify.matching
(service-agnostic); this module holds the Apple-Music-specific orchestration
that threads matcher thresholds through and applies compilation-artist and
name-variant rules.
"""

from typing import Dict, List, Optional, Tuple

from integrations.spotify.matching import (
    calculate_similarity,
    is_compilation_artist,
    is_substring_title_match,
    normalize_for_comparison,
    normalize_name_variants,
)


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
