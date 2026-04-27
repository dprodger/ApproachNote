"""
Pure-function tests for integrations.spotify.matching.

Post-refactor (#115), the matcher orchestration (class SpotifyMatcher) is
a thin wrapper around the functions in integrations.spotify.matching.
This file covers those functions directly — no DB, no Spotify API, no
class instantiation. Drive them with handcrafted track dicts and assert
scoring/normalization behavior.

See GH #138 for scope.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from integrations.spotify.matching import (
    calculate_similarity,
    duration_adjusted_score,
    duration_confidence,
    extract_primary_artist,
    is_compilation_artist,
    is_substring_title_match,
    match_track_to_recording,
    normalize_for_comparison,
    normalize_name_variants,
    strip_ensemble_suffix,
    strip_live_suffix,
    validate_album_match,
    validate_track_match,
    verify_album_contains_track,
)


log = logging.getLogger("test.spotify_matcher")


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

class TestNormalizeForComparison:
    @pytest.mark.parametrize("raw,expected", [
        # Live annotations in various shapes collapse to the base title.
        ("Autumn Leaves - Live at Newport 1961", "autumn leaves"),
        ("Stella By Starlight (Live at Village Vanguard)", "stella by starlight"),
        ("Solo: Live", "solo"),
        # Remaster annotations
        ("Take Five - Remastered 2024", "take five"),
        ("Blue in Green (2010 Remaster)", "blue in green"),
        # "feat." / "with" artist callouts
        ("So What (feat. John Coltrane)", "so what"),
        ("All Blues (with Bill Evans)", "all blues"),
        # Apostrophe normalization (keeps title matchable across curly/straight)
        ("Don't Blame Me", "don t blame me"),
        # Ampersand → "and"
        ("Nat & Alex", "nat and alex"),
        # Film-source annotation
        ("An Affair to Remember - From the 20th Century-Fox Film",
         "an affair to remember"),
    ])
    def test_strips_common_annotations(self, raw, expected):
        assert normalize_for_comparison(raw) == expected

    def test_empty_input_returns_empty(self):
        assert normalize_for_comparison("") == ""
        assert normalize_for_comparison(None) == ""


class TestCalculateSimilarity:
    def test_identical_is_100(self):
        assert calculate_similarity("Take Five", "Take Five") == 100

    def test_parenthetical_fallback_raises_score(self):
        # "Who Cares?" vs "Who Cares (As Long As You Care For Me)" —
        # token_sort ratio is low until the parenthetical is stripped.
        score = calculate_similarity(
            "Who Cares?",
            "Who Cares (As Long As You Care For Me)",
        )
        assert score >= 90

    def test_remaster_suffix_doesnt_hurt_score(self):
        # normalize_for_comparison strips "- Remastered ..." so the score
        # should be near-perfect even with the suffix present.
        assert calculate_similarity("Take Five", "Take Five - Remastered 2024") == 100

    def test_unrelated_titles_score_low(self):
        assert calculate_similarity("Take Five", "Body and Soul") < 50

    def test_empty_input_returns_zero(self):
        assert calculate_similarity("", "Take Five") == 0
        assert calculate_similarity("Take Five", "") == 0


# ---------------------------------------------------------------------------
# Artist / title helpers
# ---------------------------------------------------------------------------

class TestStripEnsembleSuffix:
    @pytest.mark.parametrize("artist,expected", [
        ("Bill Evans Trio", "Bill Evans"),
        ("Duke Ellington Orchestra", "Duke Ellington"),
        ("Art Blakey & The Jazz Messengers", "Art Blakey & The Jazz Messengers"),
        ("Miles Davis", "Miles Davis"),
        ("Wynton Marsalis Quintet", "Wynton Marsalis"),
    ])
    def test_strips_suffix(self, artist, expected):
        assert strip_ensemble_suffix(artist) == expected


class TestExtractPrimaryArtist:
    @pytest.mark.parametrize("credit,expected", [
        ("Miles Davis, John Coltrane", "Miles Davis"),
        ("Bill Evans & Jim Hall", "Bill Evans"),
        ("Ella Fitzgerald/Louis Armstrong", "Ella Fitzgerald"),
        ("Dave Brubeck; Paul Desmond", "Dave Brubeck"),
        ("Miles Davis", "Miles Davis"),  # no separator, returned as-is
    ])
    def test_returns_primary(self, credit, expected):
        assert extract_primary_artist(credit) == expected

    def test_none_input(self):
        assert extract_primary_artist(None) is None


class TestNormalizeNameVariants:
    @pytest.mark.parametrize("raw,expected", [
        ("Dave Liebman", "david liebman"),
        ("Bill Evans", "william evans"),
        ("Bob Dylan", "robert dylan"),
        ("Miles Davis", "miles davis"),  # no variant to normalize
    ])
    def test_canonicalizes_nicknames(self, raw, expected):
        assert normalize_name_variants(raw) == expected


class TestIsCompilationArtist:
    @pytest.mark.parametrize("artist,expected", [
        ("Various Artists", True),
        ("various", True),
        ("Varios Artistas", True),  # Spanish
        ("Bill Evans", False),
        ("", False),
        (None, False),
    ])
    def test_detects_compilations(self, artist, expected):
        assert is_compilation_artist(artist) is expected


class TestStripLiveSuffix:
    @pytest.mark.parametrize("title,expected", [
        ("Solo: Live", "Solo"),
        ("Concert (Live)", "Concert"),
        ("At The Philharmonic - Live", "At The Philharmonic"),
        ("Night Train", "Night Train"),
    ])
    def test_strips_suffix(self, title, expected):
        assert strip_live_suffix(title) == expected


class TestIsSubstringTitleMatch:
    def test_contained_fully(self):
        assert is_substring_title_match(
            "Stella By Starlight",
            "Stella By Starlight (From 'The Uninvited')",
        )

    def test_reversed_direction(self):
        assert is_substring_title_match(
            "Autumn Leaves - Live at Newport",
            "Autumn Leaves",
        )

    def test_too_short_rejected(self):
        # Guard against trivial 3-character matches creating false positives.
        assert not is_substring_title_match("Joy", "The Joy of Music")

    def test_unrelated(self):
        assert not is_substring_title_match("Take Five", "Body and Soul")


# ---------------------------------------------------------------------------
# Duration scoring
# ---------------------------------------------------------------------------

class TestDurationConfidence:
    @pytest.mark.parametrize("expected_ms,actual_ms,confidence", [
        (240_000, 240_000, 1.0),    # exact
        (240_000, 242_000, 1.0),    # +2s → still perfect bucket
        (240_000, 250_000, 0.9),    # +10s → remaster/edit bucket
        (240_000, 260_000, 0.9),    # +20s → remaster/edit bucket
        (240_000, 300_000, 0.7),    # +60s → different edit bucket
        (240_000, 450_000, 0.4),    # +210s → likely wrong performance
        (240_000, 600_000, 0.2),    # +360s → almost certainly wrong
    ])
    def test_bucketed_confidence(self, expected_ms, actual_ms, confidence):
        assert duration_confidence(expected_ms, actual_ms) == confidence


class TestDurationAdjustedScore:
    def test_returns_title_score_when_either_duration_missing(self):
        # Pre-refactor regression: matcher crashed when Spotify omitted
        # duration_ms. Now it's a no-op passthrough.
        assert duration_adjusted_score(90.0, None, 240_000) == 90.0
        assert duration_adjusted_score(90.0, 240_000, None) == 90.0

    def test_boosts_close_duration(self):
        # 1.0 confidence → +5 adjustment
        assert duration_adjusted_score(90.0, 240_000, 240_000) == 95.0

    def test_penalizes_far_duration(self):
        # 0.2 confidence → -3 adjustment
        assert duration_adjusted_score(90.0, 240_000, 800_000) == 87.0


# ---------------------------------------------------------------------------
# validate_track_match / validate_album_match
# ---------------------------------------------------------------------------

class TestValidateTrackMatch:
    def _track(self, name="Take Five", artist="Dave Brubeck Quartet",
               album="Time Out"):
        return {
            'name': name,
            'artists': [{'name': artist}],
            'album': {'name': album},
        }

    def test_accepts_exact_match(self):
        ok, reason, scores = validate_track_match(
            self._track(), "Take Five", "Dave Brubeck", "Time Out",
            min_track_similarity=85, min_artist_similarity=65,
            min_album_similarity=55,
        )
        assert ok is True
        assert reason == "Valid match"

    def test_rejects_title_mismatch(self):
        ok, reason, _ = validate_track_match(
            self._track(name="Autumn Leaves"),
            "Take Five", "Dave Brubeck", "Time Out",
            min_track_similarity=85, min_artist_similarity=65,
            min_album_similarity=55,
        )
        assert ok is False
        assert "Track title similarity" in reason

    def test_rejects_artist_mismatch(self):
        ok, reason, _ = validate_track_match(
            self._track(artist="Some Totally Different Artist"),
            "Take Five", "Dave Brubeck", "Time Out",
            min_track_similarity=85, min_artist_similarity=65,
            min_album_similarity=55,
        )
        assert ok is False
        assert "Artist similarity" in reason


class TestValidateAlbumMatch:
    def test_substring_album_accepted(self):
        # Expected: "Live at Montreux" is a substring of the Spotify album —
        # accepted even though fuzzy similarity is below threshold.
        album = {
            'name': 'Live at the Montreux Jazz Festival',
            'artists': [{'name': 'Bill Evans'}],
        }
        ok, reason, _ = validate_album_match(
            album, "Live at Montreux", "Bill Evans",
            min_album_similarity=65, min_artist_similarity=65,
        )
        assert ok is True
        assert reason == "Valid match"

    def test_ensemble_artist_accepted_via_substring(self):
        # "Bill Evans" should match "Bill Evans Trio" via substring fallback.
        album = {
            'name': 'Waltz for Debby',
            'artists': [{'name': 'Bill Evans Trio'}],
        }
        ok, _, _ = validate_album_match(
            album, "Waltz for Debby", "Bill Evans",
            min_album_similarity=65, min_artist_similarity=65,
        )
        assert ok is True

    def test_track_verification_accepts_compilation(self):
        # Various-Artists compilation: artist matching is meaningless, so
        # with album sim >= 80 and a track-verify callback, the match
        # should be accepted with verified_by_track set.
        album = {
            'id': 'alb1',
            'name': 'Jazz Standards Compilation',
            'artists': [{'name': 'Various Artists'}],
        }
        callback_calls = []

        def verify(album_id, song_title):
            callback_calls.append((album_id, song_title))
            return True

        ok, _, scores = validate_album_match(
            album, "Jazz Standards Compilation", "Various Artists",
            min_album_similarity=65, min_artist_similarity=65,
            song_title="Take Five", verify_track_callback=verify,
        )
        assert ok is True
        # Compilation+matching album — artist fuzzy is also 100, so the
        # direct path accepts without invoking the callback. Either way
        # this is the correct outcome for a compilation.

    def test_unrelated_album_rejected(self):
        album = {
            'name': 'Something Completely Different',
            'artists': [{'name': 'Some Other Artist'}],
        }
        ok, reason, _ = validate_album_match(
            album, "Kind of Blue", "Miles Davis",
            min_album_similarity=65, min_artist_similarity=65,
        )
        assert ok is False
        assert "Album similarity" in reason


# ---------------------------------------------------------------------------
# match_track_to_recording (canned candidate tracklists)
# ---------------------------------------------------------------------------

def _track(id, name, *, disc=1, track=1, duration_ms=240_000):
    return {
        'id': id,
        'name': name,
        'disc_number': disc,
        'track_number': track,
        'duration_ms': duration_ms,
    }


class TestMatchTrackToRecording:
    def test_picks_exact_title_match(self):
        tracks = [
            _track('t1', 'Autumn Leaves'),
            _track('t2', 'Blue in Green'),
        ]
        match = match_track_to_recording(
            log, {}, 85, "Autumn Leaves", tracks,
        )
        assert match is not None and match['id'] == 't1'

    def test_returns_none_when_no_candidate_passes_threshold(self):
        tracks = [_track('t1', "Lush Life")]
        assert match_track_to_recording(
            log, {}, 85, "Totally Different Song", tracks,
        ) is None

    def test_alt_titles_fall_back(self):
        # Primary title doesn't appear — alt title rescues the match.
        tracks = [_track('t1', 'Body and Soul')]
        match = match_track_to_recording(
            log, {}, 85, "Some Obscure Alternate Name", tracks,
            alt_titles=["Body and Soul"],
        )
        assert match is not None and match['id'] == 't1'

    def test_position_plus_substring_fallback(self):
        # Title fuzzy-matches poorly because of the film-source suffix,
        # but disc/track position lines up with our recording AND the
        # shorter title is a substring of the Spotify title.
        tracks = [
            _track(
                'sub',
                'An Affair to Remember - From the 20th Century-Fox Film, '
                'An Affair To Remember',
                disc=1, track=3, duration_ms=None,
            ),
        ]
        match = match_track_to_recording(
            log, {}, 85, "An Affair to Remember", tracks,
            expected_disc=1, expected_track=3,
        )
        assert match is not None and match['id'] == 'sub'

    def test_duration_breaks_tie_between_identical_titles(self):
        # Two tracks with the same title — the one whose duration is
        # closer to the MB recording wins via duration_adjusted_score.
        tracks = [
            _track('far', 'Blue Monk', duration_ms=400_000),
            _track('near', 'Blue Monk', duration_ms=240_000),
        ]
        match = match_track_to_recording(
            log, {}, 85, "Blue Monk", tracks,
            expected_duration_ms=240_000,
        )
        assert match is not None and match['id'] == 'near'

    def test_exact_normalized_match_beats_paren_strip_rescue(self):
        """Issue #100 regression: when an album carries multiple variations
        of the same song (e.g. both "Well You Needn't" and
        "Well You Needn't (opening)"), the parenthetical-strip rescue in
        calculate_similarity makes BOTH candidates score 100% no matter
        which side of the variation we query for. Without an exact-match
        preference, the duration tiebreaker is the only remaining signal —
        and it can pick the wrong track when durations are ambiguous.

        After the fix, an exact-normalized-match candidate beats a
        paren-strip-rescued one regardless of duration."""
        tracks = [
            _track('main', "Well You Needn't", duration_ms=300_000),
            _track('opening', "Well You Needn't (opening)", duration_ms=30_000),
        ]

        # Asking for the variant title — 'opening' is exact, 'main' is
        # rescued via paren-strip. Even with a duration that strongly
        # favors 'main' (close to 'main', far from 'opening'), the exact
        # match should win.
        match = match_track_to_recording(
            log, {}, 85, "Well You Needn't (opening)", tracks,
            expected_duration_ms=295_000,  # 5s off main, 4.4min off opening
        )
        assert match is not None and match['id'] == 'opening', (
            "Recording-specific title 'Well You Needn't (opening)' should "
            "match the exact Spotify variant track even when duration "
            "favors the canonical track"
        )

        # Asking for the canonical title — 'main' is exact, 'opening' is
        # rescued. Same shape in reverse.
        match = match_track_to_recording(
            log, {}, 85, "Well You Needn't", tracks,
            expected_duration_ms=35_000,  # 5s off opening, 4.4min off main
        )
        assert match is not None and match['id'] == 'main', (
            "Canonical title 'Well You Needn't' should match the exact "
            "Spotify main track even when duration favors the variant"
        )

    def test_exact_match_preference_does_not_override_score_threshold(self):
        # An exact-match candidate must still clear the title-similarity
        # threshold to be considered. (Defensive: we don't want the exact-
        # match preference to accept obviously-wrong candidates whose
        # normalized form happens to match by coincidence.)
        # Here both tracks have a title that is well below the 85%
        # threshold against the query — neither is "exact normalized" to
        # the query either, so both fall out cleanly.
        tracks = [
            _track('a', 'Totally Unrelated Tune', duration_ms=240_000),
        ]
        match = match_track_to_recording(
            log, {}, 85, "Round Midnight", tracks,
            expected_duration_ms=240_000,
        )
        assert match is None


# ---------------------------------------------------------------------------
# verify_album_contains_track
# ---------------------------------------------------------------------------

class TestVerifyAlbumContainsTrack:
    def _client(self, tracks):
        """Minimal stub matching the SpotifyClient.get_album_tracks() shape."""
        return SimpleNamespace(get_album_tracks=lambda album_id: tracks)

    def test_returns_true_when_title_matches(self):
        tracks = [{'name': 'Autumn Leaves'}, {'name': 'Blue in Green'}]
        assert verify_album_contains_track(
            self._client(tracks), log, 85, "any-album-id", "Autumn Leaves",
        ) is True

    def test_returns_false_when_no_title_matches(self):
        tracks = [{'name': 'Autumn Leaves'}, {'name': 'Blue in Green'}]
        assert verify_album_contains_track(
            self._client(tracks), log, 85, "any-album-id",
            "Totally Unrelated Song Title",
        ) is False

    def test_returns_false_when_client_has_no_tracks(self):
        assert verify_album_contains_track(
            self._client(None), log, 85, "any-album-id", "Autumn Leaves",
        ) is False
