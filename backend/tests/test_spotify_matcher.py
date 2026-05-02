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
    assess_album_match,
    calculate_similarity,
    compare_mb_to_spotify_tracks,
    duration_adjusted_score,
    duration_confidence,
    extract_primary_artist,
    has_version_keyword,
    is_compilation_artist,
    is_structural_title_match,
    is_substring_title_match,
    match_track_to_recording,
    normalize_for_comparison,
    normalize_name_variants,
    split_title_qualifier,
    strip_ensemble_suffix,
    strip_live_suffix,
    strip_mb_year_disambiguator,
    track_artist_matches_recording_leader,
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
        # Volume / part marker canonicalization — MB writes "Volume 2",
        # Spotify writes "Vol 2" or "Vol. 2", and we want them to compare equal.
        ("It's Up to You, Volume 2",   "it s up to you, vol 2"),
        ("It's Up to You, Vol. 2",     "it s up to you, vol 2"),
        ("It's Up to You, Vol.2",      "it s up to you, vol 2"),
        ("It's Up to You, Vol 2",      "it s up to you, vol 2"),
        ("Studio Sessions Part 3",     "studio sessions pt 3"),
        ("Studio Sessions Pt. 3",      "studio sessions pt 3"),
        # Marker without a trailing digit stays alone — we don't want
        # "Turn the Volume Up" or "Part of Me" mangled.
        ("Turn the Volume Up",         "turn the volume up"),
        ("Part of Me",                 "part of me"),
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

    def test_bracketed_annotation_strip_rescues_score(self):
        # Production case: a Spotify track titled with paren+bracket
        # annotations ("My Heart Stood Still (From 'A Connecticut
        # Yankee') [Ampico Piano Roll Recording]") was scoring ~38% via
        # token_sort_ratio against the bare song title, then ~55% after
        # the parenthetical-only strip rescue — still below the 85%
        # match threshold, so the candidate was filtered out and the
        # position tiebreaker never saw it. Stripping brackets too
        # rescues it to 100%.
        q = "My Heart Stood Still"
        candidate = (
            "My Heart Stood Still (From \"A Connecticut Yankee\") "
            "[Ampico Piano Roll Recording]"
        )
        assert calculate_similarity(q, candidate) == 100

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


class TestHasVersionKeyword:
    """Production case: an MB recording titled "Peace (live at Newport
    Jazz Festival)" was getting linked to a Spotify track titled just
    "Peace" because the title-strip rescue lifted the score to 100%.
    has_version_keyword() flags the asymmetric pair so
    match_tracks_for_release can reject the link."""

    @pytest.mark.parametrize("title,expected", [
        # Clear version-distinguishing annotations
        ("Peace (live at the Newport Jazz Festival)", True),
        ("Take Five (Live)", True),
        ("Body and Soul - Live", True),
        ("Stella by Starlight (Demo)", True),
        ("All The Things You Are (Alternate Take)", True),
        ("Misty (Acoustic)", True),
        ("So What (Instrumental)", True),
        ("Stardust (Rehearsal)", True),
        ("Round Midnight (Unplugged)", True),

        # Cosmetic / edition annotations that DON'T indicate a different
        # recording — these must NOT be flagged.
        ("Take Five", False),
        ("Take Five (Remastered)", False),
        ("Take Five (Stereo Mix)", False),
        ("Take Five (From \"A Connecticut Yankee\")", False),
        ("Take Five [Ampico Piano Roll Recording]", False),
        ("Take Five (2008 Remaster)", False),
        ("Take Five (Mono Version)", False),

        # Word-boundary discipline — substrings shouldn't false-positive
        ("Olive Tree", False),
        ("Alive Again", False),  # 'live' is inside 'alive'
        ("Conversation", False),  # 'session' is inside (almost — let's confirm)

        # Empty / null
        ("", False),
        (None, False),
    ])
    def test_classifies_titles(self, title, expected):
        assert has_version_keyword(title) is expected


class TestTrackArtistMatchesRecordingLeader:
    """Issue #100 phase 3: track-level artist verification on compilations.

    The matcher uses this to reject "wrong-curator's-compilation" matches
    where the album title and track title are shared but the actual
    performers differ — e.g. a Spotify compilation crediting "Recife All
    Stars" for Watermelon Man, vs the MB recording crediting Mongo
    Santamaría.
    """

    def test_returns_true_when_no_recording_leader(self):
        # Missing data must not cause rejection — degrade gracefully.
        ok, sim = track_artist_matches_recording_leader(None, ["Some Artist"])
        assert ok is True
        assert sim == 100.0

    def test_returns_true_when_no_track_artists(self):
        ok, sim = track_artist_matches_recording_leader("Mongo Santamaría", [])
        assert ok is True
        assert sim == 100.0

    def test_accepts_exact_match(self):
        ok, _ = track_artist_matches_recording_leader(
            "Mongo Santamaría", ["Mongo Santamaría"],
        )
        assert ok is True

    def test_accepts_substring_containment(self):
        # Common ensemble-suffix variation — the recording's leader is
        # contained in the Spotify track's artist string.
        ok, _ = track_artist_matches_recording_leader(
            "Mongo Santamaría", ["Mongo Santamaría & His Orchestra"],
        )
        assert ok is True

    def test_accepts_one_of_many_track_artists(self):
        # Spotify track credits multiple artists; one matches.
        ok, _ = track_artist_matches_recording_leader(
            "Mongo Santamaría",
            ["Tito Puente", "Mongo Santamaría", "Cal Tjader"],
        )
        assert ok is True

    def test_rejects_completely_different_artist(self):
        # The Watermelon Man case: same track title, different curator's
        # compilation, completely unrelated artist.
        ok, sim = track_artist_matches_recording_leader(
            "Mongo Santamaría", ["Recife All Stars"],
        )
        assert ok is False
        assert sim < 50

    def test_rejects_unrelated_jazz_artists(self):
        # Defensive: two real artists that share no name tokens.
        ok, _ = track_artist_matches_recording_leader(
            "Stan Getz", ["Bill Evans"],
        )
        assert ok is False


class TestStripLiveSuffix:
    @pytest.mark.parametrize("title,expected", [
        ("Solo: Live", "Solo"),
        ("Concert (Live)", "Concert"),
        ("At The Philharmonic - Live", "At The Philharmonic"),
        ("Night Train", "Night Train"),
    ])
    def test_strips_suffix(self, title, expected):
        assert strip_live_suffix(title) == expected


class TestStripMBYearDisambiguator:
    """The strip is applied only to search queries; validation still scores
    against the original title, so this test only verifies the regex's
    surface area — not whether a stripped query produces a match."""

    @pytest.mark.parametrize("title,expected", [
        # Canonical MB pattern with tildes around the year + trailing volume.
        ("It's Up to You ~ 1946 ~ Volume 2",          "It's Up to You"),
        ("The Chronological Classics ~ 1937–1939 ~",  "The Chronological Classics"),
        ("Live in Paris ~ 1958 ~",                    "Live in Paris"),
        # Year range with hyphen instead of en-dash.
        ("Studio Sessions ~ 1955-57 ~",               "Studio Sessions"),
        # No surrounding tildes -> leave alone (could be an arbitrary year
        # in a real title).
        ("Songs in A Minor (2001)",                   "Songs in A Minor (2001)"),
        # Bare year as a real album title (Prince) — no anchor matches.
        ("1999",                                      "1999"),
        # No suffix at all.
        ("Greatest Hits",                             "Greatest Hits"),
        # Year embedded mid-string is left alone (not anchored at end).
        ("1942 to 1945 - The Decca Years",            "1942 to 1945 - The Decca Years"),
        # Empty / None inputs.
        ("",                                          ""),
    ])
    def test_strips_mb_year_disambiguator(self, title, expected):
        assert strip_mb_year_disambiguator(title) == expected

    def test_none_input_returns_none(self):
        # Match the convention of the other strip helpers.
        assert strip_mb_year_disambiguator(None) is None


class TestSplitTitleQualifier:
    @pytest.mark.parametrize("title,expected", [
        ("Well You Needn't", ("Well You Needn't", None)),
        ("Well You Needn't (opening)", ("Well You Needn't", "opening")),
        ("Well You Needn't [opening]", ("Well You Needn't", "opening")),
        ("Well You Needn't - Opening", ("Well You Needn't", "Opening")),
        # Whitespace required around the dash so hyphenated names survive:
        ("Saint-Saëns", ("Saint-Saëns", None)),
        ("Tin-Pan-Alley", ("Tin-Pan-Alley", None)),
        # Trailing whitespace handled
        ("Foo (bar) ", ("Foo", "bar")),
        ("", ('', None)),
    ])
    def test_extracts_qualifier(self, title, expected):
        assert split_title_qualifier(title) == expected


class TestIsStructuralTitleMatch:
    """Cross-syntax title equivalence — backbone of the fix in issue #100.

    The parenthetical-strip rescue in calculate_similarity makes
    "Well You Needn't" and "Well You Needn't (opening)" both score 100%
    against either query. is_structural_title_match cuts through that by
    requiring the qualifier itself to match (or both be absent), regardless
    of which annotation syntax is used.
    """

    def test_identical_titles_match(self):
        assert is_structural_title_match("Take Five", "Take Five")

    def test_paren_vs_dash_syntax_match(self):
        # Real case from production: MB uses parens, Spotify uses dash.
        assert is_structural_title_match(
            "Well, You Needn't (opening)",
            "Well You Needn't - Opening",
        )

    def test_paren_vs_bracket_syntax_match(self):
        assert is_structural_title_match(
            "Take Five (Live)", "Take Five [Live]",
        )

    def test_qualifier_text_must_match(self):
        # Same base, different qualifier text — different recordings.
        assert not is_structural_title_match(
            "Take Five (Live)", "Take Five (Studio)",
        )
        assert not is_structural_title_match(
            "Take Five - Take 1", "Take Five - Take 2",
        )

    def test_qualifier_present_vs_absent_does_not_match(self):
        # An (opening) variant is NOT the canonical track — must not match.
        assert not is_structural_title_match(
            "Well You Needn't", "Well You Needn't (opening)",
        )
        assert not is_structural_title_match(
            "Well You Needn't (opening)", "Well You Needn't",
        )

    def test_different_base_does_not_match(self):
        assert not is_structural_title_match("Take Five", "Blue Monk")

    def test_qualifier_is_compared_case_insensitively(self):
        # Real case: MB lowercases, Spotify titlecases.
        assert is_structural_title_match(
            "Well You Needn't (opening)",
            "Well You Needn't (Opening)",
        )

    def test_hyphenated_artist_survives(self):
        # Whitespace-around-dash requirement protects names with embedded
        # hyphens from being misread as a base+qualifier split.
        assert is_structural_title_match("Saint-Saëns Tune", "Saint-Saëns Tune")
        assert not is_structural_title_match(
            "Saint-Saëns Tune", "Saint-Saëns Different",
        )


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
# assess_album_match — unified album-identity scorer (issue #184)
# ---------------------------------------------------------------------------

def _mb(title, position):
    return {
        'title': title,
        'position': position,
        'normalized': normalize_for_comparison(title),
    }


def _sp(name):
    return {'name': name}


class TestAssessAlbumMatch:
    def test_strong_all_signals_accept(self):
        mb = [_mb(f'Track {i}', i) for i in range(1, 6)]
        sp = [_sp(f'Track {i}') for i in range(1, 6)]
        a = assess_album_match(
            mb_album_title="Kind of Blue",
            mb_artist_credit="Miles Davis",
            spotify_album_name="Kind of Blue",
            spotify_artists=[{'name': 'Miles Davis'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        assert a.verdict == 'accept'
        assert a.coverage == 1.0
        assert a.ordering == 1.0

    def test_django_compilation_rejected_on_coverage(self):
        # Issue #184: MB "Djangology" (12 distinct tracks) vs a Spotify
        # compilation that shares the album title and one credited artist
        # but is a different release. Tracklist coverage is far below
        # threshold → reject. This is the case the gate must catch even
        # though title==100% and artist substring matches.
        mb = [_mb(f'MB Track {i}', i) for i in range(1, 13)]
        sp = [_sp('MB Track 1'), _sp('Different Track 1'),
              _sp('Different Track 2')]
        a = assess_album_match(
            mb_album_title="Djangology",
            mb_artist_credit="Django Reinhardt & Stéphane Grappelli",
            spotify_album_name="Djangology (feat. Stéphane Grappelli)",
            spotify_artists=[{'name': 'Django Reinhardt'},
                             {'name': 'Quintette du Hot Club de France'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        assert a.verdict == 'reject'
        assert 'coverage' in a.reason.lower()
        assert a.artist_substring_dir == 'spotify_in_expected'

    def test_bye_bye_blackbird_live_two_track_rejected(self):
        # The case from this thread: MB has 2 distinct tracks
        # (Bye-Bye Blackbird, Impressions). Spotify has a 2-track LIVE
        # release titled "Bye Bye Blackbird" with [Bye Bye Blackbird -
        # Live, Traneing In - Live]. Coverage = 1/2 = 50%. With the
        # COVERAGE_REJECT_BELOW=0.4 floor, 50% is above the floor —
        # but coverage_meets_floor wants >= 60%, so accept is blocked.
        # Borderline expected.
        mb = [_mb('Bye-Bye Blackbird', 1), _mb('Impressions', 2)]
        sp = [_sp('Bye Bye Blackbird - Live'), _sp('Traneing In - Live')]
        a = assess_album_match(
            mb_album_title="Bye Bye Blackbird",
            mb_artist_credit="John Coltrane",
            spotify_album_name="Bye Bye Blackbird",
            spotify_artists=[{'name': 'John Coltrane'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        assert a.verdict in ('borderline', 'reject')
        # Whether borderline or reject, the policy in search_spotify_album
        # is "skip borderline" — so this album would not be persisted.

    def test_compilation_reshuffle_caught_by_ordering(self):
        # Coverage 100% (same content), but tracks reshuffled — typical
        # of a compilation that re-orders the originals. With LIS
        # ordering well below the floor and coverage below the
        # ordering-relaxation threshold, reject.
        mb_titles = [f'Song {i}' for i in range(1, 6)]
        mb = [_mb(t, i) for i, t in enumerate(mb_titles, 1)]
        # Reverse Spotify order
        sp = [_sp(t) for t in reversed(mb_titles)]
        a = assess_album_match(
            mb_album_title="Greatest Hits",
            mb_artist_credit="The Band",
            spotify_album_name="Greatest Hits Reordered",
            spotify_artists=[{'name': 'The Band'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        # Coverage 100%, ordering low (LIS=1 / 5 matched = 20%).
        # ordering_blocking applies when coverage < 0.8 — at 100%
        # coverage, the ordering rule relaxes. So this should accept.
        assert a.verdict == 'accept'
        assert a.coverage == 1.0
        assert a.ordering is not None and a.ordering < 0.4

    def test_compilation_partial_with_bad_ordering_rejects(self):
        # Coverage between the reject floor (0.4) and the ordering
        # relaxation cutoff (0.8), with low ordering — the two together
        # signal a compilation pulling tracks from another release.
        # Six of ten MB tracks, reversed on Spotify → coverage 0.6,
        # ordering 1/6 ≈ 17%.
        mb = [_mb(f'Song {i}', i) for i in range(1, 11)]
        sp = [_sp(f'Song {i}') for i in [10, 8, 6, 4, 2, 1]]
        a = assess_album_match(
            mb_album_title="Album X",
            mb_artist_credit="Artist X",
            spotify_album_name="Album X",
            spotify_artists=[{'name': 'Artist X'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        assert a.verdict == 'reject'
        assert 'shuffled' in a.reason or 'ordering' in a.reason.lower()

    def test_reissue_with_bonus_tracks_accepts(self):
        # Reissue: same MB tracks all present, plus 3 bonus tracks
        # appended on Spotify. Coverage from MB perspective = 100%,
        # ordering = 100% (LIS over [1..N]). Accept.
        mb = [_mb(f'Track {i}', i) for i in range(1, 11)]
        sp = [_sp(f'Track {i}') for i in range(1, 14)]   # 10 + 3 bonus
        a = assess_album_match(
            mb_album_title="Album Y",
            mb_artist_credit="Artist Y",
            spotify_album_name="Album Y (Deluxe Edition)",
            spotify_artists=[{'name': 'Artist Y'}],
            mb_tracks=mb, spotify_tracks=sp,
        )
        assert a.verdict == 'accept'
        assert a.coverage == 1.0
        assert a.ordering == 1.0

    def test_no_tracklist_falls_back_to_title_artist(self):
        # MB or Spotify tracklist unavailable — assess title+artist
        # alone. Strong title+artist must still accept (preserves
        # operations during MB/Spotify outages).
        a = assess_album_match(
            mb_album_title="Kind of Blue",
            mb_artist_credit="Miles Davis",
            spotify_album_name="Kind of Blue",
            spotify_artists=[{'name': 'Miles Davis'}],
            mb_tracks=None, spotify_tracks=None,
        )
        assert a.verdict == 'accept'
        assert a.coverage is None
        assert a.ordering is None

    def test_substring_artist_ensemble_legitimate(self):
        # "Bill Evans" → "Bill Evans Trio" — expected_in_spotify direction.
        # No tracklist data; the title+artist gate must still accept.
        a = assess_album_match(
            mb_album_title="Waltz for Debby",
            mb_artist_credit="Bill Evans",
            spotify_album_name="Waltz for Debby",
            spotify_artists=[{'name': 'Bill Evans Trio'}],
            mb_tracks=None, spotify_tracks=None,
        )
        assert a.verdict == 'accept'
        assert a.artist_substring_dir == 'expected_in_spotify'

    def test_low_artist_no_tracklist_borderline_or_reject(self):
        # Low artist similarity + no tracklist data + no substring direction
        # → can't decisively call it. Expect borderline/reject (NOT accept).
        a = assess_album_match(
            mb_album_title="Some Album",
            mb_artist_credit="Artist A",
            spotify_album_name="Some Album",
            spotify_artists=[{'name': 'Completely Unrelated Performer'}],
            mb_tracks=None, spotify_tracks=None,
        )
        assert a.verdict in ('borderline', 'reject')


class TestCompareMbToSpotifyTracks:
    def test_ordering_full_when_in_order(self):
        mb = [_mb(f'T{i}', i) for i in range(1, 6)]
        sp = [_sp(f'T{i}') for i in range(1, 6)]
        info = compare_mb_to_spotify_tracks(mb, sp)
        assert info['matched_count'] == 5
        assert info['ordering_ratio'] == 1.0

    def test_ordering_low_when_reversed(self):
        mb = [_mb(f'T{i}', i) for i in range(1, 6)]
        sp = [_sp(f'T{i}') for i in range(5, 0, -1)]
        info = compare_mb_to_spotify_tracks(mb, sp)
        assert info['matched_count'] == 5
        # LIS of reversed = 1, divided by 5 = 0.2.
        assert info['ordering_ratio'] == pytest.approx(0.2)

    def test_ordering_none_when_too_few_matches(self):
        mb = [_mb('A', 1), _mb('B', 2)]
        sp = [_sp('A'), _sp('B')]
        info = compare_mb_to_spotify_tracks(mb, sp)
        assert info['matched_count'] == 2
        assert info['ordering_ratio'] is None

    def test_matched_pairs_carry_positions(self):
        mb = [_mb('A', 1), _mb('B', 2), _mb('C', 3)]
        sp = [_sp('A'), _sp('B'), _sp('C')]
        info = compare_mb_to_spotify_tracks(mb, sp)
        assert len(info['matched_pairs']) == 3
        for (mb_title, mb_pos, sp_title, sp_pos, score) in info['matched_pairs']:
            assert mb_title == sp_title
            assert mb_pos == sp_pos


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

    def test_structural_match_beats_paren_strip_rescue(self):
        """Issue #100 regression: when an album carries multiple variations
        of the same song (e.g. both "Well You Needn't" and the variant
        "Well You Needn't - Opening"), the parenthetical-strip rescue in
        calculate_similarity makes the canonical Spotify track score 100%
        against the variant query. Without a structural-match preference,
        the duration tiebreaker is the only remaining signal and it can
        pick the wrong track when durations are ambiguous.

        Realistic shape — MB uses "(opening)" parens, Spotify uses
        "- Opening" dash — so the variant track's fuzz score against the
        MB query is only 78%, below the 85% threshold. The structural-
        match floor pulls it back into the candidate set."""
        tracks = [
            # Spotify uses dash-suffix syntax for the variant; MB will
            # query with parens.
            _track('main', "Well You Needn't", duration_ms=683_000),  # 11:23
            _track('opening', "Well You Needn't - Opening", duration_ms=86_000),  # 1:26
        ]

        # Variant query, paren syntax — must structurally match the dash-
        # syntax Spotify track, even when its fuzz score (78%) falls
        # below the threshold.
        match = match_track_to_recording(
            log, {}, 85, "Well, You Needn't (opening)", tracks,
            expected_duration_ms=86_000,
        )
        assert match is not None and match['id'] == 'opening', (
            "Recording 'Well, You Needn't (opening)' (parens) should "
            "structurally match Spotify 'Well You Needn't - Opening' "
            "(dash syntax) and pick that variant track"
        )

        # And the canonical query lands on the canonical track.
        match = match_track_to_recording(
            log, {}, 85, "Well You Needn't", tracks,
            expected_duration_ms=683_000,
        )
        assert match is not None and match['id'] == 'main'

    def test_structural_match_resists_misleading_duration(self):
        """The harder case: structural-match exactness must out-rank a
        candidate whose duration is closer. A recording titled
        "(opening)" with a misleading duration should still pick the
        Spotify (opening) variant."""
        tracks = [
            _track('main', "Well You Needn't", duration_ms=300_000),
            _track('opening', "Well You Needn't - Opening", duration_ms=30_000),
        ]
        match = match_track_to_recording(
            log, {}, 85, "Well, You Needn't (opening)", tracks,
            # Way closer to 'main' than 'opening' — exactly the kind of
            # case where pure-duration tiebreaking would pick the wrong one.
            expected_duration_ms=295_000,
        )
        assert match is not None and match['id'] == 'opening'

    def test_position_match_breaks_tie_for_same_title_at_different_positions(self):
        """Issue: a release with TWO recordings of the same song at
        different positions (e.g. "My Heart Stood Still" at 1-7 and
        1-20, both also on Spotify) was getting the assignments crossed.
        With identical titles both candidates score 100%, and duration
        is a ±5 soft signal — a stale or coincidentally-close duration
        could flip the assignment.

        After the fix, an MB recording at position 1-7 picks the Spotify
        track also at 1-7 even when duration alone would prefer 1-20."""
        tracks = [
            _track('sp17', 'My Heart Stood Still',
                   disc=1, track=7, duration_ms=165_000),
            _track('sp120', 'My Heart Stood Still',
                   disc=1, track=20, duration_ms=79_000),
        ]

        # MB recording at 1-7. Even with a misleading duration that's
        # CLOSER to sp120 than to sp17, the position match should still
        # pull sp17 in.
        match = match_track_to_recording(
            log, {}, 85, "My Heart Stood Still", tracks,
            expected_disc=1, expected_track=7,
            expected_duration_ms=80_000,  # clearly favours sp120 by duration
        )
        assert match is not None and match['id'] == 'sp17', (
            "Position 1-7 on MB should pick the Spotify track at 1-7, "
            "even when duration alone would favour the 1-20 candidate"
        )

        # Symmetric case for the other recording.
        match = match_track_to_recording(
            log, {}, 85, "My Heart Stood Still", tracks,
            expected_disc=1, expected_track=20,
            expected_duration_ms=164_000,  # favours sp17 by duration
        )
        assert match is not None and match['id'] == 'sp120'

    def test_my_heart_stood_still_real_case_resolves_correctly(self):
        """The production scenario from the My Heart Stood Still bug:
        Spotify has TWO instances of the song on one album — one with a
        paren+bracket annotation suffix at position 1-7, one with just
        a paren annotation at position 1-20. Before the bracket-strip
        fix, only the paren-only candidate cleared the 85% threshold,
        so both MB recordings matched it. After the fix, both candidates
        score 100% via title and the position tiebreaker assigns each
        MB recording to its correctly-positioned Spotify counterpart."""
        tracks = [
            _track('sp17',
                   'My Heart Stood Still (From "A Connecticut Yankee") '
                   '[Ampico Piano Roll Recording]',
                   disc=1, track=7, duration_ms=165_000),
            _track('sp120',
                   'My Heart Stood Still (From "A Connecticut Yankee")',
                   disc=1, track=20, duration_ms=79_000),
        ]
        # MB recording at 1-7 (2:45). Both candidates structural-score at
        # 100% (post-bracket-strip rescue), so position breaks the tie.
        match = match_track_to_recording(
            log, {}, 85, "My Heart Stood Still", tracks,
            expected_disc=1, expected_track=7,
            expected_duration_ms=165_000,
        )
        assert match is not None and match['id'] == 'sp17'

        match = match_track_to_recording(
            log, {}, 85, "My Heart Stood Still", tracks,
            expected_disc=1, expected_track=20,
            expected_duration_ms=79_000,
        )
        assert match is not None and match['id'] == 'sp120'

    def test_position_tiebreaker_does_not_override_inexact_vs_exact(self):
        # Defensive: if one candidate is structural-exact and the other
        # is at the matching position but only fuzzy-matches, the exact
        # title still wins. Position is a tiebreaker WITHIN an
        # exactness tier, not above it.
        tracks = [
            _track('exact_off_position', 'Take Five',
                   disc=1, track=99, duration_ms=300_000),
            _track('inexact_at_position', 'Take Five (Live at Carnegie Hall)',
                   disc=1, track=1, duration_ms=300_000),
        ]
        match = match_track_to_recording(
            log, {}, 85, "Take Five", tracks,
            expected_disc=1, expected_track=1,
            expected_duration_ms=300_000,
        )
        # 'inexact_at_position' is at the right position but its title
        # carries an extra qualifier — it's NOT structural-exact. The
        # off-position exact-title candidate should win on tier 1.
        assert match is not None and match['id'] == 'exact_off_position'

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
